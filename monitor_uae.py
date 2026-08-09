#!/usr/bin/env python3
"""
UAE Pokemon TCG Retailer Monitor
────────────────────────────────
Monitors Pokemon TCG availability across UAE retailers
and sends Telegram alerts for new stock and price changes.

Sites monitored:
  - Otakume, Virgin Megastore, Legends Own The Game, Colorland Toys,
    Magrudy, ZGames, Geekay, Little Things
"""

import asyncio
import json
import logging
import os
import platform
import random
import re
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
from patchright.async_api import BrowserContext, async_playwright


def _default_chrome_path() -> str | None:
    """Resolve real Chrome path per OS. Returns None on Linux if Chrome isn't installed —
    callers then fall back to patchright's bundled Chromium."""
    override = os.environ.get("CHROME_PATH")
    if override:
        return override
    system = platform.system()
    if system == "Darwin":
        return "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    if system == "Windows":
        return "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
    # Linux (Docker / Railway): prefer /usr/bin/google-chrome if present
    for candidate in ("/usr/bin/google-chrome", "/usr/bin/chromium", "/usr/bin/chromium-browser"):
        if Path(candidate).exists():
            return candidate
    return None


# Real Chrome path — needed for sites that block Patchright's bundled Chromium (e.g. Geekay/Cloudflare, Smyths/Incapsula)
CHROME_PATH = _default_chrome_path()

# ─── CONFIG ───────────────────────────────────────────────────────────────────

# DATA_DIR lets Railway mount a persistent volume (e.g. /data) so state survives redeploys.
DATA_DIR    = Path(os.environ.get("DATA_DIR", "."))
CONFIG_FILE = Path("config_uae.json")
STATE_FILE  = DATA_DIR / "state_uae.json"

# Retailers the user can disable via env (comma-separated keys: geekay,otakume,...).
DISABLED_RETAILERS = {
    s.strip().lower() for s in os.environ.get("DISABLED_RETAILERS", "").split(",") if s.strip()
}

# ─── LORCANA GO-LIVE GATE ─────────────────────────────────────────────────────
# The Lorcana watchers stay dormant until this moment, then switch on
# automatically (checked in the monitor loop). Default: 2026-07-19 06:00 UTC
# = 07:00 BST. Override via env LORCANA_GO_LIVE_UTC (ISO, e.g. 2026-07-19T06:00:00Z).
def _parse_go_live() -> datetime:
    raw = os.environ.get("LORCANA_GO_LIVE_UTC", "").strip()
    if raw:
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            pass
    return datetime(2026, 7, 24, 0, 0, 0, tzinfo=timezone.utc)  # already past — Lorcana live immediately

LORCANA_GO_LIVE = _parse_go_live()

def lorcana_active() -> bool:
    """True once the clock passes the Lorcana go-live time (7am BST 2026-07-19)."""
    return datetime.now(timezone.utc) >= LORCANA_GO_LIVE


def _config_from_env() -> dict | None:
    """Build a config dict from environment variables (Railway / container deploys).
    Returns None if the required Telegram vars are missing."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat  = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat:
        return None
    cfg: dict = {
        "telegram_bot_token": token,
        "telegram_chat_id": chat,
        "intervals": {
            "otakume": 120, "virgin_megastore": 60, "virgin_megastore_onepiece": 60, "legends_own_the_game": 60,
            "colorland_toys": 180, "magrudy": 60, "zgames": 60,
            "geekay": 120, "little_things": 30, "toycorner": 180,
            "kinokuniya": 120, "kinokuniya_event": 120, "elctoys": 120, "lorcana": 120,
        },
        "urls": {
            "otakume": "https://otakume.com/collections/pokemon",
            "virgin_megastore": "https://www.virginmegastore.ae/en/selection/general-merchandise/pokemon-merchandise/pokemon-tcg/c/n9992162",
            "virgin_megastore_search": "https://www.virginmegastore.ae/en/search?text=pokemon+tcg",
            "virgin_megastore_accented": "https://www.virginmegastore.ae/en/search?text=Pok%C3%A9mon+tcg",
            "virgin_megastore_onepiece": "https://www.virginmegastore.ae/en/search?text=one+piece+card+game",
            "legends_own_the_game": "https://legendsownthegame.com/products/search?keyword=pokemon&categories=176121252",
            "colorland_toys": "https://colorlandtoys.com/search?q=pokemon+tcg&options%5Bprefix%5D=last&type=product",
            "colorland_toys_accented": "https://colorlandtoys.com/search?q=Pok%C3%A9mon+tcg&options%5Bprefix%5D=last&type=product",
            # ELC Toys — Shopify. 'POKEMON TCG' returns just the TCG SKUs;
            # a broader 'pokemon' search floods with plushes/figures.
            "elctoys": "https://elctoys.com/search?q=POKEMON+TCG&options%5Bprefix%5D=last&type=product",
            "elctoys_accented": "https://elctoys.com/search?q=Pok%C3%A9mon+TCG&options%5Bprefix%5D=last&type=product",
            "magrudy": "https://www.magrudy.com/search?q=tcg",
            "zgames": "https://zgames.ae/catalogsearch/result/?q=pokemon+tcg",
            "zgames_accented": "https://zgames.ae/catalogsearch/result/?q=Pok%C3%A9mon+tcg",
            "geekay": "https://www.geekay.com/en/brand/pokemon?goodstuff_genre=571&product_list_order=new&stock=1",
            "little_things": "https://littlethingsme.com/collections/pokemon-tcg/products.json?limit=250",
            "little_things_onepiece": "https://littlethingsme.com/collections/one-piece-tcg/products.json?limit=250",
            "toycorner": "https://toycorner.ae/product-category/trading-cards-toy-corner/anime-trading-cards/pokemon-trading-cards/",
            "kinokuniya": "https://uae.kinokuniya.com/products?is_searching=true&keywords=pokemon+tcg",
            "kinokuniya_accented": "https://uae.kinokuniya.com/products?is_searching=true&keywords=Pok%C3%A9mon+tcg",
            # Curated Pokémon drop/event page(s). Comma-separate more IDs via env if needed.
            "kinokuniya_event": "https://uae.kinokuniya.com/events/1243",
            # Lorcana search URLs (dormant until 7am BST 2026-07-19 via lorcana_active()).
            "kinokuniya_lorcana": "https://uae.kinokuniya.com/products?is_searching=true&keywords=lorcana",
            "virgin_lorcana_search": "https://www.virginmegastore.ae/en/search?text=lorcana",
            "colorland_lorcana": "https://colorlandtoys.com/search?q=lorcana&options%5Bprefix%5D=last&type=product",
            "toycorner_lorcana": "https://toycorner.ae/?s=lorcana&post_type=product",
        },
    }
    if os.environ.get("LEGENDS_AUTO_CHECKOUT", "").lower() == "true":
        cfg["legends_auto_checkout"] = {
            "enabled": True,
            "watchlist": [s.strip() for s in os.environ.get("LEGENDS_WATCHLIST", "").split(";") if s.strip()],
            "checkout_details": {
                "email":   os.environ.get("CHECKOUT_EMAIL", ""),
                "name":    os.environ.get("CHECKOUT_NAME", ""),
                "phone":   os.environ.get("CHECKOUT_PHONE", ""),
                "address": os.environ.get("CHECKOUT_ADDRESS", ""),
                "city":    os.environ.get("CHECKOUT_CITY", ""),
                "state":   os.environ.get("CHECKOUT_STATE", ""),
                "zip":     os.environ.get("CHECKOUT_ZIP", ""),
            },
        }
    if os.environ.get("LITTLETHINGS_AUTO_CART", "").lower() == "true":
        cfg["littlethings_auto_cart"] = {
            "enabled": True,
            "watchlist": [s.strip() for s in os.environ.get("LITTLETHINGS_WATCHLIST", "").split(";") if s.strip()],
        }
    return cfg


def load_config() -> dict:
    # Local file takes precedence (existing dev workflow unchanged).
    if CONFIG_FILE.exists():
        return json.loads(CONFIG_FILE.read_text())
    # Fallback: build from env (Railway / Docker / CI).
    env_cfg = _config_from_env()
    if env_cfg is not None:
        return env_cfg
    raise FileNotFoundError(
        "config_uae.json not found and TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID env vars not set."
    )


CFG                 = load_config()
TELEGRAM_BOT_TOKEN  = CFG["telegram_bot_token"]
TELEGRAM_CHAT_ID    = CFG["telegram_chat_id"]
INTERVALS           = CFG["intervals"]
URLS                = CFG["urls"]

# Optional second recipient — gets every alert N seconds after the primary chat.
# Unset both to disable. Defaults: no delayed forward.
TELEGRAM_CHAT_ID_DELAYED = os.environ.get("TELEGRAM_CHAT_ID_DELAYED", "").strip()
TELEGRAM_DELAY_SECONDS   = int(os.environ.get("TELEGRAM_DELAY_SECONDS", "120"))

TELEGRAM_ENABLED = (
    TELEGRAM_BOT_TOKEN != "YOUR_BOT_TOKEN"
    and TELEGRAM_CHAT_ID != "YOUR_CHAT_ID"
)

# ─── LOGGING ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("monitor_uae.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ─── USER AGENTS (Chrome / Firefox / Safari mix) ──────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]


def get_headers(referer: str = "") -> dict:
    h = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin" if referer else "none",
        "Cache-Control": "no-cache",
        "DNT": "1",
    }
    if referer:
        h["Referer"] = referer
    return h


def get_json_headers() -> dict:
    """Headers for Shopify JSON API calls."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "Accept-Language": "en-GB,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }


# ─── STATE ────────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except (json.JSONDecodeError, Exception):
            pass
    return {
        "otakume":              {},
        "virgin_megastore":     {},
        "virgin_megastore_onepiece": {},
        "legends_own_the_game": {},
        "colorland_toys":       {},
        "magrudy":              {},
        "zgames":               {},
        "geekay":               {},
        "little_things":        {},
        "toycorner":            {},
        "kinokuniya":           {},
        "kinokuniya_event":     {},
        "elctoys":              {},
    }


def save_state(state: dict) -> None:
    """Atomic write: a SIGKILL mid-write previously truncated state_uae.json,
    which load_state silently swallowed → every retailer re-baselined."""
    try:
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2))
        os.replace(tmp, STATE_FILE)
    except Exception as exc:
        log.error("save_state failed: %s", exc)


# ─── EVENT LOG (drop-timing history) ──────────────────────────────────────────
# Append-only JSONL on the persistent volume. Railway log retention is short and
# per-deployment, so stock history is otherwise destroyed on every redeploy.
# One line per stock change → lets us learn each store's upload window.

EVENTS_FILE = DATA_DIR / "events.jsonl"


def log_event(site: str, kind: str, product: dict) -> None:
    """Record a stock change. kind: new | restock | oos.
    Never raises — telemetry must never break a check."""
    try:
        rec = {
            "ts":     datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "site":   site,
            "kind":   kind,
            "title":  (product.get("title") or "")[:120],
            "price":  product.get("price"),
            "url":    product.get("url"),
        }
        with EVENTS_FILE.open("a") as fh:
            fh.write(json.dumps(rec) + "\n")
    except Exception as exc:
        log.debug("log_event failed: %s", exc)


def log_events(site: str, kind: str, products: list) -> None:
    for p in products or []:
        log_event(site, kind, p)


# ─── HONEST HEALTH SIGNAL ─────────────────────────────────────────────────────
# Previously health was `bool(state[site])`, which stays truthy forever once a
# site has baselined — so a store blind for a week still showed ✅ with a fresh
# timestamp (exactly how Otakume's dead filter went unnoticed for hours).
# Checkers now stamp a success time; "healthy" means *succeeded recently*.

def mark_ok(state: dict, site: str) -> None:
    """Called by a checker only when it actually completed and parsed products."""
    state.setdefault("_last_ok", {})[site] = time.time()


def check_is_stale(state: dict, site: str, interval: float) -> bool:
    """True if the site hasn't had a successful check in 3x its poll interval."""
    last = (state.get("_last_ok") or {}).get(site)
    if last is None:
        return False  # never succeeded yet — don't cry wolf during first pass
    return (time.time() - last) > max(3 * interval, 300)


# ─── TELEGRAM ─────────────────────────────────────────────────────────────────

MAX_TG_LENGTH = 4000  # Telegram limit is 4096, keep headroom

# Mutable heartbeat — monitor_loop writes, watchdog reads
HEARTBEAT: dict = {"last": 0.0}


async def _send_delayed_forward(original_message: str, client: httpx.AsyncClient) -> None:
    """Fire-and-forget: after N seconds, send the same message to the secondary chat.
    Wrapped in try/except so any failure (cancellation, network) never crashes the caller."""
    try:
        await asyncio.sleep(TELEGRAM_DELAY_SECONDS)
        # Re-chunk independently — original_message is the full pre-split text
        msg = original_message
        chunks = []
        while len(msg) > MAX_TG_LENGTH:
            split_at = msg.rfind("\n", 0, MAX_TG_LENGTH)
            if split_at == -1:
                split_at = MAX_TG_LENGTH
            chunks.append(msg[:split_at])
            msg = msg[split_at:].lstrip("\n")
        chunks.append(msg)
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        for chunk in chunks:
            try:
                resp = await client.post(
                    url,
                    json={
                        "chat_id": TELEGRAM_CHAT_ID_DELAYED,
                        "text": chunk,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                    },
                    timeout=15,
                )
                if resp.status_code != 200:
                    log.warning("Delayed forward error %s: %s", resp.status_code, resp.text[:160])
                else:
                    log.info("Delayed forward sent (%d chars)", len(chunk))
                await asyncio.sleep(0.5)
            except Exception as exc:
                log.warning("Delayed forward chunk failed: %s", exc)
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        log.warning("Delayed forward outer error: %s", exc)


async def send_telegram(message: str, client: httpx.AsyncClient) -> int | None:
    """Send a Telegram message, splitting if needed. Returns message_id of first chunk."""
    if not TELEGRAM_ENABLED:
        log.info("[TELEGRAM DISABLED] %s", message[:120])
        return None

    # Schedule the delayed forward BEFORE we await the primary send, so its
    # 120s timer starts from approximately the same moment we send to the user.
    if TELEGRAM_CHAT_ID_DELAYED:
        asyncio.create_task(_send_delayed_forward(message, client))

    chunks = []
    while len(message) > MAX_TG_LENGTH:
        split_at = message.rfind("\n", 0, MAX_TG_LENGTH)
        if split_at == -1:
            split_at = MAX_TG_LENGTH
        chunks.append(message[:split_at])
        message = message[split_at:].lstrip("\n")
    chunks.append(message)

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    first_id: int | None = None
    for chunk in chunks:
        try:
            resp = await client.post(
                url,
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": chunk,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
            if resp.status_code != 200:
                log.error("Telegram error %s: %s", resp.status_code, resp.text[:200])
            else:
                if first_id is None:
                    first_id = resp.json().get("result", {}).get("message_id")
                log.info("Telegram message sent (%d chars)", len(chunk))
            await asyncio.sleep(0.5)
        except Exception as exc:
            log.error("Telegram send failed: %s", exc)
    return first_id


async def edit_telegram(message_id: int, message: str, client: httpx.AsyncClient) -> bool:
    """Edit an existing Telegram message. Returns True on success."""
    if not TELEGRAM_ENABLED:
        return True
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
    try:
        resp = await client.post(
            url,
            json={
                "chat_id":                  TELEGRAM_CHAT_ID,
                "message_id":               message_id,
                "text":                     message[:MAX_TG_LENGTH],
                "parse_mode":               "HTML",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        # 400 with "message is not modified" is fine — not a real error
        if resp.status_code == 400 and "not modified" in resp.text:
            return True
        return resp.status_code == 200
    except Exception as exc:
        log.error("Telegram edit failed: %s", exc)
        return False


async def run_watchdog(holder: dict, client: httpx.AsyncClient, respawn=None) -> None:
    """
    Runs alongside monitor_loop. holder["task"] is the live monitor task, so the
    watchdog keeps watching across self-heal restarts.

    • Heartbeat stale > 10 min → alert AND auto-restart the loop
    • Task crashes            → alert AND auto-restart the loop

    Previously it only *told* the user to send stop/start, which meant a 3am
    stall or crash left the monitor dead until a human intervened. With respawn
    supplied it now recovers itself, backing off on repeated failures.
    """
    STALE_THRESHOLD = 600   # 10 minutes
    CHECK_EVERY     = 120   # check every 2 minutes
    alert_sent      = False
    restarts        = 0
    MAX_RESTARTS    = 5     # then stop trying and leave the alert standing

    async def _self_heal(reason: str) -> bool:
        nonlocal restarts
        if respawn is None or restarts >= MAX_RESTARTS:
            return False
        restarts += 1
        old = holder.get("task")
        if old and not old.done():
            old.cancel()
            try:
                await old
            except (asyncio.CancelledError, Exception):
                pass
        HEARTBEAT["last"] = 0.0
        holder["task"] = respawn()
        log.warning("Watchdog self-heal #%d (%s) — monitor restarted", restarts, reason)
        await send_telegram(
            f"🔄 <b>Monitor auto-restarted</b> ({reason})\n\n"
            f"Recovery attempt {restarts}/{MAX_RESTARTS}. No action needed.",
            client,
        )
        return True

    # Grace period — let the loop start up before we start watching
    await asyncio.sleep(60)

    while True:
        await asyncio.sleep(CHECK_EVERY)
        task = holder.get("task")

        # ── Task died? ────────────────────────────────────────────────────
        if task is None or task.done():
            if task is not None and not task.cancelled():
                try:
                    exc = task.exception()
                except (asyncio.CancelledError, asyncio.InvalidStateError):
                    exc = None
                if exc:
                    await send_telegram(
                        f"🚨 <b>Monitor crashed!</b>\n\n"
                        f"<code>{type(exc).__name__}: {exc}</code>",
                        client,
                    )
                    if await _self_heal("crash"):
                        continue
                    await send_telegram(
                        "❗ Auto-restart limit reached. Send <code>start</code> to retry.", client
                    )
            return  # stopped deliberately, or we've given up

        # ── Heartbeat stale? ──────────────────────────────────────────────
        age = time.monotonic() - HEARTBEAT["last"]
        if HEARTBEAT["last"] > 0 and age > STALE_THRESHOLD:
            if not alert_sent:
                await send_telegram(
                    "🚨 <b>Monitor is not responding!</b>\n\n"
                    "The loop has been silent for over 10 minutes — restarting it now.",
                    client,
                )
                alert_sent = True
            if await _self_heal("stalled"):
                alert_sent = False
                continue
        else:
            if alert_sent:
                await send_telegram("✅ <b>Monitor has recovered.</b>", client)
            alert_sent = False


async def poll_telegram(offset: int, client: httpx.AsyncClient) -> list:
    """Long-poll Telegram for new messages. Blocks up to 30s."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    try:
        resp = await client.get(
            url,
            params={"offset": offset, "timeout": 30},
            timeout=35,
        )
        if resp.status_code == 200:
            return resp.json().get("result", [])
    except Exception as exc:
        log.error("Telegram poll error: %s", exc)
        await asyncio.sleep(5)
    return []


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def strip_accents(text: str) -> str:
    """Lowercase and strip diacritics: 'Pokémon' → 'pokemon'.

    Retailers list the same product both ways ('Pokemon TCG …' and
    'Pokémon TCG …'), so every title match must normalise first or the
    accented spelling is silently dropped."""
    return "".join(
        ch for ch in unicodedata.normalize("NFD", (text or "").lower())
        if unicodedata.category(ch) != "Mn"
    )


# Brands/product lines Gurps never wants alerts for (accessories, figures,
# model kits — not sealed TCG). Matched accent-insensitively as substrings.
EXCLUDED_TITLE_WORDS = ("evoretro", "blokees", "funko", "plamo", "takara tomy")


def title_excluded(title: str) -> bool:
    """True if the title contains an excluded brand (EVORETRO cases, Blokees,
    Funko figures, Plamo model kits)."""
    t = strip_accents(title)
    return any(w in t for w in EXCLUDED_TITLE_WORDS)


def is_pokemon_title(title: str) -> bool:
    """True if a product title looks like a Pokemon product (accent-insensitive),
    excluding blocked brands (EVORETRO/Blokees/Funko/Plamo)."""
    t = strip_accents(title)
    if any(w in t for w in EXCLUDED_TITLE_WORDS):
        return False
    return "pokemon" in t or "pikachu" in t


def product_key(title: str) -> str:
    """Normalise a product title to a stable dictionary key."""
    return title.lower().strip().replace(" ", "-").replace("/", "-")[:80]


def fmt_product(prod: dict, status_icon: str = "") -> str:
    icon = status_icon or ("✅" if prod.get("available") else "❌")
    price = prod.get("price", "")
    price_str = f" — {price}" if price and price != "N/A" else ""
    url = prod.get("url", "")
    title = prod.get("title", "Unknown")
    if url:
        return f'  {icon} <a href="{url}">{title}</a>{price_str}'
    return f"  {icon} {title}{price_str}"


# ─── OTAKUME ──────────────────────────────────────────────────────────────────

async def check_otakume(state: dict, client: httpx.AsyncClient) -> dict:
    log.info("Checking Otakume...")
    current: dict[str, dict] = {}

    try:
        await asyncio.sleep(random.uniform(1.5, 4))

        # Otakume paginates at 16 products/page, so page 1 alone misses most of
        # the collection. Walk pages until one yields nothing new.
        # partial=True means a page failed mid-walk (Otakume 503s under our
        # cadence) — the scrape is incomplete, so downstream must NOT treat
        # missing products as gone, and must NOT baseline from it (a partial
        # baseline makes the next clean scrape re-alert everything it missed).
        product_links = []
        seen_hrefs: set[str] = set()
        partial = False
        for page_num in range(1, 8):  # safety cap
            page_url = URLS["otakume"] if page_num == 1 else f"{URLS['otakume']}?page={page_num}"

            resp = None
            for attempt in range(2):  # one retry on transient 5xx
                resp = await client.get(
                    page_url,
                    headers={
                        "User-Agent": random.choice(USER_AGENTS),
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language": "en-GB,en;q=0.9",
                    },
                    timeout=25,
                )
                if resp.status_code == 200 or resp.status_code < 500:
                    break
                await asyncio.sleep(random.uniform(4, 7))

            if resp.status_code != 200:
                log.warning("Otakume returned HTTP %s on page %d", resp.status_code, page_num)
                if page_num == 1:
                    return state
                partial = True
                break

            if page_num == 1:
                log.info("Otakume: response %d chars, encoding=%s", len(resp.text), resp.encoding)
            soup = BeautifulSoup(resp.text, "html.parser")

            page_links = soup.select("a[href*='/products/']")
            fresh = [a for a in page_links if a.get("href", "") not in seen_hrefs]
            if not fresh:
                break
            seen_hrefs.update(a.get("href", "") for a in fresh)
            product_links.extend(fresh)
            await asyncio.sleep(random.uniform(1, 2))

        log.info("Otakume: found %d product links across %d page(s)%s",
                 len(product_links), page_num, " [PARTIAL]" if partial else "")

        for item in product_links:
            # Title from image alt attribute
            img = item.select_one("img")
            title = img.get("alt", "") if img else ""
            if not title or len(title) < 3:
                continue
            if title_excluded(title):
                continue

            # Product URL
            href = item.get("href", "")
            url = href if href.startswith("http") else f"https://otakume.com{href}"

            # Price — look for price element in or near the product item
            price_el = item.select_one("[class*='price'], .price, [data-price]")
            price_text = price_el.get_text(strip=True) if price_el else "N/A"
            price = price_text if "£" in price_text or "$" in price_text else price_text

            # Availability — check for sold out indicators
            oos_el = item.select_one(".sold-out, .out-of-stock, [class*='sold'], [class*='unavailable']")
            available = not oos_el

            product_id = title.lower().replace(" ", "-").replace("/", "-")[:70]
            current[product_id] = {
                "title":     title,
                "available": available,
                "price":     price,
                "url":       url,
            }

        if not current:
            log.warning("Otakume: no products found — page may have changed")
            return state
        mark_ok(state, "otakume")

        log.info("Otakume: products found: %s",
                 ", ".join(p["title"] for p in current.values()))

        prev = state.get("otakume", {})
        first_run = len(prev) == 0

        # Never baseline from an incomplete walk: products on the failed pages
        # would be absent from state and re-alert as "new" on the next clean
        # scrape. Wait for a complete pass instead.
        if first_run and partial:
            log.warning("Otakume: partial scrape on first run — deferring baseline until a clean pass")
            return state

        if first_run:
            in_stock  = [v for v in current.values() if v["available"]]
            out_stock = [v for v in current.values() if not v["available"]]

            lines = [f"<b>🟡 OTAKUME — Monitoring Started ({len(current)} products)</b>"]
            if in_stock:
                lines.append("\n✅ <b>Currently In Stock:</b>")
                for p in in_stock:
                    lines.append(fmt_product(p))
            if out_stock:
                lines.append("\n❌ <b>Currently Out of Stock:</b>")
                for p in out_stock:
                    lines.append(fmt_product(p))

            await send_telegram("\n".join(lines), client)
            log.info("Otakume: baseline sent (%d products)", len(current))

        else:
            new_products = []
            restocked    = []
            went_oos     = []

            for pid, prod in current.items():
                if pid not in prev:
                    new_products.append(prod)
                elif prod["available"] != prev[pid]["available"]:
                    if prod["available"]:
                        restocked.append(prod)
                    else:
                        went_oos.append(prod)

            if new_products:
                lines = [f"<b>🆕 OTAKUME — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
                log_events("otakume", "new", new_products)

            if restocked:
                lines = ["<b>🟢 OTAKUME — Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)
                log_events("otakume", "restock", restocked)

            if went_oos:
                lines = ["<b>🔴 OTAKUME — Out of Stock</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
                log_events("otakume", "oos", went_oos)

            if not (new_products or restocked or went_oos):
                log.info("Otakume: no changes")

        # Grace window for scrape flicker: Otakume's CDN intermittently serves
        # page variants missing a product, so a product can vanish from one
        # check and return the next. If we dropped it from state immediately,
        # every return would re-alert as "new" (observed: Pitch Black Booster
        # Bundle re-pinging all morning). Keep missing products in state for
        # OTAKUME_MISS_GRACE consecutive misses before treating them as truly
        # delisted — after that, a reappearance is a genuine relist (Otakume
        # delists on sellout, so relist = restock) and SHOULD alert.
        OTAKUME_MISS_GRACE = 30  # complete checks (~30-60 min)
        merged = dict(current)
        for pid, prod in prev.items():
            if pid in current:
                continue
            # Only age misses on a COMPLETE scrape — on a partial one the
            # product may simply be on a page that 503'd, so carry unchanged.
            misses = prod.get("_misses", 0) + (0 if partial else 1)
            if misses <= OTAKUME_MISS_GRACE:
                kept = dict(prod)
                kept["_misses"] = misses
                merged[pid] = kept
            # else: drop for real — gone long enough to be a true delist
        # Products present this check reset their miss counter implicitly
        # (fresh dicts from current have no _misses key).
        state["otakume"] = merged

    except Exception as exc:
        log.error("Otakume check failed: %s", exc)

    return state


# ─── VIRGIN MEGASTORE ─────────────────────────────────────────────────────────

async def check_virgin_megastore(
    state: dict,
    client: httpx.AsyncClient,
    *,
    url_keys: tuple = ("virgin_megastore", "virgin_megastore_search", "virgin_megastore_accented"),
    state_key: str = "virgin_megastore",
    log_name: str = "Virgin Megastore",
    headline: str = "🇦🇪 VIRGIN MEGASTORE",
    keyword_filter: str = "pok",
) -> dict:
    """Virgin Megastore — SAP Hybris, product grid is server-rendered.

    Uses curl_cffi (impersonating Chrome's TLS fingerprint) instead of httpx:
    httpx on Railway's datacenter IP gets 403 because Cloudflare can tell it's
    Python from the TLS handshake alone. curl_cffi sends a ClientHello
    indistinguishable from real Chrome, which bypasses Virgin's anti-bot layer
    from the same IP that httpx can't get through. The `client` param is
    retained for Telegram calls and compatibility with the dispatch loop.

    Fetches every URL in url_keys and merges the products (dedup by key). This
    lets one category page be augmented with plain + accented keyword searches
    (e.g. 'pokemon tcg' and 'Pokémon tcg') so accented titles aren't missed.
    Results from search-type URLs (…/search?text=…) are filtered to titles
    containing keyword_filter to strip unrelated search noise; category pages
    are kept whole. Set keyword_filter='' to disable filtering."""
    log.info("Checking %s...", log_name)

    # Lazy import — isolates the dependency and keeps the file importable even
    # if curl_cffi isn't installed yet (first Railway deploy with new reqs).
    try:
        from curl_cffi.requests import AsyncSession
    except ImportError as exc:
        log.error("curl_cffi not available: %s", exc)
        raise

    try:
        await asyncio.sleep(random.uniform(1, 3))

        urls = [(k, URLS.get(k)) for k in url_keys]
        urls = [(k, u) for k, u in urls if u]
        current: dict[str, dict] = {}
        any_ok = False

        async with AsyncSession(impersonate="chrome") as cf:
            # Warm up cookies by hitting the homepage first; Cloudflare sets
            # session cookies on the first visit that are required on deep links.
            try:
                await cf.get("https://www.virginmegastore.ae/en/", timeout=15)
            except Exception:
                pass  # best-effort; the real request still carries what we got

            for key, page_url in urls:
                resp = await cf.get(
                    page_url,
                    headers={"Referer": "https://www.virginmegastore.ae/en/"},
                    timeout=25,
                )
                if resp.status_code != 200:
                    log.warning("%s returned HTTP %s for %s", log_name, resp.status_code, key)
                    continue
                any_ok = True
                is_search = "search?text=" in page_url

                soup = BeautifulSoup(resp.text, "html.parser")
                added = 0
                for item in soup.select(".product-item"):
                    name_el = item.select_one("a.product-list__name")
                    if not name_el:
                        continue
                    title = name_el.get_text(strip=True)
                    if not title or len(title) < 3:
                        continue
                    if title_excluded(title):
                        continue

                    # Strip search noise: on keyword-search pages keep only
                    # titles matching the filter (category pages keep all).
                    if is_search and keyword_filter and keyword_filter not in strip_accents(title):
                        continue

                    href = name_el.get("href", "")
                    url  = href if href.startswith("http") else f"https://www.virginmegastore.ae{href}"

                    currency = item.select_one(".price__currency")
                    number   = item.select_one(".gtm-price-number")
                    if currency and number:
                        price = f"{currency.get_text(strip=True)} {number.get_text(strip=True)}"
                    else:
                        price = "N/A"

                    oos_el    = item.select_one("[class*='out-of-stock'], [class*='sold-out'], [class*='unavailable']")
                    available = not oos_el

                    pkey = product_key(title)
                    current[pkey] = {"title": title, "url": url, "price": price, "available": available}
                    added += 1

                log.info("%s: %s → %d product(s) merged", log_name, key, added)
                await asyncio.sleep(random.uniform(0.5, 1.5))

        if not any_ok:
            log.warning("%s: all URLs failed — skipping state update", log_name)
            raise RuntimeError("all URLs failed")
        if not current:
            log.warning("%s: no products found — selectors may need updating", log_name)
            raise RuntimeError("no products parsed")

        log.info("%s: %d unique products across %d URL(s)", log_name, len(current), len(urls))
        mark_ok(state, state_key)

        prev      = state.get(state_key, {})
        first_run = len(prev) == 0

        if first_run:
            in_stock  = [v for v in current.values() if v["available"]]
            out_stock = [v for v in current.values() if not v["available"]]
            lines = [f"<b>{headline} — Monitoring Started ({len(current)} products)</b>"]
            if in_stock:
                lines.append("\n✅ <b>In Stock:</b>")
                for p in in_stock:
                    lines.append(fmt_product(p))
            if out_stock:
                lines.append("\n❌ <b>Out of Stock:</b>")
                for p in out_stock:
                    lines.append(fmt_product(p))
            await send_telegram("\n".join(lines), client)
            log.info("%s: baseline sent (%d products)", log_name, len(current))
        else:
            new_products, restocked, went_oos = [], [], []
            for pid, prod in current.items():
                if pid not in prev:
                    new_products.append(prod)
                elif prod["available"] != prev[pid]["available"]:
                    (restocked if prod["available"] else went_oos).append(prod)

            if new_products:
                lines = [f"<b>🆕 {headline} — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
                log_events(state_key, "new", new_products)
            if restocked:
                lines = [f"<b>🟢 {headline} — Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)
                log_events(state_key, "restock", restocked)
            if went_oos:
                lines = [f"<b>🔴 {headline} — Out of Stock</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
                log_events(state_key, "oos", went_oos)
            if not (new_products or restocked or went_oos):
                log.info("%s: no changes", log_name)

        state[state_key] = current

    except Exception as exc:
        log.error("%s check failed: %s", log_name, exc)
        raise

    return state


async def check_virgin_megastore_onepiece(state: dict, client: httpx.AsyncClient) -> dict:
    """Virgin Megastore — One Piece Card Game search results."""
    return await check_virgin_megastore(
        state, client,
        url_keys=("virgin_megastore_onepiece",),
        state_key="virgin_megastore_onepiece",
        log_name="Virgin Megastore (OP)",
        headline="🏴‍☠️ VIRGIN MEGASTORE (OP)",
        keyword_filter="one piece",
    )


# ─── LEGENDS OWN THE GAME ─────────────────────────────────────────────────────

ECWID_STORE_ID = "111644017"
ECWID_TOKEN    = "public_vfBW4FXuDaLUE2LLuBcU8ZLUAgZ5pKV5"


async def legends_auto_add_to_cart(products: list[dict], client: httpx.AsyncClient) -> None:
    """Open a headed browser, navigate to Legends, and add watchlist products to cart via Ecwid API.
    Then send Telegram alert to go checkout."""
    auto_cfg = CFG.get("legends_auto_checkout", {})
    if not auto_cfg.get("enabled"):
        return

    ecwid_ids = [p["ecwid_id"] for p in products if p.get("ecwid_id")]
    if not ecwid_ids:
        log.warning("Legends auto-cart: no Ecwid IDs to add")
        return

    log.info("Legends auto-cart: adding %d product(s) to cart...", len(ecwid_ids))

    try:
        from patchright.async_api import async_playwright as pw_launch
        async with pw_launch() as pw:
            browser = await pw.chromium.launch(
                headless=False,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
            )
            page = await browser.new_page()
            await page.goto("https://legendsownthegame.com", wait_until="domcontentloaded", timeout=35_000)
            await page.wait_for_timeout(4000)

            # Add each product via Ecwid JS API
            for ecwid_id in ecwid_ids:
                result = await page.evaluate(f"""
                    new Promise((resolve) => {{
                        if (typeof Ecwid !== 'undefined' && Ecwid.Cart) {{
                            Ecwid.Cart.addProduct({{id: {ecwid_id}, quantity: 1, callback: function(success, product) {{
                                resolve(success ? 'added' : 'failed');
                            }}}});
                        }} else {{
                            resolve('no_ecwid');
                        }}
                    }})
                """)
                log.info("Legends auto-cart: product %s → %s", ecwid_id, result)
                await page.wait_for_timeout(1500)

            # Navigate to cart
            await page.goto("https://legendsownthegame.com/products/cart", wait_until="domcontentloaded", timeout=20_000)
            await page.wait_for_timeout(2000)

            # Send Telegram alert
            names = "\n".join(f"  🛒 {p['title']} — {p['price']}" for p in products)
            await send_telegram(
                f"<b>🚨 LEGENDS — ITEMS ADDED TO CART!</b>\n\n"
                f"{names}\n\n"
                f"<b>👉 Cart is open on your laptop — GO CHECKOUT NOW!</b>\n\n"
                f'<a href="https://legendsownthegame.com/products/cart">Open Cart</a>',
                client,
            )
            log.info("Legends auto-cart: cart open, alert sent")

            # Keep browser open for user to checkout
            await page.wait_for_timeout(300_000)  # 5 minutes
            await browser.close()

    except Exception as exc:
        log.error("Legends auto-cart failed: %s", exc)
        await send_telegram(
            f"⚠️ <b>Legends auto-cart failed!</b>\n\n"
            f"Error: {exc}\n\n"
            f"Add manually: https://legendsownthegame.com/products/cart",
            client,
        )


async def check_legends_own_the_game(state: dict, client: httpx.AsyncClient) -> dict:
    """Legends Own The Game runs on Ecwid — uses their public JSON API directly.
    No browser needed; paginate through all enabled Pokemon products."""
    log.info("Checking Legends Own The Game...")
    current: dict[str, dict] = {}

    try:
        await asyncio.sleep(random.uniform(1, 3))

        # Paginate through all results (Ecwid API max 100 per page).
        # Search both the plain and accented spellings; merge by product key.
        limit = 100
        for kw in ("pokemon", "Pok%C3%A9mon"):  # plain + accented (Pokémon)
            offset = 0
            while True:
                api_url = (
                    f"https://app.ecwid.com/api/v3/{ECWID_STORE_ID}/products"
                    f"?keyword={kw}&category=176121252&enabled=true&limit={limit}&offset={offset}&lang=en"
                )
                resp = await client.get(
                    api_url,
                    headers={
                        "Authorization": f"Bearer {ECWID_TOKEN}",
                        "Accept":        "application/json",
                        "User-Agent":    random.choice(USER_AGENTS),
                    },
                    timeout=25,
                )

                if resp.status_code != 200:
                    log.warning("Legends Own The Game: API returned HTTP %s (keyword=%s)", resp.status_code, kw)
                    break  # skip this keyword; keep whatever the other term returned

                data  = resp.json()
                items = data.get("items", [])
                total = data.get("total", 0)
                log.info("Legends Own The Game: fetched %d/%d products (keyword=%s offset=%d)",
                         len(items), total, kw, offset)

                for item in items:
                    name = item.get("name", "")
                    if not name or len(name) < 3:
                        continue
                    price      = item.get("defaultDisplayedPriceFormatted", "N/A")
                    in_stock   = bool(item.get("inStock", False))
                    prod_url   = item.get("url", "")
                    ecwid_id   = item.get("id")
                    key        = product_key(name)
                    current[key] = {
                        "title":     name,
                        "url":       prod_url,
                        "price":     price,
                        "available": in_stock,
                        "ecwid_id":  ecwid_id,
                    }

                offset += len(items)
                if not items or offset >= total:
                    break
            await asyncio.sleep(random.uniform(0.5, 1.5))

        if not current:
            # Do NOT wipe the baseline: a transient empty 200 (Ecwid hiccup /
            # rate limit) previously reset state, so the next good pass
            # re-baselined ~100 products as a spam wall that buried real alerts.
            log.warning("Legends Own The Game: no Pokemon products returned by API — keeping previous state")
            return state

        log.info("Legends Own The Game: %d products found", len(current))

        mark_ok(state, "legends_own_the_game")

        prev      = state.get("legends_own_the_game", {})
        first_run = len(prev) == 0

        if first_run:
            in_stock_list  = [v for v in current.values() if v["available"]]
            out_stock_list = [v for v in current.values() if not v["available"]]
            lines = [f"<b>🎴 LEGENDS OWN THE GAME — Monitoring Started ({len(current)} products)</b>"]
            if in_stock_list:
                lines.append("\n✅ <b>In Stock:</b>")
                for p in in_stock_list[:20]:
                    lines.append(fmt_product(p))
                if len(in_stock_list) > 20:
                    lines.append(f"  ...and {len(in_stock_list) - 20} more in stock")
            if out_stock_list:
                lines.append("\n❌ <b>Out of Stock:</b>")
                for p in out_stock_list[:10]:
                    lines.append(fmt_product(p))
                if len(out_stock_list) > 10:
                    lines.append(f"  ...and {len(out_stock_list) - 10} more out of stock")
            await send_telegram("\n".join(lines), client)
            log.info("Legends Own The Game: baseline sent (%d products)", len(current))

        else:
            new_products, restocked, went_oos = [], [], []
            for pid, prod in current.items():
                if pid not in prev:
                    new_products.append(prod)
                elif prod["available"] != prev[pid]["available"]:
                    (restocked if prod["available"] else went_oos).append(prod)

            if new_products:
                lines = [f"<b>🆕 LEGENDS OWN THE GAME — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
            if restocked:
                lines = ["<b>🟢 LEGENDS OWN THE GAME — Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)
            if went_oos:
                lines = ["<b>🔴 LEGENDS OWN THE GAME — Out of Stock</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
            if not (new_products or restocked or went_oos):
                log.info("Legends Own The Game: no changes")

            # ── Auto-cart: check if any new/restocked products match the watchlist ──
            auto_cfg = CFG.get("legends_auto_checkout", {})
            if auto_cfg.get("enabled"):
                watchlist = [w.lower() for w in auto_cfg.get("watchlist", [])]
                cart_candidates = []
                for p in (new_products + restocked):
                    if p.get("available") and any(w in p["title"].lower() for w in watchlist):
                        cart_candidates.append(p)
                if cart_candidates:
                    log.info("Legends auto-cart: %d watchlist product(s) matched!", len(cart_candidates))
                    # Fire and forget — don't block the monitor loop
                    asyncio.create_task(legends_auto_add_to_cart(cart_candidates, client))

        state["legends_own_the_game"] = current

    except Exception as exc:
        log.error("Legends Own The Game check failed: %s", exc)

    return state


# ─── COLORLAND TOYS (Shopify SSR) ─────────────────────────────────────────────

async def check_colorland_toys(state: dict, client: httpx.AsyncClient) -> dict:
    """Colorland Toys — Shopify store with server-rendered HTML.
    Parses data-json-product attributes directly; no browser needed.
    Paginates through all pages (50 per page)."""
    log.info("Checking Colorland Toys...")
    current: dict[str, dict] = {}

    try:
        await asyncio.sleep(random.uniform(1, 3))
        # Search both plain and accented spellings; merge by handle.
        colorland_bases = [URLS["colorland_toys"], URLS.get("colorland_toys_accented")]
        colorland_bases = [b for b in colorland_bases if b]
        fetch_error = False
        for base in colorland_bases:
            page_num = 1
            while True:
                url = base if page_num == 1 else f"{base}&page={page_num}"
                resp = await client.get(
                    url,
                    headers=get_headers("https://colorlandtoys.com/"),
                    timeout=25,
                )
                if resp.status_code == 429:
                    log.warning("Colorland Toys: rate limited on page %d — waiting 30s and retrying", page_num)
                    await asyncio.sleep(30)
                    resp = await client.get(url, headers=get_headers("https://colorlandtoys.com/"), timeout=25)
                if resp.status_code != 200:
                    log.warning("Colorland Toys: HTTP %s on page %d — skipping state update", resp.status_code, page_num)
                    fetch_error = True
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                items = soup.select("div.product-item[data-json-product]")
                log.info("Colorland Toys: page %d — %d items", page_num, len(items))

                if not items:
                    break

                for item in items:
                    try:
                        data = json.loads(item["data-json-product"])
                    except Exception:
                        continue

                    handle = data.get("handle", "")
                    if not handle:
                        continue

                    variants  = data.get("variants", [{}])
                    v         = variants[0] if variants else {}
                    available = bool(v.get("available", True))
                    price_raw = v.get("price", 0)
                    price     = f"AED {int(price_raw) // 100}" if price_raw else "N/A"

                    # Title from HTML (not in JSON)
                    title_el = item.select_one("a.card-title span.text") or item.select_one("a.card-title")
                    title    = title_el.get_text(strip=True) if title_el else handle.replace("-", " ").title()
                    if not title or len(title) < 3:
                        continue

                    # Colorland's search uses options[prefix]=last, which is fuzzy
                    # enough to return ~353 products (Uno, VTech Paw Patrol, board
                    # games). Keep only Pokemon titles or the non-Pokemon items
                    # flap in/out of stock and spam alerts.
                    if not is_pokemon_title(title):
                        continue

                    prod_url = f"https://colorlandtoys.com/products/{handle}"
                    current[handle] = {"title": title, "url": prod_url, "price": price, "available": available}

                if len(items) < 50:
                    break  # last page
                page_num += 1
                await asyncio.sleep(random.uniform(3, 6))

            if fetch_error:
                break
            await asyncio.sleep(random.uniform(2, 4))

        if fetch_error:
            log.warning("Colorland Toys: pagination error — state not updated")
            return state

        if not current:
            log.warning("Colorland Toys: no products found — selectors may have changed")
            return state

        log.info("Colorland Toys: %d products found across %d page(s)", len(current), page_num)

        mark_ok(state, "colorland_toys")

        prev      = state.get("colorland_toys", {})
        first_run = len(prev) == 0

        COLORLAND_BLOCKLIST = {
            "pokemon-my-partner-pikachu-pkw0030",
            "vtech-paw-patrol-learning-watch-chase-80-551603",
            "vtech-paw-patrol-learning-watch-marshall-80-551663",
            # Uno DOS — flaps in/out of stock constantly, not Pokemon TCG anyway
            "uno-dos-refresh-card-game-hnn01",
            "uno-dos-card-game-frm36",
        }

        # Send startup summary on first check of each session
        if first_run or not state.get("_colorland_startup_sent"):
            in_stock  = [v for v in current.values() if v["available"]]
            out_stock = [v for v in current.values() if not v["available"]]
            lines = [f"<b>🧩 COLORLAND TOYS — Monitoring {'Started' if first_run else 'Resumed'} ({len(current)} products)</b>"]
            if in_stock:
                lines.append("\n✅ <b>In Stock:</b>")
                for p in in_stock[:20]:
                    lines.append(fmt_product(p))
                if len(in_stock) > 20:
                    lines.append(f"  ...and {len(in_stock) - 20} more in stock")
            if out_stock:
                lines.append(f"\n❌ <b>Out of Stock:</b> {len(out_stock)} product(s)")
            await send_telegram("\n".join(lines), client)
            state["_colorland_startup_sent"] = True
            log.info("Colorland Toys: baseline sent (%d products)", len(current))
        else:
            new_products, restocked, went_oos = [], [], []
            for pid, prod in current.items():
                if pid in COLORLAND_BLOCKLIST:
                    continue
                if pid not in prev:
                    new_products.append(prod)
                elif prod["available"] != prev[pid]["available"]:
                    (restocked if prod["available"] else went_oos).append(prod)

            if new_products:
                lines = [f"<b>🆕 COLORLAND TOYS — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
                log_events("colorland_toys", "new", new_products)
            if restocked:
                lines = ["<b>🟢 COLORLAND TOYS — Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)
                log_events("colorland_toys", "restock", restocked)
            if went_oos:
                lines = ["<b>🔴 COLORLAND TOYS — Out of Stock</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
                log_events("colorland_toys", "oos", went_oos)
            if not (new_products or restocked or went_oos):
                log.info("Colorland Toys: no changes")

        # Merge current into prev so flickering products don't re-alert
        merged = state.get("colorland_toys", {})
        merged.update(current)
        state["colorland_toys"] = merged

    except Exception as exc:
        log.error("Colorland Toys check failed: %s", exc)

    return state


# ─── TOY CORNER ───────────────────────────────────────────────────────────────

async def check_toycorner(state: dict, client: httpx.AsyncClient) -> dict:
    """Toy Corner — WooCommerce store, server-rendered HTML.
    Parses the Pokemon trading cards category page; paginates via /page/N/."""
    log.info("Checking Toy Corner...")
    current: dict[str, dict] = {}

    try:
        await asyncio.sleep(random.uniform(1, 3))
        base_url    = URLS["toycorner"]
        page_num    = 1
        fetch_error = False
        max_pages   = 12  # safety cap
        while page_num <= max_pages:
            url = base_url if page_num == 1 else f"{base_url.rstrip('/')}/page/{page_num}/"
            resp = await client.get(
                url,
                headers=get_headers("https://toycorner.ae/"),
                timeout=25,
                follow_redirects=True,
            )
            if resp.status_code == 404 and page_num > 1:
                break  # past last page
            if resp.status_code == 429:
                log.warning("Toy Corner: rate limited on page %d — waiting 30s and retrying", page_num)
                await asyncio.sleep(30)
                resp = await client.get(url, headers=get_headers("https://toycorner.ae/"), timeout=25, follow_redirects=True)
            if resp.status_code != 200:
                log.warning("Toy Corner: HTTP %s on page %d — skipping state update", resp.status_code, page_num)
                fetch_error = True
                break

            soup  = BeautifulSoup(resp.text, "html.parser")
            items = soup.find_all(attrs={"class": lambda c: c and "type-product" in c})
            log.info("Toy Corner: page %d — %d items", page_num, len(items))

            if not items:
                break

            for item in items:
                classes = " ".join(item.get("class", []))
                pid_m   = re.search(r"post-(\d+)", classes)
                if not pid_m:
                    continue
                pid       = pid_m.group(1)
                available = ("instock" in classes) and ("outofstock" not in classes)

                # Title
                title_el = item.select_one(".product-title") or item.select_one("h2") or item.select_one("h3")
                title    = title_el.get_text(strip=True) if title_el else ""
                if not title:
                    img = item.find("img")
                    title = (img.get("alt") if img else "") or ""
                if not title or len(title) < 3:
                    continue
                if title_excluded(title):
                    continue

                # URL
                a = item.find("a", href=re.compile(r"/product/"))
                prod_url = a["href"] if a else f"https://toycorner.ae/?p={pid}"

                # Price — prefer <ins> (sale price), else first .woocommerce-Price-amount
                pr_el  = item.select_one(".price")
                price  = "N/A"
                if pr_el:
                    target  = pr_el.find("ins") or pr_el
                    amt_el  = target.select_one(".woocommerce-Price-amount bdi") or target.select_one(".woocommerce-Price-amount")
                    if amt_el:
                        price = f"AED {amt_el.get_text(strip=True)}"

                current[pid] = {"title": title, "url": prod_url, "price": price, "available": available}

            if len(items) < 12:
                break  # last page (default WooCommerce page size)
            page_num += 1
            await asyncio.sleep(random.uniform(3, 6))

        if fetch_error:
            log.warning("Toy Corner: pagination error — state not updated")
            return state

        if not current:
            log.warning("Toy Corner: no products found — selectors may have changed")
            return state

        log.info("Toy Corner: %d products found across %d page(s)", len(current), page_num)

        mark_ok(state, "toycorner")

        prev      = state.get("toycorner", {})
        first_run = len(prev) == 0

        # Send startup summary on first check of each session
        if first_run or not state.get("_toycorner_startup_sent"):
            in_stock  = [v for v in current.values() if v["available"]]
            out_stock = [v for v in current.values() if not v["available"]]
            lines = [f"<b>🧸 TOY CORNER — Monitoring {'Started' if first_run else 'Resumed'} ({len(current)} products)</b>"]
            if in_stock:
                lines.append("\n✅ <b>In Stock:</b>")
                for p in in_stock[:20]:
                    lines.append(fmt_product(p))
                if len(in_stock) > 20:
                    lines.append(f"  ...and {len(in_stock) - 20} more in stock")
            if out_stock:
                lines.append(f"\n❌ <b>Out of Stock:</b> {len(out_stock)} product(s)")
            await send_telegram("\n".join(lines), client)
            state["_toycorner_startup_sent"] = True
            log.info("Toy Corner: baseline sent (%d products)", len(current))
        else:
            new_products, restocked, went_oos = [], [], []
            for pid, prod in current.items():
                if pid not in prev:
                    new_products.append(prod)
                elif prod["available"] != prev[pid]["available"]:
                    (restocked if prod["available"] else went_oos).append(prod)

            if new_products:
                lines = [f"<b>🆕 TOY CORNER — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
                log_events("toycorner", "new", new_products)
            if restocked:
                lines = ["<b>🟢 TOY CORNER — Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)
                log_events("toycorner", "restock", restocked)
            if went_oos:
                lines = ["<b>🔴 TOY CORNER — Out of Stock</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
                log_events("toycorner", "oos", went_oos)
            if not (new_products or restocked or went_oos):
                log.info("Toy Corner: no changes")

        # Merge so transient pagination flickers don't re-alert
        merged = state.get("toycorner", {})
        merged.update(current)
        state["toycorner"] = merged

    except Exception as exc:
        log.error("Toy Corner check failed: %s", exc)

    return state


# ─── KINOKUNIYA UAE ───────────────────────────────────────────────────────────

async def check_kinokuniya(state: dict, client: httpx.AsyncClient) -> dict:
    """Kinokuniya UAE — search results for 'pokemon tcg'.

    AWS ELB + WAF: plain httpx returns 403, chrome impersonation gets blocked,
    Safari impersonation + a homepage warmup GET works.

    v1: tracks which products appear in the search results (by barcode in
    /bw/{N} URLs). Alerts on new products only — search page doesn't expose
    a stock indicator so we treat all results as 'in stock'. A per-product
    stock fetch could be added later if Kinokuniya starts listing OOS items
    in search."""
    log.info("Checking Kinokuniya UAE...")

    try:
        from curl_cffi.requests import AsyncSession
    except ImportError as exc:
        log.error("curl_cffi not available: %s", exc)
        raise

    current: dict[str, dict] = {}

    try:
        await asyncio.sleep(random.uniform(1, 3))

        # Search both the plain and accented spellings — Kinokuniya's search
        # is accent-sensitive, so "pokemon" and "Pokémon" surface different
        # hits. Merge results by barcode (dedup handles overlap).
        search_urls = [URLS["kinokuniya"], URLS.get("kinokuniya_accented")]
        search_urls = [u for u in search_urls if u]

        any_ok = False
        async with AsyncSession(impersonate="safari17_0") as cf:
            # Warm up — AWS WAF cookies get set on first visit
            try:
                await cf.get("https://uae.kinokuniya.com/", timeout=15)
            except Exception:
                pass

            for su in search_urls:
                resp = await cf.get(
                    su,
                    headers={
                        "Referer": "https://uae.kinokuniya.com/",
                        "Accept-Language": "en-US,en;q=0.9",
                    },
                    timeout=25,
                )
                if resp.status_code != 200:
                    log.warning("Kinokuniya: HTTP %s for %s", resp.status_code, su)
                    continue
                any_ok = True

                soup  = BeautifulSoup(resp.text, "html.parser")
                boxes = soup.select("div#image_or_detail div.box")
                log.info("Kinokuniya: %d product card(s) for %s", len(boxes), su.split("keywords=")[-1])

                for box in boxes:
                    link = box.select_one("a[href*='/bw/']")
                    if not link:
                        continue
                    href = link.get("href", "")
                    m = re.search(r"/bw/(\d+)", href)
                    if not m:
                        continue
                    barcode = m.group(1)

                    title_el = box.select_one("span.title")
                    title = title_el.get_text(strip=True) if title_el else ""
                    if not title or len(title) < 3:
                        continue
                    if title_excluded(title):
                        continue

                    price_el = box.select_one(f"span#search_product_image_online_price_{barcode}")
                    price = price_el.get_text(strip=True) if price_el else "N/A"

                    prod_url = href if href.startswith("http") else f"https://uae.kinokuniya.com{href}"

                    current[barcode] = {
                        "title":     title,
                        "url":       prod_url,
                        "price":     price,
                        "available": True,  # search hits treated as available; no stock indicator on listing page
                    }
                await asyncio.sleep(random.uniform(1, 2))

        if not any_ok:
            log.warning("Kinokuniya: all searches failed — skipping state update")
            return state
        if not current:
            log.warning("Kinokuniya: no products parsed — selectors may have changed")
            return state

        log.info("Kinokuniya: %d unique product(s) across %d search term(s)", len(current), len(search_urls))

        mark_ok(state, "kinokuniya")

        prev      = state.get("kinokuniya", {})
        first_run = len(prev) == 0

        if first_run:
            lines = [f"<b>📚 KINOKUNIYA UAE — Monitoring Started ({len(current)} product{'s' if len(current)!=1 else ''})</b>"]
            for p in current.values():
                lines.append(fmt_product(p))
            await send_telegram("\n".join(lines), client)
            log.info("Kinokuniya: baseline sent (%d products)", len(current))
        else:
            new_products = [p for bid, p in current.items() if bid not in prev]
            if new_products:
                lines = [f"<b>🆕 KINOKUNIYA UAE — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
            else:
                log.info("Kinokuniya: no new products")

        state["kinokuniya"] = current

    except Exception as exc:
        log.error("Kinokuniya check failed: %s", exc)

    return state


async def check_kinokuniya_event(state: dict, client: httpx.AsyncClient) -> dict:
    """Kinokuniya UAE — curated Pokémon drop/event page (e.g. /events/1243).

    These pages list a small hand-picked set of Pokemon TCG products for a
    drop. Products are <a href='/bw/{barcode}'> links with an adjacent AED
    price. We track them by barcode and alert on:
      • a NEW product appearing on the event page (the drop expanding / going live)
      • a PRICE change on an existing product (catches any discount)
    """
    log.info("Checking Kinokuniya event page...")

    try:
        from curl_cffi.requests import AsyncSession
    except ImportError as exc:
        log.error("curl_cffi not available: %s", exc)
        raise

    current: dict[str, dict] = {}

    try:
        await asyncio.sleep(random.uniform(1, 3))

        async with AsyncSession(impersonate="safari17_0") as cf:
            try:
                await cf.get("https://uae.kinokuniya.com/", timeout=15)
            except Exception:
                pass
            resp = await cf.get(
                URLS["kinokuniya_event"],
                headers={"Referer": "https://uae.kinokuniya.com/", "Accept-Language": "en-US,en;q=0.9"},
                timeout=25,
            )

        if resp.status_code != 200:
            log.warning("Kinokuniya event: HTTP %s", resp.status_code)
            return state

        soup = BeautifulSoup(resp.text, "html.parser")
        title_el = soup.select_one("h2.eventDetTitle") or soup.select_one("title")
        event_name = title_el.get_text(strip=True) if title_el else "Kinokuniya Event"

        # Each product appears as one or more /bw/{barcode} links; the text
        # link carries the title, and an AED price sits nearby. Merge by barcode.
        for a in soup.select("a[href*='/bw/']"):
            href = a.get("href", "")
            m = re.search(r"/bw/(\d+)", href)
            if not m:
                continue
            barcode = m.group(1)

            txt = a.get_text(" ", strip=True)
            img = a.select_one("img")
            title = txt or (img.get("alt", "") if img else "")
            if title and title_excluded(title):
                continue

            # Price: search this link's ancestors for an AED amount
            price = "N/A"
            node = a
            for _ in range(4):
                node = node.parent
                if not node:
                    break
                pm = re.search(r"AED\s*[\d.,]+", node.get_text(" ", strip=True))
                if pm:
                    price = pm.group(0)
                    break

            prod_url = href if href.startswith("http") else f"https://uae.kinokuniya.com{href}"
            existing = current.get(barcode)
            # Prefer the entry that has a real title over an image-only link
            if existing and not title:
                continue
            current[barcode] = {
                "title":     title or (existing or {}).get("title", "") or f"Product {barcode}",
                "url":       prod_url,
                "price":     price if price != "N/A" else (existing or {}).get("price", "N/A"),
                "available": True,
            }

        if not current:
            log.warning("Kinokuniya event: no products parsed — page layout may have changed")
            return state

        log.info("Kinokuniya event '%s': %d product(s)", event_name, len(current))

        mark_ok(state, "kinokuniya_event")

        prev      = state.get("kinokuniya_event", {})
        first_run = len(prev) == 0

        if first_run:
            lines = [f"<b>🎴 KINOKUNIYA EVENT — {event_name} ({len(current)} product{'s' if len(current)!=1 else ''})</b>"]
            for p in current.values():
                lines.append(fmt_product(p))
            await send_telegram("\n".join(lines), client)
            log.info("Kinokuniya event: baseline sent (%d products)", len(current))
        else:
            new_products = [p for bid, p in current.items() if bid not in prev]
            price_drops, price_changes = [], []
            for bid, p in current.items():
                if bid in prev and p["price"] != prev[bid].get("price") and p["price"] != "N/A" and prev[bid].get("price") not in (None, "N/A"):
                    (price_drops if _aed_val(p["price"]) < _aed_val(prev[bid]["price"]) else price_changes).append((prev[bid], p))

            if new_products:
                lines = [f"<b>🆕 KINOKUNIYA EVENT — {len(new_products)} New on '{event_name}'!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
            if price_drops:
                lines = [f"<b>💰 KINOKUNIYA EVENT — Price Drop!</b>"]
                for old, new in price_drops:
                    lines.append(f"  🔻 {new['title']}: <s>{old['price']}</s> → <b>{new['price']}</b>\n  {new['url']}")
                await send_telegram("\n".join(lines), client)
            if price_changes:
                lines = [f"<b>🔺 KINOKUNIYA EVENT — Price Changed</b>"]
                for old, new in price_changes:
                    lines.append(f"  {new['title']}: {old['price']} → {new['price']}\n  {new['url']}")
                await send_telegram("\n".join(lines), client)
            if not (new_products or price_drops or price_changes):
                log.info("Kinokuniya event: no changes")

        state["kinokuniya_event"] = current

    except Exception as exc:
        log.error("Kinokuniya event check failed: %s", exc)

    return state


def _aed_val(price: str) -> float:
    """Parse 'AED 279.00' → 279.0; returns inf on failure so it never counts as a drop."""
    m = re.search(r"[\d.,]+", price or "")
    try:
        return float(m.group(0).replace(",", "")) if m else float("inf")
    except Exception:
        return float("inf")


# ─── LORCANA WATCHERS (all retailers, armed for a drop) ───────────────────────
# Each retailer runs its own search for 'lorcana', filtered to titles containing
# 'lorcana' (kills Disney-toy / sleeve noise), tracked in a separate state key.
# Dormant until lorcana_active() (7am BST 2026-07-19), then armed: baseline once,
# then alert on NEW products (a drop appearing) + restocks. A store showing 0 now
# simply stays silent until real Lorcana shows up.

LORCANA_ACCESSORY_WORDS = ("sleeve", "binder", "folio", "toploader", "top loader",
                           "deck box", "playmat", "play mat", "card case")

def _is_lorcana_product(title: str) -> bool:
    tl = strip_accents(title)
    if "lorcana" not in tl:
        return False
    if any(w in tl for w in LORCANA_ACCESSORY_WORDS):
        return False
    return True


async def _lorcana_diff_and_alert(state: dict, client: httpx.AsyncClient,
                                  state_key: str, headline: str, current: dict) -> None:
    """Shared baseline + new/restock/OOS alerting for every Lorcana watcher.
    Handles the empty-but-armed case (0 products) without spamming failures."""
    started_key = f"_{state_key}_started"
    prev = state.get(state_key, {})

    if not state.get(started_key):
        if current:
            lines = [f"<b>{headline} — Monitoring Started ({len(current)} product{'s' if len(current)!=1 else ''})</b>"]
            for p in current.values():
                lines.append(fmt_product(p))
            await send_telegram("\n".join(lines), client)
            log.info("%s: baseline sent (%d products)", state_key, len(current))
        else:
            log.info("%s: armed — 0 Lorcana products yet", state_key)
        state[started_key] = True
    else:
        new_products = [p for k, p in current.items() if k not in prev]
        restocked    = [p for k, p in current.items() if k in prev and p.get("available") and not prev[k].get("available")]
        went_oos     = [p for k, p in current.items() if k in prev and not p.get("available") and prev[k].get("available")]

        if new_products:
            lines = [f"<b>🆕 {headline} — {len(new_products)} New Lorcana Product(s)!</b>"]
            for p in new_products:
                lines.append(fmt_product(p))
            await send_telegram("\n".join(lines), client)
        if restocked:
            lines = [f"<b>🟢 {headline} — Back In Stock!</b>"]
            for p in restocked:
                lines.append(fmt_product(p, "✅"))
            await send_telegram("\n".join(lines), client)
        if went_oos:
            lines = [f"<b>🔴 {headline} — Out of Stock</b>"]
            for p in went_oos:
                lines.append(fmt_product(p, "❌"))
            await send_telegram("\n".join(lines), client)
        if not (new_products or restocked or went_oos):
            log.info("%s: no changes (%d products)", state_key, len(current))

    state[state_key] = current


async def check_kinokuniya_lorcana(state: dict, client: httpx.AsyncClient) -> dict:
    if not lorcana_active():
        return state
    log.info("Checking Kinokuniya (Lorcana)...")
    try:
        from curl_cffi.requests import AsyncSession
    except ImportError as exc:
        log.error("curl_cffi not available: %s", exc); raise
    current: dict[str, dict] = {}
    try:
        await asyncio.sleep(random.uniform(1, 3))
        async with AsyncSession(impersonate="safari17_0") as cf:
            try:
                await cf.get("https://uae.kinokuniya.com/", timeout=15)
            except Exception:
                pass
            resp = await cf.get(URLS["kinokuniya_lorcana"],
                                headers={"Referer": "https://uae.kinokuniya.com/", "Accept-Language": "en-US,en;q=0.9"},
                                timeout=25)
        if resp.status_code != 200:
            log.warning("Kinokuniya Lorcana: HTTP %s", resp.status_code); return state
        soup = BeautifulSoup(resp.text, "html.parser")
        for box in soup.select("div#image_or_detail div.box"):
            link = box.select_one("a[href*='/bw/']")
            if not link:
                continue
            m = re.search(r"/bw/(\d+)", link.get("href", ""))
            if not m:
                continue
            bc = m.group(1)
            te = box.select_one("span.title")
            title = te.get_text(strip=True) if te else ""
            if not _is_lorcana_product(title):
                continue
            pe = box.select_one(f"span#search_product_image_online_price_{bc}")
            price = pe.get_text(strip=True) if pe else "N/A"
            current[bc] = {"title": title, "url": f"https://uae.kinokuniya.com/bw/{bc}", "price": price, "available": True}
        await _lorcana_diff_and_alert(state, client, "kinokuniya_lorcana", "🃏 KINOKUNIYA (LORCANA)", current)
    except Exception as exc:
        log.error("Kinokuniya Lorcana check failed: %s", exc)
    return state


async def check_virgin_lorcana(state: dict, client: httpx.AsyncClient) -> dict:
    if not lorcana_active():
        return state
    log.info("Checking Virgin (Lorcana)...")
    try:
        from curl_cffi.requests import AsyncSession
    except ImportError as exc:
        log.error("curl_cffi not available: %s", exc); raise
    current: dict[str, dict] = {}
    try:
        await asyncio.sleep(random.uniform(1, 3))
        async with AsyncSession(impersonate="chrome") as cf:
            try:
                await cf.get("https://www.virginmegastore.ae/en/", timeout=15)
            except Exception:
                pass
            resp = await cf.get(URLS["virgin_lorcana_search"],
                                headers={"Referer": "https://www.virginmegastore.ae/en/"}, timeout=25)
        if resp.status_code != 200:
            log.warning("Virgin Lorcana: HTTP %s", resp.status_code); return state
        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.select(".product-item"):
            ne = item.select_one("a.product-list__name")
            if not ne:
                continue
            title = ne.get_text(strip=True)
            if not _is_lorcana_product(title):
                continue
            href = ne.get("href", "")
            url  = href if href.startswith("http") else f"https://www.virginmegastore.ae{href}"
            cur  = item.select_one(".price__currency")
            num  = item.select_one(".gtm-price-number")
            price = f"{cur.get_text(strip=True)} {num.get_text(strip=True)}" if cur and num else "N/A"
            oos  = item.select_one("[class*='out-of-stock'], [class*='sold-out'], [class*='unavailable']")
            current[product_key(title)] = {"title": title, "url": url, "price": price, "available": not oos}
        await _lorcana_diff_and_alert(state, client, "virgin_lorcana", "🃏 VIRGIN (LORCANA)", current)
    except Exception as exc:
        log.error("Virgin Lorcana check failed: %s", exc)
    return state


async def check_magrudy_lorcana(state: dict, client: httpx.AsyncClient) -> dict:
    if not lorcana_active():
        return state
    log.info("Checking Magrudy (Lorcana)...")
    current: dict[str, dict] = {}
    try:
        await asyncio.sleep(random.uniform(1, 3))
        resp = await client.post(
            "https://www.magrudy.com/api/search/do-search",
            json={"q": "lorcana", "stype": "item", "pagenum": 1, "pagesize": 80, "appliedFilters": {}, "sortOption": ""},
            headers={"User-Agent": random.choice(USER_AGENTS), "Content-Type": "application/json",
                     "Accept": "application/json", "Referer": "https://www.magrudy.com/search?q=lorcana",
                     "Origin": "https://www.magrudy.com"},
            timeout=25,
        )
        if resp.status_code != 200:
            log.warning("Magrudy Lorcana: HTTP %s", resp.status_code); return state
        for item in resp.json().get("data", []):
            title = (item.get("title", "") or "").strip()
            if not _is_lorcana_product(title):
                continue
            isbn = item.get("isbn", "")
            pv   = item.get("unitPriceInclVAT", 0)
            price = f"AED {pv:.0f}" if pv else "N/A"
            url   = f"https://www.magrudy.com/product/{isbn}" if isbn else "https://www.magrudy.com/search?q=lorcana"
            current[product_key(title)] = {"title": title, "url": url, "price": price, "available": True}
        await _lorcana_diff_and_alert(state, client, "magrudy_lorcana", "🃏 MAGRUDY (LORCANA)", current)
    except Exception as exc:
        log.error("Magrudy Lorcana check failed: %s", exc)
    return state


async def check_legends_lorcana(state: dict, client: httpx.AsyncClient) -> dict:
    if not lorcana_active():
        return state
    log.info("Checking Legends (Lorcana)...")
    current: dict[str, dict] = {}
    try:
        await asyncio.sleep(random.uniform(1, 3))
        offset = 0
        while True:
            api = (f"https://app.ecwid.com/api/v3/{ECWID_STORE_ID}/products"
                   f"?keyword=lorcana&enabled=true&limit=100&offset={offset}&lang=en")
            resp = await client.get(api, headers={"Authorization": f"Bearer {ECWID_TOKEN}",
                                                  "Accept": "application/json",
                                                  "User-Agent": random.choice(USER_AGENTS)}, timeout=25)
            if resp.status_code != 200:
                log.warning("Legends Lorcana: HTTP %s", resp.status_code)
                if offset == 0:
                    return state
                break
            data  = resp.json()
            items = data.get("items", [])
            total = data.get("total", 0)
            for it in items:
                name = it.get("name", "")
                if not _is_lorcana_product(name):
                    continue
                current[product_key(name)] = {
                    "title": name, "url": it.get("url", ""),
                    "price": it.get("defaultDisplayedPriceFormatted", "N/A"),
                    "available": bool(it.get("inStock", False)),
                }
            offset += len(items)
            if not items or offset >= total:
                break
        await _lorcana_diff_and_alert(state, client, "legends_lorcana", "🃏 LEGENDS (LORCANA)", current)
    except Exception as exc:
        log.error("Legends Lorcana check failed: %s", exc)
    return state


async def check_shopify_suggest_lorcana(state: dict, client: httpx.AsyncClient, *,
                                        domain: str, state_key: str, headline: str) -> dict:
    """Generic Shopify search-suggest Lorcana watcher (Little Things, Otakume)."""
    if not lorcana_active():
        return state
    log.info("Checking %s (Lorcana)...", state_key)
    current: dict[str, dict] = {}
    try:
        await asyncio.sleep(random.uniform(1, 3))
        url = f"https://{domain}/search/suggest.json?q=lorcana&resources[type]=product&resources[limit]=10"
        resp = await client.get(url, headers=get_json_headers(), timeout=20)
        if resp.status_code != 200:
            log.warning("%s Lorcana: HTTP %s", state_key, resp.status_code); return state
        prods = (json.loads(resp.content).get("resources", {}).get("results", {}).get("products", []))
        for p in prods:
            title  = p.get("title", "")
            handle = p.get("handle", "")
            if not handle or not _is_lorcana_product(title):
                continue
            current[handle] = {
                "title": title, "url": f"https://{domain}/products/{handle}",
                "price": f"AED {p.get('price', '0')}", "available": bool(p.get("available")),
            }
        await _lorcana_diff_and_alert(state, client, state_key, headline, current)
    except Exception as exc:
        log.error("%s Lorcana check failed: %s", state_key, exc)
    return state


async def check_little_things_lorcana(state: dict, client: httpx.AsyncClient) -> dict:
    return await check_shopify_suggest_lorcana(
        state, client, domain="littlethingsme.com",
        state_key="little_things_lorcana", headline="🃏 LITTLE THINGS (LORCANA)")


async def check_otakume_lorcana(state: dict, client: httpx.AsyncClient) -> dict:
    return await check_shopify_suggest_lorcana(
        state, client, domain="otakume.com",
        state_key="otakume_lorcana", headline="🃏 OTAKUME (LORCANA)")


async def check_colorland_lorcana(state: dict, client: httpx.AsyncClient) -> dict:
    if not lorcana_active():
        return state
    log.info("Checking Colorland (Lorcana)...")
    current: dict[str, dict] = {}
    try:
        await asyncio.sleep(random.uniform(1, 3))
        resp = await client.get(URLS["colorland_lorcana"], headers=get_headers("https://colorlandtoys.com/"), timeout=25)
        if resp.status_code != 200:
            log.warning("Colorland Lorcana: HTTP %s", resp.status_code); return state
        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.select("div.product-item[data-json-product]"):
            try:
                data = json.loads(item["data-json-product"])
            except Exception:
                continue
            handle = data.get("handle", "")
            te = item.select_one("a.card-title span.text") or item.select_one("a.card-title")
            title = te.get_text(strip=True) if te else handle.replace("-", " ").title()
            if not handle or not _is_lorcana_product(title):
                continue
            v = (data.get("variants") or [{}])[0]
            available = bool(v.get("available", True))
            pr = v.get("price", 0)
            price = f"AED {int(pr) // 100}" if pr else "N/A"
            current[handle] = {"title": title, "url": f"https://colorlandtoys.com/products/{handle}",
                               "price": price, "available": available}
        await _lorcana_diff_and_alert(state, client, "colorland_lorcana", "🃏 COLORLAND (LORCANA)", current)
    except Exception as exc:
        log.error("Colorland Lorcana check failed: %s", exc)
    return state


async def check_toycorner_lorcana(state: dict, client: httpx.AsyncClient) -> dict:
    if not lorcana_active():
        return state
    log.info("Checking Toy Corner (Lorcana)...")
    current: dict[str, dict] = {}
    try:
        await asyncio.sleep(random.uniform(1, 3))
        resp = await client.get(URLS["toycorner_lorcana"], headers=get_headers("https://toycorner.ae/"),
                                timeout=25, follow_redirects=True)
        if resp.status_code != 200:
            log.warning("Toy Corner Lorcana: HTTP %s", resp.status_code); return state
        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.find_all(attrs={"class": lambda c: c and "type-product" in c}):
            classes = " ".join(item.get("class", []))
            m = re.search(r"post-(\d+)", classes)
            if not m:
                continue
            pid = m.group(1)
            te = item.select_one(".product-title") or item.select_one("h2") or item.select_one("h3")
            title = te.get_text(strip=True) if te else ""
            if not title:
                img = item.find("img")
                title = (img.get("alt") if img else "") or ""
            if not _is_lorcana_product(title):
                continue
            available = ("instock" in classes) and ("outofstock" not in classes)
            a = item.find("a", href=re.compile(r"/product/"))
            url = a["href"] if a else f"https://toycorner.ae/?p={pid}"
            pe = item.select_one(".price")
            price = "N/A"
            if pe:
                tg = pe.find("ins") or pe
                ae = tg.select_one(".woocommerce-Price-amount bdi") or tg.select_one(".woocommerce-Price-amount")
                if ae:
                    price = f"AED {ae.get_text(strip=True)}"
            current[pid] = {"title": title, "url": url, "price": price, "available": available}
        await _lorcana_diff_and_alert(state, client, "toycorner_lorcana", "🃏 TOY CORNER (LORCANA)", current)
    except Exception as exc:
        log.error("Toy Corner Lorcana check failed: %s", exc)
    return state


# Dispatch table — (state_key, checker, status-board label). Driven in monitor_loop.
LORCANA_CHECKS = [
    ("kinokuniya_lorcana",    check_kinokuniya_lorcana,    "🃏 Kino (Lorcana)"),
    ("virgin_lorcana",        check_virgin_lorcana,        "🃏 Virgin (Lorcana)"),
    ("magrudy_lorcana",       check_magrudy_lorcana,       "🃏 Magrudy (Lorcana)"),
    ("legends_lorcana",       check_legends_lorcana,       "🃏 Legends (Lorcana)"),
    ("little_things_lorcana", check_little_things_lorcana, "🃏 LittleThings (Lorcana)"),
    ("otakume_lorcana",       check_otakume_lorcana,       "🃏 Otakume (Lorcana)"),
    ("colorland_lorcana",     check_colorland_lorcana,     "🃏 Colorland (Lorcana)"),
    ("toycorner_lorcana",     check_toycorner_lorcana,     "🃏 ToyCorner (Lorcana)"),
]


# ─── ELC TOYS ─────────────────────────────────────────────────────────────────

# ELC's catalogue is mostly plushes/figures, so search hits must look like TCG.
ELC_TCG_HINTS = ("tcg", "booster", "elite trainer", "etb", "blister", "deck",
                 "tin", "collection box", "premium collection", "trading card")


async def check_elctoys(state: dict, client: httpx.AsyncClient) -> dict:
    """ELC Toys — Shopify store, same server-rendered data-json-product markup
    as Colorland. Searches the plain + accented 'POKEMON TCG' terms and merges
    by handle. Captures variant IDs so alerts carry a tap-to-checkout cart
    permalink (https://elctoys.com/cart/{variant_id}:1)."""
    log.info("Checking ELC Toys...")
    current: dict[str, dict] = {}

    try:
        await asyncio.sleep(random.uniform(1, 3))
        bases = [URLS["elctoys"], URLS.get("elctoys_accented")]
        bases = [b for b in bases if b]
        fetch_error = False

        for base in bases:
            page_num = 1
            while page_num <= 8:  # safety cap
                url = base if page_num == 1 else f"{base}&page={page_num}"
                resp = await client.get(
                    url,
                    headers=get_headers("https://elctoys.com/"),
                    timeout=25,
                )
                if resp.status_code == 429:
                    log.warning("ELC Toys: rate limited on page %d — waiting 30s", page_num)
                    await asyncio.sleep(30)
                    resp = await client.get(url, headers=get_headers("https://elctoys.com/"), timeout=25)
                if resp.status_code != 200:
                    log.warning("ELC Toys: HTTP %s on page %d — skipping state update", resp.status_code, page_num)
                    fetch_error = True
                    break

                soup  = BeautifulSoup(resp.text, "html.parser")
                items = soup.select("div.product-item[data-json-product]")
                log.info("ELC Toys: page %d — %d items", page_num, len(items))
                if not items:
                    break

                for item in items:
                    try:
                        data = json.loads(item["data-json-product"])
                    except Exception:
                        continue

                    handle = data.get("handle", "")
                    if not handle:
                        continue

                    title_el = item.select_one("a.card-title span.text") or item.select_one("a.card-title")
                    title    = title_el.get_text(strip=True) if title_el else handle.replace("-", " ").title()
                    if not title or len(title) < 3:
                        continue

                    # Pokemon only, and TCG only — ELC is heavy on plushes/figures
                    if not is_pokemon_title(title):
                        continue
                    if not any(h in strip_accents(title) for h in ELC_TCG_HINTS):
                        continue

                    variants   = data.get("variants") or [{}]
                    v          = variants[0]
                    available  = any(var.get("available") for var in variants)
                    avail_var  = next((var for var in variants if var.get("available")), v)
                    variant_id = avail_var.get("id") or v.get("id")

                    price_raw = v.get("price", 0)
                    try:
                        price = f"AED {int(price_raw) / 100:.2f}" if price_raw else "N/A"
                    except Exception:
                        price = "N/A"

                    current[handle] = {
                        "title": title,
                        "url": f"https://elctoys.com/products/{handle}",
                        "price": price,
                        "available": available,
                        "variant_id": variant_id,
                    }

                if len(items) < 15:
                    break  # last page
                page_num += 1
                await asyncio.sleep(random.uniform(2, 4))

            if fetch_error:
                break
            await asyncio.sleep(random.uniform(1, 2))

        if fetch_error:
            log.warning("ELC Toys: fetch error — state not updated")
            return state
        if not current:
            log.warning("ELC Toys: no Pokemon TCG products parsed")
            return state

        log.info("ELC Toys: %d Pokemon TCG product(s)", len(current))

        mark_ok(state, "elctoys")

        prev      = state.get("elctoys", {})
        first_run = len(prev) == 0

        def _cart_line(p: dict) -> str:
            if p.get("available") and p.get("variant_id"):
                return f'\n  <a href="https://elctoys.com/cart/{p["variant_id"]}:1">🛒 Tap to checkout</a>'
            return ""

        if first_run:
            in_stock  = [v for v in current.values() if v["available"]]
            out_stock = [v for v in current.values() if not v["available"]]
            lines = [f"<b>🧸 ELC TOYS — Monitoring Started ({len(current)} products)</b>"]
            if in_stock:
                lines.append("\n✅ <b>In Stock:</b>")
                for p in in_stock:
                    lines.append(fmt_product(p) + _cart_line(p))
            if out_stock:
                lines.append("\n❌ <b>Out of Stock:</b>")
                for p in out_stock:
                    lines.append(fmt_product(p))
            await send_telegram("\n".join(lines), client)
            log.info("ELC Toys: baseline sent (%d products)", len(current))
        else:
            new_products, restocked, went_oos = [], [], []
            for pid, prod in current.items():
                if pid not in prev:
                    new_products.append(prod)
                elif prod["available"] != prev[pid].get("available"):
                    (restocked if prod["available"] else went_oos).append(prod)

            if new_products:
                lines = [f"<b>🆕 ELC TOYS — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p) + _cart_line(p))
                await send_telegram("\n".join(lines), client)
                log_events("elctoys", "new", new_products)
            if restocked:
                lines = ["<b>🟢 ELC TOYS — Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅") + _cart_line(p))
                await send_telegram("\n".join(lines), client)
                log_events("elctoys", "restock", restocked)
            if went_oos:
                lines = ["<b>🔴 ELC TOYS — Out of Stock</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
                log_events("elctoys", "oos", went_oos)
            if not (new_products or restocked or went_oos):
                log.info("ELC Toys: no changes")

        merged = state.get("elctoys", {})
        merged.update(current)
        state["elctoys"] = merged

    except Exception as exc:
        log.error("ELC Toys check failed: %s", exc)

    return state


# ─── MAGRUDY ──────────────────────────────────────────────────────────────────

async def check_magrudy(state: dict, client: httpx.AsyncClient) -> dict:
    """Magrudy — Next.js store with internal POST search API.
    No browser needed; uses /api/search/do-search directly."""
    log.info("Checking Magrudy...")
    current: dict[str, dict] = {}

    try:
        await asyncio.sleep(random.uniform(1, 3))

        resp = await client.post(
            "https://www.magrudy.com/api/search/do-search",
            json={
                "q":              "tcg",
                "stype":          "item",
                "pagenum":        1,
                "pagesize":       80,
                "appliedFilters": {},
                "sortOption":     "",
            },
            headers={
                "User-Agent":   random.choice(USER_AGENTS),
                "Content-Type": "application/json",
                "Accept":       "application/json",
                "Referer":      "https://www.magrudy.com/search?q=tcg",
                "Origin":       "https://www.magrudy.com",
            },
            timeout=25,
        )

        if resp.status_code != 200:
            log.warning("Magrudy: API returned HTTP %s", resp.status_code)
            return state

        data  = resp.json()
        items = data.get("data", [])
        log.info("Magrudy: %d products returned", len(items))

        for item in items:
            title = item.get("title", "").strip()
            if not title or len(title) < 3:
                continue
            if title_excluded(title):
                continue
            isbn      = item.get("isbn", "")
            price_val = item.get("unitPriceInclVAT", 0)
            price     = f"AED {price_val:.0f}" if price_val else "N/A"
            prod_url  = f"https://www.magrudy.com/product/{isbn}" if isbn else URLS["magrudy"]
            key = product_key(title)
            current[key] = {"title": title, "url": prod_url, "price": price, "available": True}

        if current:
            mark_ok(state, "magrudy")
        if not current:
            # Same as Legends: never wipe the baseline on a transient empty
            # response, or the next clean pass re-alerts the whole catalogue.
            log.warning("Magrudy: no products returned by API — keeping previous state")
            return state

        prev      = state.get("magrudy", {})
        first_run = len(prev) == 0

        if first_run:
            lines = [f"<b>📚 MAGRUDY — Monitoring Started ({len(current)} products)</b>"]
            lines.append("\n✅ <b>In Stock:</b>")
            for p in list(current.values())[:20]:
                lines.append(fmt_product(p))
            if len(current) > 20:
                lines.append(f"  ...and {len(current) - 20} more")
            await send_telegram("\n".join(lines), client)
            log.info("Magrudy: baseline sent (%d products)", len(current))
        else:
            new_products = [prod for pid, prod in current.items() if pid not in prev]
            went_oos     = [prev[pid] for pid in prev if pid not in current]

            if new_products:
                lines = [f"<b>🆕 MAGRUDY — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
            if went_oos:
                lines = ["<b>🔴 MAGRUDY — No Longer Listed</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
            if not (new_products or went_oos):
                log.info("Magrudy: no changes")

        state["magrudy"] = current

    except Exception as exc:
        log.error("Magrudy check failed: %s", exc)

    return state


# ─── ZGAMES ──────────────────────────────────────────────────────────────────

async def check_zgames(state: dict, client: httpx.AsyncClient, context: BrowserContext) -> dict:
    log.info("Checking ZGames...")
    current: dict[str, dict] = {}

    try:
        # Search both plain and accented spellings; merge by product key.
        zgames_urls = [URLS["zgames"], URLS.get("zgames_accented")]
        zgames_urls = [u for u in zgames_urls if u]

        for zu in zgames_urls:
            page = await context.new_page()
            try:
                await page.goto(
                    zu,
                    wait_until="domcontentloaded",
                    timeout=35_000,
                )
                await page.wait_for_timeout(3000)
                html = await page.content()
            finally:
                await page.close()

            soup = BeautifulSoup(html, "html.parser")

            for item in soup.select("li.product-item"):
                title_el = item.select_one("a.product-item-link")
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                if not title or len(title) < 3:
                    continue
                if title_excluded(title):
                    continue

                href = title_el.get("href", "")
                url = href if href.startswith("http") else f"https://zgames.ae{href}"

                price_el = item.select_one(".price")
                price = price_el.get_text(strip=True) if price_el else "N/A"

                stock_el = item.select_one(".stock")
                stock_text = stock_el.get_text(strip=True).lower() if stock_el else ""
                available = "out of stock" not in stock_text

                key = product_key(title)
                current[key] = {"title": title, "url": url, "price": price, "available": available}

        if not current:
            log.warning("ZGames: no products found — selectors may have changed")
            return state

        log.info("ZGames: %d products found", len(current))

        mark_ok(state, "zgames")

        prev      = state.get("zgames", {})
        first_run = len(prev) == 0

        if first_run:
            in_stock  = [v for v in current.values() if v["available"]]
            out_stock = [v for v in current.values() if not v["available"]]
            lines = [f"<b>🎮 ZGAMES — Monitoring Started ({len(current)} products)</b>"]
            if in_stock:
                lines.append("\n✅ <b>In Stock:</b>")
                for p in in_stock[:20]:
                    lines.append(fmt_product(p))
                if len(in_stock) > 20:
                    lines.append(f"  ...and {len(in_stock) - 20} more in stock")
            if out_stock:
                lines.append(f"\n❌ <b>Out of Stock:</b> {len(out_stock)} product(s)")
            await send_telegram("\n".join(lines), client)
            log.info("ZGames: baseline sent (%d products)", len(current))
        else:
            new_products, restocked, went_oos = [], [], []
            for pid, prod in current.items():
                if pid not in prev:
                    new_products.append(prod)
                elif prod["available"] and not prev[pid]["available"]:
                    restocked.append(prod)
                elif not prod["available"] and prev[pid]["available"]:
                    went_oos.append(prod)

            if new_products:
                lines = [f"<b>🆕 ZGAMES — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
            if restocked:
                lines = [f"<b>🔥 ZGAMES — {len(restocked)} Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)
            if went_oos:
                lines = [f"<b>🔴 ZGAMES — {len(went_oos)} Now Out of Stock</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
            if not (new_products or restocked or went_oos):
                log.info("ZGames: no changes")

        state["zgames"] = current

    except Exception as exc:
        log.error("ZGames check failed: %s", exc)

    return state


# ─── GEEKAY ──────────────────────────────────────────────────────────────────

GEEKAY_PROFILE = Path.home() / ".geekay_chrome_profile"

async def check_geekay(state: dict, client: httpx.AsyncClient) -> dict:
    """Geekay — Magento store, Cloudflare-protected. Uses real Chrome with persistent
    profile so Cloudflare clearance cookies survive between runs."""
    log.info("Checking Geekay...")
    current: dict[str, dict] = {}

    try:
        async with async_playwright() as pw:
            # Use persistent context so CF cookies are reused across runs
            ctx = await pw.chromium.launch_persistent_context(
                user_data_dir=str(GEEKAY_PROFILE),
                headless=False,
                executable_path=CHROME_PATH,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled",
                      "--window-size=1280,800", "--window-position=9999,9999"],
                viewport={"width": 1280, "height": 800},
                locale="en-GB",
            )
            page = await ctx.new_page()
            try:
                await page.goto(URLS["geekay"], wait_until="domcontentloaded", timeout=35_000)
                await page.wait_for_timeout(10000)
                html = await page.content()
                title = await page.title()
                log.info("Geekay page title: %s  length: %d", title, len(html))
            finally:
                await page.close()
                await ctx.close()

        soup = BeautifulSoup(html, "html.parser")

        for item in soup.select("li.product-item"):
            title_el = item.select_one("a.product-item-link") or item.select_one(".product-item-name a")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            if not title or len(title) < 3:
                continue
            if title_excluded(title):
                continue

            href = title_el.get("href", "")
            url = href if href.startswith("http") else f"https://www.geekay.com{href}"

            price_el = item.select_one(".price")
            price = price_el.get_text(strip=True) if price_el else "N/A"

            stock_el = item.select_one(".stock")
            stock_text = stock_el.get_text(strip=True).lower() if stock_el else ""
            available = "out of stock" not in stock_text

            key = product_key(title)
            current[key] = {"title": title, "url": url, "price": price, "available": available}

        if not current:
            log.warning("Geekay: no products found — selectors may have changed")
            return state

        log.info("Geekay: %d products found", len(current))

        prev      = state.get("geekay", {})
        first_run = len(prev) == 0

        if first_run:
            in_stock  = [v for v in current.values() if v["available"]]
            out_stock = [v for v in current.values() if not v["available"]]
            lines = [f"<b>🛒 GEEKAY — Monitoring Started ({len(current)} products)</b>"]
            if in_stock:
                lines.append("\n✅ <b>In Stock:</b>")
                for p in in_stock[:20]:
                    lines.append(fmt_product(p))
                if len(in_stock) > 20:
                    lines.append(f"  ...and {len(in_stock) - 20} more in stock")
            if out_stock:
                lines.append(f"\n❌ <b>Out of Stock:</b> {len(out_stock)} product(s)")
            await send_telegram("\n".join(lines), client)
            log.info("Geekay: baseline sent (%d products)", len(current))
        else:
            new_products, restocked, went_oos = [], [], []
            for pid, prod in current.items():
                if pid not in prev:
                    new_products.append(prod)
                elif prod["available"] and not prev[pid]["available"]:
                    restocked.append(prod)
                elif not prod["available"] and prev[pid]["available"]:
                    went_oos.append(prod)

            if new_products:
                lines = [f"<b>🆕 GEEKAY — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
            if restocked:
                lines = [f"<b>🔥 GEEKAY — {len(restocked)} Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)
            if went_oos:
                lines = [f"<b>🔴 GEEKAY — {len(went_oos)} Now Out of Stock</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
            if not (new_products or restocked or went_oos):
                log.info("Geekay: no changes")

        state["geekay"] = current

    except Exception as exc:
        log.error("Geekay check failed: %s", exc)

    return state


# ─── LITTLE THINGS ME (Shopify JSON) ─────────────────────────────────────────

async def check_little_things(state: dict, client: httpx.AsyncClient) -> dict:
    """Little Things ME — Shopify store. Pure HTTP JSON API, no browser needed.
    Only alerts on in-stock items — too many OOS products to list."""
    log.info("Checking Little Things...")
    current: dict[str, dict] = {}

    try:
        await asyncio.sleep(random.uniform(1, 3))
        page_num = 1
        while True:
            url = f"{URLS['little_things']}&page={page_num}" if page_num > 1 else URLS["little_things"]
            resp = await client.get(url, headers=get_json_headers(), timeout=25)
            if resp.status_code != 200:
                log.warning("Little Things: HTTP %s on page %d", resp.status_code, page_num)
                return state

            products = json.loads(resp.content).get("products", [])
            if not products:
                break

            for p in products:
                handle = p.get("handle", "")
                title = p.get("title", "")
                if not handle or not title or len(title) < 3:
                    continue
                # Skip non-Pokemon products (accent-insensitive: 'Pokémon' counts)
                if not is_pokemon_title(title):
                    continue
                variants = p.get("variants", [{}])
                v = variants[0] if variants else {}
                available = any(var.get("available") for var in variants)
                # Pick the first available variant for cart URLs (fall back to v[0])
                avail_variant = next((var for var in variants if var.get("available")), v)
                variant_id = avail_variant.get("id") or v.get("id")
                price_raw = v.get("price", "0")
                price = f"AED {price_raw}"
                prod_url = f"https://littlethingsme.com/products/{handle}"
                current[handle] = {
                    "title": title,
                    "url": prod_url,
                    "price": price,
                    "available": available,
                    "variant_id": variant_id,
                }

            if len(products) < 250:
                break
            page_num += 1
            await asyncio.sleep(random.uniform(1, 2))

        # Supplement the collection with Shopify search (plain + accented
        # 'Pokémon') to catch Pokemon products filed OUTSIDE the pokemon-tcg
        # collection. suggest.json lacks variant IDs, so search-only items get
        # variant_id=None (they won't auto-cart) — acceptable, since the
        # collection above covers the TCG catalogue with full data. Products
        # already found in the collection keep their richer collection data.
        collection_count = len(current)
        for q in ("pokemon", "pok%C3%A9mon"):
            try:
                surl = (
                    "https://littlethingsme.com/search/suggest.json"
                    f"?q={q}&resources[type]=product&resources[limit]=10"
                )
                sresp = await client.get(surl, headers=get_json_headers(), timeout=20)
                if sresp.status_code != 200:
                    log.warning("Little Things search q=%s: HTTP %s", q, sresp.status_code)
                    continue
                sprods = (json.loads(sresp.content)
                          .get("resources", {}).get("results", {}).get("products", []))
                for p in sprods:
                    handle = p.get("handle", "")
                    title  = p.get("title", "")
                    if not handle or not title or len(title) < 3:
                        continue
                    if handle in current:
                        continue  # already have full variant data from the collection
                    tl = strip_accents(title)
                    if not is_pokemon_title(title):
                        continue
                    # Search-sourced items must look like TCG — the search also
                    # surfaces Pokemon figures/Lego/Funko which we don't track.
                    TCG_HINTS = ("tcg", "booster", "elite trainer", "card", "deck",
                                 "blister", "etb", "tin", "ex box", "premium collection")
                    if not any(h in tl for h in TCG_HINTS):
                        continue
                    current[handle] = {
                        "title": title,
                        "url": f"https://littlethingsme.com/products/{handle}",
                        "price": f"AED {p.get('price', '0')}",
                        "available": bool(p.get("available")),
                        "variant_id": None,  # suggest.json omits variants
                    }
            except Exception as exc:
                log.warning("Little Things search q=%s failed: %s", q, exc)
            await asyncio.sleep(random.uniform(0.5, 1.5))

        if len(current) > collection_count:
            log.info("Little Things: search added %d product(s) outside the collection",
                     len(current) - collection_count)

        if not current:
            log.warning("Little Things: no Pokemon products found")
            return state

        log.info("Little Things: %d products found", len(current))
        mark_ok(state, "little_things")

        prev = state.get("little_things", {})
        first_run = len(prev) == 0

        if first_run:
            in_stock = [v for v in current.values() if v["available"]]
            lines = [f"<b>🛍️ LITTLE THINGS — Monitoring Started ({len(current)} products)</b>"]
            if in_stock:
                lines.append(f"\n✅ <b>In Stock ({len(in_stock)}):</b>")
                for p in in_stock[:20]:
                    lines.append(fmt_product(p))
            else:
                lines.append("\n❌ Nothing currently in stock")
            await send_telegram("\n".join(lines), client)
            log.info("Little Things: baseline sent (%d products)", len(current))

            # Auto-cart on baseline if watchlist items are already in stock
            auto_cfg = CFG.get("littlethings_auto_cart", {})
            if auto_cfg.get("enabled"):
                watchlist = [w.lower() for w in auto_cfg.get("watchlist", []) if w]
                cart_hits = [
                    p for p in current.values()
                    if p.get("available")
                    and p.get("variant_id")
                    and any(w in p["title"].lower() for w in watchlist)
                ]
                if cart_hits:
                    log.info("Little Things auto-cart (baseline): %d watchlist match(es)", len(cart_hits))
                    pairs = ",".join(f"{p['variant_id']}:1" for p in cart_hits)
                    cart_url = f"https://littlethingsme.com/cart/{pairs}"
                    names = "\n".join(f"  🛒 {p['title']} — {p['price']}" for p in cart_hits)
                    await send_telegram(
                        f"<b>🚨 LITTLE THINGS — WATCHLIST IN STOCK — TAP TO CHECKOUT!</b>\n\n"
                        f"{names}\n\n"
                        f'<a href="{cart_url}">👉 Open pre-filled cart</a>',
                        client,
                    )
        else:
            new_products, restocked, went_oos = [], [], []
            for pid, prod in current.items():
                if pid not in prev:
                    if prod["available"]:
                        new_products.append(prod)
                elif prod["available"] and not prev[pid]["available"]:
                    restocked.append(prod)
                elif not prod["available"] and prev[pid]["available"]:
                    went_oos.append(prod)

            if new_products:
                lines = [f"<b>🆕 LITTLE THINGS — {len(new_products)} New In-Stock Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
                log_events("little_things", "new", new_products)
            if restocked:
                lines = [f"<b>🔥 LITTLE THINGS — {len(restocked)} Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)

            # ── Auto-cart: build a Shopify cart permalink for watchlist hits ──
            auto_cfg = CFG.get("littlethings_auto_cart", {})
            if auto_cfg.get("enabled"):
                watchlist = [w.lower() for w in auto_cfg.get("watchlist", []) if w]
                cart_hits = []
                for p in (new_products + restocked):
                    if (
                        p.get("available")
                        and p.get("variant_id")
                        and any(w in p["title"].lower() for w in watchlist)
                    ):
                        cart_hits.append(p)
                if cart_hits:
                    log.info("Little Things auto-cart: %d watchlist match(es)", len(cart_hits))
                    pairs = ",".join(f"{p['variant_id']}:1" for p in cart_hits)
                    cart_url = f"https://littlethingsme.com/cart/{pairs}"
                    names = "\n".join(f"  🛒 {p['title']} — {p['price']}" for p in cart_hits)
                    await send_telegram(
                        f"<b>🚨 LITTLE THINGS — WATCHLIST HIT — TAP TO CHECKOUT!</b>\n\n"
                        f"{names}\n\n"
                        f'<a href="{cart_url}">👉 Open pre-filled cart</a>',
                        client,
                    )

            if went_oos:
                log.info("Little Things: %d went OOS (not alerting)", len(went_oos))
            if not (new_products or restocked):
                log.info("Little Things: no changes")

        state["little_things"] = current

    except Exception as exc:
        log.error("Little Things check failed: %s", exc)

    return state


# ─── LITTLE THINGS ME — ONE PIECE (Shopify JSON) ─────────────────────────────

async def check_little_things_onepiece(state: dict, client: httpx.AsyncClient) -> dict:
    """Little Things ME — One Piece TCG. Same Shopify JSON approach as Pokemon."""
    log.info("Checking Little Things (One Piece)...")
    current: dict[str, dict] = {}

    try:
        await asyncio.sleep(random.uniform(1, 3))
        page_num = 1
        while True:
            url = f"{URLS['little_things_onepiece']}&page={page_num}" if page_num > 1 else URLS["little_things_onepiece"]
            resp = await client.get(url, headers=get_json_headers(), timeout=25)
            if resp.status_code != 200:
                log.warning("Little Things OP: HTTP %s on page %d", resp.status_code, page_num)
                return state

            products = json.loads(resp.content).get("products", [])
            if not products:
                break

            for p in products:
                handle = p.get("handle", "")
                title = p.get("title", "")
                if not handle or not title or len(title) < 3:
                    continue
                variants = p.get("variants", [{}])
                v = variants[0] if variants else {}
                available = any(var.get("available") for var in variants)
                # Pick the first available variant for cart URLs (fall back to v[0])
                avail_variant = next((var for var in variants if var.get("available")), v)
                variant_id = avail_variant.get("id") or v.get("id")
                price_raw = v.get("price", "0")
                price = f"AED {price_raw}"
                prod_url = f"https://littlethingsme.com/products/{handle}"
                current[handle] = {
                    "title": title,
                    "url": prod_url,
                    "price": price,
                    "available": available,
                    "variant_id": variant_id,
                }

            if len(products) < 250:
                break
            page_num += 1
            await asyncio.sleep(random.uniform(1, 2))

        if not current:
            log.warning("Little Things OP: no One Piece products found")
            return state

        log.info("Little Things OP: %d products found", len(current))
        mark_ok(state, "little_things_onepiece")

        prev = state.get("little_things_onepiece", {})
        first_run = len(prev) == 0

        if first_run:
            in_stock = [v for v in current.values() if v["available"]]
            lines = [f"<b>🏴‍☠️ LITTLE THINGS (One Piece) — Monitoring Started ({len(current)} products)</b>"]
            if in_stock:
                lines.append(f"\n✅ <b>In Stock ({len(in_stock)}):</b>")
                for p in in_stock[:20]:
                    lines.append(fmt_product(p))
            else:
                lines.append("\n❌ Nothing currently in stock")
            await send_telegram("\n".join(lines), client)
            log.info("Little Things OP: baseline sent (%d products)", len(current))
        else:
            new_products, restocked, went_oos = [], [], []
            for pid, prod in current.items():
                if pid not in prev:
                    if prod["available"]:
                        new_products.append(prod)
                elif prod["available"] and not prev[pid]["available"]:
                    restocked.append(prod)
                elif not prod["available"] and prev[pid]["available"]:
                    went_oos.append(prod)

            if new_products:
                lines = [f"<b>🆕 LITTLE THINGS (One Piece) — {len(new_products)} New In-Stock Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
                log_events("little_things_onepiece", "new", new_products)
            if restocked:
                lines = [f"<b>🔥 LITTLE THINGS (One Piece) — {len(restocked)} Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)
            if went_oos:
                log.info("Little Things OP: %d went OOS (not alerting)", len(went_oos))
            if not (new_products or restocked):
                log.info("Little Things OP: no changes")

        state["little_things_onepiece"] = current

    except Exception as exc:
        log.error("Little Things OP check failed: %s", exc)

    return state


# ─── BROWSER FACTORY ──────────────────────────────────────────────────────────

STEALTH_SCRIPT = """
    Object.defineProperty(navigator, 'webdriver',  { get: () => undefined });
    Object.defineProperty(navigator, 'languages',  { get: () => ['en-GB', 'en'] });
    Object.defineProperty(navigator, 'plugins',    { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, 'platform',   { get: () => 'Win32' });
    window.chrome = { runtime: {}, loadTimes: () => {}, csi: () => {} };
"""

VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1440, "height": 900},
    {"width": 1366, "height": 768},
]


async def make_browser_context(browser):
    ctx = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        viewport=random.choice(VIEWPORTS),
        locale="en-GB",
        timezone_id="Europe/London",
        java_script_enabled=True,
    )
    await ctx.add_init_script(STEALTH_SCRIPT)
    return ctx


async def launch_headless_browser(pw):
    return await pw.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--disable-accelerated-2d-canvas",
            "--no-first-run",
            "--no-zygote",
            "--disable-gpu",
        ],
    )


# ─── MONITOR LOOP ─────────────────────────────────────────────────────────────

BROWSER_REFRESH_INTERVAL = 3_600  # Rotate browser context every hour


async def monitor_loop(client: httpx.AsyncClient, browser, headless_browser, pw) -> None:
    """The core monitoring loop — runs until cancelled."""
    state = load_state()

    # QUIET RESTARTS (was: wipe every site's state so all baselines re-send).
    # Wiping meant a redeploy near drop time buried the one alert that mattered
    # under a dozen baseline walls, AND folded anything that changed while we
    # were down silently into the new baseline. State now persists on the
    # volume, so a restart resumes diffing where it left off.
    # Set REBASELINE=true to force the old full-resend behaviour once.
    if os.environ.get("REBASELINE", "").lower() == "true":
        log.warning("REBASELINE=true — wiping all retailer state, full baselines will re-send")
        for k in ("otakume", "virgin_megastore", "virgin_megastore_onepiece",
                  "legends_own_the_game", "colorland_toys", "magrudy", "zgames",
                  "geekay", "little_things", "little_things_onepiece", "toycorner",
                  "kinokuniya", "kinokuniya_event", "elctoys"):
            state[k] = {}
        state["_colorland_startup_sent"] = False
        state["_toycorner_startup_sent"] = False
    else:
        kept = {k: len(v) for k, v in state.items()
                if isinstance(v, dict) and v and not k.startswith("_")}
        log.info("Quiet restart — resuming with existing state: %s", kept or "(empty, will baseline)")
        # These two already gate their own startup summary on a session flag;
        # leave the flags set so a restart doesn't re-send their summaries.
        state.setdefault("_colorland_startup_sent", bool(state.get("colorland_toys")))
        state.setdefault("_toycorner_startup_sent", bool(state.get("toycorner")))

    # ── Status board ──────────────────────────────────────────────────────────
    # One Telegram message that gets edited after each check
    CHECK_STATUS: dict[str, dict] = {
        "otakume":              {"label": "🟡 Otakume",              "ok": None, "time": ""},
        "virgin_megastore":     {"label": "🇦🇪 Virgin Megastore",    "ok": None, "time": ""},
        "virgin_megastore_onepiece": {"label": "🏴‍☠️ Virgin Megastore (OP)", "ok": None, "time": ""},
        "legends_own_the_game": {"label": "🎴 Legends Own The Game", "ok": None, "time": ""},
        "colorland_toys":       {"label": "🧩 Colorland Toys",       "ok": None, "time": ""},
        "magrudy":              {"label": "📚 Magrudy",               "ok": None, "time": ""},
        "zgames":               {"label": "🕹️ ZGames",               "ok": None, "time": ""},
        "geekay":               {"label": "🛒 Geekay",               "ok": None, "time": ""},
        "little_things":        {"label": "🛍️ Little Things",        "ok": None, "time": ""},
        "little_things_onepiece": {"label": "🏴‍☠️ Little Things (OP)", "ok": None, "time": ""},
        "toycorner":            {"label": "🧸 Toy Corner",            "ok": None, "time": ""},
        "kinokuniya":           {"label": "📚 Kinokuniya",            "ok": None, "time": ""},
        "kinokuniya_event":     {"label": "🎴 Kinokuniya Event",      "ok": None, "time": ""},
        "elctoys":              {"label": "🧸 ELC Toys",              "ok": None, "time": ""},
    }
    # Lorcana watchers — added to the board (shown ⏳ until 7am BST go-live).
    for _sk, _fn, _label in LORCANA_CHECKS:
        CHECK_STATUS[_sk] = {"label": _label, "ok": None, "time": ""}

    status_msg_id: int | None = state.get("status_msg_id")

    HEADLESS_SITES = {"otakume", "virgin_megastore", "virgin_megastore_onepiece", "legends_own_the_game", "colorland_toys", "magrudy", "zgames", "little_things", "little_things_onepiece", "toycorner", "kinokuniya", "kinokuniya_event", "elctoys"}
    HEADLESS_SITES |= {sk for sk, _fn, _lbl in LORCANA_CHECKS}
    HEADED_SITES = set()  # empty — Geekay uses its own Chrome instance, not the headed batch

    def _fmt_status() -> str:
        lines = ["<b>📊 UAE Monitor Status</b>"]
        lines.append("\n<b>⚡ Every 1 min (headless)</b>")
        for k, v in CHECK_STATUS.items():
            if k in HEADLESS_SITES:
                icon = "✅" if v["ok"] is True else ("❌" if v["ok"] is False else "⏳")
                t    = f" <i>({v['time']})</i>" if v["time"] else ""
                lines.append(f"{icon} {v['label']}{t}")
        # Geekay standalone
        gk = CHECK_STATUS.get("geekay", {})
        icon = "✅" if gk.get("ok") is True else ("❌" if gk.get("ok") is False else "⏳")
        t = f" <i>({gk['time']})</i>" if gk.get("time") else ""
        lines.append(f"\n<b>🖥️ Standalone (own Chrome)</b>")
        lines.append(f"{icon} {gk['label']}{t}")
        return "\n".join(lines)

    async def _push_status() -> None:
        nonlocal status_msg_id
        text = _fmt_status()
        if status_msg_id:
            await edit_telegram(status_msg_id, text, client)
        else:
            status_msg_id = await send_telegram(text, client)
            state["status_msg_id"] = status_msg_id
            save_state(state)

    def _mark(site: str, ok: bool) -> None:
        CHECK_STATUS[site]["ok"]   = ok
        CHECK_STATUS[site]["time"] = time.strftime("%H:%M")

    # ── Error tracking — alert on repeated failures ──────────────────────────
    FAIL_COUNTS: dict[str, int] = {}      # consecutive failures per site
    FAIL_ALERTED: dict[str, bool] = {}    # whether we already sent an alert

    async def _track_failure(site: str, error_msg: str) -> None:
        FAIL_COUNTS[site] = FAIL_COUNTS.get(site, 0) + 1
        if FAIL_COUNTS[site] >= 3 and not FAIL_ALERTED.get(site):
            await send_telegram(
                f"⚠️ <b>{site.upper().replace('_', ' ')} — FAILING</b>\n\n"
                f"Failed {FAIL_COUNTS[site]} checks in a row.\n"
                f"Error: <code>{error_msg[:200]}</code>",
                client,
            )
            FAIL_ALERTED[site] = True
            log.warning("%s: alert sent after %d consecutive failures", site, FAIL_COUNTS[site])

    def _track_success(site: str) -> None:
        if FAIL_COUNTS.get(site, 0) > 0:
            log.info("%s: recovered after %d failures", site, FAIL_COUNTS[site])
        FAIL_COUNTS[site] = 0
        FAIL_ALERTED[site] = False

    # ── Timers ────────────────────────────────────────────────────────────────
    last_otakume = 0.0
    last_virgin = last_virgin_op = last_legends = last_colorland = last_magrudy = last_zgames = last_geekay = last_little_things = last_little_things_op = last_toycorner = last_kinokuniya = last_kinokuniya_event = last_elctoys = 0.0
    last_lorcana: dict[str, float] = {}   # per-Lorcana-watcher timers
    lorcana_announced = False             # one-time "Lorcana now live" banner
    last_ctx_refresh = 0.0

    headless_context = await make_browser_context(headless_browser)

    # Post initial (all-pending) status board
    await _push_status()

    try:
        while True:
            now = time.monotonic()
            HEARTBEAT["last"] = now          # feed the watchdog

            # ── Rotate browser context hourly ──────────────────────────────
            # The relaunch path must be wrapped too: if the patchright driver
            # itself is dead (not just the browser), launch_headless_browser
            # or the second new_context call will raise the same error. Let
            # that propagate and the watchdog fires "Monitor crashed!".
            if now - last_ctx_refresh >= BROWSER_REFRESH_INTERVAL:
                try:
                    await headless_context.close()
                except Exception:
                    pass
                try:
                    headless_context = await make_browser_context(headless_browser)
                    last_ctx_refresh = now
                    log.info("Browser context rotated")
                except Exception as exc:
                    log.warning("new_context failed (%s); relaunching headless browser", exc)
                    try:
                        await headless_browser.close()
                    except Exception:
                        pass
                    try:
                        headless_browser = await launch_headless_browser(pw)
                        headless_context = await make_browser_context(headless_browser)
                        last_ctx_refresh = now
                        log.info("Headless browser relaunched and context created")
                    except Exception as exc2:
                        # Driver is likely dead too; skip rotation and retry next hour.
                        # Headless-context checks will fail until then, but the loop
                        # survives — no watchdog crash alert.
                        log.error("Browser relaunch failed: %s; will retry next rotation", exc2)
                        last_ctx_refresh = now

            # ════ PRIORITY: LITTLE THINGS — runs first, every 30s ══════════
            if "little_things" not in DISABLED_RETAILERS and now - last_little_things >= INTERVALS.get("little_things", 30):
                try:
                    state = await check_little_things(state, client)
                    _mark("little_things", bool(state.get("little_things")))
                    _track_success("little_things")
                except Exception as exc:
                    log.error("Little Things check failed: %s", exc)
                    _mark("little_things", False)
                    await _track_failure("little_things", str(exc))
                save_state(state)
                await _push_status()
                last_little_things = now

            # ════ PRIORITY: LITTLE THINGS ONE PIECE — runs first, every 30s ══
            if "little_things_onepiece" not in DISABLED_RETAILERS and now - last_little_things_op >= INTERVALS.get("little_things", 30):
                try:
                    state = await check_little_things_onepiece(state, client)
                    _mark("little_things_onepiece", bool(state.get("little_things_onepiece")))
                    _track_success("little_things_onepiece")
                except Exception as exc:
                    log.error("Little Things OP check failed: %s", exc)
                    _mark("little_things_onepiece", False)
                    await _track_failure("little_things_onepiece", str(exc))
                save_state(state)
                await _push_status()
                last_little_things_op = now

            # ════ BATCH 1: HEADLESS / HTTP — run concurrently ════════════
            # These don't need a visible browser, safe to run in parallel

            headless_tasks = []

            if "otakume" not in DISABLED_RETAILERS and now - last_otakume >= INTERVALS["otakume"]:
                headless_tasks.append(("otakume", check_otakume(state, client)))
                last_otakume = now

            if "virgin_megastore" not in DISABLED_RETAILERS and now - last_virgin >= INTERVALS["virgin_megastore"]:
                headless_tasks.append(("virgin_megastore", check_virgin_megastore(state, client)))
                last_virgin = now

            if "virgin_megastore_onepiece" not in DISABLED_RETAILERS and now - last_virgin_op >= INTERVALS.get("virgin_megastore_onepiece", 60):
                headless_tasks.append(("virgin_megastore_onepiece", check_virgin_megastore_onepiece(state, client)))
                last_virgin_op = now

            if "legends_own_the_game" not in DISABLED_RETAILERS and now - last_legends >= INTERVALS["legends_own_the_game"]:
                headless_tasks.append(("legends_own_the_game", check_legends_own_the_game(state, client)))
                last_legends = now

            if "colorland_toys" not in DISABLED_RETAILERS and now - last_colorland >= INTERVALS.get("colorland_toys", 180):
                headless_tasks.append(("colorland_toys", check_colorland_toys(state, client)))
                last_colorland = now

            if "magrudy" not in DISABLED_RETAILERS and now - last_magrudy >= INTERVALS.get("magrudy", 180):
                headless_tasks.append(("magrudy", check_magrudy(state, client)))
                last_magrudy = now

            if "zgames" not in DISABLED_RETAILERS and now - last_zgames >= INTERVALS.get("zgames", 180):
                headless_tasks.append(("zgames", check_zgames(state, client, headless_context)))
                last_zgames = now

            if "toycorner" not in DISABLED_RETAILERS and now - last_toycorner >= INTERVALS.get("toycorner", 180):
                headless_tasks.append(("toycorner", check_toycorner(state, client)))
                last_toycorner = now

            if "kinokuniya" not in DISABLED_RETAILERS and now - last_kinokuniya >= INTERVALS.get("kinokuniya", 120):
                headless_tasks.append(("kinokuniya", check_kinokuniya(state, client)))
                last_kinokuniya = now

            if "kinokuniya_event" not in DISABLED_RETAILERS and now - last_kinokuniya_event >= INTERVALS.get("kinokuniya_event", 120):
                headless_tasks.append(("kinokuniya_event", check_kinokuniya_event(state, client)))
                last_kinokuniya_event = now

            if "elctoys" not in DISABLED_RETAILERS and now - last_elctoys >= INTERVALS.get("elctoys", 120):
                headless_tasks.append(("elctoys", check_elctoys(state, client)))
                last_elctoys = now

            # ── Lorcana watchers — dormant until 7am BST go-live, then every 2 min ──
            if lorcana_active():
                if not lorcana_announced and not state.get("_lorcana_announced"):
                    await send_telegram(
                        "<b>🃏 LORCANA WATCH IS NOW LIVE</b>\n\n"
                        "Now watching all UAE retailers for Disney Lorcana. "
                        "You'll get an alert the moment products appear or restock.",
                        client,
                    )
                    lorcana_announced = True
                    state["_lorcana_announced"] = True
                    save_state(state)
                for _sk, _fn, _label in LORCANA_CHECKS:
                    if _sk not in DISABLED_RETAILERS and now - last_lorcana.get(_sk, 0.0) >= INTERVALS.get("lorcana", 120):
                        headless_tasks.append((_sk, _fn(state, client)))
                        last_lorcana[_sk] = now

            if headless_tasks:
                log.info("Running %d headless checks concurrently...", len(headless_tasks))
                results = await asyncio.gather(*[t[1] for t in headless_tasks], return_exceptions=True)
                for (site_name, _), result in zip(headless_tasks, results):
                    if isinstance(result, Exception):
                        log.error("%s concurrent check failed: %s", site_name, result)
                        _mark(site_name, False)
                        await _track_failure(site_name, str(result))
                    else:
                        state = result
                        # Lorcana watchers are healthy once they've run once, even
                        # with 0 products (armed-but-empty is a valid state).
                        if site_name.endswith("_lorcana"):
                            ok = bool(state.get(f"_{site_name}_started"))
                        else:
                            # Honest health: did this site actually complete a
                            # check recently? A blind site (dead filter, 403,
                            # empty parse) stops refreshing its _last_ok stamp
                            # and correctly flips to ❌ instead of showing a
                            # stale ✅ forever.
                            ok = not check_is_stale(
                                state, site_name,
                                INTERVALS.get(site_name, 120),
                            )
                        _mark(site_name, ok)
                        if ok:
                            _track_success(site_name)
                        else:
                            await _track_failure(site_name, "check completed but returned no usable data (site may be blind)")
                save_state(state)
                await _push_status()

            # Geekay — uses its own real Chrome instance (bypasses Cloudflare)
            # Geekay disabled temporarily (returning 0 products after first check)
            # if now - last_geekay >= INTERVALS.get("geekay", 120):
            #     try:
            #         state = await check_geekay(state, client)
            #         _mark("geekay", bool(state.get("geekay")))
            #         _track_success("geekay")
            #     except Exception as exc:
            #         log.error("Geekay check failed: %s", exc)
            #         _mark("geekay", False)
            #         await _track_failure("geekay", str(exc))
            #     save_state(state)
            #     last_geekay = now
            #     await _push_status()

            await asyncio.sleep(10)

    except asyncio.CancelledError:
        log.info("Monitor loop cancelled")
    finally:
        try:
            await headless_context.close()
        except Exception:
            pass


# ─── TELEGRAM COMMAND LISTENER ────────────────────────────────────────────────

async def telegram_listener(client: httpx.AsyncClient, browser, headless_browser, pw) -> None:
    """
    Listens for Telegram messages from the authorised chat.
    Responds to:
      start — begin monitoring
      stop  — pause monitoring
      status — report current state
    """
    # holder["task"] is the live monitor task. The watchdog mutates this when it
    # self-heals, so both sides always see the current task.
    holder: dict = {"task": None}
    watchdog_task: asyncio.Task | None = None

    def _spawn_monitor() -> asyncio.Task:
        return asyncio.create_task(monitor_loop(client, browser, headless_browser, pw))

    # Skip any messages that arrived before the script started
    updates = await poll_telegram(0, client)
    offset  = (updates[-1]["update_id"] + 1) if updates else 0
    log.info("Telegram listener ready (skipped %d old message(s))", len(updates))

    # AUTO-START (default on). Previously a Railway redeploy / OOM-kill / host
    # migration left the monitor dead until a human noticed one Telegram message
    # and replied "start" — the single biggest blind window in the system.
    # Set AUTO_START=false to go back to manual start.
    auto_start = os.environ.get("AUTO_START", "true").lower() != "false"

    if auto_start:
        HEARTBEAT["last"] = 0.0
        holder["task"] = _spawn_monitor()
        watchdog_task  = asyncio.create_task(run_watchdog(holder, client, _spawn_monitor))
        log.info("Auto-started monitoring on boot")
        await send_telegram(
            "🤖 <b>UAE Pokemon TCG Monitor is online.</b>\n\n"
            "▶️ Monitoring <b>auto-started</b>.\n"
            "Send <code>stop</code> to pause, <code>status</code> for a snapshot.",
            client,
        )
    else:
        await send_telegram(
            "🤖 <b>UAE Pokemon TCG Monitor is online.</b>\n\nSend <code>start</code> to begin monitoring.",
            client,
        )

    while True:
        updates = await poll_telegram(offset, client)

        for upd in updates:
            offset = upd["update_id"] + 1

            msg     = upd.get("message", {})
            chat_id = str(msg.get("chat", {}).get("id", ""))
            text    = msg.get("text", "").strip().lower()

            # Only accept commands from the authorised user
            if chat_id != str(TELEGRAM_CHAT_ID):
                continue

            log.info("Telegram command received: %r", text)

            if text == "start":
                if holder["task"] and not holder["task"].done():
                    await send_telegram("⚠️ Monitor is already running.", client)
                else:
                    HEARTBEAT["last"] = 0.0   # reset before new run
                    holder["task"] = _spawn_monitor()
                    watchdog_task  = asyncio.create_task(run_watchdog(holder, client, _spawn_monitor))
                    await send_telegram(
                        "✅ <b>UAE Retailer Monitor started!</b>\n\n"
                        f"🟡 Otakume: every {INTERVALS['otakume'] // 60} min\n"
                        f"🇦🇪 Virgin Megastore: every {INTERVALS['virgin_megastore'] // 60} min\n"
                        f"🎴 Legends Own The Game: every {INTERVALS['legends_own_the_game'] // 60} min\n"
                        f"🧩 Colorland Toys: every {INTERVALS['colorland_toys'] // 60} min\n"
                        f"📚 Magrudy: every {INTERVALS['magrudy'] // 60} min\n"
                        f"🕹️ ZGames: every {INTERVALS['zgames'] // 60} min\n"
                        f"🛒 Geekay: every {INTERVALS.get('geekay', 180) // 60} min\n"
                        f"🛍️ Little Things: every {INTERVALS.get('little_things', 60) // 60} min\n"
                        f"🧸 Toy Corner: every {INTERVALS.get('toycorner', 180) // 60} min\n"
                        f"📚 Kinokuniya: every {INTERVALS.get('kinokuniya', 120) // 60} min\n"
                        f"🎴 Kinokuniya Event: every {INTERVALS.get('kinokuniya_event', 120) // 60} min\n"
                        + (f"🃏 Lorcana ({len(LORCANA_CHECKS)} stores): LIVE, every {INTERVALS.get('lorcana', 120) // 60} min\n\n"
                           if lorcana_active() else
                           f"🃏 Lorcana ({len(LORCANA_CHECKS)} stores): armed, activates {LORCANA_GO_LIVE:%H:%M UTC %d %b}\n\n")
                        + "Send <code>stop</code> to pause.",
                        client,
                    )
                    log.info("Monitor started via Telegram")

            elif text == "stop":
                if not holder["task"] or holder["task"].done():
                    await send_telegram("⚠️ Monitor is not running.", client)
                else:
                    holder["task"].cancel()
                    if watchdog_task and not watchdog_task.done():
                        watchdog_task.cancel()
                    await asyncio.sleep(1)
                    await send_telegram("🛑 <b>Monitor stopped.</b>\n\nSend <code>start</code> to resume.", client)
                    log.info("Monitor stopped via Telegram")

            elif text == "status":
                running = holder["task"] and not holder["task"].done()
                icon    = "🟢 Running" if running else "🔴 Stopped"
                await send_telegram(f"<b>Status:</b> {icon}", client)

            else:
                await send_telegram(
                    "Commands:\n"
                    "  <code>start</code>  — begin monitoring\n"
                    "  <code>stop</code>   — pause monitoring\n"
                    "  <code>status</code> — check if running",
                    client,
                )


# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def main() -> None:
    log.info("=" * 60)
    log.info("UAE Pokemon TCG Monitor — Starting")
    log.info("Otakume=%ds  VirginMegastore=%ds  LegendsOwnTheGame=%ds  ColorlandToys=%ds  Magrudy=%ds  ZGames=%ds  Geekay=%ds  LittleThings=%ds",
             INTERVALS["otakume"], INTERVALS["virgin_megastore"], INTERVALS["legends_own_the_game"], INTERVALS["colorland_toys"], INTERVALS["magrudy"], INTERVALS.get("zgames", 180), INTERVALS.get("geekay", 180), INTERVALS.get("little_things", 60))
    log.info("=" * 60)

    async with httpx.AsyncClient(
        timeout=35,
        follow_redirects=True,
    ) as client:
        async with async_playwright() as pw:
            headless_browser = await launch_headless_browser(pw)

            try:
                await telegram_listener(client, None, headless_browser, pw)
            except (KeyboardInterrupt, asyncio.CancelledError):
                log.info("Shutting down")
            finally:
                try:
                    await headless_browser.close()
                except Exception:
                    pass
                try:
                    import httpx as _httpx
                    _httpx.post(
                        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                        json={"chat_id": TELEGRAM_CHAT_ID, "text": "⚠️ UAE Monitor has stopped (PC shutdown or restart). Will resume when PC comes back online."},
                        timeout=10,
                    )
                except Exception:
                    pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass  # clean exit, no traceback
