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
            "otakume": 60, "virgin_megastore": 60, "legends_own_the_game": 60,
            "colorland_toys": 180, "magrudy": 60, "zgames": 60,
            "geekay": 120, "little_things": 30,
        },
        "urls": {
            "otakume": "https://otakume.com/collections/trading-cards?filter.v.price.gte=&filter.v.price.lte=&filter.p.m.custom.manufacturer=Pokemon+Company",
            "virgin_megastore": "https://www.virginmegastore.ae/en/selection/general-merchandise/pokemon-merchandise/pokemon-tcg/c/n9992162",
            "legends_own_the_game": "https://legendsownthegame.com/products/search?keyword=pokemon&categories=176121252",
            "colorland_toys": "https://colorlandtoys.com/search?q=pokemon+tcg&options%5Bprefix%5D=last&type=product",
            "magrudy": "https://www.magrudy.com/search?q=tcg",
            "zgames": "https://zgames.ae/catalogsearch/result/?q=pokemon+tcg",
            "geekay": "https://www.geekay.com/en/brand/pokemon?goodstuff_genre=571&product_list_order=new&stock=1",
            "little_things": "https://littlethingsme.com/collections/pokemon-tcg/products.json?limit=250",
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
        "legends_own_the_game": {},
        "colorland_toys":       {},
        "magrudy":              {},
        "zgames":               {},
        "geekay":               {},
        "little_things":        {},
    }


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ─── TELEGRAM ─────────────────────────────────────────────────────────────────

MAX_TG_LENGTH = 4000  # Telegram limit is 4096, keep headroom

# Mutable heartbeat — monitor_loop writes, watchdog reads
HEARTBEAT: dict = {"last": 0.0}


async def send_telegram(message: str, client: httpx.AsyncClient) -> int | None:
    """Send a Telegram message, splitting if needed. Returns message_id of first chunk."""
    if not TELEGRAM_ENABLED:
        log.info("[TELEGRAM DISABLED] %s", message[:120])
        return None

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


async def run_watchdog(monitor_task: "asyncio.Task[None]", client: httpx.AsyncClient) -> None:
    """
    Runs alongside monitor_loop.
    • If the heartbeat goes stale > 10 min  → "not responding" alert
    • If the task crashes with an exception  → "crashed" alert
    """
    STALE_THRESHOLD = 600   # 10 minutes
    CHECK_EVERY     = 120   # check every 2 minutes
    alert_sent      = False

    # Grace period — let the loop start up before we start watching
    await asyncio.sleep(60)

    while not monitor_task.done():
        await asyncio.sleep(CHECK_EVERY)
        age = time.monotonic() - HEARTBEAT["last"]
        if HEARTBEAT["last"] > 0 and age > STALE_THRESHOLD:
            if not alert_sent:
                await send_telegram(
                    "🚨 <b>Monitor is not responding!</b>\n\n"
                    "The loop has been silent for over 10 minutes.\n"
                    "Send <code>stop</code> then <code>start</code> to restart.",
                    client,
                )
                alert_sent = True
        else:
            if alert_sent:
                await send_telegram("✅ <b>Monitor has recovered.</b>", client)
            alert_sent = False

    # Task finished — check if it died with an uncaught exception
    if not monitor_task.cancelled():
        try:
            exc = monitor_task.exception()
            if exc:
                await send_telegram(
                    f"🚨 <b>Monitor crashed!</b>\n\n"
                    f"<code>{type(exc).__name__}: {exc}</code>\n\n"
                    "Send <code>start</code> to restart.",
                    client,
                )
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            pass


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
        resp = await client.get(
            URLS["otakume"],
            headers={
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            },
            timeout=25,
        )

        if resp.status_code != 200:
            log.warning("Otakume returned HTTP %s", resp.status_code)
            return state

        page_text = resp.text
        log.info("Otakume: response %d chars, encoding=%s", len(page_text), resp.encoding)
        soup = BeautifulSoup(page_text, "html.parser")

        # Find product links (Otakume product URLs)
        product_links = soup.select("a[href*='/products/']")
        log.info("Otakume: found %d product links", len(product_links))

        for item in product_links:
            # Title from image alt attribute
            img = item.select_one("img")
            title = img.get("alt", "") if img else ""
            if not title or len(title) < 3:
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

        log.info("Otakume: products found: %s",
                 ", ".join(p["title"] for p in current.values()))

        prev = state.get("otakume", {})
        first_run = len(prev) == 0

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

            if restocked:
                lines = ["<b>🟢 OTAKUME — Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)

            if went_oos:
                lines = ["<b>🔴 OTAKUME — Out of Stock</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)

            if not (new_products or restocked or went_oos):
                log.info("Otakume: no changes")

        state["otakume"] = current

    except Exception as exc:
        log.error("Otakume check failed: %s", exc)

    return state


# ─── VIRGIN MEGASTORE ─────────────────────────────────────────────────────────

async def check_virgin_megastore(state: dict, client: httpx.AsyncClient, context: BrowserContext) -> dict:
    log.info("Checking Virgin Megastore...")
    page = None

    try:
        await asyncio.sleep(random.uniform(1, 3))
        page = await context.new_page()

        await page.goto(URLS["virgin_megastore"], wait_until="domcontentloaded", timeout=35_000)
        await asyncio.sleep(random.uniform(2, 4))

        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        current: dict[str, dict] = {}

        for item in soup.select(".product-item"):
            name_el = item.select_one("a.product-list__name")
            if not name_el:
                continue
            title = name_el.get_text(strip=True)
            if not title or len(title) < 3:
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

            key = product_key(title)
            current[key] = {"title": title, "url": url, "price": price, "available": available}

        if not current:
            log.warning("Virgin Megastore: no products found — selectors may need updating")
            return state

        prev      = state.get("virgin_megastore", {})
        first_run = len(prev) == 0

        if first_run:
            in_stock  = [v for v in current.values() if v["available"]]
            out_stock = [v for v in current.values() if not v["available"]]
            lines = [f"<b>🇦🇪 VIRGIN MEGASTORE — Monitoring Started ({len(current)} products)</b>"]
            if in_stock:
                lines.append("\n✅ <b>In Stock:</b>")
                for p in in_stock:
                    lines.append(fmt_product(p))
            if out_stock:
                lines.append("\n❌ <b>Out of Stock:</b>")
                for p in out_stock:
                    lines.append(fmt_product(p))
            await send_telegram("\n".join(lines), client)
            log.info("Virgin Megastore: baseline sent (%d products)", len(current))
        else:
            new_products, restocked, went_oos = [], [], []
            for pid, prod in current.items():
                if pid not in prev:
                    new_products.append(prod)
                elif prod["available"] != prev[pid]["available"]:
                    (restocked if prod["available"] else went_oos).append(prod)

            if new_products:
                lines = [f"<b>🆕 VIRGIN MEGASTORE — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
            if restocked:
                lines = ["<b>🟢 VIRGIN MEGASTORE — Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)
            if went_oos:
                lines = ["<b>🔴 VIRGIN MEGASTORE — Out of Stock</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
            if not (new_products or restocked or went_oos):
                log.info("Virgin Megastore: no changes")

        state["virgin_megastore"] = current

    except Exception as exc:
        log.error("Virgin Megastore check failed: %s", exc)
    finally:
        if page:
            await page.close()

    return state


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

        # Paginate through all results (Ecwid API max 100 per page)
        offset = 0
        limit  = 100
        while True:
            api_url = (
                f"https://app.ecwid.com/api/v3/{ECWID_STORE_ID}/products"
                f"?keyword=pokemon&category=176121252&enabled=true&limit={limit}&offset={offset}&lang=en"
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
                log.warning("Legends Own The Game: API returned HTTP %s", resp.status_code)
                return state

            data  = resp.json()
            items = data.get("items", [])
            total = data.get("total", 0)
            log.info("Legends Own The Game: fetched %d/%d products (offset=%d)",
                     len(items), total, offset)

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

        if not current:
            log.warning("Legends Own The Game: no Pokemon products returned by API")
            state["legends_own_the_game"] = current
            return state

        log.info("Legends Own The Game: %d products found", len(current))

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
        page_num    = 1
        fetch_error = False
        while True:
            url = URLS["colorland_toys"] if page_num == 1 else f"{URLS['colorland_toys']}&page={page_num}"
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

                prod_url = f"https://colorlandtoys.com/products/{handle}"
                current[handle] = {"title": title, "url": prod_url, "price": price, "available": available}

            if len(items) < 50:
                break  # last page
            page_num += 1
            await asyncio.sleep(random.uniform(3, 6))

        if fetch_error:
            log.warning("Colorland Toys: pagination error — state not updated")
            return state

        if not current:
            log.warning("Colorland Toys: no products found — selectors may have changed")
            return state

        log.info("Colorland Toys: %d products found across %d page(s)", len(current), page_num)

        prev      = state.get("colorland_toys", {})
        first_run = len(prev) == 0

        COLORLAND_BLOCKLIST = {
            "pokemon-my-partner-pikachu-pkw0030",
            "vtech-paw-patrol-learning-watch-chase-80-551603",
            "vtech-paw-patrol-learning-watch-marshall-80-551663",
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
            if restocked:
                lines = ["<b>🟢 COLORLAND TOYS — Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)
            if went_oos:
                lines = ["<b>🔴 COLORLAND TOYS — Out of Stock</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
            if not (new_products or restocked or went_oos):
                log.info("Colorland Toys: no changes")

        # Merge current into prev so flickering products don't re-alert
        merged = state.get("colorland_toys", {})
        merged.update(current)
        state["colorland_toys"] = merged

    except Exception as exc:
        log.error("Colorland Toys check failed: %s", exc)

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
            isbn      = item.get("isbn", "")
            price_val = item.get("unitPriceInclVAT", 0)
            price     = f"AED {price_val:.0f}" if price_val else "N/A"
            prod_url  = f"https://www.magrudy.com/product/{isbn}" if isbn else URLS["magrudy"]
            key = product_key(title)
            current[key] = {"title": title, "url": prod_url, "price": price, "available": True}

        if not current:
            log.warning("Magrudy: no products returned by API")
            state["magrudy"] = current
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
        page = await context.new_page()
        try:
            await page.goto(
                URLS["zgames"],
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
                # Skip non-Pokemon products
                if "pokemon" not in title.lower() and "pikachu" not in title.lower():
                    continue
                variants = p.get("variants", [{}])
                v = variants[0] if variants else {}
                available = any(var.get("available") for var in variants)
                price_raw = v.get("price", "0")
                price = f"AED {price_raw}"
                prod_url = f"https://littlethingsme.com/products/{handle}"
                current[handle] = {"title": title, "url": prod_url, "price": price, "available": available}

            if len(products) < 250:
                break
            page_num += 1
            await asyncio.sleep(random.uniform(1, 2))

        if not current:
            log.warning("Little Things: no Pokemon products found")
            return state

        log.info("Little Things: %d products found", len(current))

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
            if restocked:
                lines = [f"<b>🔥 LITTLE THINGS — {len(restocked)} Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)
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
                price_raw = v.get("price", "0")
                price = f"AED {price_raw}"
                prod_url = f"https://littlethingsme.com/products/{handle}"
                current[handle] = {"title": title, "url": prod_url, "price": price, "available": available}

            if len(products) < 250:
                break
            page_num += 1
            await asyncio.sleep(random.uniform(1, 2))

        if not current:
            log.warning("Little Things OP: no One Piece products found")
            return state

        log.info("Little Things OP: %d products found", len(current))

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


# ─── MONITOR LOOP ─────────────────────────────────────────────────────────────

BROWSER_REFRESH_INTERVAL = 3_600  # Rotate browser context every hour


async def monitor_loop(client: httpx.AsyncClient, browser, headless_browser) -> None:
    """The core monitoring loop — runs until cancelled."""
    state = load_state()

    # Always re-send every site's full product list on every start
    state["otakume"]              = {}
    state["virgin_megastore"]     = {}
    state["legends_own_the_game"] = {}
    # Don't wipe colorland — pagination causes products to flicker in/out
    # state["colorland_toys"]     = {}
    state["_colorland_startup_sent"] = False
    state["magrudy"]              = {}
    state["zgames"]               = {}
    state["geekay"]               = {}
    state["little_things"]        = {}
    state["little_things_onepiece"] = {}

    # ── Status board ──────────────────────────────────────────────────────────
    # One Telegram message that gets edited after each check
    CHECK_STATUS: dict[str, dict] = {
        "otakume":              {"label": "🟡 Otakume",              "ok": None, "time": ""},
        "virgin_megastore":     {"label": "🇦🇪 Virgin Megastore",    "ok": None, "time": ""},
        "legends_own_the_game": {"label": "🎴 Legends Own The Game", "ok": None, "time": ""},
        "colorland_toys":       {"label": "🧩 Colorland Toys",       "ok": None, "time": ""},
        "magrudy":              {"label": "📚 Magrudy",               "ok": None, "time": ""},
        "zgames":               {"label": "🕹️ ZGames",               "ok": None, "time": ""},
        "geekay":               {"label": "🛒 Geekay",               "ok": None, "time": ""},
        "little_things":        {"label": "🛍️ Little Things",        "ok": None, "time": ""},
        "little_things_onepiece": {"label": "🏴‍☠️ Little Things (OP)", "ok": None, "time": ""},
    }
    status_msg_id: int | None = state.get("status_msg_id")

    HEADLESS_SITES = {"otakume", "virgin_megastore", "legends_own_the_game", "colorland_toys", "magrudy", "zgames", "little_things", "little_things_onepiece"}
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
    last_virgin = last_legends = last_colorland = last_magrudy = last_zgames = last_geekay = last_little_things = last_little_things_op = 0.0
    last_ctx_refresh = 0.0

    headless_context = await make_browser_context(headless_browser)

    # Post initial (all-pending) status board
    await _push_status()

    try:
        while True:
            now = time.monotonic()
            HEARTBEAT["last"] = now          # feed the watchdog

            # ── Rotate browser context hourly ──────────────────────────────
            if now - last_ctx_refresh >= BROWSER_REFRESH_INTERVAL:
                try:
                    await headless_context.close()
                except Exception:
                    pass
                headless_context = await make_browser_context(headless_browser)
                last_ctx_refresh = now
                log.info("Browser context rotated")

            # ════ PRIORITY: LITTLE THINGS — runs first, every 30s ══════════
            if now - last_little_things >= INTERVALS.get("little_things", 30):
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
            if now - last_little_things_op >= INTERVALS.get("little_things", 30):
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

            if now - last_otakume >= INTERVALS["otakume"]:
                headless_tasks.append(("otakume", check_otakume(state, client)))
                last_otakume = now

            if now - last_virgin >= INTERVALS["virgin_megastore"]:
                headless_tasks.append(("virgin_megastore", check_virgin_megastore(state, client, headless_context)))
                last_virgin = now

            if now - last_legends >= INTERVALS["legends_own_the_game"]:
                headless_tasks.append(("legends_own_the_game", check_legends_own_the_game(state, client)))
                last_legends = now

            if now - last_colorland >= INTERVALS.get("colorland_toys", 180):
                headless_tasks.append(("colorland_toys", check_colorland_toys(state, client)))
                last_colorland = now

            if now - last_magrudy >= INTERVALS.get("magrudy", 180):
                headless_tasks.append(("magrudy", check_magrudy(state, client)))
                last_magrudy = now

            if now - last_zgames >= INTERVALS.get("zgames", 180):
                headless_tasks.append(("zgames", check_zgames(state, client, headless_context)))
                last_zgames = now

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
                        _mark(site_name, bool(state.get(site_name)))
                        _track_success(site_name)
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

async def telegram_listener(client: httpx.AsyncClient, browser, headless_browser) -> None:
    """
    Listens for Telegram messages from the authorised chat.
    Responds to:
      start — begin monitoring
      stop  — pause monitoring
      status — report current state
    """
    monitor_task:  asyncio.Task | None = None
    watchdog_task: asyncio.Task | None = None

    # Skip any messages that arrived before the script started
    updates = await poll_telegram(0, client)
    offset  = (updates[-1]["update_id"] + 1) if updates else 0
    log.info("Telegram listener ready (skipped %d old message(s))", len(updates))

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
                if monitor_task and not monitor_task.done():
                    await send_telegram("⚠️ Monitor is already running.", client)
                else:
                    HEARTBEAT["last"] = 0.0   # reset before new run
                    monitor_task  = asyncio.create_task(monitor_loop(client, browser, headless_browser))
                    watchdog_task = asyncio.create_task(run_watchdog(monitor_task, client))
                    await send_telegram(
                        "✅ <b>UAE Retailer Monitor started!</b>\n\n"
                        f"🟡 Otakume: every {INTERVALS['otakume'] // 60} min\n"
                        f"🇦🇪 Virgin Megastore: every {INTERVALS['virgin_megastore'] // 60} min\n"
                        f"🎴 Legends Own The Game: every {INTERVALS['legends_own_the_game'] // 60} min\n"
                        f"🧩 Colorland Toys: every {INTERVALS['colorland_toys'] // 60} min\n"
                        f"📚 Magrudy: every {INTERVALS['magrudy'] // 60} min\n"
                        f"🕹️ ZGames: every {INTERVALS['zgames'] // 60} min\n"
                        f"🛒 Geekay: every {INTERVALS.get('geekay', 180) // 60} min\n"
                        f"🛍️ Little Things: every {INTERVALS.get('little_things', 60) // 60} min\n\n"
                        "Send <code>stop</code> to pause.",
                        client,
                    )
                    log.info("Monitor started via Telegram")

            elif text == "stop":
                if not monitor_task or monitor_task.done():
                    await send_telegram("⚠️ Monitor is not running.", client)
                else:
                    monitor_task.cancel()
                    if watchdog_task and not watchdog_task.done():
                        watchdog_task.cancel()
                    await asyncio.sleep(1)
                    await send_telegram("🛑 <b>Monitor stopped.</b>\n\nSend <code>start</code> to resume.", client)
                    log.info("Monitor stopped via Telegram")

            elif text == "status":
                running = monitor_task and not monitor_task.done()
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
            headless_browser = await pw.chromium.launch(
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

            try:
                await telegram_listener(client, None, headless_browser)
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
