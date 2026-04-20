#!/usr/bin/env python3
"""
UK Pokemon TCG Retailer Monitor
────────────────────────────────
Monitors Pokemon TCG availability across UK retailers
and sends Telegram alerts for new stock and price changes.

Sites monitored:
  - Menkind, Game, Currys, John Lewis, Freemans, ASDA George
"""

import asyncio
import json
import logging
import platform
import random
import re
import subprocess
import time
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
from patchright.async_api import BrowserContext, async_playwright

# Real Chrome path — needed for sites that block Patchright's bundled Chromium (e.g. Geekay/Cloudflare, Smyths/Incapsula)
CHROME_PATH = (
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    if platform.system() == "Darwin"
    else "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
)

# ─── CONFIG ───────────────────────────────────────────────────────────────────

CONFIG_FILE = Path("config_uk.json")
STATE_FILE  = Path("state_uk.json")


def load_config() -> dict:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError("config_uk.json not found. Run setup first.")
    return json.loads(CONFIG_FILE.read_text())


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
        logging.FileHandler("monitor_uk.log"),
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
        "menkind":    {},
        "game":       {},
        "currys":     {},
        "john_lewis": {},
        "freemans":   {},
        "asda":       {},
        "very":       {},
        "selfridges": {},
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


# ─── MENKIND ──────────────────────────────────────────────────────────────────

async def check_menkind(state: dict, client: httpx.AsyncClient, context: BrowserContext) -> dict:
    log.info("Checking Menkind...")
    page = None

    try:
        await asyncio.sleep(random.uniform(1, 3))
        page = await context.new_page()

        await page.goto(URLS["menkind"], wait_until="domcontentloaded", timeout=35_000)
        # Menkind uses Algolia JS to render results — wait for first card
        try:
            await page.wait_for_selector("article.product-card", timeout=15_000)
        except Exception:
            log.warning("Menkind: timed out waiting for product cards")
        await asyncio.sleep(random.uniform(1, 2))

        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        current: dict[str, dict] = {}

        for item in soup.select("article.product-card"):
            title_el = item.select_one("h1.product-card__title, .product-card__title-container")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            if not title or len(title) < 3:
                continue

            link_el = item.select_one("a.product-card__link")
            href = link_el["href"] if link_el else ""
            url = href if href.startswith("http") else f"https://www.menkind.co.uk{href}"
            # Strip tracking query params for stable URL
            url = url.split("?")[0]

            price_el = item.select_one(".product-card__price")
            price = price_el.get_text(strip=True) if price_el else "N/A"

            oos_el = item.select_one("[class*='sold-out'], [class*='out-of-stock'], [class*='unavailable']")
            available = not oos_el

            key = product_key(title)
            current[key] = {"title": title, "url": url, "price": price, "available": available}

        if not current:
            log.warning("Menkind: no products found — selectors may need updating")
            return state

        prev      = state.get("menkind", {})
        first_run = len(prev) == 0

        if first_run:
            in_stock  = [v for v in current.values() if v["available"]]
            out_stock = [v for v in current.values() if not v["available"]]
            lines = [f"<b>🎁 MENKIND — Monitoring Started ({len(current)} products)</b>"]
            if in_stock:
                lines.append("\n✅ <b>In Stock:</b>")
                for p in in_stock:
                    lines.append(fmt_product(p))
            if out_stock:
                lines.append("\n❌ <b>Out of Stock:</b>")
                for p in out_stock:
                    lines.append(fmt_product(p))
            await send_telegram("\n".join(lines), client)
            log.info("Menkind: baseline sent (%d products)", len(current))
        else:
            new_products, restocked, went_oos = [], [], []
            for pid, prod in current.items():
                if pid not in prev:
                    new_products.append(prod)
                elif prod["available"] != prev[pid]["available"]:
                    (restocked if prod["available"] else went_oos).append(prod)

            if new_products:
                lines = [f"<b>🆕 MENKIND — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
            if restocked:
                lines = ["<b>🟢 MENKIND — Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)
            if went_oos:
                lines = ["<b>🔴 MENKIND — Out of Stock</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
            if not (new_products or restocked or went_oos):
                log.info("Menkind: no changes")

        state["menkind"] = current

    except Exception as exc:
        log.error("Menkind check failed: %s", exc)
    finally:
        if page:
            await page.close()

    return state


# ─── GAME.CO.UK ───────────────────────────────────────────────────────────────

async def check_game(state: dict, client: httpx.AsyncClient, context: BrowserContext) -> dict:
    log.info("Checking Game.co.uk...")
    page = None

    try:
        await asyncio.sleep(random.uniform(1, 3))
        page = await context.new_page()

        await page.goto(URLS["game"], wait_until="domcontentloaded", timeout=35_000)
        # Give JS a moment to inject ld+json into the DOM
        await asyncio.sleep(random.uniform(2, 4))

        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        current: dict[str, dict] = {}

        # Game.co.uk embeds all product data in a schema.org ItemList ld+json block
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(script.string or "")
                if data.get("type") == "ItemList" or data.get("@type") == "ItemList":
                    for item in data.get("itemListElement", []):
                        product = item.get("item", item)
                        title = product.get("name", "")
                        if not title or len(title) < 3:
                            continue
                        url = product.get("url", "").split("#")[0]  # strip colour-code fragment
                        offers = product.get("offers", {})
                        price_val = offers.get("price", "")
                        price = f"£{price_val}" if price_val else "N/A"
                        avail = offers.get("availability", "")
                        available = "InStock" in avail or avail == ""
                        key = product_key(title)
                        current[key] = {
                            "title":     title,
                            "url":       url,
                            "price":     price,
                            "available": available,
                        }
                    break
            except Exception:
                continue

        if not current:
            log.warning("Game.co.uk: no products found in ld+json — page may have changed")
            return state

        log.info("Game.co.uk: products found: %s",
                 ", ".join(p["title"] for p in current.values()))

        prev      = state.get("game", {})
        first_run = len(prev) == 0

        if first_run:
            in_stock  = [v for v in current.values() if v["available"]]
            out_stock = [v for v in current.values() if not v["available"]]
            lines = [f"<b>🎮 GAME — Monitoring Started ({len(current)} products)</b>"]
            if in_stock:
                lines.append("\n✅ <b>In Stock:</b>")
                for p in in_stock:
                    lines.append(fmt_product(p))
            if out_stock:
                lines.append("\n❌ <b>Out of Stock:</b>")
                for p in out_stock:
                    lines.append(fmt_product(p))
            await send_telegram("\n".join(lines), client)
            log.info("Game.co.uk: baseline sent (%d products)", len(current))
        else:
            new_products, restocked, went_oos = [], [], []
            for pid, prod in current.items():
                if pid not in prev:
                    new_products.append(prod)
                elif prod["available"] != prev[pid]["available"]:
                    (restocked if prod["available"] else went_oos).append(prod)

            if new_products:
                lines = [f"<b>🆕 GAME — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
            if restocked:
                lines = ["<b>🟢 GAME — Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)
            if went_oos:
                lines = ["<b>🔴 GAME — Out of Stock</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
            if not (new_products or restocked or went_oos):
                log.info("Game.co.uk: no changes")

        state["game"] = current

    except Exception as exc:
        log.error("Game.co.uk check failed: %s", exc)
    finally:
        if page:
            await page.close()

    return state


# ─── CURRYS ───────────────────────────────────────────────────────────────────

async def check_currys(state: dict, client: httpx.AsyncClient, context: BrowserContext) -> dict:
    log.info("Checking Currys...")
    page = None

    try:
        await asyncio.sleep(random.uniform(1, 3))
        page = await context.new_page()

        await page.goto(URLS["currys"], wait_until="domcontentloaded", timeout=35_000)
        # Wait for Cloudflare if present
        for _cf_wait in range(10):
            await asyncio.sleep(3)
            _title = await page.title()
            if "moment" not in _title.lower() and "attention" not in _title.lower():
                break
        try:
            await page.wait_for_selector(".product-item-element", timeout=15_000)
        except Exception:
            log.warning("Currys: timed out waiting for product list")
        await asyncio.sleep(random.uniform(1, 2))

        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        current: dict[str, dict] = {}

        for item in soup.select(".product-item-element"):
            # Title (use any viewport variant)
            title_el = item.select_one(".list-product-tile-name")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            if not title or len(title) < 3:
                continue

            # Link
            link_el = item.select_one('a[href*="/products/"]')
            href = link_el.get("href", "") if link_el else ""
            url = href if href.startswith("http") else f"https://www.currys.co.uk{href}"

            # Price
            price_el = item.select_one(".price-info")
            price = price_el.get_text(strip=True) if price_el else "N/A"

            # Availability — outof-stockprice class on .price-info
            price_classes = " ".join(price_el.get("class", [])) if price_el else ""
            oos_by_class = "outof-stockprice" in price_classes
            # Also check for "Out of stock" button text
            buttons_text = " ".join(
                btn.get_text(strip=True).lower()
                for btn in item.select("button")
            )
            oos_by_btn = "out of stock" in buttons_text
            available = not (oos_by_class or oos_by_btn)

            key = product_key(title)
            current[key] = {
                "title":     title,
                "url":       url,
                "price":     price,
                "available": available,
            }

        if not current:
            log.warning("Currys: no products found — selectors may need updating")
            return state

        log.info("Currys: products found: %s",
                 ", ".join(p["title"][:40] for p in current.values()))

        prev      = state.get("currys", {})
        first_run = len(prev) == 0

        if first_run:
            in_stock  = [v for v in current.values() if v["available"]]
            out_stock = [v for v in current.values() if not v["available"]]
            lines = [f"<b>🟠 CURRYS — Monitoring Started ({len(current)} products)</b>"]
            if in_stock:
                lines.append("\n✅ <b>In Stock:</b>")
                for p in in_stock:
                    lines.append(fmt_product(p))
            if out_stock:
                lines.append("\n❌ <b>Out of Stock:</b>")
                for p in out_stock:
                    lines.append(fmt_product(p))
            await send_telegram("\n".join(lines), client)
            log.info("Currys: baseline sent (%d products)", len(current))
        else:
            new_products, restocked, went_oos = [], [], []
            for pid, prod in current.items():
                if pid not in prev:
                    new_products.append(prod)
                elif prod["available"] != prev[pid]["available"]:
                    (restocked if prod["available"] else went_oos).append(prod)

            if new_products:
                lines = [f"<b>🆕 CURRYS — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
            if restocked:
                lines = ["<b>🟢 CURRYS — Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)
            if went_oos:
                lines = ["<b>🔴 CURRYS — Out of Stock</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
            if not (new_products or restocked or went_oos):
                log.info("Currys: no changes")

        state["currys"] = current

    except Exception as exc:
        log.error("Currys check failed: %s", exc)
    finally:
        if page:
            await page.close()

    return state


# ─── JOHN LEWIS ───────────────────────────────────────────────────────────────

async def check_john_lewis(state: dict, client: httpx.AsyncClient, context: BrowserContext) -> dict:
    log.info("Checking John Lewis...")
    page = None

    try:
        await asyncio.sleep(random.uniform(1, 3))
        page = await context.new_page()

        await page.goto(URLS["john_lewis"], wait_until="domcontentloaded", timeout=35_000)
        # John Lewis is a Next.js app — wait for JS to render product cards
        try:
            await page.wait_for_selector('article[data-product-id]', timeout=15_000)
        except Exception:
            log.warning("John Lewis: timed out waiting for product cards")
        await asyncio.sleep(random.uniform(1, 2))

        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        current: dict[str, dict] = {}

        for card in soup.select("article[data-product-id]"):
            # Title — brand + description spans inside [data-testid="product-title"]
            title_el = card.select_one('[data-testid="product-title"]')
            if not title_el:
                continue
            brand_el = title_el.select_one('[class*="Brand"]')
            desc_el = title_el.select_one('[class*="desc"]')
            brand = brand_el.get_text(strip=True) if brand_el else ""
            desc = desc_el.get_text(strip=True) if desc_el else ""
            title = f"{brand} {desc}".strip()
            if not title or len(title) < 3:
                continue

            # Link
            link_el = card.select_one("a[href]")
            href = link_el.get("href", "") if link_el else ""
            url = href if href.startswith("http") else f"https://www.johnlewis.com{href}"

            # Price — look for price text anywhere in the card
            card_text = card.get_text(separator=" ", strip=True)
            price_match = re.search(r"£[\d,.]+", card_text)
            price = price_match.group(0) if price_match else "N/A"

            # Availability
            lower_text = card_text.lower()
            available = "out of stock" not in lower_text and "temporarily unavailable" not in lower_text

            key = product_key(title)
            current[key] = {
                "title":     title,
                "url":       url,
                "price":     price,
                "available": available,
            }

        if not current:
            log.warning("John Lewis: no products found — selectors may need updating")
            return state

        prev      = state.get("john_lewis", {})
        first_run = len(prev) == 0

        if first_run:
            in_stock  = [v for v in current.values() if v["available"]]
            out_stock = [v for v in current.values() if not v["available"]]
            lines = [f"<b>🟤 JOHN LEWIS — Monitoring Started ({len(current)} products)</b>"]
            if in_stock:
                lines.append("\n✅ <b>In Stock:</b>")
                for p in in_stock:
                    lines.append(fmt_product(p))
            if out_stock:
                lines.append("\n❌ <b>Out of Stock:</b>")
                for p in out_stock:
                    lines.append(fmt_product(p))
            await send_telegram("\n".join(lines), client)
            log.info("John Lewis: baseline sent (%d products)", len(current))
        else:
            new_products, restocked, went_oos = [], [], []
            for pid, prod in current.items():
                if pid not in prev:
                    new_products.append(prod)
                elif prod["available"] != prev[pid]["available"]:
                    (restocked if prod["available"] else went_oos).append(prod)

            if new_products:
                lines = [f"<b>🆕 JOHN LEWIS — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
            if restocked:
                lines = ["<b>🟢 JOHN LEWIS — Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)
            if went_oos:
                lines = ["<b>🔴 JOHN LEWIS — Out of Stock</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
            if not (new_products or restocked or went_oos):
                log.info("John Lewis: no changes")

        state["john_lewis"] = current

    except Exception as exc:
        log.error("John Lewis check failed: %s", exc)
    finally:
        if page:
            await page.close()

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


# ─── FREEMANS (HTTP) ─────────────────────────────────────────────────────────

async def check_freemans(state: dict, client: httpx.AsyncClient) -> dict:
    """Freemans — plain HTTP, no bot protection. Scrapes Pokemon TCG search results."""
    log.info("Checking Freemans...")
    current: dict[str, dict] = {}

    try:
        await asyncio.sleep(random.uniform(1, 3))
        resp = await client.get(
            URLS["freemans"],
            headers=get_headers("https://www.freemans.com/"),
            timeout=20,
        )
        if resp.status_code != 200:
            log.warning("Freemans: HTTP %s", resp.status_code)
            return state

        soup = BeautifulSoup(resp.text, "html.parser")

        for item in soup.select("li.productContainer"):
            link_el = item.select_one("a[href*='/products/']")
            if not link_el:
                continue
            url = link_el.get("href", "")
            if not url.startswith("http"):
                url = f"https://www.freemans.com{url}"

            # Title is the last text block inside the link
            texts = [t.strip() for t in link_el.stripped_strings]
            # Pattern: [price, brand, product_name]
            title = texts[-1] if texts else ""
            if not title or len(title) < 3:
                continue

            # Price is the first text that starts with £
            price = "N/A"
            for t in texts:
                if t.startswith("£"):
                    price = t
                    break

            key = product_key(title)
            current[key] = {"title": title, "url": url, "price": price, "available": True}

        if not current:
            log.warning("Freemans: no products found")
            state["freemans"] = current
            return state

        log.info("Freemans: %d products found", len(current))

        prev = state.get("freemans", {})
        first_run = len(prev) == 0

        if first_run:
            lines = [f"<b>🛒 FREEMANS — Monitoring Started ({len(current)} products)</b>"]
            lines.append("\n✅ <b>In Stock:</b>")
            for p in list(current.values())[:20]:
                lines.append(fmt_product(p))
            if len(current) > 20:
                lines.append(f"  ...and {len(current) - 20} more")
            await send_telegram("\n".join(lines), client)
            log.info("Freemans: baseline sent (%d products)", len(current))
        else:
            new_products = [prod for pid, prod in current.items() if pid not in prev]
            went_oos = [prev[pid] for pid in prev if pid not in current]

            if new_products:
                lines = [f"<b>🆕 FREEMANS — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
            if went_oos:
                lines = ["<b>🔴 FREEMANS — No Longer Listed</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
            if not (new_products or went_oos):
                log.info("Freemans: no changes")

        state["freemans"] = current

    except Exception as exc:
        log.error("Freemans check failed: %s", exc)

    return state


# ─── ASDA GEORGE (Headless Patchright) ───────────────────────────────────────

async def check_asda(state: dict, client: httpx.AsyncClient, context: BrowserContext) -> dict:
    """ASDA George — Salesforce Commerce Cloud, headless Patchright works."""
    log.info("Checking ASDA George...")
    current: dict[str, dict] = {}

    try:
        page = await context.new_page()
        try:
            await page.goto(URLS["asda"], wait_until="domcontentloaded", timeout=30_000)
            await page.wait_for_timeout(8000)

            tiles = await page.query_selector_all(".product-mini")
            for tile in tiles:
                # Name
                name_el = await tile.query_selector("a.text-underline-hover img.primary-image")
                name = await name_el.get_attribute("alt") if name_el else None
                if not name_el:
                    name_el2 = await tile.query_selector("a.text-underline-hover")
                    name = (await name_el2.inner_text()).strip() if name_el2 else None
                if not name or len(name) < 3:
                    continue
                # Clean name — remove "Product name " prefix if present
                name = re.sub(r"^Product name\s*", "", name).strip()

                # URL
                link_el = await tile.query_selector("a[href*='/george/']")
                href = await link_el.get_attribute("href") if link_el else ""
                url = f"https://direct.asda.com{href}" if href and not href.startswith("http") else href

                # Price
                price_el = await tile.query_selector(".price")
                price_text = (await price_el.inner_text()).strip() if price_el else "N/A"
                # Clean "Price is now\n£29.98" → "£29.98"
                price_match = re.search(r"£[\d.]+", price_text)
                price = price_match.group(0) if price_match else price_text

                # Stock — has "Add to basket" button means in stock
                add_btn = await tile.query_selector("button[aria-label*='Add']")
                available = add_btn is not None

                key = product_key(name)
                current[key] = {"title": name, "url": url, "price": price, "available": available}
        finally:
            await page.close()

        if not current:
            log.warning("ASDA: no products found")
            state["asda"] = current
            return state

        log.info("ASDA: %d products found", len(current))

        prev = state.get("asda", {})
        first_run = len(prev) == 0

        if first_run:
            in_stock = [v for v in current.values() if v["available"]]
            out_stock = [v for v in current.values() if not v["available"]]
            lines = [f"<b>🛒 ASDA GEORGE — Monitoring Started ({len(current)} products)</b>"]
            if in_stock:
                lines.append("\n✅ <b>In Stock:</b>")
                for p in in_stock[:20]:
                    lines.append(fmt_product(p))
            if out_stock:
                lines.append(f"\n❌ <b>Out of Stock:</b> {len(out_stock)} product(s)")
            await send_telegram("\n".join(lines), client)
            log.info("ASDA: baseline sent (%d products)", len(current))
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
                lines = [f"<b>🆕 ASDA GEORGE — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
            if restocked:
                lines = [f"<b>🔥 ASDA GEORGE — {len(restocked)} Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)
            if went_oos:
                lines = [f"<b>🔴 ASDA GEORGE — {len(went_oos)} Now Out of Stock</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
            if not (new_products or restocked or went_oos):
                log.info("ASDA: no changes")

        state["asda"] = current

    except Exception as exc:
        log.error("ASDA check failed: %s", exc)

    return state


# ─── VERY ────────────────────────────────────────────────────────────────────


# ─── EDGE CDP (for sites that block Patchright — Akamai/Varnish) ─────────────

EDGE_PATH = "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe"
CDP_PORT = 9444
_cdp_edge_proc = None


def _ensure_cdp_edge() -> None:
    """Launch Microsoft Edge with remote-debugging if not already running."""
    global _cdp_edge_proc
    if _cdp_edge_proc and _cdp_edge_proc.poll() is None:
        return  # still running
    user_data = str(Path.home() / ".edge_monitor_profile")
    _cdp_edge_proc = subprocess.Popen(
        [
            EDGE_PATH,
            f"--remote-debugging-port={CDP_PORT}",
            f"--user-data-dir={user_data}",
            "--no-first-run",
            "--window-size=1,1",
            "--window-position=9999,9999",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(3)
    log.info("CDP Edge launched on port %d (PID %d)", CDP_PORT, _cdp_edge_proc.pid)


def _kill_cdp_edge() -> None:
    global _cdp_edge_proc
    if _cdp_edge_proc:
        try:
            _cdp_edge_proc.terminate()
            _cdp_edge_proc.wait(timeout=5)
        except Exception:
            pass
        _cdp_edge_proc = None


async def check_jd_williams(state: dict, client: httpx.AsyncClient, edge_ctx) -> dict:
    """JD Williams — uses shared Edge CDP context."""
    log.info("Checking JD Williams...")
    current: dict[str, dict] = {}

    try:
        page = await edge_ctx.new_page()
        await page.set_viewport_size({"width": 1280, "height": 800})
        try:
            await page.goto(URLS["jd_williams"], wait_until="domcontentloaded", timeout=40_000)
            await page.wait_for_timeout(10000)

            title = await page.title()
            if "Access Denied" in title:
                log.warning("JD Williams: Access Denied (Akamai block)")
                return state

            products = await page.evaluate("""() => {
                const cards = document.querySelectorAll('article.productCardArticle');
                return Array.from(cards).map(c => {
                    const link = c.querySelector('a[href*="/p/"]');
                    const titleEl = c.querySelector('[data-testid*="title"], h3, h2');
                    const ariaLabel = c.querySelector('[aria-label]');
                    const priceEl = c.querySelector('[data-testid*="price"], [class*="price"], [class*="Price"]');
                    const title = (titleEl ? titleEl.innerText : (ariaLabel ? ariaLabel.getAttribute('aria-label') : '')).trim();
                    const price = priceEl ? priceEl.innerText.trim() : 'N/A';
                    const href = link ? link.href : '';
                    return { title, price, href };
                });
            }""")

            for p in products:
                title = p.get("title", "")
                if not title or len(title) < 3:
                    continue
                href = p.get("href", "")
                url = href if href.startswith("http") else f"https://www.jdwilliams.co.uk{href}"
                price_raw = p.get("price", "N/A")
                price_match = re.search(r"£[\d,.]+", price_raw)
                price = price_match.group(0) if price_match else price_raw
                key = product_key(title)
                current[key] = {"title": title, "url": url, "price": price, "available": True}

        finally:
            await page.close()

        if not current:
            log.warning("JD Williams: no products found")
            state["jd_williams"] = current
            return state

        log.info("JD Williams: %d products found", len(current))
        prev = state.get("jd_williams", {})
        first_run = len(prev) == 0

        if first_run:
            lines = [f"<b>🛒 JD WILLIAMS — Monitoring Started ({len(current)} products)</b>\n✅ <b>In Stock:</b>"]
            for p in list(current.values())[:20]:
                lines.append(fmt_product(p))
            if len(current) > 20:
                lines.append(f"  ...and {len(current) - 20} more")
            await send_telegram("\n".join(lines), client)
        else:
            new_products = [p for pid, p in current.items() if pid not in prev]
            went_oos = [prev[pid] for pid in prev if pid not in current]
            if new_products:
                lines = [f"<b>🆕 JD WILLIAMS — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
            if went_oos:
                lines = ["<b>🔴 JD WILLIAMS — No Longer Listed</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
            if not (new_products or went_oos):
                log.info("JD Williams: no changes")

        state["jd_williams"] = current

    except Exception as exc:
        log.error("JD Williams check failed: %s", exc)

    return state


# ─── HAMLEYS (Edge CDP — Varnish blocks direct search URL) ───────────────────

async def check_hamleys(state: dict, client: httpx.AsyncClient, edge_ctx) -> dict:
    """Hamleys — uses shared Edge CDP. Must load homepage first, then submit search form."""
    log.info("Checking Hamleys...")
    current: dict[str, dict] = {}

    try:
        page = await edge_ctx.new_page()
        await page.set_viewport_size({"width": 1280, "height": 800})
        try:
            # Must load homepage first — direct search URL returns 403
            await page.goto("https://www.hamleys.com/", wait_until="domcontentloaded", timeout=40_000)
            await page.wait_for_timeout(4000)

            # Submit search via JS — use expect_navigation to wait for page load
            async with page.expect_navigation(wait_until="domcontentloaded", timeout=30_000):
                await page.evaluate("""() => {
                    const input = document.querySelector('input[name="q"], input[type="search"]');
                    if (!input) throw new Error('no search input');
                    input.value = 'pokemon tcg';
                    input.dispatchEvent(new Event('input', {bubbles: true}));
                    input.closest('form').submit();
                }""")
            await page.wait_for_timeout(5000)

            title = await page.title()
            if "403" in title or "Forbidden" in title:
                log.warning("Hamleys: 403 Forbidden")
                return state

            products = await page.evaluate("""() => {
                const items = document.querySelectorAll('.product-item');
                return Array.from(items).map(item => {
                    const linkEl = item.querySelector('a[href]');
                    const nameEl = item.querySelector('.product-item-name');
                    const priceEl = item.querySelector('.price-container, .ds-sdk-product-price');
                    return {
                        title: nameEl ? nameEl.innerText.trim() : '',
                        price: priceEl ? priceEl.innerText.trim() : 'N/A',
                        href: linkEl ? linkEl.href : ''
                    };
                });
            }""")

            for p in products:
                title = p.get("title", "")
                if not title or len(title) < 3:
                    continue
                href = p.get("href", "")
                url = href if href.startswith("http") else f"https://www.hamleys.com{href}"
                price_raw = p.get("price", "N/A")
                price_match = re.search(r"£[\d,.]+", price_raw)
                price = price_match.group(0) if price_match else price_raw
                key = product_key(title)
                current[key] = {"title": title, "url": url, "price": price, "available": True}

        finally:
            await page.close()

        if not current:
            log.warning("Hamleys: no products found")
            state["hamleys"] = current
            return state

        log.info("Hamleys: %d products found", len(current))
        prev = state.get("hamleys", {})
        first_run = len(prev) == 0

        if first_run:
            lines = [f"<b>🧸 HAMLEYS — Monitoring Started ({len(current)} products)</b>\n✅ <b>In Stock:</b>"]
            for p in list(current.values())[:20]:
                lines.append(fmt_product(p))
            if len(current) > 20:
                lines.append(f"  ...and {len(current) - 20} more")
            await send_telegram("\n".join(lines), client)
        else:
            new_products = [p for pid, p in current.items() if pid not in prev]
            went_oos = [prev[pid] for pid in prev if pid not in current]
            if new_products:
                lines = [f"<b>🆕 HAMLEYS — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
            if went_oos:
                lines = ["<b>🔴 HAMLEYS — No Longer Listed</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
            if not (new_products or went_oos):
                log.info("Hamleys: no changes")

        state["hamleys"] = current

    except Exception as exc:
        log.error("Hamleys check failed: %s", exc)

    return state


# ─── ASDA GEORGE (Edge CDP) ──────────────────────────────────────────────────

async def check_asda_cdp(state: dict, client: httpx.AsyncClient, edge_ctx) -> dict:
    """ASDA George — uses shared Edge CDP context."""
    log.info("Checking ASDA George...")
    current: dict[str, dict] = {}

    try:
        page = await edge_ctx.new_page()
        await page.set_viewport_size({"width": 1280, "height": 800})
        try:
            await page.goto(URLS["asda"], wait_until="domcontentloaded", timeout=40_000)
            await page.wait_for_timeout(10000)

            tiles = await page.query_selector_all(".product-mini")
            for tile in tiles:
                name_el = await tile.query_selector("a.text-underline-hover img.primary-image")
                name = await name_el.get_attribute("alt") if name_el else None
                if not name_el:
                    name_el2 = await tile.query_selector("a.text-underline-hover")
                    name = (await name_el2.inner_text()).strip() if name_el2 else None
                if not name or len(name) < 3:
                    continue
                name = re.sub(r"^Product name\s*", "", name).strip()

                link_el = await tile.query_selector("a[href*='/george/']")
                href = await link_el.get_attribute("href") if link_el else ""
                url = f"https://direct.asda.com{href}" if href and not href.startswith("http") else href

                price_el = await tile.query_selector(".price")
                price_text = (await price_el.inner_text()).strip() if price_el else "N/A"
                price_match = re.search(r"£[\d.]+", price_text)
                price = price_match.group(0) if price_match else price_text

                add_btn = await tile.query_selector("button[aria-label*='Add']")
                available = add_btn is not None

                key = product_key(name)
                current[key] = {"title": name, "url": url, "price": price, "available": available}

        finally:
            await page.close()

        if not current:
            log.warning("ASDA: no products found")
            state["asda"] = current
            return state

        log.info("ASDA: %d products found", len(current))
        prev = state.get("asda", {})
        first_run = len(prev) == 0

        if first_run:
            in_stock = [v for v in current.values() if v["available"]]
            out_stock = [v for v in current.values() if not v["available"]]
            lines = [f"<b>🛒 ASDA GEORGE — Monitoring Started ({len(current)} products)</b>"]
            if in_stock:
                lines.append("\n✅ <b>In Stock:</b>")
                for p in in_stock[:20]:
                    lines.append(fmt_product(p))
            if out_stock:
                lines.append(f"\n❌ <b>Out of Stock:</b> {len(out_stock)} product(s)")
            await send_telegram("\n".join(lines), client)
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
                lines = [f"<b>🆕 ASDA GEORGE — {len(new_products)} New Product(s)!</b>"]
                for p in new_products:
                    lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
            if restocked:
                lines = [f"<b>🔥 ASDA GEORGE — {len(restocked)} Back In Stock!</b>"]
                for p in restocked:
                    lines.append(fmt_product(p, "✅"))
                await send_telegram("\n".join(lines), client)
            if went_oos:
                lines = [f"<b>🔴 ASDA GEORGE — {len(went_oos)} Now Out of Stock</b>"]
                for p in went_oos:
                    lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
            if not (new_products or restocked or went_oos):
                log.info("ASDA: no changes")

        state["asda"] = current

    except Exception as exc:
        log.error("ASDA check failed: %s", exc)

    return state


# ─── SELFRIDGES ───────────────────────────────────────────────────────────────

async def check_selfridges(state: dict, client: httpx.AsyncClient) -> dict:
    """Selfridges — uses real Chrome off-screen (styled-components, JS-heavy)."""
    log.info("Checking Selfridges...")
    current: dict[str, dict] = {}

    try:
        async with async_playwright() as pw:
            ctx = await pw.chromium.launch_persistent_context(
                user_data_dir=str(Path.home() / ".selfridges_chrome_profile"),
                headless=False,
                executable_path=CHROME_PATH,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled",
                      "--window-size=1,1", "--window-position=9999,9999"],
                viewport={"width": 1280, "height": 800},
                locale="en-GB",
            )
            page = await ctx.new_page()
            try:
                await page.goto(URLS["selfridges"], wait_until="domcontentloaded", timeout=35_000)
                await page.wait_for_timeout(8000)

                # Product cards contain brand (h2) + title link (a) + price (li)
                cards = await page.query_selector_all("[data-analytics-link='product_card_link']")
                for link_el in cards:
                    try:
                        href  = await link_el.get_attribute("href") or ""
                        url   = f"https://www.selfridges.com{href}" if href and not href.startswith("http") else href
                        title = (await link_el.inner_text()).strip()
                        if not title or len(title) < 3:
                            continue

                        container = await link_el.evaluate_handle("el => el.closest('[class*=\"sc-183775ea\"]') || el.parentElement.parentElement")
                        price_el  = await container.query_selector("li[data-testid='product-price'], ol li")
                        price_text = (await price_el.inner_text()).strip() if price_el else "N/A"
                        price = re.sub(r"Price:\s*", "", price_text).strip()

                        key = product_key(title)
                        current[key] = {"title": title, "url": url, "price": price, "available": True}
                    except Exception:
                        continue
            finally:
                await page.close()
                await ctx.close()

        if not current:
            log.warning("Selfridges: no products found")
            state["selfridges"] = current
            return state

        log.info("Selfridges: %d products found", len(current))
        prev      = state.get("selfridges", {})
        first_run = len(prev) == 0

        if first_run:
            lines = [f"<b>🏪 SELFRIDGES — Monitoring Started ({len(current)} products)</b>\n✅ <b>In Stock:</b>"]
            for p in list(current.values())[:20]:
                lines.append(fmt_product(p))
            if len(current) > 20:
                lines.append(f"  ...and {len(current) - 20} more")
            await send_telegram("\n".join(lines), client)
        else:
            new_products = [p for pid, p in current.items() if pid not in prev]
            went_oos     = [prev[pid] for pid in prev if pid not in current]
            if new_products:
                lines = [f"<b>🆕 SELFRIDGES — {len(new_products)} New Product(s)!</b>"]
                for p in new_products: lines.append(fmt_product(p))
                await send_telegram("\n".join(lines), client)
            if went_oos:
                lines = ["<b>🔴 SELFRIDGES — No Longer Listed</b>"]
                for p in went_oos: lines.append(fmt_product(p, "❌"))
                await send_telegram("\n".join(lines), client)
            if not (new_products or went_oos):
                log.info("Selfridges: no changes")

        state["selfridges"] = current

    except Exception as exc:
        log.error("Selfridges check failed: %s", exc)

    return state


# ─── SMYTHS SLOUGH (TRIAL: 4 Apr 08:00 – 7 Apr 16:00 BST) ───────────────────

_SMYTHS_PRODUCT_ID  = "256561"
_SMYTHS_PRODUCT_URL = (
    "https://www.smythstoys.com/uk/en-gb/brand/pokemon/pokemon-trading-card-game/"
    "pokemon-trading-card-game-tcg-mega-evolution-ascended-heroes-first-partners-pin-collection/p/256561"
)
_SMYTHS_LAT = "51.510665"
_SMYTHS_LNG = "-0.59888"


def _smyths_in_window() -> bool:
    """Returns True only between 4 Apr 08:00 and 7 Apr 16:00 BST (UTC+1)."""
    from datetime import datetime, timezone, timedelta
    BST = timezone(timedelta(hours=1))
    now = datetime.now(BST)
    start = datetime(2026, 4, 4,  8, 0, tzinfo=BST)
    end   = datetime(2026, 4, 7, 16, 0, tzinfo=BST)
    return start <= now <= end


async def check_smyths_slough(state: dict, client: httpx.AsyncClient, context) -> dict:
    """Smyths Slough store — monitors First Partners Pin Collection stock.
    Uses the shared headed browser context (no extra browser needed).
    Trial window: 4 Apr 08:00 – 7 Apr 16:00 BST."""
    if not _smyths_in_window():
        return state

    log.info("Checking Smyths Slough...")
    page = await context.new_page()
    try:
        await page.goto(_SMYTHS_PRODUCT_URL, wait_until="domcontentloaded", timeout=35_000)
        await page.wait_for_timeout(3000)

        store_data = await page.evaluate(f"""
            fetch('/api/uk/en-gb/store-pickup/pointOfServices?productId={_SMYTHS_PRODUCT_ID}&selectedStore=Northampton&latitude={_SMYTHS_LAT}&longitude={_SMYTHS_LNG}&searchThroughGeoPointFirst=true&cartPage=false', {{
                headers: {{ 'Accept': 'application/json' }}
            }}).then(r => r.json()).catch(() => null)
        """)
        inv_data = await page.evaluate(f"""
            fetch('/api/uk/en-gb/product/product-inventory?code={_SMYTHS_PRODUCT_ID}&userId=anonymous&bundle=false', {{
                headers: {{ 'Accept': 'application/json' }}
            }}).then(r => r.json()).catch(() => null)
        """)
    finally:
        await page.close()

    if not store_data or not inv_data:
        log.warning("Smyths Slough: API call returned null")
        return state

    stores = store_data.get("stores", [])
    slough = next((s for s in stores if s.get("name", "").lower() == "slough"), None)
    if not slough:
        log.warning("Smyths Slough: store not found in API response")
        return state

    slough_status = slough.get("stockLevelStatusCode", "UNKNOWN")
    expected_date = inv_data.get("hdSection", {}).get("expectedStockDate", "")
    log.info("Smyths Slough: status=%s expected=%s", slough_status, expected_date)

    prev          = state.get("smyths_slough", {})
    prev_status   = prev.get("status", "OUTOFSTOCK")
    prev_expected = prev.get("expected", "")

    if slough_status != "OUTOFSTOCK" and prev_status == "OUTOFSTOCK":
        await send_telegram(
            f"🚨 <b>Smyths Slough IN STOCK!</b>\n\n"
            f"Pokémon TCG Mega Evolution Ascended Heroes First Partners Pin Collection\n"
            f"Store status: <b>{slough_status}</b>\n"
            f"Expected online delivery: {expected_date}\n\n"
            f'<a href="{_SMYTHS_PRODUCT_URL}">Buy now →</a>',
            client,
        )
    elif slough_status == "OUTOFSTOCK" and prev_status not in ("OUTOFSTOCK", ""):
        await send_telegram(
            "ℹ️ Smyths Slough: First Partners Pin Collection is back out of stock.",
            client,
        )

    if expected_date and prev_expected and expected_date != prev_expected:
        await send_telegram(
            f"📅 <b>Smyths expected date changed</b>\n\n"
            f"First Partners Pin Collection\n"
            f"Was: {prev_expected}\n"
            f"Now: <b>{expected_date}</b>",
            client,
        )

    state["smyths_slough"] = {"status": slough_status, "expected": expected_date}
    return state


# ─── SMYTHS SLOUGH — Poster Collection (256300) ──────────────────────────────

_SMYTHS_POSTER_ID  = "256300"
_SMYTHS_POSTER_URL = (
    "https://www.smythstoys.com/uk/en-gb/brand/pokemon/pokemon-trading-card-game/"
    "pokemon-trading-card-game-tcg-mega-evolution-ascended-heroes-poster-collection-assortment/p/256300"
)


def _smyths_poster_in_window() -> bool:
    """Returns True only between 4 Apr 08:00 and 10 Apr 23:59 BST (UTC+1)."""
    from datetime import datetime, timezone, timedelta
    BST = timezone(timedelta(hours=1))
    now = datetime.now(BST)
    start = datetime(2026, 4, 4,  8, 0, tzinfo=BST)
    end   = datetime(2026, 4, 10, 23, 59, tzinfo=BST)
    return start <= now <= end


async def check_smyths_poster(state: dict, client: httpx.AsyncClient, context) -> dict:
    """Smyths Slough store — monitors Poster Collection Assortment stock.
    Trial window: 4 Apr 08:00 – 10 Apr 23:59 BST."""
    if not _smyths_poster_in_window():
        return state

    log.info("Checking Smyths Slough (Poster Collection)...")
    page = await context.new_page()
    try:
        await page.goto(_SMYTHS_POSTER_URL, wait_until="domcontentloaded", timeout=35_000)
        await page.wait_for_timeout(3000)

        store_data = await page.evaluate(f"""
            fetch('/api/uk/en-gb/store-pickup/pointOfServices?productId={_SMYTHS_POSTER_ID}&selectedStore=Northampton&latitude={_SMYTHS_LAT}&longitude={_SMYTHS_LNG}&searchThroughGeoPointFirst=true&cartPage=false', {{
                headers: {{ 'Accept': 'application/json' }}
            }}).then(r => r.json()).catch(() => null)
        """)
        inv_data = await page.evaluate(f"""
            fetch('/api/uk/en-gb/product/product-inventory?code={_SMYTHS_POSTER_ID}&userId=anonymous&bundle=false', {{
                headers: {{ 'Accept': 'application/json' }}
            }}).then(r => r.json()).catch(() => null)
        """)
    finally:
        await page.close()

    if not store_data or not inv_data:
        log.warning("Smyths Poster: API call returned null")
        return state

    stores = store_data.get("stores", [])
    slough = next((s for s in stores if s.get("name", "").lower() == "slough"), None)
    if not slough:
        log.warning("Smyths Poster: store not found in API response")
        return state

    slough_status = slough.get("stockLevelStatusCode", "UNKNOWN")
    expected_date = inv_data.get("hdSection", {}).get("expectedStockDate", "")
    log.info("Smyths Poster: status=%s expected=%s", slough_status, expected_date)

    prev          = state.get("smyths_poster", {})
    prev_status   = prev.get("status", "OUTOFSTOCK")
    prev_expected = prev.get("expected", "")

    if slough_status != "OUTOFSTOCK" and prev_status == "OUTOFSTOCK":
        await send_telegram(
            f"🚨 <b>Smyths Slough IN STOCK!</b>\n\n"
            f"Pokémon TCG Mega Evolution Ascended Heroes Poster Collection Assortment\n"
            f"Store status: <b>{slough_status}</b>\n"
            f"Expected online delivery: {expected_date}\n\n"
            f'<a href="{_SMYTHS_POSTER_URL}">Buy now →</a>',
            client,
        )
    elif slough_status == "OUTOFSTOCK" and prev_status not in ("OUTOFSTOCK", ""):
        await send_telegram(
            "ℹ️ Smyths Slough: Poster Collection Assortment is back out of stock.",
            client,
        )

    if expected_date and prev_expected and expected_date != prev_expected:
        await send_telegram(
            f"📅 <b>Smyths expected date changed</b>\n\n"
            f"Poster Collection Assortment\n"
            f"Was: {prev_expected}\n"
            f"Now: <b>{expected_date}</b>",
            client,
        )

    state["smyths_poster"] = {"status": slough_status, "expected": expected_date}
    return state


# ─── MONITOR LOOP ─────────────────────────────────────────────────────────────

BROWSER_REFRESH_INTERVAL = 3_600  # Rotate browser context every hour


async def monitor_loop(client: httpx.AsyncClient, browser, headless_browser) -> None:
    """The core monitoring loop — runs until cancelled."""
    state = load_state()

    # Always re-send every site's full product list on every start
    state["menkind"]    = {}
    state["game"]       = {}
    state["currys"]     = {}
    state["john_lewis"] = {}
    state["freemans"]   = {}
    state["asda"]       = {}
    state["selfridges"] = {}
    state["jd_williams"] = {}
    state["hamleys"] = {}
    state["asda"] = {}

    # ── Status board ──────────────────────────────────────────────────────────
    # One Telegram message that gets edited after each check
    CHECK_STATUS: dict[str, dict] = {
        "menkind":    {"label": "🎁 Menkind",      "ok": None, "time": ""},
        "game":       {"label": "🎮 Game",          "ok": None, "time": ""},
        "currys":     {"label": "🟠 Currys",        "ok": None, "time": ""},
        "john_lewis": {"label": "🟤 John Lewis",    "ok": None, "time": ""},
        "selfridges": {"label": "🏪 Selfridges",    "ok": None, "time": ""},
        "freemans":   {"label": "🇬🇧 Freemans",     "ok": None, "time": ""},
        "asda":       {"label": "🇬🇧 ASDA George",  "ok": None, "time": ""},
        "jd_williams": {"label": "🛒 JD Williams",  "ok": None, "time": ""},
        "hamleys":    {"label": "🧸 Hamleys",       "ok": None, "time": ""},
        "smyths_slough": {"label": "🧸 Smyths Slough (Pin)", "ok": None, "time": ""},
        "smyths_poster": {"label": "🧸 Smyths Slough (Poster)", "ok": None, "time": ""},
    }
    status_msg_id: int | None = state.get("status_msg_id")

    HEADLESS_SITES = {"freemans"}
    HEADED_SITES = {"menkind", "game", "currys", "john_lewis", "selfridges", "smyths_slough", "smyths_poster"}
    EDGE_CDP_SITES = {"jd_williams", "hamleys", "asda"}

    def _fmt_status() -> str:
        lines = ["<b>📊 UK Monitor Status</b>"]
        lines.append("\n<b>⚡ Every 1 min (headless)</b>")
        for k, v in CHECK_STATUS.items():
            if k in HEADLESS_SITES:
                icon = "✅" if v["ok"] is True else ("❌" if v["ok"] is False else "⏳")
                t    = f" <i>({v['time']})</i>" if v["time"] else ""
                lines.append(f"{icon} {v['label']}{t}")
        lines.append("\n<b>🖥️ Every 1 min (browser)</b>")
        for k, v in CHECK_STATUS.items():
            if k in HEADED_SITES:
                icon = "✅" if v["ok"] is True else ("❌" if v["ok"] is False else "⏳")
                t    = f" <i>({v['time']})</i>" if v["time"] else ""
                lines.append(f"{icon} {v['label']}{t}")
        lines.append("\n<b>🌐 Every 1 min (Edge)</b>")
        for k, v in CHECK_STATUS.items():
            if k in EDGE_CDP_SITES:
                icon = "✅" if v["ok"] is True else ("❌" if v["ok"] is False else "⏳")
                t    = f" <i>({v['time']})</i>" if v["time"] else ""
                lines.append(f"{icon} {v['label']}{t}")
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
    last_menkind = last_game = last_currys = 0.0
    last_john_lewis = last_selfridges = last_jd_williams = last_hamleys = 0.0
    last_freemans = last_asda = last_smyths_slough = last_smyths_poster = 0.0
    last_ctx_refresh = 0.0

    context = await make_browser_context(browser)
    headless_context = await make_browser_context(headless_browser)

    # Launch Edge for CDP checks (JD Williams, Hamleys, ASDA)
    _ensure_cdp_edge()
    edge_pw = await async_playwright().start()
    edge_browser = await edge_pw.chromium.connect_over_cdp(f"http://127.0.0.1:{CDP_PORT}")
    edge_ctx = edge_browser.contexts[0]
    log.info("Edge CDP connected")

    # Post initial (all-pending) status board
    await _push_status()

    try:
        while True:
            now = time.monotonic()
            HEARTBEAT["last"] = now          # feed the watchdog

            # ── Rotate browser context hourly ──────────────────────────────
            if now - last_ctx_refresh >= BROWSER_REFRESH_INTERVAL:
                try:
                    await context.close()
                except Exception:
                    pass
                try:
                    await headless_context.close()
                except Exception:
                    pass
                context = await make_browser_context(browser)
                headless_context = await make_browser_context(headless_browser)
                last_ctx_refresh = now
                log.info("Browser contexts rotated")

            # ════ BATCH 1: HEADLESS / HTTP — run concurrently ════════════
            # These don't need a visible browser, safe to run in parallel

            headless_tasks = []

            if now - last_freemans >= INTERVALS.get("freemans", 60):
                headless_tasks.append(("freemans", check_freemans(state, client)))
                last_freemans = now

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

            # ════ BATCH 2: HEADED — run sequentially (shared browser) ═════
            # These need a visible browser due to Cloudflare/bot detection
            # Auto-recover if browser connection dies

            async def _headed_check(name, check_fn, ctx):
                """Run a headed check with auto-recovery on browser crash."""
                nonlocal context
                try:
                    result = await check_fn(state, client, ctx)
                    _track_success(name)
                    return result
                except Exception as exc:
                    if "Connection closed" in str(exc) or "Target page" in str(exc):
                        log.warning("%s: browser crashed, recreating context...", name)
                        try:
                            await context.close()
                        except Exception:
                            pass
                        context = await make_browser_context(browser)
                        try:
                            result = await check_fn(state, client, context)
                            _track_success(name)
                            return result
                        except Exception as exc2:
                            log.error("%s retry failed: %s", name, exc2)
                            await _track_failure(name, str(exc2))
                    else:
                        log.error("%s check failed: %s", name, exc)
                        await _track_failure(name, str(exc))
                    return state

            if now - last_menkind >= INTERVALS["menkind"]:
                state = await _headed_check("menkind", check_menkind, context)
                _mark("menkind", bool(state.get("menkind")))
                save_state(state)
                last_menkind = now
                await _push_status()
                await asyncio.sleep(random.uniform(2, 5))

            if now - last_game >= INTERVALS["game"]:
                state = await _headed_check("game", check_game, context)
                _mark("game", bool(state.get("game")))
                save_state(state)
                last_game = now
                await _push_status()
                await asyncio.sleep(random.uniform(2, 5))

            if now - last_currys >= INTERVALS["currys"]:
                state = await _headed_check("currys", check_currys, context)
                _mark("currys", bool(state.get("currys")))
                save_state(state)
                last_currys = now
                await _push_status()
                await asyncio.sleep(random.uniform(2, 5))

            if now - last_john_lewis >= INTERVALS["john_lewis"]:
                state = await _headed_check("john_lewis", check_john_lewis, context)
                _mark("john_lewis", bool(state.get("john_lewis")))
                save_state(state)
                last_john_lewis = now
                await _push_status()
                await asyncio.sleep(random.uniform(2, 5))

            # Smyths Slough — trial 4 Apr 08:00 to 7 Apr 16:00 BST
            if _smyths_in_window() and now - last_smyths_slough >= 120:
                state = await _headed_check("smyths_slough", check_smyths_slough, context)
                _mark("smyths_slough", state.get("smyths_slough", {}).get("status") != "OUTOFSTOCK")
                save_state(state)
                last_smyths_slough = now
                await _push_status()
                await asyncio.sleep(random.uniform(2, 5))

            # Smyths Slough — Poster Collection, 4 Apr 08:00 to 10 Apr 23:59 BST
            if _smyths_poster_in_window() and now - last_smyths_poster >= 120:
                state = await _headed_check("smyths_poster", check_smyths_poster, context)
                _mark("smyths_poster", state.get("smyths_poster", {}).get("status") != "OUTOFSTOCK")
                save_state(state)
                last_smyths_poster = now
                await _push_status()
                await asyncio.sleep(random.uniform(2, 5))

            # Selfridges — uses its own real Chrome instance (off-screen)
            if now - last_selfridges >= INTERVALS.get("selfridges", 120):
                try:
                    state = await check_selfridges(state, client)
                    _mark("selfridges", bool(state.get("selfridges")))
                    _track_success("selfridges")
                except Exception as exc:
                    log.error("Selfridges check failed: %s", exc)
                    _mark("selfridges", False)
                    await _track_failure("selfridges", str(exc))
                save_state(state)
                last_selfridges = now
                await _push_status()

            # ════ EDGE CDP CHECKS — JD Williams, Hamleys, ASDA ═══════════
            if now - last_jd_williams >= INTERVALS.get("jd_williams", 60):
                try:
                    state = await check_jd_williams(state, client, edge_ctx)
                    _mark("jd_williams", bool(state.get("jd_williams")))
                    _track_success("jd_williams")
                except Exception as exc:
                    log.error("JD Williams check failed: %s", exc)
                    _mark("jd_williams", False)
                    await _track_failure("jd_williams", str(exc))
                save_state(state)
                last_jd_williams = now
                await _push_status()

            if now - last_hamleys >= INTERVALS.get("hamleys", 60):
                try:
                    state = await check_hamleys(state, client, edge_ctx)
                    _mark("hamleys", bool(state.get("hamleys")))
                    _track_success("hamleys")
                except Exception as exc:
                    log.error("Hamleys check failed: %s", exc)
                    _mark("hamleys", False)
                    await _track_failure("hamleys", str(exc))
                save_state(state)
                last_hamleys = now
                await _push_status()

            if now - last_asda >= INTERVALS.get("asda", 60):
                try:
                    state = await check_asda_cdp(state, client, edge_ctx)
                    _mark("asda", bool(state.get("asda")))
                    _track_success("asda")
                except Exception as exc:
                    log.error("ASDA check failed: %s", exc)
                    _mark("asda", False)
                    await _track_failure("asda", str(exc))
                save_state(state)
                last_asda = now
                await _push_status()

            await asyncio.sleep(10)

    except asyncio.CancelledError:
        log.info("Monitor loop cancelled")
    finally:
        try:
            await edge_pw.stop()
        except Exception:
            pass
        _kill_cdp_edge()
        try:
            await context.close()
        except Exception:
            pass
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
        "🤖 <b>UK Pokemon TCG Monitor is online.</b>\n\nSend <code>start</code> to begin monitoring.",
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
                        "✅ <b>UK Retailer Monitor started!</b>\n\n"
                        f"🎁 Menkind: every {INTERVALS['menkind'] // 60} min\n"
                        f"🎮 Game: every {INTERVALS['game'] // 60} min\n"
                        f"🟠 Currys: every {INTERVALS['currys'] // 60} min\n"
                        f"🟤 John Lewis: every {INTERVALS['john_lewis'] // 60} min\n"
                        f"🇬🇧 Freemans: every {INTERVALS.get('freemans', 60) // 60} min\n"
                        f"🇬🇧 ASDA George: every {INTERVALS.get('asda', 60) // 60} min\n\n"
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
    log.info("UK Pokemon TCG Monitor — Starting")
    log.info("Menkind=%ds  Game=%ds  Currys=%ds  JohnLewis=%ds  Freemans=%ds  ASDA=%ds",
             INTERVALS["menkind"], INTERVALS["game"], INTERVALS["currys"], INTERVALS["john_lewis"], INTERVALS.get("freemans", 60), INTERVALS.get("asda", 60))
    log.info("=" * 60)

    async with httpx.AsyncClient(
        timeout=35,
        follow_redirects=True,
    ) as client:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=False,
                executable_path=CHROME_PATH,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--disable-accelerated-2d-canvas",
                    "--no-first-run",
                    "--no-zygote",
                    "--disable-gpu",
                    "--window-size=1,1",
                    "--window-position=9999,9999",
                ],
            )
            headless_browser = await pw.chromium.launch(
                headless=True,
                executable_path=CHROME_PATH,
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
                await telegram_listener(client, browser, headless_browser)
            except (KeyboardInterrupt, asyncio.CancelledError):
                log.info("Shutting down")
            finally:
                try:
                    await browser.close()
                except Exception:
                    pass
                try:
                    await headless_browser.close()
                except Exception:
                    pass
                try:
                    import httpx as _httpx
                    _httpx.post(
                        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                        json={"chat_id": TELEGRAM_CHAT_ID, "text": "⚠️ UK Monitor has stopped (PC shutdown or restart). Will resume when PC comes back online."},
                        timeout=10,
                    )
                except Exception:
                    pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass  # clean exit, no traceback
