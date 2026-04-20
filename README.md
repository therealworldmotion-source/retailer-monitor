# Pokemon Retailer Monitor

Stock monitor for UAE and UK Pokemon TCG retailers. Sends Telegram alerts on new stock, with optional auto-checkout for selected items.

## Branches

- **`main`** — local-run working version (Windows `.bat` workflow). Use if running on a PC / local always-on machine.
- **`online-monitor`** — Railway-ready version. Reads secrets from environment variables, runs in a Docker container with `patchright` + Chromium preinstalled.

## Local run (main branch)

1. Copy `config_uae.example.json` → `config_uae.json` and fill in Telegram token + checkout details.
2. `pip install -r requirements.txt`
3. `python -m patchright install chromium`
4. `py monitor_uae.py` (or use `start_uae_monitor.bat` on Windows).

## Online run (online-monitor branch)

See `online-monitor` branch README for Railway deploy steps. No config files — everything via env vars.

## Stack

- `patchright` — stealth Playwright fork (anti-bot bypass)
- `httpx`, `beautifulsoup4`, `lxml`
- Telegram bot API for alerts
- Auto-checkout via browser automation (Legends Own The Game)

## Monitored retailers

UAE: Otaku.me, Virgin Megastore, Legends Own The Game, Colorland Toys, Magrudy, Zgames, Geekay, Little Things.
UK: see `config_uk.example.json`.
