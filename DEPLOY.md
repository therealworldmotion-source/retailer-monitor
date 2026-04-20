# Deploying to Railway

This branch (`online-monitor`) is set up to run on Railway without any config files — all secrets come from environment variables.

## 1. Create a new Telegram bot

1. Open Telegram → chat with **@BotFather**
2. `/newbot` → give it a name → save the **bot token**
3. Start a chat with your new bot (send it any message)
4. Get your **chat ID** from **@userinfobot** or via browser:
   `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates` → look for `"chat":{"id":...}`

## 2. Create the Railway project

1. railway.app → **New Project** → **Deploy from GitHub repo**
2. Pick `therealworldmotion-source/retailer-monitor`
3. In the service settings, set **Branch** = `online-monitor`
4. Railway auto-detects the Dockerfile and builds.

## 3. Set environment variables

In the service → **Variables** tab → paste these in (minimum):

| Variable | Value |
|---|---|
| `TELEGRAM_BOT_TOKEN` | from BotFather |
| `TELEGRAM_CHAT_ID` | from userinfobot |
| `DATA_DIR` | `/data` |

Optional (only if you want auto-checkout at Legends Own The Game):

| Variable | Value |
|---|---|
| `LEGENDS_AUTO_CHECKOUT` | `true` |
| `LEGENDS_WATCHLIST` | `Ascended Heroes Pin Collection;First Partner Booster Collection` |
| `CHECKOUT_EMAIL` | your email |
| `CHECKOUT_NAME` | your name |
| `CHECKOUT_PHONE` | your phone |
| `CHECKOUT_ADDRESS` | your street address |
| `CHECKOUT_CITY` | your city |
| `CHECKOUT_STATE` | your state / emirate |
| `CHECKOUT_ZIP` | postcode (optional) |

## 4. Attach a persistent volume

Railway → service → **Volumes** tab → **New Volume**:
- Mount path: `/data`
- Size: 1 GB is plenty

This is where `state_uae.json` lives so alert dedup + last-seen stock survives redeploys. Without this, every redeploy re-alerts on every currently-in-stock item.

## 5. Deploy

Railway auto-deploys on push to `online-monitor`. Once build finishes, check the **Logs** tab — you should see the monitor tick every minute and send a Telegram status message.

## 6. Verify

- Send `/start` to your new bot — it won't reply (this bot only pushes), but it confirms the token is valid.
- Watch Railway logs — within ~1 minute you should see `Checking Otakume...` etc.
- You should get a pinned status message in your Telegram chat showing ✅/❌ per retailer.

## Troubleshooting

| Problem | Fix |
|---|---|
| Build fails on `patchright install chromium` | Railway ran out of memory during build — set build resources higher, or retry. |
| No Telegram messages | Check token + chat ID; verify the bot can DM you (send it `/start` in Telegram first). |
| Every redeploy re-alerts on same items | Persistent volume not mounted — confirm `/data` volume is attached. |
| Site X keeps failing | Add it to `DISABLED_RETAILERS=site_x` env var. |
| Sky-high Railway bill | Free tier gives ~500 hours/mo. A 24/7 bot uses ~720. Budget ~£5–10/mo on Hobby tier. |

## Costs

Railway Hobby: **$5/mo** base + usage. This bot is light (one small process, no big DB) — expect ~$5–8/mo total.
