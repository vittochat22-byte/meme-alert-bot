import os
import re
import asyncio
import aiohttp
import logging
from datetime import datetime, timezone
from collections import defaultdict, Counter

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
CHAT_ID        = os.environ["CHAT_ID"]
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL_SECONDS", "180"))
MIN_SCORE      = int(os.getenv("MIN_SCORE", "60"))
APIFY_TOKEN    = os.getenv("APIFY_TOKEN", "")

# ── State ─────────────────────────────────────────────────────────────────────
spotted:    dict[str, set[str]] = defaultdict(set)
token_data: dict[str, dict]     = {}
alerted:    set[str]            = set()

# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────────────────────────────────────
async def send_telegram(session: aiohttp.ClientSession, message: str) -> None:
    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message,
                "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        async with session.post(url, json=payload,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                log.warning("Telegram error %s: %s", r.status, await r.text())
    except Exception as e:
        log.error("Telegram send failed: %s", e)

# ─────────────────────────────────────────────────────────────────────────────
# RUGCHECK — report completo
# ─────────────────────────────────────────────────────────────────────────────
async def get_rugcheck_report(session: aiohttp.ClientSession, mint: str) -> dict:
    """Fetch full RugCheck report — ha holders, liquidità, flags contratto, ecc."""
    url = f"https://api.rugcheck.xyz/v1/tokens/{mint}/report"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status == 200:
                return await r.json()
    except Exception as e:
        log.error("RugCheck report %s: %s", mint[:8], e)
    return {}

def parse_rugcheck(report: dict) -> dict:
    """
    Estrae tutti i dati utili dal report completo di RugCheck.
    Restituisce un dict con flag booleani e metriche.
    """
    if not report:
        return {"available": False}

    risks      = report.get("risks", [])
    risk_names = {r.get("name", "").lower() for r in risks}
    risk_level = report.get("score", 0)  # 0=safe, 1000=rugged

    # Top holders
    top_holders     = report.get("topHolders", [])
    top10_pct       = sum(h.get("pct", 0) for h in top_holders[:10])
    holder_count    = report.get("totalHolders", 0)
    dev_wallet_pct  = top_holders[0].get("pct", 0) if top_holders else 0

    # Liquidità
    markets         = report.get("markets", [])
    lp_locked       = any(m.get("lp", {}).get("locked", False) for m in markets)
    lp_burned       = any("burned" in m.get("lp", {}).get("lockType", "").lower() for m in markets)
    lp_lock_pct     = max((m.get("lp", {}).get("pct", 0) for m in markets), default=0)

    # Contratto
    mint_authority  = report.get("mintAuthority")   # None = rinunciato ✅
    freeze_authority= report.get("freezeAuthority") # None = rinunciato ✅
    is_mutable      = report.get("mutable", True)

    # Flag rischio da RugCheck
    has_honeypot    = "honeypot" in risk_names
    has_copycat     = any("copycat" in r or "copy" in r for r in risk_names)
    has_low_liq     = any("low liquidity" in r or "liquidity" in r for r in risk_names)

    return {
        "available":        True,
        "risk_score":       risk_level,
        "top10_pct":        top10_pct,
        "dev_wallet_pct":   dev_wallet_pct,
        "holder_count":     holder_count,
        "lp_locked":        lp_locked,
        "lp_burned":        lp_burned,
        "lp_lock_pct":      lp_lock_pct,
        "mint_renounced":   mint_authority is None,
        "freeze_renounced": freeze_authority is None,
        "is_immutable":     not is_mutable,
        "has_honeypot":     has_honeypot,
        "has_copycat":      has_copycat,
        "has_low_liq_flag": has_low_liq,
        "risk_names":       list(risk_names),
    }

# ─────────────────────────────────────────────────────────────────────────────
# HARD FILTERS — scarta subito se fallisce anche uno solo
# ─────────────────────────────────────────────────────────────────────────────
def passes_hard_filters(dex: dict, rug: dict) -> tuple[bool, str]:
    """
    Restituisce (True, "") se il token passa tutti i filtri duri.
    Restituisce (False, motivo) se va scartato immediatamente.
    """
    liq  = dex.get("liquidity", {}).get("usd", 0)
    vol  = dex.get("volume",    {}).get("h1",  0)
    mc   = dex.get("marketCap", 0)

    # ── Filtri on-chain base ──────────────────────────────────────────────────
    if liq < 8_000:
        return False, f"Liquidità troppo bassa (${liq:,.0f} < $8k)"
    if vol < 5_000:
        return False, f"Volume troppo basso (${vol:,.0f} < $5k/h)"
    if mc > 50_000_000:
        return False, f"Market cap troppo alto (>${mc/1e6:.1f}M) — pump già avvenuto"

    # ── Filtri RugCheck ───────────────────────────────────────────────────────
    if not rug.get("available"):
        return True, ""  # se RugCheck non risponde, non scartare (potrebbe essere nuovo)

    if rug.get("has_honeypot"):
        return False, "🍯 HONEYPOT rilevato da RugCheck"
    if rug.get("risk_score", 0) > 700:
        return False, f"Rug score critico ({rug['risk_score']}/1000)"
    if rug.get("top10_pct", 0) > 60:
        return False, f"Top 10 wallet tengono {rug['top10_pct']:.0f}% supply — rischio dump"
    if rug.get("dev_wallet_pct", 0) > 15:
        return False, f"Dev wallet tiene {rug['dev_wallet_pct']:.0f}% supply"
    if not rug.get("mint_renounced"):
        return False, "Mint authority NON rinunciata — dev può creare nuovi token"
    if rug.get("holder_count", 999) < 50:
        return False, f"Troppo pochi holder ({rug['holder_count']}) — manipolazione facile"

    return True, ""

# ─────────────────────────────────────────────────────────────────────────────
# SCORING — solo per token che hanno passato i filtri duri
# ─────────────────────────────────────────────────────────────────────────────
SOURCE_BONUS = {
    "DEXScreener": 15,
    "CoinGecko":   12,
    "Reddit":      10,
    "Nitter/X":    18,
}

def score_token(dex: dict, rug: dict, sources: set[str]) -> tuple[int, list[str]]:
    score   = 20
    signals = []

    liq   = dex.get("liquidity", {}).get("usd", 0)
    vol   = dex.get("volume",    {}).get("h1",  0)
    buys  = dex.get("txns",      {}).get("h1",  {}).get("buys",  0)
    sells = dex.get("txns",      {}).get("h1",  {}).get("sells", 0)
    mc    = dex.get("marketCap", 0)
    age_m = dex.get("pairAge",   0) / 60

    # ── Confluenza ────────────────────────────────────────────────────────────
    n = len(sources)
    if n >= 4:
        score += 35; signals.append("🌟 CONFLUENZA MASSIMA — tutte le fonti concordano!")
    elif n == 3:
        score += 22; signals.append(f"🔥 Confluenza alta — {n} fonti")
    elif n == 2:
        score += 10; signals.append(f"📡 Visto su {n} fonti")
    else:
        signals.append(f"📌 Segnale singolo ({next(iter(sources))})")
    for src in sources:
        score += SOURCE_BONUS.get(src, 5)

    # ── Liquidità ─────────────────────────────────────────────────────────────
    if liq > 80_000:
        score += 18; signals.append(f"✅ Liquidità solida (${liq:,.0f})")
    elif liq > 30_000:
        score += 10; signals.append(f"💧 Buona liquidità (${liq:,.0f})")
    else:
        signals.append(f"💧 Liquidità minima (${liq:,.0f})")

    # ── Volume ────────────────────────────────────────────────────────────────
    if vol > 200_000:
        score += 20; signals.append(f"🚀 Volume esplosivo (${vol:,.0f}/h)")
    elif vol > 50_000:
        score += 12; signals.append(f"🔥 Volume alto (${vol:,.0f}/h)")
    elif vol > 15_000:
        score += 6;  signals.append(f"📈 Volume discreto (${vol:,.0f}/h)")

    # ── Buy pressure ──────────────────────────────────────────────────────────
    if buys > 0:
        ratio = buys / (buys + max(sells, 1))
        if ratio > 0.72:
            score += 14; signals.append(f"🟢 Forte pressione acquisti ({ratio:.0%} buy)")
        elif ratio > 0.58:
            score += 6;  signals.append(f"🟡 Acquisti prevalenti ({ratio:.0%})")
        elif ratio < 0.38:
            score -= 8;  signals.append(f"🔴 Vendite prevalenti ({ratio:.0%})")

    # ── Market cap ────────────────────────────────────────────────────────────
    if mc < 100_000:
        score += 14; signals.append(f"🎯 Micro early-stage (<$100k mcap)")
    elif mc < 500_000:
        score += 8;  signals.append(f"📊 Early-stage (<$500k mcap)")
    elif mc < 2_000_000:
        score += 3;  signals.append(f"📊 Small-cap (<$2M mcap)")

    # ── Età ───────────────────────────────────────────────────────────────────
    if 0 < age_m < 30:
        signals.append(f"🆕 Appena lanciato ({age_m:.0f} min fa)")
    elif age_m < 120:
        signals.append(f"⏱️ Relativamente nuovo ({age_m:.0f} min fa)")

    # ── Bonus RugCheck ────────────────────────────────────────────────────────
    if rug.get("available"):
        if rug.get("lp_burned"):
            score += 12; signals.append("🔥 Liquidità bruciata (ottimo segnale)")
        elif rug.get("lp_locked") and rug.get("lp_lock_pct", 0) > 80:
            score += 8;  signals.append(f"🔒 Liquidità bloccata ({rug['lp_lock_pct']:.0f}%)")
        elif rug.get("lp_locked"):
            score += 4;  signals.append("🔒 Liquidità parzialmente bloccata")

        if rug.get("freeze_renounced"):
            score += 5;  signals.append("✅ Freeze authority rinunciata")

        holders = rug.get("holder_count", 0)
        if holders > 500:
            score += 8;  signals.append(f"👥 Buona distribuzione ({holders} holder)")
        elif holders > 200:
            score += 4;  signals.append(f"👥 {holders} holder")

        top10 = rug.get("top10_pct", 0)
        if top10 < 25:
            score += 8;  signals.append(f"✅ Supply distribuita (top 10 = {top10:.0f}%)")
        elif top10 < 40:
            score += 3;  signals.append(f"⚠️ Concentrazione moderata (top 10 = {top10:.0f}%)")

        rug_score = rug.get("risk_score", 0)
        if rug_score < 100:
            score += 10; signals.append(f"🟢 RugCheck: rischio basso ({rug_score}/1000)")
        elif rug_score < 300:
            score += 5;  signals.append(f"🟡 RugCheck: rischio medio ({rug_score}/1000)")

    return max(0, min(100, score)), signals

# ─────────────────────────────────────────────────────────────────────────────
# SOURCES
# ─────────────────────────────────────────────────────────────────────────────
async def fetch_dexscreener(session: aiohttp.ClientSession) -> list[tuple[str, dict]]:
    url = "https://api.dexscreener.com/latest/dex/tokens/solana"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                return []
            data  = await r.json()
            pairs = data.get("pairs", []) or []
            return [
                (p["baseToken"]["address"], p)
                for p in pairs
                if p.get("volume", {}).get("h1", 0) > 5_000
                and p.get("baseToken", {}).get("address")
            ]
    except Exception as e:
        log.error("DEXScreener: %s", e)
        return []


async def fetch_coingecko_trending(session: aiohttp.ClientSession) -> list[tuple[str, dict]]:
    url = "https://api.coingecko.com/api/v3/search/trending"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                return []
            data    = await r.json()
            results = []
            for item in data.get("coins", []):
                coin   = item.get("item", {})
                symbol = coin.get("symbol", "").upper()
                mint   = coin.get("platforms", {}).get("solana", symbol)
                if not mint:
                    continue
                mapped = {
                    "liquidity": {"usd": 0}, "volume": {"h1": 0},
                    "txns":      {"h1": {"buys": 0, "sells": 0}},
                    "marketCap": coin.get("data", {}).get("market_cap", 0),
                    "pairAge":   0,
                    "baseToken": {"name": coin.get("name",""), "symbol": symbol, "address": mint},
                    "priceUsd":  coin.get("data", {}).get("price", "N/D"),
                    "url":       f"https://www.coingecko.com/en/coins/{coin.get('id','')}",
                }
                results.append((mint, mapped))
            return results
    except Exception as e:
        log.error("CoinGecko: %s", e)
        return []


IGNORED = {"SOL","BTC","ETH","USDC","USDT","USD","NFT","DEX","AI","APE","DOGE"}

async def fetch_reddit(session: aiohttp.ClientSession) -> list[tuple[str, dict]]:
    subreddits = ["SolanaMemeCoins", "cryptomoonshots", "solana"]
    found: dict[str, str] = {}
    headers = {"User-Agent": "MemeAlertBot/2.0"}
    for sub in subreddits:
        try:
            async with session.get(
                f"https://www.reddit.com/r/{sub}/hot.json?limit=15",
                headers=headers, timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status != 200:
                    continue
                posts = (await r.json()).get("data", {}).get("children", [])
                for post in posts:
                    if post["data"].get("score", 0) < 20:
                        continue
                    for t in re.findall(r'\$([A-Z]{2,10})', post["data"].get("title","").upper()):
                        if t not in IGNORED:
                            found[t] = post["data"]["title"]
        except Exception as e:
            log.error("Reddit %s: %s", sub, e)
    return [
        (sym, {
            "liquidity": {"usd": 0}, "volume": {"h1": 0},
            "txns": {"h1": {"buys": 0, "sells": 0}},
            "marketCap": 0, "pairAge": 0,
            "baseToken": {"name": sym, "symbol": sym, "address": sym},
            "priceUsd": "N/D",
            "url": f"https://www.reddit.com/r/SolanaMemeCoins/search/?q={sym}",
        })
        for sym, _ in found.items()
    ]


async def fetch_nitter_x(session: aiohttp.ClientSession) -> list[tuple[str, dict]]:
    if APIFY_TOKEN:
        url    = "https://api.apify.com/v2/acts/apidojo~tweet-scraper/run-sync-get-dataset-items"
        params = {"token": APIFY_TOKEN,
                  "searchTerms": ["$SOL meme coin new launch", "solana meme 100x"],
                  "maxItems": 30, "lang": "en"}
        try:
            async with session.post(url, json=params,
                                    timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    return []
                found = Counter()
                for tweet in await r.json():
                    for t in re.findall(r'\$([A-Z]{2,10})', tweet.get("text","").upper()):
                        if t not in IGNORED:
                            found[t] += 1
                return _tickers_to_results({t: c for t, c in found.items() if c >= 2})
        except Exception as e:
            log.error("Apify: %s", e)
            return []
    else:
        headers   = {"User-Agent": "Mozilla/5.0"}
        instances = [
            "https://nitter.net/search?q=%24SOL+meme+coin&f=tweets",
            "https://nitter.privacydev.net/search?q=%24SOL+meme+coin&f=tweets",
        ]
        for url in instances:
            try:
                async with session.get(url, headers=headers,
                                       timeout=aiohttp.ClientTimeout(total=12)) as r:
                    if r.status != 200:
                        continue
                    found = Counter(
                        t for t in re.findall(r'\$([A-Z]{2,10})', (await r.text()).upper())
                        if t not in IGNORED
                    )
                    trending = {t: c for t, c in found.items() if c >= 3}
                    if trending:
                        return _tickers_to_results(trending)
            except Exception:
                continue
        return []

def _tickers_to_results(found: dict) -> list[tuple[str, dict]]:
    return [
        (ticker, {
            "liquidity": {"usd": 0}, "volume": {"h1": 0},
            "txns": {"h1": {"buys": 0, "sells": 0}},
            "marketCap": 0, "pairAge": 0,
            "baseToken": {"name": ticker, "symbol": ticker, "address": ticker},
            "priceUsd": "N/D",
            "url": f"https://nitter.net/search?q=%24{ticker}",
        })
        for ticker in found
    ]

# ─────────────────────────────────────────────────────────────────────────────
# FORMAT ALERT
# ─────────────────────────────────────────────────────────────────────────────
def format_alert(mint: str, sources: set[str], score: int,
                 signals: list[str], rug: dict) -> str:
    token  = token_data.get(mint, {})
    bt     = token.get("baseToken", {})
    name   = bt.get("name",   mint[:8])
    symbol = bt.get("symbol", "???")
    price  = token.get("priceUsd", "N/D")
    mc     = token.get("marketCap", 0)
    liq    = token.get("liquidity", {}).get("usd", 0)
    vol1h  = token.get("volume",    {}).get("h1",  0)
    url    = token.get("url", f"https://dexscreener.com/solana/{mint}")
    n      = len(sources)

    # RugCheck label
    rs = rug.get("risk_score")
    if rs is not None:
        if rs < 200:   rug_label = f"🟢 RugCheck: sicuro ({rs}/1000)"
        elif rs < 500: rug_label = f"🟡 RugCheck: attenzione ({rs}/1000)"
        else:          rug_label = f"🔴 RugCheck: rischioso ({rs}/1000)"
    else:
        rug_label = "⚪ RugCheck: N/D"

    score_bar    = "🟩" * (score // 20) + "⬜" * (5 - score // 20)
    header_emoji = "🚨🚨🚨" if n >= 4 else ("🚨🚨" if n >= 3 else "🚨")

    lines = [
        f"{header_emoji} <b>{name} (${symbol})</b>",
        f"📡 Fonti ({n}/4): {' • '.join(sorted(sources))}",
        f"",
    ]
    if mc > 0:    lines.append(f"📊 Market Cap: <code>${mc:,.0f}</code>")
    if liq > 0:   lines.append(f"💧 Liquidità:  <code>${liq:,.0f}</code>")
    if vol1h > 0: lines.append(f"📈 Volume 1h:  <code>${vol1h:,.0f}</code>")
    if str(price) not in ("N/D","0","0.0"):
        lines.append(f"💰 Prezzo:     <code>{price}</code>")
    if rug.get("available"):
        lines.append(f"👥 Holders:    <code>{rug.get('holder_count', 'N/D')}</code>")
        lines.append(f"🐋 Top 10:     <code>{rug.get('top10_pct', 0):.0f}% supply</code>")

    lines += [f"", f"🧪 Score: {score_bar} <b>{score}/100</b>", rug_label, f"", "<b>Segnali:</b>"]
    for s in signals:
        lines.append(f"  {s}")

    is_mint = len(mint) > 20
    if is_mint:
        lines += [
            f"",
            f"🔗 <a href='{url}'>DEXScreener</a>  |  "
            f"<a href='https://rugcheck.xyz/tokens/{mint}'>RugCheck</a>  |  "
            f"<a href='https://axiom.trade/meme/{mint}'>Axiom</a>",
        ]
    else:
        lines.append(f"\n🔍 Cerca <b>${symbol}</b> su DEXScreener / Axiom")

    lines.append(
        f"\n<i>⏰ {datetime.now(timezone.utc).strftime('%H:%M UTC')} — "
        "Non è consulenza finanziaria</i>"
    )
    return "\n".join(lines)

# ─────────────────────────────────────────────────────────────────────────────
# MAIN SCAN
# ─────────────────────────────────────────────────────────────────────────────
async def scan_once(session: aiohttp.ClientSession) -> None:
    log.info("🔍 Scanning 4 fonti...")

    results = await asyncio.gather(
        fetch_dexscreener(session),
        fetch_coingecko_trending(session),
        fetch_reddit(session),
        fetch_nitter_x(session),
        return_exceptions=True,
    )
    for source, result in zip(["DEXScreener","CoinGecko","Reddit","Nitter/X"], results):
        if isinstance(result, Exception):
            log.error("Fonte %s: %s", source, result)
            continue
        for mint, data in result:
            spotted[mint].add(source)
            if mint not in token_data:
                token_data[mint] = data

    alerts_sent = 0
    skipped_filter = 0

    for mint, sources in list(spotted.items()):
        if mint in alerted:
            continue

        dex = token_data.get(mint, {})

        # Solo token con indirizzo reale on-chain vanno a RugCheck
        is_on_chain = len(mint) > 20
        rug = parse_rugcheck(
            await get_rugcheck_report(session, mint) if is_on_chain else {}
        )

        # ── Hard filters prima — scarta subito i token cattivi ────────────────
        ok, reason = passes_hard_filters(dex, rug)
        if not ok:
            log.info("❌ Scartato %s (%s): %s", mint[:8], next(iter(sources)), reason)
            alerted.add(mint)  # non riprovare
            skipped_filter += 1
            continue

        # ── Score sui token sopravvissuti ai filtri ───────────────────────────
        score, signals = score_token(dex, rug, sources)

        if score >= MIN_SCORE:
            msg = format_alert(mint, sources, score, signals, rug)
            await send_telegram(session, msg)
            alerted.add(mint)
            alerts_sent += 1
            await asyncio.sleep(1.5)

    log.info(
        "✅ Done — %d alert | %d scartati dai filtri | %d in watch",
        alerts_sent, skipped_filter, len(spotted)
    )

# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
async def main() -> None:
    log.info("🤖 Meme Confluence Bot v3 avviato")
    async with aiohttp.ClientSession() as session:
        await send_telegram(session, (
            "🤖 <b>Meme Confluence Bot v3 attivo!</b>\n\n"
            "📡 <b>4 fonti:</b> DEXScreener • CoinGecko • Reddit • X\n\n"
            "🛡️ <b>Filtri automatici (scarta subito):</b>\n"
            "  • Liquidità &lt; $8k\n"
            "  • Volume &lt; $5k/h\n"
            "  • Honeypot rilevato\n"
            "  • Mint authority non rinunciata\n"
            "  • Dev wallet &gt; 15% supply\n"
            "  • Top 10 wallet &gt; 60% supply\n"
            "  • Meno di 50 holder\n"
            "  • Rug score &gt; 700/1000\n\n"
            "🟩 <b>Bonus score per:</b> LP bruciata/bloccata,\n"
            "  supply distribuita, confluenza fonti, volume alto\n\n"
            f"⏱️ Scan ogni <b>{CHECK_INTERVAL}s</b> | "
            f"Score minimo: <b>{MIN_SCORE}/100</b>\n\n"
            "<i>Non è consulenza finanziaria</i>"
        ))
        while True:
            try:
                await scan_once(session)
            except Exception as e:
                log.error("Errore ciclo: %s", e)
            await asyncio.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
