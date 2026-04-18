"""
app.py — StockSense AI v3
=========================
Yahoo Finance stock scanner for NSE with 4 accuracy filters:
  1. Nifty Trend Filter       — only scan when Nifty > EMA20
  2. Sector Momentum Filter   — prefer stocks in strong sectors
  3. Earnings Date Check      — skip if earnings within 3 days, warn within 7
  4. Market Breadth Filter    — advance/decline ratio check

  Phase 1 -> Yahoo Finance  : 413 NSE stocks, quick filter
  Phase 2 -> Yahoo Finance  : Deep analysis (RSI, EMA, MACD, BB)
  Email   -> Resend API     : Rich HTML with entry, T1, T2, SL, reasons
  Target  -> 1-2% profit in 2-3 days swing trades

Scan schedule (IST):
  Intraday  : 09:45 . 10:25 . 11:30 . 13:30 . 14:45  (market hours only)
  EOD report: 15:45 (performance analytics email — win rate, P&L per stock)
  Manual    : "Run Scan Now" button available anytime
"""

import os, time, logging, threading, json, gc
from datetime import datetime, timedelta
from io import StringIO

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
from flask import Flask, jsonify, render_template_string

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("stockalert")

IST = pytz.timezone("Asia/Kolkata")

# ── PHASE 1 FILTERS ──────────────────────────────────────────────────────────
P1_MIN_MOVE  = 0.5   # min daily move — removes flat stocks
P1_MAX_MOVE  = 7.0   # max move — avoids parabolic stocks already done
P1_VOL_MIN   = 1.5   # minimum volume vs 10-day avg
P1_MIN_PRICE = 50.0  # raised: avoid illiquid penny stocks
P1_MAX_PASS  = 80    # top 80 by volume ratio go to Phase 2

# ── PHASE 2 SCORING WEIGHTS (total = 10.5) ───────────────────────────────────
S = {
    "breakout":  2.5,
    "trend":     2.0,
    "macd":      1.5,
    "rsi":       1.5,
    "volume":    1.0,
    "bb":        1.0,
    "candle":    0.5,
    "news":      0.5,
}
MAX_SCORE            = sum(S.values())
MIN_SCORE            = 6.0
MIN_SCORE_BAD_MARKET = 7.5

# ── TARGET LEVELS ─────────────────────────────────────────────────────────────
ENTRY_BUF = 0.2
T1_PCT    = 1.2
T2_PCT    = 2.2
SL_PCT    = 0.8

# ── SCAN MODE LABELS ──────────────────────────────────────────────────────────
SCAN_MODE_INTRADAY   = "INTRADAY"
SCAN_MODE_POSTMARKET = "POST-MARKET"

# ── POSITIONS FILE — persists across restarts ─────────────────────────────────
POSITIONS_FILE = "positions.json"

NSE_CSV_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"

SECTOR_MAP = {
    "IT":      ["TCS.NS","INFY.NS","WIPRO.NS","HCLTECH.NS","TECHM.NS","MPHASIS.NS","COFORGE.NS","PERSISTENT.NS"],
    "BANK":    ["HDFCBANK.NS","ICICIBANK.NS","KOTAKBANK.NS","AXISBANK.NS","SBIN.NS","INDUSINDBK.NS","FEDERALBNK.NS","IDFCFIRSTB.NS"],
    "PHARMA":  ["SUNPHARMA.NS","DRREDDY.NS","CIPLA.NS","DIVISLAB.NS","ALKEM.NS","TORNTPHARM.NS","GLENMARK.NS","BIOCON.NS"],
    "AUTO":    ["TATAMOTORS.NS","MARUTI.NS","BAJAJ-AUTO.NS","HEROMOTOCO.NS","EICHERMOT.NS","MOTHERSON.NS","BHARATFORG.NS","APOLLOTYRE.NS"],
    "ENERGY":  ["RELIANCE.NS","ONGC.NS","NTPC.NS","POWERGRID.NS","COALINDIA.NS","TATAPOWER.NS","ADANIGREEN.NS","JSWENERGY.NS"],
    "FMCG":    ["HINDUNILVR.NS","ITC.NS","NESTLEIND.NS","BRITANNIA.NS","DABUR.NS","MARICO.NS","GODREJCP.NS","TATACONSUM.NS"],
    "METAL":   ["TATASTEEL.NS","JSWSTEEL.NS","HINDALCO.NS","VEDL.NS","COALINDIA.NS","NMDC.NS","SAIL.NS","NATIONALUM.NS"],
    "REALTY":  ["DLF.NS","GODREJPROP.NS","OBEROIRLTY.NS","PRESTIGE.NS","BRIGADE.NS","SOBHA.NS","PHOENIXLTD.NS"],
    "INFRA":   ["LT.NS","ADANIPORTS.NS","NBCC.NS","RVNL.NS","IRFC.NS","HAL.NS","BEL.NS","CONCOR.NS"],
    "FINANCE": ["BAJFINANCE.NS","BAJAJFINSV.NS","CHOLAFIN.NS","SHRIRAMFIN.NS","M&MFIN.NS","MUTHOOTFIN.NS","ANGELONE.NS"],
}

NIFTY500_SYMBOLS = [
    "RELIANCE.NS","TCS.NS","HDFCBANK.NS","INFY.NS","ICICIBANK.NS",
    "SBIN.NS","BAJFINANCE.NS","BHARTIARTL.NS","KOTAKBANK.NS","AXISBANK.NS",
    "INDUSINDBK.NS","WIPRO.NS","HCLTECH.NS","TECHM.NS","SUNPHARMA.NS",
    "DRREDDY.NS","CIPLA.NS","DIVISLAB.NS","NTPC.NS","POWERGRID.NS",
    "ONGC.NS","COALINDIA.NS","IRCTC.NS","TATAMOTORS.NS","MARUTI.NS",
    "ADANIPORTS.NS","ADANIENT.NS","BAJAJFINSV.NS","HINDUNILVR.NS","LTIM.NS",
    "LT.NS","ULTRACEMCO.NS","TITAN.NS","NESTLEIND.NS","ASIANPAINT.NS",
    "HDFCLIFE.NS","SBILIFE.NS","JSWSTEEL.NS","TATASTEEL.NS","HINDALCO.NS",
    "VEDL.NS","GRASIM.NS","BRITANNIA.NS","ITC.NS","TATACONSUM.NS",
    "BAJAJ-AUTO.NS","HEROMOTOCO.NS","EICHERMOT.NS","M&M.NS","TATAPOWER.NS",
    "DABUR.NS","MARICO.NS","GODREJCP.NS","PIDILITIND.NS","BERGEPAINT.NS",
    "HAVELLS.NS","VOLTAS.NS","CONCOR.NS","DELHIVERY.NS","APOLLOTYRE.NS",
    "CEATLTD.NS","MRF.NS","BALKRISIND.NS","MOTHERSON.NS","BOSCHLTD.NS",
    "BHARATFORG.NS","BANKBARODA.NS","CANBK.NS","PNB.NS","UNIONBANK.NS",
    "FEDERALBNK.NS","IDFCFIRSTB.NS","BANDHANBNK.NS","CHOLAFIN.NS","SHRIRAMFIN.NS",
    "MUTHOOTFIN.NS","MANAPPURAM.NS","MPHASIS.NS","COFORGE.NS","PERSISTENT.NS",
    "LTTS.NS","KPIT.NS","DIXON.NS","TATAELXSI.NS","ZOMATO.NS",
    "NAUKRI.NS","INDIAMART.NS","AFFLE.NS","HAL.NS","BEL.NS",
    "RVNL.NS","IRFC.NS","DLF.NS","GODREJPROP.NS","PRESTIGE.NS",
    "APOLLOHOSP.NS","FORTIS.NS","LALPATHLAB.NS","COLPAL.NS","PAGEIND.NS",
    "MCDOWELL-N.NS","UBL.NS","RADICO.NS","OBEROIRLTY.NS","BRIGADE.NS",
    "SOBHA.NS","PHOENIXLTD.NS","KOLTEPATIL.NS","SUNTECK.NS","MAHLIFE.NS",
    "LODHA.NS","CENTURYPLY.NS","GREENPANEL.NS","ASTRAL.NS","PRINCEPIPE.NS",
    "SUPREMEIND.NS","NILKAMAL.NS","ACCELYA.NS","BATAINDIA.NS","RELAXO.NS",
    "METROBRAND.NS","CAMPUS.NS","VIPIND.NS","SAFARI.NS","TRENT.NS",
    "DMART.NS","VMART.NS","SHOPERSTOP.NS","NYKAA.NS","PAYTM.NS",
    "POLICYBZR.NS","CARTRADE.NS","EASEMYTRIP.NS","HUDCO.NS","RECLTD.NS",
    "PFC.NS","NHPC.NS","SJVN.NS","CESC.NS","TORNTPOWER.NS",
    "ADANIGREEN.NS","ADANIPOWER.NS","JSWENERGY.NS","KAYNES.NS","SYRMA.NS",
    "AMBER.NS","MAXHEALTH.NS","ASTERDM.NS","NH.NS","METROPOLIS.NS",
    "THYROCARE.NS","KRSNAA.NS","VIJAYA.NS","ALKEM.NS","TORNTPHARM.NS",
    "IPCALAB.NS","AJANTPHARM.NS","GRANULES.NS","LAURUSLABS.NS","NATCOPHARM.NS",
    "GLENMARK.NS","BIOCON.NS","AUROPHARMA.NS","ABBOTINDIA.NS","PFIZER.NS",
    "GLAXO.NS","SANOFI.NS","ASTRAZEN.NS","BLUEDART.NS","MAHLOG.NS",
    "GATI.NS","EXIDEIND.NS","AMARAJABAT.NS","SUNDRMFAST.NS","SCHAEFFLER.NS",
    "SKFINDIA.NS","TIINDIA.NS","ENDURANCE.NS","NMDC.NS","SAIL.NS",
    "NATIONALUM.NS","MOIL.NS","WELCORP.NS","HINDCOPPER.NS","RATNAMANI.NS",
    "APL.NS","JSWINFRA.NS","ADANIENSOL.NS","KARURVYSYA.NS","DCBBANK.NS",
    "RBLBANK.NS","UJJIVANSFB.NS","EQUITASBNK.NS","CSBBANK.NS","SOUTHBANK.NS",
    "IOB.NS","MAHABANK.NS","CENTRALBK.NS","UCOBANK.NS","INDIANB.NS",
    "BANKINDIA.NS","ANGELONE.NS","ICICIPRULI.NS","HDFCAMC.NS","NIPPONLIFE.NS",
    "UTIAMC.NS","CAMS.NS","KFINTECH.NS","BSE.NS","MCX.NS",
    "CDSL.NS","IIFL.NS","AAVAS.NS","HOMEFIRST.NS","APTUS.NS",
    "BAJAJHLDNG.NS","CHOLAHLDNG.NS","PNBHOUSING.NS","RAILTEL.NS","NBCC.NS",
    "RITES.NS","BEML.NS","MAZAGON.NS","COCHINSHIP.NS","GRSE.NS",
    "GARDENREACH.NS","MTAR.NS","PARAS.NS","AIAENG.NS","THERMAX.NS",
    "CUMMINSIND.NS","BHEL.NS","SIEMENS.NS","ABB.NS","HONAUT.NS",
    "BLUESTARCO.NS","KALPATPOWR.NS","POWERMECH.NS","KEC.NS","INOXWIND.NS",
    "SUZLON.NS","POLYCAB.NS","KEI.NS","FINOLEX.NS","CABLEIND.NS",
    "ZENSARTECH.NS","RATEGAIN.NS","TANLA.NS","MASTEK.NS","SONATSOFTW.NS",
    "NAZARA.NS","JUSTDIAL.NS","TATACOMM.NS","HFCL.NS","ROUTE.NS",
    "INDIACEM.NS","DALMIACEM.NS","JKCEMENT.NS","RAMCOCEM.NS","HEIDELBERG.NS",
    "BIRLACORPN.NS","PRISM.NS","SHREECEM.NS","KANSAINER.NS","AKZONOBEL.NS",
    "GNFC.NS","DEEPAKNI.NS","ATUL.NS","NAVINFLUOR.NS","FLUOROCHEM.NS",
    "SRF.NS","AARTIIND.NS","VINATIORGA.NS","GALAXYSURF.NS","BALRAMCHIN.NS",
    "RENUKA.NS","TRIVENI.NS","KRBL.NS","LTFOODS.NS","AVANTIFEED.NS",
    "TRIL.NS","INDIGRID.NS","PGHH.NS","GILLETTE.NS","EMAMILTD.NS",
    "JYOTHYLAB.NS","BAJAJCON.NS","ZYDUSWELL.NS","LICHOUSING.NS","CANFINHOME.NS",
    "REPCO.NS","GRUH.NS","AHLUCONT.NS","NESCO.NS","MAHINDCIE.NS",
    "SUNDARMFIN.NS","MGFL.NS","CREDITACC.NS","UJJIVAN.NS","FUSION.NS",
    "SURYODAY.NS","SPANDANA.NS","AROHAN.NS","INOXLEISUR.NS","PVR.NS",
    "NCLIND.NS","WONDERLA.NS","DEVYANI.NS","WESTLIFE.NS","JUBLFOOD.NS",
    "BARBEQUE.NS","SAPPHIRE.NS","RRKABEL.NS","TATATECH.NS","CYIENT.NS",
    "NIITLTD.NS","HEXAWARE.NS","ZENSAR.NS","BIRLASOFT.NS","MINDTREE.NS",
    "HAPPSTMNDS.NS","INTELLECT.NS","NEWGEN.NS","NUCLEUS.NS","DATAMATICS.NS",
    "RAMCOIND.NS","TCIEXP.NS","VRL.NS","MAHEXPRESS.NS","ALLCARGO.NS",
    "TVSSCS.NS","GATEWAY.NS","GPPL.NS","ESABINDIA.NS","GRINDWELL.NS",
    "ELGIEQUIP.NS","GMMPFAUDLR.NS","JYOTISTRUC.NS","KIRLOSENG.NS","KNRCON.NS",
    "PNCINFRA.NS","HG.NS","SARDAEN.NS","MSTCLTD.NS","IRCON.NS",
    "RAILVIKAS.NS","HSCL.NS","WABCOINDIA.NS","CRAFTSMAN.NS","SANSERA.NS",
    "SUPRAJIT.NS","LUMAXTECH.NS","MINDA.NS","GABRIEL.NS","JAMNAUTO.NS",
    "SETCO.NS","STEELCAS.NS","IFBIND.NS","WHIRLPOOL.NS","SYMPHONY.NS",
    "HAWKINCOOK.NS","TTKHLTCARE.NS","VGUARD.NS","CROMPTON.NS","ORIENTELEC.NS",
    "BAJAJELEC.NS","WAAREEENER.NS","PREMIER.NS","RPOWER.NS","JPPOWER.NS",
    "SWANENERGY.NS","ADANITRANS.NS","PTCIL.NS","GIPCL.NS","GSPL.NS",
    "GUJGASLTD.NS","IGL.NS","MGL.NS","ATGL.NS","HINDPETRO.NS",
    "BPCL.NS","IOC.NS","MRPL.NS","CHENNPETRO.NS","CASTROLIND.NS",
    "GULFOILLUB.NS","TIDEWATER.NS","SOTL.NS","BASF.NS","FINEORG.NS",
    "NOCIL.NS","TATACHEM.NS","GSFC.NS","CHAMBLFERT.NS","COROMANDEL.NS",
    "PIIND.NS","DHANUKA.NS","RALLIS.NS","BAYERCROP.NS","SUMICHEM.NS",
    "INSECTICID.NS","EXCEL.NS","HERANBA.NS","IIFL.NS","TVSHLTD.NS",
    "ESCORTS.NS","FORCEMOT.NS","ASHOKLEY.NS","OLECTRA.NS","TVSMOTOR.NS",
    "NUVOCO.NS","DEEPAKNTR.NS","JMFINANCL.NS",
]

FALLBACK_SYMBOLS = NIFTY500_SYMBOLS

state = {
    "last_run": "Never", "last_result": "No run yet",
    "alerts_sent": 0, "last_stocks": [], "next_run": "",
    "running": False, "log_lines": [],
    "phase1_count": 0, "phase2_count": 0, "td_calls_today": 0,
    "market_health": {
        "nifty_trend": "unknown", "breadth": "unknown",
        "strong_sectors": [], "weak_sectors": [],
        "score": 0, "is_bad": False,
        "nifty_ok": True, "nifty_value": 0, "nifty_ema20": 0,
        "breadth_ratio": 1.0, "breadth_pct_up": 50.0, "breadth_ok": True,
        "min_score_used": MIN_SCORE,
    },
    "active_positions": {},
    "exited_positions": [],
    "macro_intel": {
        "macro_score": 0, "summary": "Not run yet", "signals": [],
        "sector_boosts": set(), "sector_suppresses": set(),
        "skip_scan": False, "min_score_adj": MIN_SCORE,
        "vix": 0, "sp500_chg": 0, "oil": 0, "usdinr": 0,
        "news_headlines": [], "bear_keywords": [], "bull_keywords": [],
    },
    "sector_strength": {
        "sector_stats": {}, "strong_sectors": [],
        "stock_stats": {}, "sector_avg_chg": {},
    },
}

# ═══════════════════════════════════════════════════════════════════════════════
# FIX 1 — POSITION PERSISTENCE
# Save/load active_positions to positions.json so they survive restarts.
# ═══════════════════════════════════════════════════════════════════════════════

def save_positions():
    """Write active_positions to disk. Called every time positions change."""
    try:
        with open(POSITIONS_FILE, "w") as f:
            json.dump(state["active_positions"], f, indent=2)
        add_log(f"Positions saved to disk ({len(state['active_positions'])} active)")
    except Exception as e:
        add_log(f"Position save error: {e}")

def load_positions():
    """Load active_positions from disk on startup."""
    if not os.path.exists(POSITIONS_FILE):
        add_log("No positions.json found — starting fresh")
        return
    try:
        with open(POSITIONS_FILE, "r") as f:
            loaded = json.load(f)
        state["active_positions"] = loaded
        add_log(f"Loaded {len(loaded)} position(s) from disk: {list(loaded.keys())}")
    except Exception as e:
        add_log(f"Position load error (starting fresh): {e}")

# ═══════════════════════════════════════════════════════════════════════════════

def add_log(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    log.info(msg)
    state["log_lines"].append(line)
    if len(state["log_lines"]) > 120:
        state["log_lines"] = state["log_lines"][-120:]

def fetch_nse_symbols():
    add_log(f"Using Nifty 500 list ({len(NIFTY500_SYMBOLS)} stocks) - memory safe")
    return NIFTY500_SYMBOLS

def yf_history_safe(ticker_obj, **kwargs):
    for attempt in range(3):
        try:
            df = ticker_obj.history(**kwargs)
            if not df.empty:
                return df
            if attempt < 2:
                time.sleep(4 * (attempt + 1))
        except Exception as e:
            log.debug(f"yf_history_safe attempt {attempt+1} failed: {e}")
            if attempt < 2:
                time.sleep(4 * (attempt + 1))
    return pd.DataFrame()

def compute_rsi(close, period=14):
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta).clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    val = 100 - 100 / (1 + rs.iloc[-1])
    return float(val) if not np.isnan(val) else 50.0

# ═══════════════════════════════════════════════════════════════════════════════
# GLOBAL MACRO INTELLIGENCE LAYER
# ═══════════════════════════════════════════════════════════════════════════════

VIX_DANGER     = 35.0   # CATASTROPHIC fear — e.g. COVID/GFC level. Was 25 (too sensitive)
VIX_ELEVATED   = 25.0   # Elevated fear — caution but still scan
VIX_CALM       = 15.0   # Calm market — good for swing trades
OIL_SPIKE_PCT  = 2.5    # oil move threshold — only sector impact, never triggers skip
SP500_CRASH_PCT = 3.0   # S&P CRASH (>3% down) — skip scan. Was 2% (too sensitive)
SP500_FALL_PCT  = 1.0   # S&P mild fall — just a warning, minor score adjust
USDINR_WEAK    = 84.5
TNX_SPIKE      = 0.15

OIL_BENEFICIARY_SECTORS  = {"ENERGY"}
OIL_HURT_SECTORS         = {"AUTO","FMCG"}
WEAK_RUPEE_HURT          = {"FMCG","AUTO"}
WEAK_RUPEE_HELP          = {"IT"}
RATE_CUT_HELP            = {"BANK","FINANCE","REALTY"}
RATE_CUT_HURT            = {}
RATE_HIKE_HURT           = {"BANK","FINANCE","REALTY"}
GLOBAL_SELLOFF_HURT      = set(["IT","METAL","ENERGY"])


def fetch_macro_signal(ticker_sym, period="5d", interval="1d"):
    try:
        df = yf.Ticker(ticker_sym).history(period=period, interval=interval)
        close = df["Close"].dropna()
        if len(close) < 2:
            return None, 0.0, False
        latest = float(close.iloc[-1])
        prev   = float(close.iloc[-2])
        chg    = (latest - prev) / prev * 100 if prev else 0.0
        return round(latest, 2), round(chg, 2), True
    except Exception as e:
        log.debug(f"Macro fetch {ticker_sym}: {e}")
        return None, 0.0, False


def fetch_macro_news():
    news_tickers = ["^GSPC", "^NSEI", "GC=F", "CL=F"]
    BULLISH_KEYWORDS = [
        "rate cut", "stimulus", "rate cuts", "gdp beat", "gdp growth",
        "ceasefire", "peace deal", "trade deal", "fed pause", "rbi cut",
        "record profit", "surplus", "rally", "bull", "recovery",
    ]
    BEARISH_KEYWORDS = [
        "rate hike", "war", "escalation", "sanctions", "recession",
        "gdp miss", "inflation surge", "default", "crisis", "crash",
        "selloff", "sell-off", "fed hike", "rbi hike", "stagflation",
        "tariff", "geopolitical", "conflict", "strike", "attack",
        "bank collapse", "credit downgrade", "debt ceiling",
    ]
    all_headlines = []
    hits_bull = []
    hits_bear = []
    try:
        for sym in news_tickers:
            try:
                items = yf.Ticker(sym).news or []
                for n in items[:4]:
                    title = (n.get("title") or "").lower()
                    if title and title not in all_headlines:
                        all_headlines.append(title)
            except Exception:
                continue
        text = " ".join(all_headlines)
        hits_bull = [kw for kw in BULLISH_KEYWORDS if kw in text]
        hits_bear = [kw for kw in BEARISH_KEYWORDS if kw in text]
        sentiment = min(2, len(hits_bull)) - min(3, len(hits_bear))
        sentiment = max(-3, min(2, sentiment))
        return sentiment, all_headlines[:6], hits_bull, hits_bear
    except Exception as e:
        log.debug(f"Macro news error: {e}")
        return 0, [], [], []


def run_macro_intelligence():
    """
    Macro intelligence with calibrated sensitivity:
    - Oil spike: sector impact only, max -1, NEVER triggers skip alone
    - Skip scan: ONLY on true catastrophe (VIX>35 OR S&P crash >3%)
    - Caution email: sent at score -2, but min_score raised modestly (max 7.0)
    - Score -3 or worse: skip scan + caution email
    """
    add_log("=== Macro Intelligence Check ===")
    signals      = []
    sector_boost = set()
    sector_supp  = set()
    score        = 0
    catastrophe  = False   # only True for VIX>35 or S&P crash >3%

    # ── 1. VIX ────────────────────────────────────────────────────────────────
    vix, vix_chg, vix_ok = fetch_macro_signal("^VIX")
    if vix_ok:
        if vix > VIX_DANGER:          # >35 — true catastrophe (COVID/GFC level)
            score -= 3
            catastrophe = True
            signals.append(f"🚨 VIX {vix:.1f} — CATASTROPHIC FEAR (>{VIX_DANGER}) · Market in freefall · Scan paused")
            sector_supp.update(GLOBAL_SELLOFF_HURT)
        elif vix > VIX_ELEVATED:      # 25-35 — serious fear, not catastrophic
            score -= 2
            signals.append(f"⚠ VIX {vix:.1f} — High fear ({VIX_ELEVATED}-{VIX_DANGER}) · Cautious · Prefer strong setups only")
            sector_supp.update(GLOBAL_SELLOFF_HURT)
        elif vix > 20:                # 20-25 — elevated, manageable
            score -= 1
            signals.append(f"⚠ VIX {vix:.1f} — Elevated fear · Cautious market · Prefer defensive setups")
        elif vix < VIX_CALM:          # <15 — calm, ideal
            score += 1
            signals.append(f"✓ VIX {vix:.1f} — Calm market (<{VIX_CALM}) · Favourable for swing trades")
        else:
            signals.append(f"→ VIX {vix:.1f} — Neutral")
        add_log(f"  VIX: {vix:.1f} (chg {vix_chg:+.2f}%) → score {score}")

    # ── 2. S&P 500 ────────────────────────────────────────────────────────────
    sp, sp_chg, sp_ok = fetch_macro_signal("^GSPC")
    if sp_ok:
        if sp_chg <= -SP500_CRASH_PCT:    # >3% crash — catastrophic
            score -= 3
            catastrophe = True
            signals.append(f"🚨 S&P 500 {sp_chg:+.2f}% — CRASH · India will open RED hard · Scan paused")
            sector_supp.update(GLOBAL_SELLOFF_HURT)
        elif sp_chg <= -SP500_FALL_PCT * 2:  # -2% to -3% — heavy fall
            score -= 2
            signals.append(f"⚠ S&P 500 {sp_chg:+.2f}% — Heavy US selloff · India likely opens RED · Skip weak setups")
            sector_supp.update(GLOBAL_SELLOFF_HURT)
        elif sp_chg <= -SP500_FALL_PCT:   # -1% to -2% — mild fall
            score -= 1
            signals.append(f"⚠ S&P 500 {sp_chg:+.2f}% — US market down · Indian sentiment negative")
        elif sp_chg >= 1.5:
            score += 1
            signals.append(f"✓ S&P 500 {sp_chg:+.2f}% — US market strong · Positive global sentiment for India")
        else:
            signals.append(f"→ S&P 500 {sp_chg:+.2f}% — Neutral US session")
        add_log(f"  S&P500: {sp:,.0f} ({sp_chg:+.2f}%) → score {score}")

    # ── 3. Nasdaq ─────────────────────────────────────────────────────────────
    nq, nq_chg, nq_ok = fetch_macro_signal("^IXIC")
    if nq_ok:
        if nq_chg <= -1.5:
            score -= 1
            signals.append(f"⚠ Nasdaq {nq_chg:+.2f}% — Tech selloff · IT sector under pressure")
            sector_supp.add("IT")
        elif nq_chg >= 2.0:
            signals.append(f"✓ Nasdaq {nq_chg:+.2f}% — Tech rally · IT sector tailwind")
            sector_boost.add("IT")
        add_log(f"  Nasdaq: {nq:,.0f} ({nq_chg:+.2f}%)")

    # ── 4. Crude Oil — sector impact only, MAX -1, never triggers skip ────────
    oil, oil_chg, oil_ok = fetch_macro_signal("CL=F")
    if oil_ok:
        if oil_chg >= OIL_SPIKE_PCT:
            score -= 1   # capped at -1 regardless of spike size
            signals.append(f"⚠ Crude Oil +{oil_chg:.1f}% (${oil:.1f}) — Oil spike · Hurts AUTO/FMCG · Helps ENERGY · Score capped at -1")
            sector_supp.update(OIL_HURT_SECTORS)
            sector_boost.update(OIL_BENEFICIARY_SECTORS)
        elif oil_chg <= -OIL_SPIKE_PCT:
            score += 1
            signals.append(f"✓ Crude Oil {oil_chg:.1f}% (${oil:.1f}) — Oil falling · Good for AUTO/FMCG · Negative for ENERGY")
            sector_boost.update(OIL_HURT_SECTORS)
            sector_supp.update(OIL_BENEFICIARY_SECTORS)
        else:
            signals.append(f"→ Crude Oil {oil_chg:+.2f}% (${oil:.1f}) — Stable")
        add_log(f"  Oil: ${oil:.1f} ({oil_chg:+.2f}%)")

    # ── 5. USD/INR ────────────────────────────────────────────────────────────
    usdinr, inr_chg, inr_ok = fetch_macro_signal("INR=X")
    if inr_ok:
        if usdinr > USDINR_WEAK:
            score -= 1
            signals.append(f"⚠ USD/INR {usdinr:.2f} — Weak rupee (>{USDINR_WEAK}) · FII selling pressure · IT earns more in INR")
            sector_supp.update(WEAK_RUPEE_HURT)
            sector_boost.update(WEAK_RUPEE_HELP)
        elif usdinr < 83.0:
            score += 1
            signals.append(f"✓ USD/INR {usdinr:.2f} — Strong rupee · FII inflows likely · Positive for market")
        else:
            signals.append(f"→ USD/INR {usdinr:.2f} — Stable rupee")
        add_log(f"  USD/INR: {usdinr:.2f} ({inr_chg:+.2f}%)")

    # ── 6. US 10Y Treasury Yield ──────────────────────────────────────────────
    tnx, tnx_chg, tnx_ok = fetch_macro_signal("^TNX")
    if tnx_ok:
        if tnx_chg >= 2.0:
            score -= 1
            signals.append(f"⚠ US 10Y Yield {tnx:.2f}% (+{tnx_chg:.2f}%) — Rate stress · FIIs may exit India for US bonds")
        elif tnx_chg <= -2.0:
            score += 1
            signals.append(f"✓ US 10Y Yield {tnx:.2f}% ({tnx_chg:.2f}%) — Yields falling · Rate cut expectations · Bullish for BANK/FINANCE/REALTY")
            sector_boost.update(RATE_CUT_HELP)
        else:
            signals.append(f"→ US 10Y Yield {tnx:.2f}% — Stable rates")
        add_log(f"  US 10Y: {tnx:.2f}% ({tnx_chg:+.2f}%)")

    # ── 7. Gold ───────────────────────────────────────────────────────────────
    gold, gold_chg, gold_ok = fetch_macro_signal("GC=F")
    if gold_ok:
        if gold_chg >= 1.5:
            score -= 1
            signals.append(f"⚠ Gold +{gold_chg:.1f}% (${gold:,.0f}) — Risk-off · Investors fleeing to safety · Cautious")
        elif gold_chg <= -1.0:
            signals.append(f"✓ Gold {gold_chg:.1f}% — Risk-on · Investors moving into equities")
        else:
            signals.append(f"→ Gold {gold_chg:+.2f}% (${gold:,.0f}) — Neutral")
        add_log(f"  Gold: ${gold:,.0f} ({gold_chg:+.2f}%)")

    # ── 8. Emerging Markets ETF ───────────────────────────────────────────────
    eem, eem_chg, eem_ok = fetch_macro_signal("EEM")
    if eem_ok:
        if eem_chg <= -1.5:
            score -= 1
            signals.append(f"⚠ EM ETF {eem_chg:+.2f}% — FII outflow from Emerging Markets · India likely affected")
        elif eem_chg >= 1.5:
            score += 1
            signals.append(f"✓ EM ETF {eem_chg:+.2f}% — FII inflow into Emerging Markets · India likely beneficiary")
        add_log(f"  EM ETF: {eem_chg:+.2f}%")

    # ── 9. News ───────────────────────────────────────────────────────────────
    news_sent, headlines, bull_kw, bear_kw = fetch_macro_news()
    score += news_sent
    if bear_kw:
        signals.append(f"⚠ Bearish news detected: {', '.join(bear_kw[:4])}")
        add_log(f"  News bearish hits: {bear_kw[:4]}")
    if bull_kw:
        signals.append(f"✓ Bullish news: {', '.join(bull_kw[:3])}")
        add_log(f"  News bullish hits: {bull_kw[:3]}")

    score = max(-5, min(2, score))

    # ── Final decision ────────────────────────────────────────────────────────
    # skip_scan: ONLY if catastrophic event (VIX>35 or S&P crash >3%)
    # min_score: raised modestly on bad days, hard cap at 7.0 (was going to 8.0+)
    # caution email: sent at -2 but scan still runs with moderate bar
    if score >= 2:
        min_score_adj = max(3.5, MIN_SCORE - 1.5)
        skip_scan     = False
        summary       = f"VERY BULLISH macro (score +{score}) — expanded scan, more setups"
    elif score == 1:
        min_score_adj = max(4.0, MIN_SCORE - 0.5)
        skip_scan     = False
        summary       = f"Bullish macro (score +{score}) — normal scan with slight relaxation"
    elif score == 0:
        min_score_adj = MIN_SCORE
        skip_scan     = False
        summary       = "Neutral macro — normal scan"
    elif score == -1:
        min_score_adj = MIN_SCORE + 0.5   # was +1.0 — less aggressive
        skip_scan     = False
        summary       = f"Mild macro risk (score {score}) — slight bar raise to {min_score_adj:.1f}"
    elif score == -2:
        min_score_adj = MIN_SCORE + 1.0   # was +2.0 — was too aggressive
        skip_scan     = False
        summary       = f"Moderate macro risk (score {score}) — bar raised to {min_score_adj:.1f}, caution email sent"
    elif score == -3:
        min_score_adj = min(MIN_SCORE + 1.5, 7.0)  # hard cap 7.0, was going to 9.0
        skip_scan     = catastrophe        # skip ONLY if actual catastrophe flagged
        summary       = f"{'SCAN SKIPPED — ' if catastrophe else ''}High macro risk (score {score}) — bar {min_score_adj:.1f}, caution email sent"
    else:  # -4 or -5
        min_score_adj = 7.0               # hard cap — never go above 7.0
        skip_scan     = catastrophe
        summary       = f"{'SCAN SKIPPED — ' if catastrophe else ''}Severe macro risk (score {score}) — caution email sent"

    sector_boost -= sector_supp

    result = {
        "macro_score":       score,
        "min_score_adj":     min_score_adj,
        "skip_scan":         skip_scan,
        "signals":           signals,
        "sector_boosts":     sector_boost,
        "sector_suppresses": sector_supp,
        "summary":           summary,
        "news_headlines":    headlines,
        "bear_keywords":     bear_kw,
        "bull_keywords":     bull_kw,
        "vix":               vix,
        "sp500_chg":         sp_chg if sp_ok else 0,
        "oil":               oil if oil_ok else 0,
        "usdinr":            usdinr if inr_ok else 0,
    }
    state["macro_intel"] = result
    add_log(f"Macro score: {score} → {summary}")
    return result


def send_macro_caution_email(macro):
    resend_key = os.getenv("RESEND_API_KEY", "")
    sender     = os.getenv("ALERT_EMAIL_SENDER", "")
    recips     = [r.strip() for r in os.getenv("ALERT_EMAIL_RECIPIENTS", "").split(",") if r.strip()]
    if not resend_key or not sender or not recips:
        return False

    now = datetime.now(IST).strftime("%d %b %Y  %H:%M IST")
    score = macro["macro_score"]
    signals_html = "".join(
        f'<div style="display:flex;gap:8px;margin-bottom:8px">'
        f'<span style="color:#FF9900;font-size:14px">⚠</span>'
        f'<span style="color:#CFD8DC;font-size:13px;line-height:1.5">{s}</span></div>'
        for s in macro["signals"]
    )
    bear_html = ", ".join(macro["bear_keywords"][:5]) if macro["bear_keywords"] else "None"
    subject = f"⚠ MACRO CAUTION: Scan Paused | Score {score} | StockSense AI"

    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#0B1826;font-family:'Segoe UI',sans-serif">
<table width="100%" cellpadding="0" cellspacing="0"><tr><td align="center" style="padding:28px 12px">
<table width="600" style="max-width:600px;width:100%">
  <tr><td style="background:#1A1000;border-radius:16px 16px 0 0;padding:28px;border-bottom:3px solid #FF9900">
    <div style="font-size:11px;color:#FF9900;letter-spacing:3px;margin-bottom:8px">STOCKSENSE AI · MACRO INTELLIGENCE</div>
    <div style="font-size:30px;font-weight:900;color:#fff">⚠ CAUTION ALERT</div>
    <div style="font-size:16px;color:#FF9900;margin-top:4px">{macro["summary"]}</div>
    <div style="font-size:12px;color:#607D8B;margin-top:8px">{now}</div>
  </td></tr>
  <tr><td style="background:#0F1F2E;padding:24px 28px">
    <div style="background:#FF990015;border:1px solid #FF990044;border-radius:10px;padding:16px 20px;margin-bottom:20px">
      <div style="font-size:10px;color:#607D8B;text-transform:uppercase;letter-spacing:1px">Macro Risk Score</div>
      <div style="font-size:40px;font-weight:900;color:#FF1744;line-height:1">{score}</div>
    </div>
    <div style="background:#0A1520;border:1px solid #1e3248;border-radius:10px;padding:16px 18px;margin-bottom:16px">
      <div style="font-size:10px;color:#FF9900;letter-spacing:2px;text-transform:uppercase;margin-bottom:12px">Why We're Cautious</div>
      {signals_html}
    </div>
  </td></tr>
  <tr><td style="background:#080f1a;border-radius:0 0 16px 16px;padding:14px 28px;border-top:1px solid #1A2B3C">
    <div style="font-size:11px;color:#37474F">Automated macro signal — not financial advice.</div>
  </td></tr>
</table></td></tr></table></body></html>"""

    plain = (
        f"STOCKSENSE AI — MACRO CAUTION\n{now}\n{'='*50}\n\n"
        f"Score: {score}\n{macro['summary']}\n\n"
        + "\n".join(macro["signals"])
        + f"\n\nSuggested: {'Avoid new trades' if score<=-3 else 'High caution only'}\n"
    )
    try:
        resp = requests.post("https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {resend_key}", "Content-Type": "application/json"},
            json={"from": f"StockSense AI <{sender}>", "to": recips,
                  "subject": subject, "text": plain, "html": html},
            timeout=20)
        ok = resp.status_code in (200, 201)
        add_log(f"Macro caution email {'sent' if ok else 'failed'}: score {score}")
        return ok
    except Exception as e:
        add_log(f"Macro caution email error: {e}")
        return False

# ---- FILTER 1: NIFTY TREND ----

def check_nifty_trend():
    for attempt in range(3):
        try:
            if attempt > 0:
                add_log(f"  Nifty retry attempt {attempt+1}...")
                time.sleep(15 * attempt)
            df = yf.Ticker("^NSEI").history(period="60d", interval="1d")
            close = df["Close"].dropna()
            if len(close) < 2:
                raise Exception("Insufficient data")
            curr = float(close.iloc[-1])
            ema20 = float(close.ewm(span=20, adjust=False).mean().iloc[-1])
            chg1d = float((close.iloc[-1] - close.iloc[-2]) / close.iloc[-2] * 100)
            above = curr > ema20
            gap = round((curr - ema20) / ema20 * 100, 2)
            add_log(f"  Nifty {curr:,.0f} | EMA20 {ema20:,.0f} | Gap {gap:+.2f}% | {'BULLISH' if above else 'BEARISH'}")
            return {"is_ok": above, "status": "bullish" if above else "bearish",
                    "value": round(curr, 2), "ema20": round(ema20, 2), "gap": gap, "chg1d": round(chg1d, 2)}
        except Exception as e:
            add_log(f"  Nifty check failed (attempt {attempt+1}): {e}")
            if attempt == 2:
                add_log("  Nifty check giving up — assuming bullish to allow scan")
                return {"is_ok": True, "status": "unknown", "value": 0, "ema20": 0, "gap": 0, "chg1d": 0}

# ---- FILTER 2: SECTOR MOMENTUM ----

def check_sector_momentum():
    strong, weak = [], []
    add_log("  Checking sector momentum ...")
    for sector, stocks in SECTOR_MAP.items():
        sample = stocks[:3]
        scores = []
        try:
            raw = yf.download(" ".join(sample), period="10d", interval="1d",
                              group_by="ticker", auto_adjust=True, progress=False, timeout=20)
            for sym in sample:
                try:
                    df = raw[sym] if len(sample) > 1 else raw
                    df = df.dropna(subset=["Close"])
                    if len(df) < 10: continue
                    close = df["Close"]
                    curr = float(close.iloc[-1])
                    ema10 = float(close.ewm(span=10, adjust=False).mean().iloc[-1])
                    scores.append(1 if curr > ema10 else 0)
                except Exception:
                    continue
        except Exception:
            continue
        if scores:
            if sum(scores)/len(scores) >= 0.6:
                strong.append(sector)
            else:
                weak.append(sector)
        time.sleep(2)
    add_log(f"  Strong sectors: {strong}")
    add_log(f"  Weak sectors: {weak}")
    return {"strong": strong, "weak": weak}

# ---- FILTER 3: EARNINGS CHECK ----

def check_earnings(symbol):
    try:
        cal = yf.Ticker(symbol).calendar
        if cal is None or (hasattr(cal, "empty") and cal.empty):
            return {"skip": False, "warn": False, "days": None}
        if "Earnings Date" in cal.index:
            ed = cal.loc["Earnings Date"]
        elif "Earnings Date" in cal.columns:
            ed = cal["Earnings Date"].iloc[0]
        else:
            return {"skip": False, "warn": False, "days": None}
        if hasattr(ed, "__iter__") and not isinstance(ed, str):
            ed = list(ed)[0]
        earn_dt = pd.Timestamp(ed).tz_localize(None)
        days = (earn_dt - datetime.now().replace(tzinfo=None)).days
        if days < 0:    return {"skip": False, "warn": False, "days": days}
        elif days <= 3: return {"skip": True,  "warn": False, "days": days}
        elif days <= 7: return {"skip": False, "warn": True,  "days": days}
        else:           return {"skip": False, "warn": False, "days": days}
    except Exception:
        return {"skip": False, "warn": False, "days": None}

# ---- FILTER 4: MARKET BREADTH ----

def check_market_breadth():
    try:
        add_log("  Checking market breadth ...")
        sample = NIFTY500_SYMBOLS[:50]
        raw = yf.download(" ".join(sample), period="3d", interval="1d",
                          group_by="ticker", auto_adjust=True, progress=False, timeout=30)
        advances = declines = unchanged = 0
        for sym in sample:
            try:
                df = raw[sym] if len(sample) > 1 else raw
                df = df.dropna(subset=["Close"])
                if len(df) < 2: continue
                chg = float(df["Close"].iloc[-1]) - float(df["Close"].iloc[-2])
                if chg > 0:    advances += 1
                elif chg < 0:  declines += 1
                else:          unchanged += 1
            except Exception:
                continue
        total = advances + declines + unchanged
        ratio = round(advances / declines, 2) if declines > 0 else 3.0
        pct_up = round(advances / total * 100, 1) if total > 0 else 50.0
        is_ok = ratio >= 1.2
        add_log(f"  Breadth: {advances} up / {declines} down | ratio {ratio:.2f} | {pct_up}% advancing")
        return {"is_ok": is_ok, "advances": advances, "declines": declines, "ratio": ratio, "pct_up": pct_up}
    except Exception as e:
        add_log(f"  Breadth check failed: {e}")
        return {"is_ok": True, "advances": 0, "declines": 0, "ratio": 1.0, "pct_up": 50.0}

# ---- MARKET HEALTH AGGREGATOR ----

def run_market_health_checks():
    add_log("=== Running 4 market health checks ===")
    nifty   = check_nifty_trend()
    sectors = check_sector_momentum()
    breadth = check_market_breadth()

    health_score = 0
    # Nifty contributes only 0.5 points (was binary 1/0) — individual stocks matter more
    if nifty["is_ok"]:              health_score += 1
    if len(sectors["strong"]) >= 3: health_score += 1
    if breadth["is_ok"]:            health_score += 1
    if breadth["ratio"] >= 1.5:     health_score += 1

    is_bad = health_score <= 1
    nifty_below = not nifty["is_ok"]  # flag passed to Phase 2 for per-stock logic

    result = {
        "nifty_ok": nifty["is_ok"], "nifty_value": nifty["value"],
        "nifty_ema20": nifty["ema20"], "nifty_chg1d": nifty["chg1d"],
        "nifty_trend": nifty["status"],
        "nifty_below": nifty_below,    # NEW — used in Phase 2 for relative strength detection
        "strong_sectors": sectors["strong"], "weak_sectors": sectors["weak"],
        "breadth_ratio": breadth["ratio"], "breadth_pct_up": breadth["pct_up"],
        "breadth_ok": breadth["is_ok"],
        "score": health_score, "is_bad": is_bad,
        "min_score_used": MIN_SCORE_BAD_MARKET if is_bad else MIN_SCORE,
    }
    state["market_health"] = result
    label = "WEAK MARKET" if is_bad else "HEALTHY MARKET"
    add_log(f"Market health: {health_score}/4 - {label} | Nifty: {'below' if nifty_below else 'above'} EMA20")
    if is_bad:
        add_log(f"  Raising min score to {MIN_SCORE_BAD_MARKET} - only high-confidence alerts")
    return result

# ═══════════════════════════════════════════════════════════════════════════════
# FIX 3 — MEMORY OPTIMIZATION
# Phase 1: delete raw dataframe + gc.collect() after each batch
# Phase 2: delete indicators dict + gc.collect() after each stock
# ═══════════════════════════════════════════════════════════════════════════════

def phase1_scan(symbols, strong_sectors):
    """
    Phase 1 — Quick broad filter across all stocks.
    FIX 3: Explicitly del raw df + gc.collect() after each batch
    to prevent RAM accumulation across ~20 batches.
    """
    candidates = []
    add_log(f"Phase 1 — scanning {len(symbols)} stocks ...")
    sym_to_sector = {s: sec for sec, stocks in SECTOR_MAP.items() for s in stocks}
    batch_size = 20
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        raw = None  # ensure always defined for cleanup
        try:
            raw = yf.download(" ".join(batch), period="15d", interval="1d",
                              group_by="ticker", auto_adjust=True, progress=False, timeout=30)
        except Exception as e:
            log.debug(f"Batch error: {e}")
            time.sleep(2)
            continue
        for sym in batch:
            try:
                df = raw[sym].copy() if len(batch) > 1 else raw.copy()
                df = df.dropna(subset=["Close","Volume","Open","High","Low"])
                if len(df) < 10: continue

                close = df["Close"]; vol = df["Volume"]
                high  = df["High"];  low = df["Low"]; opn = df["Open"]

                curr       = float(close.iloc[-1])
                prev       = float(close.iloc[-2])
                open_today = float(opn.iloc[-1])
                high_today = float(high.iloc[-1])
                low_today  = float(low.iloc[-1])

                if curr < P1_MIN_PRICE: continue

                chg = (curr - prev) / prev * 100 if prev > 0 else 0
                if not (P1_MIN_MOVE <= chg <= P1_MAX_MOVE): continue

                gap_down = (open_today - prev) / prev * 100 if prev > 0 else 0
                if gap_down < -0.5: continue

                vol10 = float(vol.iloc[-10:].mean())
                vol_r = float(vol.iloc[-1]) / vol10 if vol10 > 0 else 0
                if vol_r < P1_VOL_MIN: continue

                sma5  = float(close.rolling(5).mean().iloc[-1])
                sma10 = float(close.rolling(10).mean().iloc[-1])
                if curr < sma5 or sma5 < sma10: continue

                low10d    = float(close.iloc[-10:].min())
                rally_10d = (curr - low10d) / low10d * 100 if low10d > 0 else 0
                if rally_10d > 20: continue

                candle_range = high_today - low_today
                candle_body  = abs(curr - open_today)
                if candle_range > 0 and candle_body / candle_range < 0.25: continue

                sector    = sym_to_sector.get(sym, "OTHER")
                in_strong = sector in strong_sectors if strong_sectors else True
                quality   = vol_r * (1.3 if in_strong else 1.0) * (1.1 if chg >= 1.0 else 1.0)

                candidates.append({
                    "symbol":           sym,
                    "chg":              round(chg, 2),
                    "vol_ratio":        round(vol_r, 2),
                    "close":            curr,
                    "high_today":       round(high_today, 2),
                    "low_today":        round(low_today, 2),
                    "open_today":       round(open_today, 2),
                    "sector":           sector,
                    "in_strong_sector": in_strong,
                    "quality":          round(quality, 3),
                    "news":             [],
                })
            except Exception:
                continue

        # ── FIX 3: Free batch memory immediately ─────────────────────────────
        if raw is not None:
            del raw
        gc.collect()
        # ─────────────────────────────────────────────────────────────────────
        time.sleep(3)

    candidates.sort(key=lambda x: x["quality"], reverse=True)
    top = candidates[:P1_MAX_PASS]
    add_log(f"Phase 1 done — {len(candidates)} passed filters, top {len(top)} to Phase 2")
    state["phase1_count"] = len(top)
    return top


# ═══════════════════════════════════════════════════════════════════════════════
# SECTOR STRENGTH ANALYSIS MODULE  (ADD-ON — does NOT touch existing logic)
# Single batch yfinance download per scan — no per-stock extra API calls.
# Called once in run_pipeline; result cached in state["sector_strength"].
# ═══════════════════════════════════════════════════════════════════════════════

# Dedicated sector map for intra-day strength calculation.
# Intentionally separate from SECTOR_MAP so neither is modified.
SECTOR_STRENGTH_MAP = {
    "IT":      ["TCS.NS","INFY.NS","WIPRO.NS","HCLTECH.NS","TECHM.NS","MPHASIS.NS","COFORGE.NS","PERSISTENT.NS"],
    "BANK":    ["HDFCBANK.NS","ICICIBANK.NS","KOTAKBANK.NS","AXISBANK.NS","SBIN.NS","INDUSINDBK.NS","FEDERALBNK.NS","IDFCFIRSTB.NS"],
    "PHARMA":  ["SUNPHARMA.NS","DRREDDY.NS","CIPLA.NS","DIVISLAB.NS","ALKEM.NS","TORNTPHARM.NS","GLENMARK.NS","BIOCON.NS"],
    "AUTO":    ["TATAMOTORS.NS","MARUTI.NS","BAJAJ-AUTO.NS","HEROMOTOCO.NS","EICHERMOT.NS","MOTHERSON.NS","BHARATFORG.NS","APOLLOTYRE.NS"],
    "ENERGY":  ["RELIANCE.NS","ONGC.NS","NTPC.NS","POWERGRID.NS","COALINDIA.NS","TATAPOWER.NS","ADANIGREEN.NS","JSWENERGY.NS"],
    "FMCG":    ["HINDUNILVR.NS","ITC.NS","NESTLEIND.NS","BRITANNIA.NS","DABUR.NS","MARICO.NS","GODREJCP.NS","TATACONSUM.NS"],
    "METAL":   ["TATASTEEL.NS","JSWSTEEL.NS","HINDALCO.NS","VEDL.NS","COALINDIA.NS","NMDC.NS","SAIL.NS","NATIONALUM.NS"],
    "REALTY":  ["DLF.NS","GODREJPROP.NS","OBEROIRLTY.NS","PRESTIGE.NS","BRIGADE.NS","SOBHA.NS","PHOENIXLTD.NS"],
    "INFRA":   ["LT.NS","ADANIPORTS.NS","NBCC.NS","RVNL.NS","IRFC.NS","HAL.NS","BEL.NS","CONCOR.NS"],
    "FINANCE": ["BAJFINANCE.NS","BAJAJFINSV.NS","CHOLAFIN.NS","SHRIRAMFIN.NS","M&MFIN.NS","MUTHOOTFIN.NS","ANGELONE.NS"],
}

# Reverse lookup: symbol → sector name
_SECTOR_STRENGTH_SYM_MAP = {
    sym: sec for sec, syms in SECTOR_STRENGTH_MAP.items() for sym in syms
}

SECTOR_STRENGTH_MIN_CHG = 1.0   # sector avg 1D% must exceed this to be "strong"
SECTOR_STRENGTH_TOP_N   = 3     # keep only top-3 qualifying sectors


def run_sector_strength_analysis():
    """
    Downloads 10 days of daily OHLCV for all stocks in SECTOR_STRENGTH_MAP
    in ONE batch call — no extra per-stock API calls.

    Steps 1-3 of the spec:
      1. Group stocks by sector.
      2. Calculate sector strength = avg 1D% of all sector stocks.
      3. Select strong_sectors: avg > 1% AND top-3.

    Returns dict stored in state["sector_strength"]:
      {
        "sector_stats":  { sector: {"avg_chg_1d": float, "strength": float} },
        "strong_sectors": [ list of sector names ],
        "stock_stats":   { symbol: {"chg_1d", "chg_5d", "vol_spike", "strength_score"} },
        "sector_avg_chg": { sector: float },
      }
    """
    add_log("=== Sector Strength Analysis starting ===")
    all_syms = list(dict.fromkeys(                        # deduplicate (COALINDIA in 2 sectors)
        s for syms in SECTOR_STRENGTH_MAP.values() for s in syms
    ))
    try:
        raw = yf.download(
            " ".join(all_syms),
            period="10d", interval="1d",
            group_by="ticker", auto_adjust=True,
            progress=False, timeout=30,
        )
    except Exception as e:
        add_log(f"  Sector strength download failed: {e} — filter bypassed")
        return _empty_sector_strength()

    # ── Step 4: per-stock calculations ───────────────────────────────────────
    stock_stats  = {}
    sector_chgs  = {sec: [] for sec in SECTOR_STRENGTH_MAP}

    for sym in all_syms:
        try:
            df  = (raw[sym] if len(all_syms) > 1 else raw).dropna(subset=["Close", "Volume"])
            if len(df) < 6:
                continue
            close = df["Close"]
            vol   = df["Volume"]

            chg_1d   = float((close.iloc[-1] - close.iloc[-2]) / close.iloc[-2] * 100) \
                       if close.iloc[-2] > 0 else 0.0
            chg_5d   = float((close.iloc[-1] - close.iloc[-6]) / close.iloc[-6] * 100) \
                       if close.iloc[-6] > 0 else 0.0
            avg_vol5 = float(vol.iloc[-6:-1].mean())
            vol_spike = float(vol.iloc[-1]) / avg_vol5 if avg_vol5 > 0 else 1.0

            # Step 5: stock_strength_score
            strength_score = round(chg_1d * 0.5 + chg_5d * 0.3 + vol_spike * 0.2, 4)

            stock_stats[sym] = {
                "chg_1d":         round(chg_1d,   3),
                "chg_5d":         round(chg_5d,   3),
                "vol_spike":      round(vol_spike, 3),
                "strength_score": strength_score,
            }
            sec = _SECTOR_STRENGTH_SYM_MAP.get(sym)
            if sec:
                sector_chgs[sec].append(chg_1d)
        except Exception:
            continue

    # ── Step 2: sector averages ───────────────────────────────────────────────
    sector_stats   = {}
    sector_avg_chg = {}
    for sec, chgs in sector_chgs.items():
        if not chgs:
            continue
        avg = round(sum(chgs) / len(chgs), 3)
        sector_avg_chg[sec] = avg
        sector_stats[sec]   = {"avg_chg_1d": avg, "strength": avg}

    # ── Step 3: select strong sectors ────────────────────────────────────────
    qualifying = [
        (sec, v) for sec, v in sector_avg_chg.items()
        if v > SECTOR_STRENGTH_MIN_CHG
    ]
    qualifying.sort(key=lambda x: x[1], reverse=True)
    strong_sectors = [sec for sec, _ in qualifying[:SECTOR_STRENGTH_TOP_N]]

    add_log(
        f"  Sector strength strong (>{SECTOR_STRENGTH_MIN_CHG}%): "
        f"{strong_sectors if strong_sectors else 'none today'} | "
        + " | ".join(
            f"{s}:{v:+.2f}%"
            for s, v in sorted(sector_avg_chg.items(), key=lambda x: -x[1])[:6]
        )
    )

    result = {
        "sector_stats":   sector_stats,
        "strong_sectors": strong_sectors,
        "stock_stats":    stock_stats,
        "sector_avg_chg": sector_avg_chg,
    }
    state["sector_strength"] = result
    return result


def _empty_sector_strength():
    """Safe fallback when download fails — all stocks pass the filter."""
    empty = {
        "sector_stats":   {},
        "strong_sectors": [],   # empty list → filter bypassed in apply_sector_filter
        "stock_stats":    {},
        "sector_avg_chg": {},
    }
    state["sector_strength"] = empty
    return empty


def apply_sector_filter(candidate, ss):
    """
    Step 6: per-stock sector filter gate.

    Returns:
      (passes, reason, sector_strength, sector_avg_chg,
       stock_strength_score, sector_bonus)

    Rejection rules (Step 6):
      1. Sector not in strong_sectors
      2. Stock 1D% <= sector average  (not outperforming its sector)
      3. Volume spike <= 1.5

    Bypass: if strong_sectors is empty (download failed), pass everything.
    """
    sym    = candidate["symbol"]
    sector = candidate.get("sector", "OTHER")

    # Bypass when no data available
    if not ss["strong_sectors"] and not ss["stock_stats"]:
        return True, "sector_filter_bypassed", 0.0, 0.0, 0.0, 0.0

    strong    = ss["strong_sectors"]
    s_stat    = ss["stock_stats"].get(sym, {})
    avg_chg   = ss["sector_avg_chg"].get(sector, 0.0)
    sec_str   = ss["sector_stats"].get(sector, {}).get("avg_chg_1d", 0.0)

    chg_1d    = s_stat.get("chg_1d",         candidate.get("chg", 0.0))
    vol_spike = s_stat.get("vol_spike",       candidate.get("vol_ratio", 1.0))
    sss       = s_stat.get("strength_score",  0.0)

    # Rule 1 — sector must be strong
    if strong and sector not in strong:
        return (False,
                f"Sector {sector} not in today's top-{SECTOR_STRENGTH_TOP_N} "
                f"({', '.join(strong)})",
                sec_str, avg_chg, sss, 0.0)

    # Rule 2 — stock must outperform sector average
    if chg_1d <= avg_chg:
        return (False,
                f"1D move {chg_1d:+.2f}% ≤ sector avg {avg_chg:+.2f}% — "
                f"not outperforming {sector}",
                sec_str, avg_chg, sss, 0.0)

    # Rule 3 — sufficient volume
    if vol_spike <= 1.5:
        return (False,
                f"Volume spike {vol_spike:.2f}x ≤ 1.5 — insufficient buying interest",
                sec_str, avg_chg, sss, 0.0)

    # Passed — apply sector bonus
    sector_bonus = 2.0 if sector in strong else 0.0
    return True, "ok", sec_str, avg_chg, sss, sector_bonus

# ═══════════════════════════════════════════════════════════════════════════════
# CANDLESTICK CONFIRMATION MODULE  (ADD-ON — does NOT touch existing logic)
# Uses OHLC data already fetched in Phase 2 — zero extra API calls.
# ═══════════════════════════════════════════════════════════════════════════════

def bullish_engulfing(df):
    """True if the last candle fully engulfs the previous bearish candle."""
    try:
        if len(df) < 2:
            return False
        prev_o = float(df["Open"].iloc[-2]); prev_c = float(df["Close"].iloc[-2])
        curr_o = float(df["Open"].iloc[-1]); curr_c = float(df["Close"].iloc[-1])
        prev_bearish = prev_c < prev_o
        curr_bullish = curr_c > curr_o
        engulfs = curr_o <= prev_c and curr_c >= prev_o
        return prev_bearish and curr_bullish and engulfs
    except Exception:
        return False


def three_green_candles(df):
    """True if the last 3 candles are all green (close > open) with higher closes."""
    try:
        if len(df) < 3:
            return False
        for i in range(-3, 0):
            if float(df["Close"].iloc[i]) <= float(df["Open"].iloc[i]):
                return False
        # Each close higher than the previous
        return (float(df["Close"].iloc[-1]) > float(df["Close"].iloc[-2]) >
                float(df["Close"].iloc[-3]))
    except Exception:
        return False


def strong_breakout_candle(df):
    """True if last candle: body > 60% of range, close in top 20%, volume spike."""
    try:
        if len(df) < 6:
            return False
        o = float(df["Open"].iloc[-1]); c = float(df["Close"].iloc[-1])
        h = float(df["High"].iloc[-1]); l = float(df["Low"].iloc[-1])
        candle_range = h - l
        if candle_range <= 0:
            return False
        body = c - o
        if body <= 0:
            return False
        body_ratio = body / candle_range
        close_pos  = (c - l) / candle_range
        avg_vol    = float(df["Volume"].iloc[-6:-1].mean())
        vol_spike  = float(df["Volume"].iloc[-1]) > avg_vol * 1.5 if avg_vol > 0 else False
        return body_ratio > 0.60 and close_pos > 0.80 and vol_spike
    except Exception:
        return False


def upper_wick_rejection(df):
    """True if the last candle has a small upper wick (< 10% of range)."""
    try:
        o = float(df["Open"].iloc[-1]); c = float(df["Close"].iloc[-1])
        h = float(df["High"].iloc[-1]); l = float(df["Low"].iloc[-1])
        candle_range = h - l
        if candle_range <= 0:
            return True   # doji-like — treat as no wick
        upper_wick = h - max(c, o)
        return (upper_wick / candle_range) < 0.10
    except Exception:
        return False


def candle_score(df):
    """
    Score the last candle(s) using the 4 pattern functions.
    Returns (score_float, pattern_label_str).
    Scoring:
      Bullish Engulfing   → +2.0
      3 Green Candles     → +1.5
      Breakout Candle     → +2.0
      No Upper Wick       → +1.0
      Weak candle (body < 25% of range, bearish) → −2.0
    """
    score   = 0.0
    patterns = []

    be  = bullish_engulfing(df)
    tgc = three_green_candles(df)
    sbc = strong_breakout_candle(df)
    nuw = upper_wick_rejection(df)

    if be:
        score += 2.0
        patterns.append("Bullish Engulfing")
    if tgc:
        score += 1.5
        patterns.append("3 Green Candles")
    if sbc:
        score += 2.0
        patterns.append("Breakout Candle")
    if nuw:
        score += 1.0
        patterns.append("No Upper Wick")

    # Weak candle check — small-body bearish candle
    try:
        o = float(df["Open"].iloc[-1]); c = float(df["Close"].iloc[-1])
        h = float(df["High"].iloc[-1]); l = float(df["Low"].iloc[-1])
        rng = h - l
        body_ratio = abs(c - o) / rng if rng > 0 else 0
        if c < o and body_ratio < 0.25:
            score -= 2.0
            patterns.append("Weak Candle ⚠")
    except Exception:
        pass

    label = " + ".join(patterns) if patterns else "No Pattern"
    return round(score, 2), label

# ═══════════════════════════════════════════════════════════════════════════════

def phase2_analyze(candidates, market, scan_mode=SCAN_MODE_INTRADAY, ss=None):
    """
    Phase 2 — Deep analysis.
    FIX 3: del ind dict + gc.collect() after each stock to prevent
    RAM buildup when analyzing up to 80 candidates.
    """
    results = []
    min_score = market["min_score_used"]
    is_postmarket = (scan_mode == SCAN_MODE_POSTMARKET)
    if ss is None:
        ss = state.get("sector_strength", _empty_sector_strength())
    add_log(f"Phase 2 [{scan_mode}] - {len(candidates)} candidates (min score: {min_score}) ...")
    for c in candidates:
        sym = c["symbol"]
        add_log(f"  Analyzing {sym.replace('.NS','')} ...")
        earn = check_earnings(sym)
        if earn["skip"]:
            add_log(f"    Skipped - earnings in {earn['days']} days")
            continue
        ind = None  # ensure defined for cleanup block
        try:
            tk    = yf.Ticker(sym)
            df    = tk.history(period="60d", interval="1d")
            close = df["Close"].dropna()
            vol   = df["Volume"].dropna()
            if len(close) < 21:
                log.debug(f"Not enough data for {sym}"); continue
            cmp   = float(close.iloc[-1])
            rsi   = compute_rsi(close)
            ema20 = float(close.ewm(span=20, adjust=False).mean().iloc[-1])
            ema50 = float(close.ewm(span=50, adjust=False).mean().iloc[-1])
            ema9  = float(close.ewm(span=9,  adjust=False).mean().iloc[-1])
            roll  = close.rolling(20)
            bb_mid   = float(roll.mean().iloc[-1])
            bb_upper = bb_mid + 2*float(roll.std().iloc[-1])
            bb_lower = bb_mid - 2*float(roll.std().iloc[-1])
            ema12    = float(close.ewm(span=12, adjust=False).mean().iloc[-1])
            ema26    = float(close.ewm(span=26, adjust=False).mean().iloc[-1])
            macd_val = ema12 - ema26
            streak_ok = False
            if is_postmarket and len(close) >= 4:
                recent_chgs = [close.iloc[-i] > close.iloc[-i-1] for i in range(1,4)]
                streak_ok = sum(recent_chgs) >= 2
            ind = {"cmp": cmp, "rsi": rsi, "macd": macd_val, "macd_sig": macd_val*0.9,
                   "ema9": ema9, "ema20": ema20, "ema50": ema50,
                   "bb_upper": bb_upper, "bb_lower": bb_lower, "bb_mid": bb_mid,
                   "streak_ok": streak_ok, "_tk": tk}
        except Exception as e:
            log.debug(f"Analysis failed {sym}: {e}")
            gc.collect()
            continue

        cmp = ind["cmp"]
        bd = {}; reasons = []; warns = []

        if earn["warn"]:
            warns.append(f"Earnings in ~{earn['days']} days — use smaller position size")
        if c["in_strong_sector"] and c["sector"] != "OTHER":
            reasons.append(f"Sector tailwind — {c['sector']} outperforming market")

        vol_r = c["vol_ratio"]; chg = c["chg"]
        if vol_r >= 2.5 and chg >= 1.5:
            bd["breakout"] = S["breakout"]
            reasons.append(f"Strong breakout — {vol_r:.1f}x volume surge with {chg:.1f}% move")
        elif vol_r >= 2.0 and chg >= 0.8:
            bd["breakout"] = S["breakout"] * 0.8
            reasons.append(f"Breakout — {vol_r:.1f}x volume with {chg:.1f}% move")
        elif vol_r >= 1.8 and chg >= 0.5:
            bd["breakout"] = S["breakout"] * 0.5
            reasons.append(f"Early breakout forming — {vol_r:.1f}x volume, {chg:.1f}% move")

        # ── TREND signal — based on stock's OWN EMAs, not Nifty ──────────────
        # Nifty being below EMA20 is informational only, not a gate.
        # A stock above its own EMA20 while Nifty is down = RELATIVE STRENGTH.
        e9 = ind["ema9"]; e20 = ind["ema20"]; e50 = ind["ema50"]
        nifty_below = market.get("nifty_below", False)

        if cmp > e9 > e20 > e50:
            bd["trend"] = S["trend"]
            if nifty_below:
                # Relative strength — stock strong while market weak
                bd["trend"] = min(S["trend"] + 0.5, S["trend"] * 1.25)
                reasons.append(f"⭐ RELATIVE STRENGTH — Perfect EMA alignment while Nifty below EMA20 · Institutions accumulating")
            else:
                reasons.append(f"Perfect EMA alignment — price > EMA9 > EMA20 > EMA50 (strong uptrend)")
        elif cmp > e20 > e50:
            bd["trend"] = S["trend"] * 0.75
            if nifty_below:
                bd["trend"] = min(S["trend"], bd["trend"] + 0.3)
                reasons.append(f"⭐ RELATIVE STRENGTH — Uptrend intact (EMA20+EMA50) while Nifty weak · Bullish divergence")
            else:
                reasons.append(f"Uptrend confirmed — price above EMA20({e20:.0f}) and EMA50({e50:.0f})")
        elif cmp > e20:
            bd["trend"] = S["trend"] * 0.4
            if nifty_below:
                reasons.append(f"Stock holding above EMA20({e20:.0f}) while Nifty is weak — watch closely")
            else:
                reasons.append("Price above EMA20 — short-term uptrend, watch for EMA50 crossover")
        else:
            # Stock below own EMA20 — genuine weakness regardless of Nifty
            warns.append(f"Price below own EMA20({e20:.0f}) — stock in downtrend, skip unless very high score")
            if nifty_below:
                warns.append("Both stock and Nifty below EMA20 — avoid this trade")

        if is_postmarket and ind.get("streak_ok"):
            bd["trend"] = min(S["trend"], bd.get("trend", 0) + 0.4)
            reasons.append("Closed higher in 2 of last 3 sessions — sustained momentum")

        macd = ind["macd"]; msig = ind["macd_sig"]
        if macd > msig and macd > 0:
            bd["macd"] = S["macd"]
            reasons.append(f"MACD bullish — above signal line AND above zero (strong momentum)")
        elif macd > msig and macd > -0.5:
            bd["macd"] = S["macd"] * 0.7
            reasons.append("MACD crossed above signal — bullish momentum building")
        elif macd > msig:
            bd["macd"] = S["macd"] * 0.4
            reasons.append("MACD crossover — early signal, wait for confirmation")
        elif macd < msig and macd < 0:
            warns.append("MACD negative and below signal — bearish momentum, be cautious")

        rsi = ind["rsi"]
        if 52 <= rsi <= 65:
            bd["rsi"] = S["rsi"]
            reasons.append(f"RSI {rsi:.1f} — ideal momentum zone (52-65), room to reach targets")
        elif 65 < rsi <= 72:
            bd["rsi"] = S["rsi"] * 0.6
            reasons.append(f"RSI {rsi:.1f} — elevated but ok, target T1 first")
            warns.append(f"RSI {rsi:.1f} approaching overbought — book T1 quickly")
        elif rsi > 72:
            warns.append(f"RSI {rsi:.1f} overbought — high reversal risk, only enter on dip")
        elif 45 <= rsi < 52:
            bd["rsi"] = S["rsi"] * 0.7
            reasons.append(f"RSI {rsi:.1f} — recovering from neutral, momentum building")
        elif 38 <= rsi < 45 and is_postmarket:
            bd["rsi"] = S["rsi"] * 0.5
            reasons.append(f"RSI {rsi:.1f} — oversold bounce candidate for tomorrow")
        elif rsi < 38:
            warns.append(f"RSI {rsi:.1f} — deeply oversold, may continue falling before bounce")

        if vol_r >= 3.0:
            bd["volume"] = S["volume"]
            reasons.append(f"Exceptional volume — {vol_r:.1f}x average (institutional activity)")
        elif vol_r >= 2.0:
            bd["volume"] = S["volume"] * 0.8
            reasons.append(f"Strong volume — {vol_r:.1f}x average (genuine buying)")
        elif vol_r >= 1.5:
            bd["volume"] = S["volume"] * 0.5
            reasons.append(f"Above-average volume — {vol_r:.1f}x (interest building)")

        bb_range = ind["bb_upper"] - ind["bb_lower"]
        bb_pos   = (cmp - ind["bb_lower"]) / bb_range if bb_range > 0 else 0.5
        bb_width_pct = bb_range / ind["bb_mid"] * 100 if ind["bb_mid"] > 0 else 0
        if 0.5 <= bb_pos <= 0.80:
            bd["bb"] = S["bb"]
            reasons.append(f"Price in upper BB zone ({bb_pos:.0%}) — momentum with room before resistance")
        elif 0.35 <= bb_pos < 0.5:
            bd["bb"] = S["bb"] * 0.5
            reasons.append(f"Price mid-BB — building towards upper zone")
        elif bb_pos > 0.80:
            warns.append(f"Near upper Bollinger Band ({bb_pos:.0%}) — watch for resistance, target T1 first")
        if bb_width_pct < 2.0:
            warns.append("Bollinger Bands squeezing — expect big move soon, direction uncertain")

        open_p = c.get("open_today", cmp)
        high_p = c.get("high_today", cmp)
        low_p  = c.get("low_today", cmp)
        body   = cmp - open_p
        rng    = high_p - low_p if high_p > low_p else 1
        lower_wick = open_p - low_p if cmp >= open_p else cmp - low_p
        if body > 0 and body/rng > 0.6 and (high_p - cmp)/rng < 0.15:
            bd["candle"] = S["candle"]
            reasons.append("Strong bullish candle — closed near day high with small upper wick")
        elif lower_wick > 2 * abs(body) and (high_p - max(cmp, open_p))/rng < 0.2:
            bd["candle"] = S["candle"] * 0.8
            reasons.append("Hammer candle — strong rejection of lower prices, buyers in control")

        try:
            raw_news  = (ind.get("_tk") or yf.Ticker(sym)).news or []
            headlines = [n.get("title","") for n in raw_news[:6] if n.get("title")]
            nl = " ".join(headlines).lower()
            pos_words = ["surge","rally","buy","upgrade","record","beat","profit",
                         "growth","strong","bullish","order","win","contract","expand"]
            neg_words = ["fall","drop","loss","sell","downgrade","weak","penalty",
                         "fraud","probe","fir","sebi","crash","default","recall","debt"]
            pos_hits = sum(w in nl for w in pos_words)
            neg_hits = sum(w in nl for w in neg_words)
            if neg_hits >= 2:
                warns.append(f"Negative news detected — {neg_hits} risk keywords found, be cautious")
            elif pos_hits > neg_hits and pos_hits >= 2:
                bd["news"] = S["news"]
                reasons.append(f"Positive news tailwind — {pos_hits} bullish keywords in recent headlines")
            elif pos_hits > neg_hits and pos_hits == 1:
                bd["news"] = S["news"] * 0.5
                reasons.append("Mild positive news sentiment")
            c["news"] = headlines[:3]
        except Exception:
            c["news"] = []

        hard_reject = False
        if rsi > 78:
            hard_reject = True
            add_log(f"    REJECT {sym.replace('.NS','')} — RSI {rsi:.1f} too overbought")
        if cmp < e20 * 0.97:
            hard_reject = True
            add_log(f"    REJECT {sym.replace('.NS','')} — price significantly below EMA20")
        if hard_reject:
            # ── FIX 3: free memory on hard reject too ────────────────────────
            del ind
            gc.collect()
            continue

        score = round(sum(bd.values()), 2)
        if score < min_score:
            add_log(f"    Score {score:.1f} < {min_score} - skipped")
            del ind
            gc.collect()
            continue

        # ── CANDLESTICK CONFIRMATION (ADD-ON — existing score untouched) ─────
        c_score, c_pattern = candle_score(df)
        if c_score <= 0:
            add_log(f"    REJECT {sym.replace('.NS','')} — candle_score {c_score:.1f} ({c_pattern})")
            del ind
            gc.collect()
            continue
        if c_score >= 2.0:
            reasons.append(f"📊 Candle: {c_pattern} (candle score +{c_score:.1f})")
        else:
            reasons.append(f"📊 Candle confirmation: {c_pattern} (+{c_score:.1f})")
        # ─────────────────────────────────────────────────────────────────────

        # ── SECTOR STRENGTH FILTER (ADD-ON — existing score untouched) ───────
        sec_pass, sec_reason, sec_strength, sec_avg_chg, stock_sss, sec_bonus = \
            apply_sector_filter(c, ss)
        if not sec_pass:
            add_log(f"    REJECT {sym.replace('.NS','')} — sector: {sec_reason}")
            del ind
            gc.collect()
            continue
        if sec_bonus > 0:
            reasons.append(
                f"🏆 Strong sector: {c['sector']} avg +{sec_strength:.2f}% today · "
                f"stock +{c['chg']:.2f}% vs sector avg +{sec_avg_chg:.2f}% · bonus +{sec_bonus:.0f}"
            )
        # ─────────────────────────────────────────────────────────────────────

        enhanced_score = round(score + c_score + sec_bonus, 2)

        buf = cmp * ENTRY_BUF / 100
        el = round(cmp - buf, 2); eh = round(cmp + buf, 2); mid = (el+eh)/2
        t1 = round(mid * (1 + T1_PCT/100), 2)
        t2 = round(mid * (1 + T2_PCT/100), 2)
        sl = round(el  * (1 - SL_PCT/100), 2)
        rr = round((t1 - mid) / (mid - sl), 2) if mid > sl else 0
        add_log(
            f"    PASS {sym.replace('.NS','')} score={score:.1f} candle={c_score:.1f} "
            f"sec_bonus={sec_bonus:.0f} enhanced={enhanced_score:.1f} "
            f"RSI={ind['rsi']:.1f} RR=1:{rr:.1f}"
        )
        results.append({
            "symbol": sym, "score": score, "enhanced_score": enhanced_score,
            "candle_score": c_score, "candle_pattern": c_pattern,
            "sector_bonus": sec_bonus,
            "sector_strength": round(sec_strength, 3),
            "sector_avg_chg": round(sec_avg_chg, 3),
            "stock_strength_score": round(stock_sss, 3),
            "breakdown": bd, "ind": ind,
            "chg": c["chg"], "vol_ratio": c["vol_ratio"], "sector": c["sector"],
            "news": c["news"], "reasons": reasons, "warns": warns, "earn": earn,
            "levels": {"cmp": cmp, "entry_low": el, "entry_high": eh,
                       "target1": t1, "target2": t2, "stop_loss": sl, "rr_ratio": rr},
        })
        # Note: do NOT del ind here — it's referenced in results["ind"] above.
        # GC handles it after the result dict goes out of scope post-email.

    results.sort(key=lambda x: x["enhanced_score"], reverse=True)
    add_log(f"Phase 2 complete - {len(results)} qualify")
    state["phase2_count"] = len(results)
    return results

# ---- EMAIL ----

def build_and_send_email(stocks, market, scan_mode=SCAN_MODE_INTRADAY):
    resend_key = os.getenv("RESEND_API_KEY","")
    sender     = os.getenv("ALERT_EMAIL_SENDER","")
    recips     = [r.strip() for r in os.getenv("ALERT_EMAIL_RECIPIENTS","").split(",") if r.strip()]
    if not resend_key or not sender or not recips:
        add_log("Email config missing"); return False

    now  = datetime.now(IST).strftime("%d %b %Y  %H:%M IST")
    syms = ", ".join(s["symbol"].replace(".NS","") for s in stocks)
    mode_tag = "[POST-MKT] " if scan_mode == SCAN_MODE_POSTMARKET else "[INTRADAY] "
    subject = f"{'WEAK MARKET - ' if market['is_bad'] else ''}{mode_tag}BUY SIGNAL | {syms} | StockSense AI"

    macro     = state.get("macro_intel", {})
    mscore    = macro.get("macro_score", 0)
    msummary  = macro.get("summary", "")
    mc        = "#00C853" if mscore >= 1 else ("#FF9900" if mscore >= -1 else "#FF1744")
    mbg       = f"{mc}15"
    mborder   = f"{mc}44"
    macro_signals_html = "".join(
        f'<div style="font-size:11px;color:#B0BEC5;margin-bottom:3px">{s}</div>'
        for s in macro.get("signals", [])[:5]
    )
    macro_banner = f"""
    <div style="background:{mbg};border:1px solid {mborder};border-radius:10px;padding:14px 18px;margin-bottom:16px">
      <div style="font-size:10px;font-weight:700;color:{mc};letter-spacing:1px;margin-bottom:4px">
        MACRO INTELLIGENCE · Score {mscore:+d}
      </div>
      <div style="font-size:12px;color:#CFD8DC;margin-bottom:8px;font-weight:600">{msummary}</div>
      {macro_signals_html}
    </div>""" if macro else ""

    mh_color  = "#FF9900" if market["is_bad"] else "#00C853"
    mh_bg     = "#FF990015" if market["is_bad"] else "#00C85315"
    mh_border = "#FF990044" if market["is_bad"] else "#00C85344"
    mh_label  = "WEAK MARKET CONDITIONS" if market["is_bad"] else "HEALTHY MARKET CONDITIONS"
    mh_msg = (
        f"Market health {market['score']}/4. "
        + ("Nifty BELOW EMA20 (bearish). " if not market.get("nifty_ok") else "Nifty above EMA20 (bullish). ")
        + (f"Strong sectors: {', '.join(market['strong_sectors'])}. " if market["strong_sectors"] else "")
        + f"Breadth: {market.get('breadth_pct_up',50):.0f}% stocks advancing. "
        + (f"Only stocks scoring above {MIN_SCORE_BAD_MARKET}/10 included." if market["is_bad"] else "")
    )
    market_banner = f"""
    <div style="background:{mh_bg};border:1px solid {mh_border};border-radius:10px;padding:14px 18px;margin-bottom:20px">
      <div style="font-size:11px;font-weight:700;color:{mh_color};letter-spacing:1px;margin-bottom:6px">{'WARNING - ' if market['is_bad'] else ''}{mh_label}</div>
      <div style="font-size:12px;color:#B0BEC5;line-height:1.6">{mh_msg}</div>
      <div style="margin-top:10px;font-size:11px;font-family:monospace;color:#607D8B">
        Nifty <span style="color:{'#00C853' if market.get('nifty_ok') else '#FF1744'}">{market.get('nifty_value',0):,}</span>
        &nbsp;&nbsp; A/D <span style="color:#E8F4FD">{market.get('breadth_ratio',1)}</span>
        &nbsp;&nbsp; Health <span style="color:#FFD600">{market['score']}/4</span>
      </div>
    </div>"""

    def tag(active, label, color="#4CAF50"):
        bg = f"{color}20" if active else "#37474F20"
        tc = color if active else "#546E7A"
        return f'<span style="background:{bg};color:{tc};border:1px solid {tc}40;padding:3px 10px;border-radius:4px;font-size:11px;font-weight:600;margin:2px 3px 2px 0;display:inline-block">{label}</span>'

    cards = ""
    plain = f"STOCKSENSE AI — BUY SIGNAL\n{now}\n{'='*52}\n"
    plain += f"Market Health: {market['score']}/4 | Macro: {state.get('macro_intel',{}).get('macro_score',0):+d}\n\n"
    if market["is_bad"]:
        plain += f"WEAK MARKET — only score >{MIN_SCORE_BAD_MARKET} signals included.\n\n"

    for s in stocks:
        sym = s["symbol"].replace(".NS","")
        tl = s["levels"]; ind = s["ind"]; bd = s["breakdown"]
        score_pct = int(s["score"] / MAX_SCORE * 100)
        plain += (
            f"{'='*52}\n"
            f"  {sym}  [{s['sector']}]  Score {s['score']:.1f}/{MAX_SCORE:.0f} ({score_pct}%)\n"
            f"{'='*52}\n"
            f"  CMP        : Rs.{tl['cmp']:,.2f}\n"
            f"  Buy Range  : Rs.{tl['entry_low']:,.2f} — Rs.{tl['entry_high']:,.2f}\n"
            f"  Target 1   : Rs.{tl['target1']:,.2f}  (+{T1_PCT}%)  book 50% here\n"
            f"  Target 2   : Rs.{tl['target2']:,.2f}  (+{T2_PCT}%)  book rest here\n"
            f"  Stop Loss  : Rs.{tl['stop_loss']:,.2f}  (-{SL_PCT}%)  exit immediately if hit\n"
            f"  R:R Ratio  : 1:{tl['rr_ratio']:.1f}\n"
            f"  RSI        : {ind['rsi']:.1f}  |  Volume: {s['vol_ratio']:.1f}x avg\n"
            f"  Candle     : {s['candle_pattern']}  |  Candle Score: +{s['candle_score']:.1f}\n"
            f"  Enhanced   : {s['enhanced_score']:.1f}/{MAX_SCORE:.0f}  "
            f"(base {s['score']:.1f} + candle {s['candle_score']:.1f} + sector bonus {s['sector_bonus']:.0f})\n"
            f"  Sector     : {s['sector']}  |  Sector Strength: {s['sector_strength']:+.2f}% avg  "
            f"|  Sector Bonus: +{s['sector_bonus']:.0f}\n"
            f"  Stock SSS  : {s['stock_strength_score']:.3f}  "
            f"(1D {s['chg']:+.2f}% vs sector avg {s['sector_avg_chg']:+.2f}%)\n\n"
            f"  WHY BUY:\n"
        )
        plain += "".join(f"   + {r}\n" for r in s["reasons"])
        if s["warns"]:
            plain += "\n  CAUTION:\n"
            plain += "".join(f"   ! {w}\n" for w in s["warns"])
        plain += "\n"
    plain += f"{'='*52}\nNot financial advice. Always place stop loss immediately after buying.\n"

    mode_color = "#FF9900" if market["is_bad"] else ("#7C4DFF" if scan_mode == SCAN_MODE_POSTMARKET else "#2196F3")
    mode_label = "POST-MARKET SETUP" if scan_mode == SCAN_MODE_POSTMARKET else "INTRADAY SIGNAL"
    hdr_title  = "WEAK MARKET ALERT" if market["is_bad"] else f"BUY ALERT — {mode_label}"

    for s in stocks:
        sym = s["symbol"].replace(".NS","")
        tl = s["levels"]; ind = s["ind"]; bd = s["breakdown"]
        score_pct = int(s["score"] / MAX_SCORE * 100)
        bar_color = "#4CAF50" if score_pct >= 70 else ("#FF9800" if score_pct >= 50 else "#F44336")

        tags_html = (
            tag("breakout" in bd, "BREAKOUT", "#2196F3") +
            tag("trend"    in bd, "UPTREND",  "#4CAF50") +
            tag("macd"     in bd, "MACD",     "#9C27B0") +
            tag("rsi"      in bd, f"RSI {ind['rsi']:.0f}", "#FF9800") +
            tag("volume"   in bd, f"VOL {s['vol_ratio']:.1f}x", "#00BCD4") +
            tag("candle"   in bd, "CANDLE",   "#FF5722") +
            tag("bb"       in bd, "BB ZONE",  "#607D8B") +
            tag(s["sector"] in market.get("strong_sectors",[]), s["sector"], "#4CAF50")
        )

        reasons_html = "".join(
            '<tr>'
            '<td style="padding:5px 0;vertical-align:top;color:#4CAF50;font-size:14px;width:20px">&#10003;</td>'
            f'<td style="padding:5px 0;color:#ECEFF1;font-size:13px;line-height:1.5">{r}</td>'
            '</tr>'
            for r in s["reasons"]
        )

        warns_html = ""
        if s["warns"]:
            w_rows = "".join(
                '<tr>'
                '<td style="padding:4px 0;vertical-align:top;color:#FF9800;font-size:13px;width:20px">!</td>'
                f'<td style="padding:4px 0;color:#FFB74D;font-size:12px;line-height:1.5">{w}</td>'
                '</tr>'
                for w in s["warns"]
            )
            warns_html = (
                '<div style="background:#FF980008;border-left:3px solid #FF9800;'
                'border-radius:0 6px 6px 0;padding:12px 14px;margin-top:14px">'
                '<div style="font-size:10px;color:#FF9800;font-weight:700;letter-spacing:1px;margin-bottom:8px">CAUTION</div>'
                f'<table style="width:100%;border-collapse:collapse">{w_rows}</table>'
                '</div>'
            )

        news_html = ""
        if s["news"]:
            news_rows = "".join(
                f'<div style="padding:5px 0;border-bottom:1px solid #1E3248;'
                f'color:#90A4AE;font-size:12px;line-height:1.5">{h[:120]}</div>'
                for h in s["news"]
            )
            news_html = (
                '<div style="margin-top:14px">'
                '<div style="font-size:10px;color:#546E7A;font-weight:700;'
                'letter-spacing:1px;margin-bottom:8px">RECENT NEWS</div>'
                f'{news_rows}</div>'
            )

        cards += (
            '<div style="background:#0D1F2D;border:1px solid #1E3248;'
            'border-radius:10px;margin-bottom:20px;overflow:hidden">'
            '<div style="background:#112233;padding:18px 20px;border-bottom:1px solid #1E3248">'
            '<div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:8px">'
            '<div>'
            f'<div style="font-size:24px;font-weight:800;color:#FFFFFF">{sym}</div>'
            f'<div style="font-size:12px;color:#546E7A;margin-top:2px">{s["sector"]} · NSE · +{s["chg"]:.2f}% today</div>'
            '</div>'
            '<div style="text-align:right">'
            f'<div style="font-size:26px;font-weight:800;color:{bar_color}">'
            f'{s["score"]:.1f}<span style="font-size:12px;color:#546E7A">/{MAX_SCORE:.0f}</span></div>'
            '</div></div>'
            f'<div style="margin-top:12px">{tags_html}</div>'
            '</div>'
            '<div style="padding:0"><table style="width:100%;border-collapse:collapse">'
            '<tr style="background:#0A1929">'
            '<td style="padding:14px 20px;color:#78909C;font-size:12px;width:45%">Current Price</td>'
            f'<td style="padding:14px 20px;text-align:right;color:#FFFFFF;font-weight:800;font-size:20px">Rs.{tl["cmp"]:,.2f}</td>'
            '</tr>'
            '<tr style="background:#0D1F2D;border-top:1px solid #1E3248">'
            '<td style="padding:11px 20px;color:#78909C;font-size:12px">Buy Range</td>'
            f'<td style="padding:11px 20px;text-align:right;color:#64B5F6;font-weight:700">Rs.{tl["entry_low"]:,.2f} — Rs.{tl["entry_high"]:,.2f}</td>'
            '</tr>'
            '<tr style="background:#071A14;border-top:1px solid #1E3248">'
            f'<td style="padding:11px 20px;color:#78909C;font-size:12px">Target 1 (book 50%)</td>'
            f'<td style="padding:11px 20px;text-align:right;color:#66BB6A;font-weight:800">Rs.{tl["target1"]:,.2f} <span style="font-size:11px">+{T1_PCT}%</span></td>'
            '</tr>'
            '<tr style="background:#071A14;border-top:1px solid #1A2E1E">'
            f'<td style="padding:11px 20px;color:#78909C;font-size:12px">Target 2 (book rest)</td>'
            f'<td style="padding:11px 20px;text-align:right;color:#81C784;font-weight:800">Rs.{tl["target2"]:,.2f} <span style="font-size:11px">+{T2_PCT}%</span></td>'
            '</tr>'
            '<tr style="background:#1A0A0A;border-top:1px solid #2E1A1A">'
            '<td style="padding:11px 20px;color:#78909C;font-size:12px">Stop Loss</td>'
            f'<td style="padding:11px 20px;text-align:right;color:#EF5350;font-weight:800">Rs.{tl["stop_loss"]:,.2f} <span style="font-size:11px">-{SL_PCT}%</span></td>'
            '</tr>'
            '</table></div>'
            '<div style="background:#0A1929;padding:10px 20px;border-top:1px solid #1E3248">'
            f'<span style="font-size:12px;color:#546E7A">R:R <strong style="color:#FFD54F">1:{tl["rr_ratio"]:.1f}</strong></span>'
            f'&nbsp;&nbsp;<span style="font-size:12px;color:#546E7A">RSI <strong style="color:#ECEFF1">{ind["rsi"]:.1f}</strong></span>'
            f'&nbsp;&nbsp;<span style="font-size:12px;color:#546E7A">Vol <strong style="color:#ECEFF1">{s["vol_ratio"]:.1f}x</strong></span>'
            f'&nbsp;&nbsp;<span style="font-size:12px;color:#546E7A">Enhanced Score <strong style="color:#00E5FF">{s["enhanced_score"]:.1f}</strong></span>'
            '</div>'
            '<div style="background:#071220;padding:9px 20px;border-top:1px solid #1E3248">'
            f'<span style="font-size:11px;color:#546E7A">📊 Candle: </span>'
            f'<strong style="font-size:11px;color:#FF7043">{s["candle_pattern"]}</strong>'
            f'<span style="font-size:11px;color:#546E7A;margin-left:10px">Candle Score: </span>'
            f'<strong style="font-size:11px;color:#{"00C853" if s["candle_score"] >= 2 else "FF9800"}">'
            f'+{s["candle_score"]:.1f}</strong>'
            '</div>'
            '<div style="background:#061810;padding:9px 20px;border-top:1px solid #1E3248">'
            f'<span style="font-size:11px;color:#546E7A">🏆 Sector: </span>'
            f'<strong style="font-size:11px;color:#00C853">{s["sector"]}</strong>'
            f'<span style="font-size:11px;color:#546E7A;margin-left:10px">Strength: </span>'
            f'<strong style="font-size:11px;color:#{"00C853" if s["sector_strength"] >= 1 else "FF9800"}">'
            f'{s["sector_strength"]:+.2f}%</strong>'
            f'<span style="font-size:11px;color:#546E7A;margin-left:10px">SSS: </span>'
            f'<strong style="font-size:11px;color:#B0BEC5">{s["stock_strength_score"]:.2f}</strong>'
            f'<span style="font-size:11px;color:#546E7A;margin-left:10px">Sector Bonus: </span>'
            f'<strong style="font-size:11px;color:#00E5FF">+{s["sector_bonus"]:.0f}</strong>'
            '</div>'
            '<div style="padding:16px 20px;border-top:1px solid #1E3248">'
            '<div style="font-size:10px;color:#546E7A;font-weight:700;letter-spacing:1px;margin-bottom:12px">WHY THIS STOCK</div>'
            f'<table style="width:100%;border-collapse:collapse">{reasons_html}</table>'
            f'{warns_html}{news_html}'
            '</div>'
            '</div>'
        )

    html = (
        "<!DOCTYPE html><html><head>"
        '<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
        "</head>"
        '<body style="margin:0;padding:0;background:#060E18;font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',Roboto,sans-serif">'
        '<table width="100%" cellpadding="0" cellspacing="0" style="background:#060E18">'
        '<tr><td align="center" style="padding:24px 12px">'
        '<table width="600" style="max-width:600px;width:100%">'
        f'<tr><td style="background:#0D1F2D;border-radius:10px 10px 0 0;padding:28px 24px;border-bottom:3px solid {mode_color}">'
        f'<div style="font-size:10px;color:{mode_color};letter-spacing:2px;font-weight:700;margin-bottom:6px">STOCKSENSE AI</div>'
        f'<div style="font-size:28px;font-weight:800;color:#FFFFFF">{hdr_title}</div>'
        f'<div style="font-size:12px;color:#546E7A;margin-top:8px">{now} · {len(stocks)} stock{"s" if len(stocks)!=1 else ""}</div>'
        '</td></tr>'
        '<tr><td style="background:#0A1929;padding:16px 24px;border-bottom:1px solid #1E3248">'
        f'{macro_banner}{market_banner}'
        '</td></tr>'
        f'<tr><td style="background:#060E18;padding:16px 24px">{cards}</td></tr>'
        '<tr><td style="background:#0D1F2D;border-radius:0 0 10px 10px;padding:16px 24px;border-top:1px solid #1E3248">'
        '<div style="font-size:11px;color:#37474F">Automated signal — not financial advice. Place stop loss immediately.</div>'
        '</td></tr>'
        '</table></td></tr></table></body></html>'
    )

    try:
        resp = requests.post("https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {resend_key}", "Content-Type": "application/json"},
            json={"from": f"StockSense AI <{sender}>", "to": recips, "subject": subject, "text": plain, "html": html},
            timeout=20)
        if resp.status_code in (200,201):
            add_log(f"Email sent to {recips}"); return True
        else:
            add_log(f"Resend error {resp.status_code}: {resp.text[:200]}"); return False
    except Exception as e:
        add_log(f"Email failed: {e}"); return False


# ═══════════════════════════════════════════════════════════════════════════════
# EXIT MONITOR
# ═══════════════════════════════════════════════════════════════════════════════

EXIT_MONITOR_INTERVAL = 10

def send_exit_email(position, exit_reason, current_price, triggers):
    resend_key = os.getenv("RESEND_API_KEY", "")
    sender     = os.getenv("ALERT_EMAIL_SENDER", "")
    recips     = [r.strip() for r in os.getenv("ALERT_EMAIL_RECIPIENTS", "").split(",") if r.strip()]
    if not resend_key or not sender or not recips:
        add_log("EXIT email: config missing"); return False

    sym          = position["display"]
    entry        = position["entry_price"]
    pnl_pct      = round((current_price - entry) / entry * 100, 2)
    pnl_sign     = "+" if pnl_pct >= 0 else ""
    pnl_color    = "#00C853" if pnl_pct >= 0 else "#FF1744"
    action_color = "#FF1744" if pnl_pct < 0 else "#FF9900"
    now          = datetime.now(IST).strftime("%d %b %Y  %H:%M IST")

    subject = f"🚨 EXIT NOW: {sym} | {pnl_sign}{pnl_pct}% | {exit_reason} | StockSense AI"

    trigger_html = "".join(
        f'<div style="display:flex;gap:8px;margin-bottom:8px">'
        f'<span style="color:#FF1744;font-size:14px">⚠</span>'
        f'<span style="color:#CFD8DC;font-size:13px;line-height:1.5">{t}</span></div>'
        for t in triggers
    )
    trigger_plain = "".join(f"  ! {t}\n" for t in triggers)

    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#0B1826;font-family:'Segoe UI',Tahoma,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0"><tr><td align="center" style="padding:28px 12px">
<table width="600" style="max-width:600px;width:100%">
  <tr><td style="background:#1A0A0A;border-radius:16px 16px 0 0;padding:30px 28px;border-bottom:3px solid #FF1744">
    <div style="font-size:11px;color:#FF1744;letter-spacing:3px;margin-bottom:8px">StockSense AI · Exit Alert</div>
    <div style="font-size:34px;font-weight:900;color:#fff">🚨 EXIT NOW</div>
    <div style="font-size:22px;font-weight:800;color:#FF6B6B;margin-top:4px">{sym}</div>
    <div style="font-size:12px;color:#607D8B;margin-top:8px">{now}</div>
  </td></tr>
  <tr><td style="background:#0F1F2E;padding:24px 28px">
    <div style="background:{"#00C85315" if pnl_pct >= 0 else "#FF174415"};border:1px solid {"#00C85344" if pnl_pct >= 0 else "#FF174444"};border-radius:12px;padding:18px 22px;margin-bottom:20px">
      <div style="font-size:10px;color:#607D8B;letter-spacing:2px">P&L</div>
      <div style="font-size:36px;font-weight:900;color:{pnl_color}">{pnl_sign}{pnl_pct}%</div>
      <div style="font-size:12px;color:#78909C">Entry Rs.{entry:,.2f} → Now Rs.{current_price:,.2f}</div>
    </div>
    <div style="background:#1A0A0A;border:1px solid #FF174433;border-radius:10px;padding:16px 18px;margin-bottom:16px">
      <div style="font-size:10px;color:#FF1744;letter-spacing:2px;margin-bottom:12px">WHY EXIT NOW</div>
      {trigger_html}
    </div>
    <div style="background:#FF174410;border:2px solid #FF1744;border-radius:10px;padding:14px 18px;text-align:center">
      <div style="font-size:15px;font-weight:700;color:#FF6B6B">Place exit order immediately at market price</div>
    </div>
  </td></tr>
  <tr><td style="background:#080f1a;border-radius:0 0 16px 16px;padding:14px 28px;border-top:1px solid #1A2B3C">
    <div style="font-size:11px;color:#37474F">Automated exit signal — not financial advice.</div>
  </td></tr>
</table></td></tr></table></body></html>"""

    plain = (
        f"STOCKSENSE AI — EXIT ALERT\n{now}\n{'='*50}\n\n"
        f"STOCK: {sym}\nEXIT REASON: {exit_reason}\n\n"
        f"Entry: Rs.{entry:,.2f} | Now: Rs.{current_price:,.2f} | P&L: {pnl_sign}{pnl_pct}%\n\n"
        f"TRIGGERS:\n{trigger_plain}\nACTION: Exit immediately.\n"
    )

    try:
        resp = requests.post("https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {resend_key}", "Content-Type": "application/json"},
            json={"from": f"StockSense AI <{sender}>", "to": recips,
                  "subject": subject, "text": plain, "html": html},
            timeout=20)
        if resp.status_code in (200, 201):
            add_log(f"EXIT email sent: {sym} | {exit_reason} | {pnl_sign}{pnl_pct}%")
            return True
        else:
            add_log(f"EXIT email error {resp.status_code}: {resp.text[:100]}")
            return False
    except Exception as e:
        add_log(f"EXIT email failed: {e}")
        return False


def check_exit_signals(position):
    sym = position["symbol"]
    entry = position["entry_price"]
    triggers = []
    should_exit = False
    primary_reason = ""

    try:
        tk = yf.Ticker(sym)
        df_intra = tk.history(period="2d", interval="15m")
        df_daily = tk.history(period="30d", interval="1d")

        if df_intra.empty or df_daily.empty:
            return False, "", 0, []

        close_d = df_daily["Close"].dropna()
        current_price   = float(df_intra["Close"].iloc[-1])
        current_vol_15m = float(df_intra["Volume"].iloc[-1])

        if current_price <= position["sl"]:
            triggers.append(f"Stop Loss breached — Rs.{current_price:,.2f} below SL Rs.{position['sl']:,.2f}")
            primary_reason = "Stop Loss Hit"
            should_exit = True

        if current_price >= position["t2"]:
            triggers.append(f"Target 2 hit — Rs.{current_price:,.2f} reached T2 Rs.{position['t2']:,.2f}")
            primary_reason = "Target 2 Hit — Book Full Profit"
            should_exit = True

        elif current_price >= position["t1"] and not position.get("t1_hit"):
            triggers.append(f"Target 1 reached Rs.{position['t1']:,.2f} — consider booking 50-60% position")
            position["t1_hit"] = True
            primary_reason = "Target 1 Hit — Partial Profit"
            should_exit = True

        if not should_exit:
            ema20 = float(close_d.ewm(span=20, adjust=False).mean().iloc[-1])
            if current_price < ema20 and entry > ema20:
                triggers.append(f"Price Rs.{current_price:,.2f} broke BELOW EMA20 Rs.{ema20:,.2f}")
                should_exit = True
                primary_reason = "EMA20 Breakdown"

            ema12 = float(close_d.ewm(span=12, adjust=False).mean().iloc[-1])
            ema26 = float(close_d.ewm(span=26, adjust=False).mean().iloc[-1])
            macd_now = ema12 - ema26
            ema12_p  = float(close_d.iloc[:-1].ewm(span=12, adjust=False).mean().iloc[-1])
            ema26_p  = float(close_d.iloc[:-1].ewm(span=26, adjust=False).mean().iloc[-1])
            macd_prev = ema12_p - ema26_p
            if macd_now < 0 and macd_prev >= 0:
                triggers.append("MACD crossed BELOW zero — bearish momentum confirmed")
                should_exit = True
                if not primary_reason: primary_reason = "MACD Bearish Crossover"

            rsi_now = compute_rsi(close_d)
            if rsi_now > 78:
                triggers.append(f"RSI {rsi_now:.1f} extremely overbought — high reversal probability")
                should_exit = True
                if not primary_reason: primary_reason = "RSI Overbought Reversal Risk"

            if len(df_intra) >= 3:
                last_close_5m = float(df_intra["Close"].iloc[-1])
                last_open_5m  = float(df_intra["Open"].iloc[-1])
                avg_vol_15m   = float(df_intra["Volume"].iloc[-10:].mean()) if len(df_intra) >= 10 else float(df_intra["Volume"].mean())
                if last_close_5m < last_open_5m and current_vol_15m > avg_vol_15m * 2.5:
                    triggers.append(f"Heavy volume RED candle — {current_vol_15m/avg_vol_15m:.1f}x avg selling")
                    should_exit = True
                    if not primary_reason: primary_reason = "High-Volume Selling Detected"

            try:
                news_items = tk.news or []
                headlines  = [n.get("title", "") for n in news_items[:5] if n.get("title")]
                nl = " ".join(headlines).lower()
                neg_words = ["crash","fraud","penalty","sebi","probe","loss","downgrade",
                             "fall","drop","bankrupt","fir","scam","raid","default","recall"]
                neg_hits  = [w for w in neg_words if w in nl]
                if len(neg_hits) >= 2:
                    triggers.append(f"Negative news: {', '.join(neg_hits[:3])}")
                    should_exit = True
                    if not primary_reason: primary_reason = "Negative News Alert"
            except Exception:
                pass

        return should_exit, primary_reason, current_price, triggers

    except Exception as e:
        log.debug(f"Exit check error {sym}: {e}")
        return False, "", 0, []


# ═══════════════════════════════════════════════════════════════════════════════
# FIX 2 — EXIT MONITOR: always runs every 10 min, emails only during market hours
#
# OLD behaviour: the entire function returned early if outside market hours.
#   Problem: positions added at 11 PM were NEVER checked the next morning
#   because the function exited before doing anything.
#
# NEW behaviour:
#   - Monitor runs every 10 min always (no time gate at the top)
#   - Fetches current price and checks signals regardless of time
#   - But only SENDS the exit email during market hours (9:10–15:40 IST)
#   - Positions added overnight are therefore ready and checked from 9:10 AM
# ═══════════════════════════════════════════════════════════════════════════════

def monitor_positions():
    now_ist = datetime.now(IST)

    # Skip weekends — markets are fully closed
    if now_ist.weekday() >= 5:
        return

    if not state["active_positions"]:
        return

    # Determine if we're in the email-sending window (market hours)
    market_open  = now_ist.replace(hour=9,  minute=10, second=0, microsecond=0)
    market_close = now_ist.replace(hour=15, minute=40, second=0, microsecond=0)
    can_email    = (market_open <= now_ist <= market_close)

    add_log(
        f"Position monitor — {len(state['active_positions'])} position(s) | "
        f"{'IN market hours — will email if exit triggered' if can_email else 'OUTSIDE market hours — monitoring only, no email'}"
    )

    exited = []
    for sym, position in list(state["active_positions"].items()):
        should_exit, reason, current_price, triggers = check_exit_signals(position)

        if should_exit and triggers and current_price > 0:
            if can_email:
                # Inside market hours — send the exit email
                sent = send_exit_email(position, reason, current_price, triggers)
                if sent:
                    pnl = round((current_price - position["entry_price"]) / position["entry_price"] * 100, 2)
                    state["exited_positions"].append({
                        "symbol":     position["display"],
                        "reason":     reason,
                        "entry":      position["entry_price"],
                        "exit_price": current_price,
                        "pnl_pct":    pnl,
                        "exited_at":  now_ist.strftime("%d %b %H:%M IST"),
                        "alerted_at": position["alerted_at"],
                    })
                    state["exited_positions"] = state["exited_positions"][-20:]

                    if "Target 1" in reason and "Target 2" not in reason:
                        add_log(f"  T1 nudge sent for {position['display']} — still monitoring")
                        # Save T1 hit flag to disk
                        save_positions()
                    else:
                        exited.append(sym)
                        add_log(f"  Position closed: {position['display']} | {reason} | PnL {pnl:+.2f}%")
            else:
                # Outside market hours — log the trigger but do NOT email
                # Prices from Yahoo Finance outside hours are unreliable (pre/post-market)
                add_log(
                    f"  [{position['display']}] Exit signal detected but outside market hours "
                    f"— will re-check at 9:10 AM. Trigger: {reason}"
                )

    for sym in exited:
        del state["active_positions"][sym]

    # ── FIX 1: persist positions after any change ─────────────────────────────
    if exited:
        save_positions()
        add_log(f"Monitor done — {len(exited)} position(s) exited, positions saved")
    else:
        add_log(f"Monitor done — {len(state['active_positions'])} position(s) holding")


# ---- PIPELINE ----

def run_pipeline(force=False, scan_mode=SCAN_MODE_INTRADAY):
    if state["running"]: add_log("Already running - skipped"); return
    now_ist = datetime.now(IST)
    if now_ist.weekday() >= 5 and not force:
        add_log("Weekend - skipped (use Run Scan Now to force)"); return
    if force: add_log("Manual run - weekend/hour check bypassed")
    state["running"] = True
    state["last_run"] = now_ist.strftime("%d %b %Y  %H:%M IST")
    add_log(f"=== StockSense AI v3 [{scan_mode}] started {state['last_run']} ===")
    try:
        macro = run_macro_intelligence()
        if macro["skip_scan"]:
            send_macro_caution_email(macro)
            state["last_result"] = f"SCAN SKIPPED — {macro['summary']}"
            state["last_stocks"] = []
            add_log(f"Scan skipped due to macro risk score {macro['macro_score']}")
            return
        elif macro["macro_score"] <= -2:
            send_macro_caution_email(macro)

        market = run_market_health_checks()
        market["strong_sectors"] = [
            s for s in market["strong_sectors"]
            if s not in macro["sector_suppresses"]
        ] + [s for s in macro["sector_boosts"] if s not in market.get("weak_sectors",[])]
        market["min_score_used"] = max(market["min_score_used"], macro["min_score_adj"])
        add_log(f"Effective min score: {market['min_score_used']:.1f}")

        symbols    = fetch_nse_symbols()
        candidates = phase1_scan(symbols, market["strong_sectors"])
        if not candidates:
            state["last_result"] = "Phase 1: No stocks passed filters"
            state["last_stocks"] = []; return

        # ── SECTOR STRENGTH ANALYSIS (ADD-ON) — single batch download ────────
        ss = run_sector_strength_analysis()
        # ─────────────────────────────────────────────────────────────────────

        finalists = phase2_analyze(candidates, market, scan_mode=scan_mode, ss=ss)
        if not finalists:
            state["last_result"] = f"No stocks met score above {market['min_score_used']}"
            state["last_stocks"] = []; return
        sent = build_and_send_email(finalists, market, scan_mode=scan_mode)

        now_reg = datetime.now(IST)
        for s in finalists:
            sym = s["symbol"]
            if sym not in state["active_positions"]:
                state["active_positions"][sym] = {
                    "symbol":        sym,
                    "display":       sym.replace(".NS",""),
                    "entry_price":   s["levels"]["cmp"],
                    "sl":            s["levels"]["stop_loss"],
                    "t1":            s["levels"]["target1"],
                    "t2":            s["levels"]["target2"],
                    "sector":        s["sector"],
                    "alerted_at":    now_reg.strftime("%d %b %H:%M IST"),
                    "t1_hit":        False,
                    "rsi_at_entry":  round(s["ind"]["rsi"], 1),
                }
                add_log(f"Position tracking started: {sym.replace('.NS','')} entry={s['levels']['cmp']:.2f}")

        # ── FIX 1: save positions to disk after every scan ────────────────────
        save_positions()

        if sent:
            state["alerts_sent"] += len(finalists)
            state["last_result"] = f"{'WEAK MKT - ' if market['is_bad'] else ''}Sent: {', '.join(s['symbol'].replace('.NS','') for s in finalists)}"
            state["last_stocks"] = [{
                "symbol": s["symbol"].replace(".NS",""), "score": s["score"],
                "enhanced_score": s["enhanced_score"],
                "candle_score": s["candle_score"], "candle_pattern": s["candle_pattern"],
                "sector_bonus": s["sector_bonus"],
                "sector_strength": s["sector_strength"],
                "sector_avg_chg": s["sector_avg_chg"],
                "stock_strength_score": s["stock_strength_score"],
                "score_max": MAX_SCORE, "cmp": s["levels"]["cmp"],
                "entry_low": s["levels"]["entry_low"], "entry_high": s["levels"]["entry_high"],
                "target1": s["levels"]["target1"], "target2": s["levels"]["target2"],
                "stop_loss": s["levels"]["stop_loss"], "rr_ratio": s["levels"]["rr_ratio"],
                "rsi": round(s["ind"]["rsi"],1), "vol_ratio": round(s["vol_ratio"],2),
                "chg": s["chg"], "sector": s["sector"],
            } for s in finalists]
        else:
            state["last_result"] = f"Email failed - positions tracked ({len(finalists)} stocks)"
    except Exception as e:
        add_log(f"Pipeline error: {e}"); state["last_result"] = f"Error: {e}"
    finally:
        state["running"] = False
        add_log(f"=== Done [{scan_mode}] ===")

# ---- FLASK ----

app = Flask(__name__)

DASH = """<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="60"><title>StockSense AI v3</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Sora:wght@400;600;800&display=swap');
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:#080f1a;color:#cdd9e5;font-family:'Sora',sans-serif;min-height:100vh}
  header{background:#0a1520;border-bottom:1px solid #1e3248;padding:16px 28px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px}
  header h1{font-size:19px;font-weight:800;color:#fff}header h1 span{color:#00e5ff}
  .badge{background:#00e5ff15;border:1px solid #00e5ff44;color:#00e5ff;font-size:10px;padding:3px 9px;border-radius:20px;font-family:'JetBrains Mono',monospace}
  .badge.live{background:#00c85315;border-color:#00c85344;color:#00c853}
  .badge.warn{background:#ff990015;border-color:#ff990044;color:#ff9900}
  .badge.bad{background:#ff174415;border-color:#ff174444;color:#ff1744}
  main{max-width:1200px;margin:0 auto;padding:24px 18px}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:11px;margin-bottom:18px}
  .card{background:#0f1e2e;border:1px solid #1e3248;border-radius:11px;padding:15px}
  .card.a{border-color:#00e5ff33;border-left:3px solid #00e5ff}
  .card.w{border-color:#ff990033;border-left:3px solid #ff9900}
  .lbl{font-size:10px;letter-spacing:2px;text-transform:uppercase;color:#4a6a8a;margin-bottom:4px;font-family:'JetBrains Mono',monospace}
  .val{font-size:17px;font-weight:800;color:#e8f4fd}
  .g{color:#00c853}.c{color:#00e5ff}.y{color:#ffd600}.r{color:#ff1744}.o{color:#ff9900}
  .mono{font-family:'JetBrains Mono',monospace;font-size:11px}
  h2{font-size:10px;letter-spacing:2px;text-transform:uppercase;color:#4a6a8a;margin-bottom:10px;font-family:'JetBrains Mono',monospace}
  .mkt{background:#0f1e2e;border-radius:11px;padding:16px;margin-bottom:18px;border:1px solid #1e3248}
  .mkt.bad{border-color:#ff990033}
  .fpill{padding:3px 12px;border-radius:20px;font-size:11px;font-family:'JetBrains Mono',monospace;border:1px solid;margin:3px 4px 3px 0;display:inline-block}
  .fpill.ok{background:#00c85315;border-color:#00c85344;color:#00c853}
  .fpill.fail{background:#ff174415;border-color:#ff174444;color:#ff1744}
  .fpill.warn{background:#ff990015;border-color:#ff990044;color:#ff9900}
  .sc{background:#0f1e2e;border:1px solid #1e3248;border-radius:11px;padding:16px;margin-bottom:12px}
  .sym{font-size:19px;font-weight:800;color:#e8f4fd}
  .pr{display:grid;grid-template-columns:repeat(auto-fit,minmax(100px,1fr));gap:7px;margin-top:9px}
  .pc{background:#162030;border-radius:7px;padding:8px 10px}
  .pl{font-size:9px;letter-spacing:1px;text-transform:uppercase;color:#4a6a8a;margin-bottom:2px;font-family:'JetBrains Mono',monospace}
  .pv{font-size:12px;font-weight:700;font-family:'JetBrains Mono',monospace}
  .bw{background:#1e3248;border-radius:4px;height:4px;margin:7px 0 10px}
  .b{height:4px;border-radius:4px;background:linear-gradient(90deg,#00e5ff,#ffd600)}
  .log{background:#060e18;border:1px solid #1e3248;border-radius:9px;padding:12px;height:230px;overflow-y:auto;font-family:'JetBrains Mono',monospace;font-size:11px;color:#4a7a9b;line-height:1.9}
  .log .ok{color:#00c853}.log .err{color:#ff1744}.log .info{color:#00e5ff}.log .wn{color:#ff9900}
  .btn{background:#00e5ff;color:#080f1a;border:none;padding:9px 20px;border-radius:7px;font-weight:700;font-size:12px;cursor:pointer;transition:opacity .2s}
  .btn:hover{opacity:.85}.btn:disabled{opacity:.4;cursor:not-allowed}
  .sp{background:#0f1e2e;border:1px solid #1e3248;border-radius:20px;padding:3px 11px;font-size:11px;color:#cdd9e5;font-family:'JetBrains Mono',monospace;display:inline-block;margin:3px}
  .sp.active{background:#00e5ff15;border-color:#00e5ff;color:#00e5ff}
</style></head><body>
<header>
  <h1>Stock<span>Sense</span> AI <span style="font-size:11px;color:#4a6a8a">v3</span></h1>
  <div style="display:flex;gap:7px;flex-wrap:wrap;align-items:center">
    {% if running %}<span class="badge warn">SCANNING</span>
    {% elif market_bad %}<span class="badge bad">WEAK MARKET</span>
    {% else %}<span class="badge live">LIVE</span>{% endif %}
    <span class="badge">Nifty 500</span><span class="badge">4-FILTER</span>
  </div>
</header>
<main>
  <div class="grid" style="margin-top:0">
    <div class="card a"><div class="lbl">Last Run</div><div class="val mono">{{ last_run }}</div></div>
    <div class="card a"><div class="lbl">Result</div><div class="val mono" style="font-size:11px">{{ last_result }}</div></div>
    <div class="card a"><div class="lbl">Alerts Sent</div><div class="val g">{{ alerts_sent }}</div></div>
    <div class="card a"><div class="lbl">Watching</div><div class="val c">{{ active_count }} <span style="font-size:10px;color:#4a6a8a">positions</span></div></div>
    <div class="card {% if macro_score < -1 %}w{% else %}a{% endif %}"><div class="lbl">Macro Score</div><div class="val {% if macro_score >= 1 %}g{% elif macro_score < -1 %}r{% else %}y{% endif %}">{{ macro_score }}</div></div>
    <div class="card a"><div class="lbl">Next Run</div><div class="val c mono" style="font-size:13px">{{ next_run }}</div></div>
    <div class="card a"><div class="lbl">TD Calls</div><div class="val y">{{ td_calls }}<span style="font-size:10px;color:#4a6a8a">/800</span></div></div>
  </div>

  <div class="mkt {% if macro_score < -1 %}bad{% endif %}" style="margin-bottom:10px">
    <h2 style="margin-bottom:6px">🌐 Macro Intelligence · Score {{ macro_score }}</h2>
    <div style="font-size:11px;color:{% if macro_score >= 1 %}#00c853{% elif macro_score < -1 %}#ff1744{% else %}#ff9900{% endif %};margin-bottom:8px;font-weight:600">{{ macro_summary }}</div>
    {% for sig in macro_signals[:5] %}
    <div style="font-size:11px;color:#4a6a8a;margin-bottom:3px">{{ sig }}</div>
    {% endfor %}
  </div>

  <div class="mkt {% if market_bad %}bad{% endif %}">
    <h2 style="margin-bottom:6px">Market Health {{ market_score }}/4</h2>
    <div style="font-size:11px;color:#4a6a8a;margin-bottom:8px">
      Nifty {{ nifty_value }} &nbsp;|&nbsp; A/D ratio {{ breadth_ratio }} &nbsp;|&nbsp; {{ breadth_pct }}% advancing
    </div>
    <div>
      <span class="fpill {{ 'ok' if nifty_ok else 'fail' }}">{{ 'OK' if nifty_ok else 'FAIL' }} Nifty above EMA20</span>
      <span class="fpill {{ 'ok' if sectors_ok else 'warn' }}">{{ 'OK' if sectors_ok else 'WEAK' }} Sectors ({{ strong_count }} strong)</span>
      <span class="fpill {{ 'ok' if breadth_ok else 'fail' }}">{{ 'OK' if breadth_ok else 'FAIL' }} Market breadth</span>
      <span class="fpill ok">OK Earnings check</span>
    </div>
    {% if strong_sectors %}<div style="margin-top:8px;font-size:11px;color:#4a6a8a">Strong: <span style="color:#00c853">{{ strong_sectors }}</span></div>{% endif %}
  </div>

  <h2>Daily Schedule</h2>
  <div style="margin-bottom:18px">
    {% for t in ["09:45","10:25","11:30","13:30","14:45"] %}
    <span class="sp {% if t == current_slot %}active{% endif %}">{{ t }} IST</span>
    {% endfor %}
    <span class="sp" style="background:#00c85315;border-color:#00c85344;color:#00c853">15:45 EOD Report</span>
  </div>
  <div style="margin-bottom:22px;display:flex;align-items:center;gap:12px">
    <button class="btn" onclick="go(this)" {% if running %}disabled{% endif %}>Run Scan Now</button>
    <span style="font-size:10px;color:#4a6a8a">Full scan ~6-10 min | Auto-refresh 60s</span>
  </div>

  {% if stocks %}
  <h2>Last Alert Stocks</h2>
  {% for s in stocks %}
  <div class="sc" style="border-left:4px solid #00e5ff">
    <div style="display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;margin-bottom:6px">
      <div>
        <span class="sym">{{ s.symbol }}</span>
        <span style="font-size:11px;color:#00e5ff;font-family:'JetBrains Mono',monospace;margin-left:8px">{{ s.sector }}</span>
        <div style="font-size:11px;color:#4a6a8a;margin-top:2px">RSI {{ s.rsi }} &middot; Vol {{ s.vol_ratio }}x &middot; +{{ s.chg }}%</div>
        <div style="font-size:11px;color:#ff7043;margin-top:2px">📊 {{ s.get('candle_pattern','—') }} &middot; Candle +{{ s.get('candle_score',0) }}</div>
        <div style="font-size:11px;color:#00c853;margin-top:2px">🏆 {{ s.sector }} &middot; Strength {{ "{:+.2f}".format(s.get('sector_strength',0)) }}% &middot; SSS {{ "{:.2f}".format(s.get('stock_strength_score',0)) }} &middot; Bonus +{{ s.get('sector_bonus',0)|int }}</div>
      </div>
      <div style="text-align:right">
        <div style="font-size:9px;color:#4a6a8a">ENHANCED SCORE</div>
        <div style="font-size:24px;font-weight:900;color:#00e5ff;line-height:1">{{ s.get('enhanced_score', s.score) }}<span style="font-size:10px;color:#4a6a8a"> (base {{ s.score }})</span></div>
      </div>
    </div>
    <div class="bw"><div class="b" style="width:{{ [[((s.get('enhanced_score', s.score)/(s.score_max+6.5))*100)|int, 100]|min, 0]|max }}%"></div></div>
    <div class="pr">
      <div class="pc"><div class="pl">CMP</div><div class="pv" style="color:#e8f4fd">Rs.{{ "{:,.2f}".format(s.cmp) }}</div></div>
      <div class="pc"><div class="pl">Buy Range</div><div class="pv c" style="font-size:10px">{{ "{:,.2f}".format(s.entry_low) }}-{{ "{:,.2f}".format(s.entry_high) }}</div></div>
      <div class="pc"><div class="pl">Target 1</div><div class="pv g">Rs.{{ "{:,.2f}".format(s.target1) }}</div></div>
      <div class="pc"><div class="pl">Target 2</div><div class="pv g">Rs.{{ "{:,.2f}".format(s.target2) }}</div></div>
      <div class="pc"><div class="pl">Stop Loss</div><div class="pv r">Rs.{{ "{:,.2f}".format(s.stop_loss) }}</div></div>
      <div class="pc"><div class="pl">R:R</div><div class="pv y">1:{{ s.rr_ratio }}</div></div>
    </div>
  </div>{% endfor %}{% endif %}

  {% if exited_positions %}
  <h2>Recent Exits</h2>
  <div style="margin-bottom:18px">
    {% for e in exited_positions|reverse %}
    <div style="background:#0f1e2e;border:1px solid #1e3248;border-radius:9px;padding:11px 14px;margin-bottom:8px;display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;border-left:3px solid {% if e.pnl_pct >= 0 %}#00c853{% else %}#ff1744{% endif %}">
      <div>
        <span style="font-weight:800;color:#e8f4fd">{{ e.symbol }}</span>
        <span style="font-size:11px;color:#4a6a8a;margin-left:8px">{{ e.reason }}</span>
      </div>
      <div style="text-align:right">
        <span style="font-weight:800;color:{% if e.pnl_pct >= 0 %}#00c853{% else %}#ff1744{% endif %}">{% if e.pnl_pct >= 0 %}+{% endif %}{{ e.pnl_pct }}%</span>
        <span style="font-size:10px;color:#4a6a8a;margin-left:8px">{{ e.exited_at }}</span>
      </div>
    </div>
    {% endfor %}
  </div>
  {% endif %}

  <h2 style="margin-top:6px">Live Log</h2>
  <div class="log" id="lb">
    {% for line in log_lines %}
    <div class="{% if 'PASS' in line or 'sent' in line|lower or 'healthy' in line|lower %}ok{% elif 'error' in line|lower or 'failed' in line|lower %}err{% elif '===' in line %}info{% elif 'weak' in line|lower or 'warn' in line|lower %}wn{% endif %}">{{ line }}</div>
    {% endfor %}
  </div>
</main>
<script>
  document.getElementById('lb').scrollTop=999999;
  function go(btn){btn.disabled=true;btn.textContent='Scanning...';
    fetch('/run').then(r=>r.json()).then(()=>setTimeout(()=>location.reload(),3000))
    .catch(()=>{btn.disabled=false;btn.textContent='Run Scan Now'});}
</script></body></html>"""


@app.route("/")
def dashboard():
    now_ist = datetime.now(IST)
    slots = ["09:45","10:25","11:30","13:30","14:45","15:45"]
    cur_t = f"{now_ist.hour:02d}:{now_ist.minute:02d}"
    current_slot = next((sl for sl in slots if cur_t >= sl), "")
    next_run = next((sl for sl in slots if cur_t < sl), slots[0]+" (tomorrow)")
    mh = state["market_health"]
    return render_template_string(DASH,
        last_run=state["last_run"], last_result=state["last_result"],
        alerts_sent=state["alerts_sent"], next_run=next_run,
        stocks=state["last_stocks"], log_lines=state["log_lines"][-50:],
        running=state["running"], current_slot=current_slot,
        td_calls=0, active_count=len(state["active_positions"]),
        exited_positions=state["exited_positions"][-5:],
        macro_score=state["macro_intel"].get("macro_score", 0),
        macro_summary=state["macro_intel"].get("summary","Not run yet"),
        macro_signals=state["macro_intel"].get("signals",[]),
        market_score=mh.get("score",0), market_bad=mh.get("is_bad",False),
        nifty_ok=mh.get("nifty_ok",True), nifty_value=f"{mh.get('nifty_value',0):,.0f}",
        breadth_ratio=mh.get("breadth_ratio","--"), breadth_pct=mh.get("breadth_pct_up",50),
        breadth_ok=mh.get("breadth_ok",True),
        strong_sectors=", ".join(mh.get("strong_sectors",[])),
        strong_count=len(mh.get("strong_sectors",[])),
        sectors_ok=len(mh.get("strong_sectors",[]))>=3,
        min_score_bad=MIN_SCORE_BAD_MARKET, max_score=MAX_SCORE)

@app.route("/run")
def manual_run():
    if state["running"]: return jsonify({"status":"already_running"})
    threading.Thread(target=lambda: run_pipeline(force=True, scan_mode=SCAN_MODE_INTRADAY), daemon=True).start()
    return jsonify({"status":"started"})

@app.route("/health")
def health():
    return "ok", 200

@app.route("/macro")
def macro_status():
    m = state["macro_intel"].copy()
    m["sector_boosts"]     = list(m.get("sector_boosts", set()))
    m["sector_suppresses"] = list(m.get("sector_suppresses", set()))
    return jsonify(m)

@app.route("/positions")
def positions():
    return jsonify({
        "active":  list(state["active_positions"].values()),
        "exited":  state["exited_positions"],
        "count_active": len(state["active_positions"]),
    })

@app.route("/clear-positions")
def clear_positions():
    count = len(state["active_positions"])
    state["active_positions"].clear()
    save_positions()
    add_log(f"Manually cleared {count} active position(s) — positions.json updated")
    return jsonify({"cleared": count})

@app.route("/status")
def status():
    return jsonify({k:state[k] for k in ["last_run","last_result","alerts_sent","running","market_health"]})

@app.route("/test-email")
def test_email():
    mock = {"score":3,"is_bad":False,"nifty_ok":True,"min_score_used":MIN_SCORE,
            "strong_sectors":["IT","BANK","AUTO"],"weak_sectors":["REALTY"],
            "breadth_ratio":1.8,"breadth_pct_up":62.0,"breadth_ok":True,"nifty_value":24350}
    ok = build_and_send_email([{
        "symbol":"RELIANCE.NS","score":8.5,"chg":1.8,"vol_ratio":2.4,"sector":"ENERGY",
        "breakdown":{"breakout":1,"trend":1,"macd":1,"rsi":1,"volume":1,"bb":1},
        "ind":{"rsi":63.2,"macd":12.5,"macd_sig":10.1,"ema20":2820.0,"ema50":2780.0,
               "bb_upper":2900.0,"bb_lower":2750.0,"bb_mid":2825.0,"cmp":2850.0},
        "news":["Reliance reports record quarterly profit"],
        "reasons":["Volume surge 2.4x","Price above EMA20","RSI 63.2 — momentum zone"],
        "warns":[],"earn":{"skip":False,"warn":False,"days":15},
        "levels":{"cmp":2850.0,"entry_low":2841.45,"entry_high":2858.55,
                  "target1":2892.0,"target2":2915.0,"stop_loss":2815.87,"rr_ratio":1.8},
    }], mock)
    return jsonify({"email_sent":ok})

def self_ping():
    try:
        port = os.getenv("PORT", "10000")
        r = requests.get(f"http://127.0.0.1:{port}/health", timeout=10)
        add_log(f"Self-ping OK ({r.status_code})")
    except Exception as e:
        add_log(f"Self-ping failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# EOD PERFORMANCE ANALYTICS  (ADD-ON — no changes to any existing logic)
#
# Runs at 15:45 IST every market day.
# For every stock alerted today, fetches the closing price and calculates:
#   • P&L % from entry
#   • Whether T1 / T2 / SL was hit during the day
#   • Win / Loss / Open status
# Sends a single rich HTML + plain-text summary email.
# ═══════════════════════════════════════════════════════════════════════════════

ANALYTICS_FILE = "analytics_history.json"   # rolling history across days


def load_analytics_history():
    if not os.path.exists(ANALYTICS_FILE):
        return []
    try:
        with open(ANALYTICS_FILE) as f:
            return json.load(f)
    except Exception:
        return []


def save_analytics_history(history):
    try:
        with open(ANALYTICS_FILE, "w") as f:
            json.dump(history[-90:], f, indent=2)   # keep last 90 days
    except Exception as e:
        add_log(f"Analytics save error: {e}")


def fetch_eod_performance(positions_snapshot):
    """
    For each position (active or exited today), fetch today's full OHLC via
    yfinance period='1d' interval='1m' — no extra daily API calls beyond what
    the exit monitor already makes.

    Returns list of dicts, one per position.
    """
    results = []
    for sym, pos in positions_snapshot.items():
        try:
            tk      = yf.Ticker(sym)
            df_1d   = tk.history(period="2d", interval="1d")
            df_intra= tk.history(period="1d", interval="1m")

            if df_1d.empty:
                continue

            entry      = pos["entry_price"]
            t1         = pos["t1"]
            t2         = pos["t2"]
            sl         = pos["sl"]
            display    = pos["display"]
            alerted_at = pos.get("alerted_at", "—")

            # End-of-day close (most recent bar)
            eod_close  = float(df_1d["Close"].iloc[-1])
            pnl_pct    = round((eod_close - entry) / entry * 100, 2)

            # Check intraday high/low to determine if T1/T2/SL was touched
            if not df_intra.empty:
                day_high = float(df_intra["High"].max())
                day_low  = float(df_intra["Low"].min())
            else:
                day_high = eod_close
                day_low  = eod_close

            t1_hit = day_high >= t1
            t2_hit = day_high >= t2
            sl_hit = day_low  <= sl

            # Outcome label
            if t2_hit:
                outcome = "TARGET 2 ✅"
                outcome_color = "#00C853"
            elif t1_hit and not sl_hit:
                outcome = "TARGET 1 ✅"
                outcome_color = "#66BB6A"
            elif sl_hit and not t1_hit:
                outcome = "STOP LOSS ❌"
                outcome_color = "#FF1744"
            elif sl_hit and t1_hit:
                outcome = "T1 then SL ⚠"
                outcome_color = "#FF9800"
            elif pnl_pct > 0:
                outcome = "PROFIT (open)"
                outcome_color = "#4CAF50"
            else:
                outcome = "LOSS (open)"
                outcome_color = "#EF5350"

            results.append({
                "symbol":     display,
                "sector":     pos.get("sector", "—"),
                "entry":      entry,
                "t1":         t1,
                "t2":         t2,
                "sl":         sl,
                "eod_close":  round(eod_close, 2),
                "pnl_pct":    pnl_pct,
                "day_high":   round(day_high, 2),
                "day_low":    round(day_low,  2),
                "t1_hit":     t1_hit,
                "t2_hit":     t2_hit,
                "sl_hit":     sl_hit,
                "outcome":    outcome,
                "outcome_color": outcome_color,
                "alerted_at": alerted_at,
            })
        except Exception as e:
            log.debug(f"EOD fetch error {sym}: {e}")
            continue
    return results


def send_eod_analytics_email(perf_list, date_str):
    """Build and send the end-of-day performance summary email."""
    resend_key = os.getenv("RESEND_API_KEY", "")
    sender     = os.getenv("ALERT_EMAIL_SENDER", "")
    recips     = [r.strip() for r in os.getenv("ALERT_EMAIL_RECIPIENTS", "").split(",") if r.strip()]
    if not resend_key or not sender or not recips:
        add_log("EOD analytics: email config missing"); return False

    if not perf_list:
        add_log("EOD analytics: no positions to report today"); return False

    # ── Summary stats ────────────────────────────────────────────────────────
    total      = len(perf_list)
    wins       = sum(1 for p in perf_list if p["pnl_pct"] > 0)
    losses     = sum(1 for p in perf_list if p["pnl_pct"] < 0)
    t1_count   = sum(1 for p in perf_list if p["t1_hit"])
    t2_count   = sum(1 for p in perf_list if p["t2_hit"])
    sl_count   = sum(1 for p in perf_list if p["sl_hit"] and not p["t1_hit"])
    win_rate   = round(wins / total * 100) if total else 0
    avg_pnl    = round(sum(p["pnl_pct"] for p in perf_list) / total, 2) if total else 0
    best       = max(perf_list, key=lambda x: x["pnl_pct"])
    worst      = min(perf_list, key=lambda x: x["pnl_pct"])

    subject = (
        f"📊 EOD Report {date_str} | "
        f"Win Rate {win_rate}% | {wins}W {losses}L of {total} | "
        f"Avg {avg_pnl:+.2f}% | StockSense AI"
    )

    # ── Plain text ───────────────────────────────────────────────────────────
    plain  = f"STOCKSENSE AI — END OF DAY PERFORMANCE REPORT\n{date_str}\n{'='*54}\n\n"
    plain += f"  Stocks Alerted : {total}\n"
    plain += f"  Winners        : {wins}  ({win_rate}%)\n"
    plain += f"  Losers         : {losses}\n"
    plain += f"  Target 1 Hit   : {t1_count}\n"
    plain += f"  Target 2 Hit   : {t2_count}\n"
    plain += f"  Stop Loss Hit  : {sl_count}\n"
    plain += f"  Average P&L    : {avg_pnl:+.2f}%\n"
    plain += f"  Best Trade     : {best['symbol']} {best['pnl_pct']:+.2f}%\n"
    plain += f"  Worst Trade    : {worst['symbol']} {worst['pnl_pct']:+.2f}%\n\n"
    plain += "=" * 54 + "\n"
    for p in perf_list:
        sign = "+" if p["pnl_pct"] >= 0 else ""
        plain += (
            f"\n  {p['symbol']}  [{p['sector']}]  —  {p['outcome']}\n"
            f"  Alerted    : {p['alerted_at']}\n"
            f"  Entry      : Rs.{p['entry']:,.2f}\n"
            f"  EOD Close  : Rs.{p['eod_close']:,.2f}   P&L: {sign}{p['pnl_pct']:.2f}%\n"
            f"  Day High   : Rs.{p['day_high']:,.2f}   Day Low: Rs.{p['day_low']:,.2f}\n"
            f"  T1 Rs.{p['t1']:,.2f} {'HIT ✓' if p['t1_hit'] else 'missed'}  |  "
            f"T2 Rs.{p['t2']:,.2f} {'HIT ✓' if p['t2_hit'] else 'missed'}  |  "
            f"SL Rs.{p['sl']:,.2f} {'HIT ✗' if p['sl_hit'] else 'safe'}\n"
        )
    plain += f"\n{'='*54}\nNot financial advice.\n"

    # ── HTML ─────────────────────────────────────────────────────────────────
    wr_color = "#00C853" if win_rate >= 60 else ("#FF9800" if win_rate >= 40 else "#FF1744")

    stock_rows_html = ""
    for p in perf_list:
        sign     = "+" if p["pnl_pct"] >= 0 else ""
        pnl_col  = "#00C853" if p["pnl_pct"] > 0 else "#FF1744"
        t1_col   = "#00C853" if p["t1_hit"] else "#37474F"
        t2_col   = "#00C853" if p["t2_hit"] else "#37474F"
        sl_col   = "#FF1744" if p["sl_hit"] and not p["t1_hit"] else ("#FF9800" if p["sl_hit"] else "#37474F")
        t1_lbl   = "HIT ✓" if p["t1_hit"] else "missed"
        t2_lbl   = "HIT ✓" if p["t2_hit"] else "missed"
        sl_lbl   = "HIT ✗" if p["sl_hit"] and not p["t1_hit"] else ("partial ✗" if p["sl_hit"] else "safe ✓")

        stock_rows_html += f"""
        <div style="background:#0D1F2D;border:1px solid #1E3248;border-radius:10px;
                    margin-bottom:14px;overflow:hidden;border-left:4px solid {p['outcome_color']}">
          <div style="background:#112233;padding:14px 18px;display:flex;
                      justify-content:space-between;align-items:center;flex-wrap:wrap;gap:6px">
            <div>
              <span style="font-size:18px;font-weight:800;color:#fff">{p['symbol']}</span>
              <span style="font-size:11px;color:#546E7A;margin-left:8px">{p['sector']}</span>
              <span style="font-size:11px;color:#546E7A;margin-left:8px">Alerted {p['alerted_at']}</span>
            </div>
            <div style="text-align:right">
              <div style="font-size:11px;color:#546E7A">OUTCOME</div>
              <div style="font-size:13px;font-weight:700;color:{p['outcome_color']}">{p['outcome']}</div>
            </div>
          </div>
          <div style="padding:12px 18px;display:grid;grid-template-columns:repeat(3,1fr);gap:10px">
            <div style="background:#0A1929;border-radius:7px;padding:9px 12px">
              <div style="font-size:9px;color:#546E7A;letter-spacing:1px">ENTRY</div>
              <div style="font-size:13px;font-weight:700;color:#E8F4FD">Rs.{p['entry']:,.2f}</div>
            </div>
            <div style="background:#0A1929;border-radius:7px;padding:9px 12px">
              <div style="font-size:9px;color:#546E7A;letter-spacing:1px">EOD CLOSE</div>
              <div style="font-size:13px;font-weight:700;color:#E8F4FD">Rs.{p['eod_close']:,.2f}</div>
            </div>
            <div style="background:#0A1929;border-radius:7px;padding:9px 12px">
              <div style="font-size:9px;color:#546E7A;letter-spacing:1px">P&amp;L</div>
              <div style="font-size:16px;font-weight:900;color:{pnl_col}">{sign}{p['pnl_pct']:.2f}%</div>
            </div>
          </div>
          <div style="padding:6px 18px 12px;display:grid;grid-template-columns:repeat(3,1fr);gap:8px">
            <div style="background:#0A1520;border:1px solid {t1_col}44;border-radius:6px;padding:7px 10px;text-align:center">
              <div style="font-size:9px;color:#546E7A">T1 Rs.{p['t1']:,.0f}</div>
              <div style="font-size:11px;font-weight:700;color:{t1_col}">{t1_lbl}</div>
            </div>
            <div style="background:#0A1520;border:1px solid {t2_col}44;border-radius:6px;padding:7px 10px;text-align:center">
              <div style="font-size:9px;color:#546E7A">T2 Rs.{p['t2']:,.0f}</div>
              <div style="font-size:11px;font-weight:700;color:{t2_col}">{t2_lbl}</div>
            </div>
            <div style="background:#0A1520;border:1px solid {sl_col}44;border-radius:6px;padding:7px 10px;text-align:center">
              <div style="font-size:9px;color:#546E7A">SL Rs.{p['sl']:,.0f}</div>
              <div style="font-size:11px;font-weight:700;color:{sl_col}">{sl_lbl}</div>
            </div>
          </div>
          <div style="padding:6px 18px 10px;font-size:10px;color:#37474F">
            Day High Rs.{p['day_high']:,.2f} &nbsp;·&nbsp; Day Low Rs.{p['day_low']:,.2f}
          </div>
        </div>"""

    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#060E18;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#060E18">
<tr><td align="center" style="padding:24px 12px">
<table width="600" style="max-width:600px;width:100%">

  <!-- Header -->
  <tr><td style="background:#0D1F2D;border-radius:10px 10px 0 0;padding:28px 24px;border-bottom:3px solid #00E5FF">
    <div style="font-size:10px;color:#00E5FF;letter-spacing:2px;font-weight:700;margin-bottom:6px">STOCKSENSE AI · PERFORMANCE REPORT</div>
    <div style="font-size:26px;font-weight:800;color:#FFFFFF">📊 End of Day Analytics</div>
    <div style="font-size:12px;color:#546E7A;margin-top:6px">{date_str} · {total} stock{"s" if total != 1 else ""} alerted today</div>
  </td></tr>

  <!-- Summary scorecard -->
  <tr><td style="background:#0A1929;padding:20px 24px;border-bottom:1px solid #1E3248">
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;text-align:center">
      <div style="background:#0D1F2D;border-radius:8px;padding:12px 6px">
        <div style="font-size:9px;color:#546E7A;letter-spacing:1px;margin-bottom:4px">WIN RATE</div>
        <div style="font-size:26px;font-weight:900;color:{wr_color}">{win_rate}%</div>
        <div style="font-size:10px;color:#37474F">{wins}W / {losses}L</div>
      </div>
      <div style="background:#0D1F2D;border-radius:8px;padding:12px 6px">
        <div style="font-size:9px;color:#546E7A;letter-spacing:1px;margin-bottom:4px">AVG P&amp;L</div>
        <div style="font-size:26px;font-weight:900;color:{'#00C853' if avg_pnl >= 0 else '#FF1744'}">{avg_pnl:+.2f}%</div>
        <div style="font-size:10px;color:#37474F">per trade</div>
      </div>
      <div style="background:#0D1F2D;border-radius:8px;padding:12px 6px">
        <div style="font-size:9px;color:#546E7A;letter-spacing:1px;margin-bottom:4px">TARGETS HIT</div>
        <div style="font-size:26px;font-weight:900;color:#00C853">{t1_count}</div>
        <div style="font-size:10px;color:#37474F">T1 · {t2_count} T2</div>
      </div>
      <div style="background:#0D1F2D;border-radius:8px;padding:12px 6px">
        <div style="font-size:9px;color:#546E7A;letter-spacing:1px;margin-bottom:4px">SL HIT</div>
        <div style="font-size:26px;font-weight:900;color:{'#FF1744' if sl_count else '#00C853'}">{sl_count}</div>
        <div style="font-size:10px;color:#37474F">stop losses</div>
      </div>
    </div>
    <div style="margin-top:12px;padding:10px 14px;background:#0D1F2D;border-radius:8px;display:flex;justify-content:space-between;flex-wrap:wrap;gap:6px;font-size:11px;color:#546E7A">
      <span>🏆 Best: <strong style="color:#00C853">{best['symbol']} {best['pnl_pct']:+.2f}%</strong></span>
      <span>📉 Worst: <strong style="color:#FF1744">{worst['symbol']} {worst['pnl_pct']:+.2f}%</strong></span>
    </div>
  </td></tr>

  <!-- Per-stock cards -->
  <tr><td style="background:#060E18;padding:16px 24px">
    <div style="font-size:10px;color:#546E7A;letter-spacing:2px;margin-bottom:14px">INDIVIDUAL STOCK PERFORMANCE</div>
    {stock_rows_html}
  </td></tr>

  <!-- Footer -->
  <tr><td style="background:#0D1F2D;border-radius:0 0 10px 10px;padding:14px 24px;border-top:1px solid #1E3248">
    <div style="font-size:11px;color:#37474F">Automated end-of-day report — not financial advice.</div>
  </td></tr>

</table></td></tr></table></body></html>"""

    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {resend_key}", "Content-Type": "application/json"},
            json={"from": f"StockSense AI <{sender}>", "to": recips,
                  "subject": subject, "text": plain, "html": html},
            timeout=20,
        )
        ok = resp.status_code in (200, 201)
        add_log(f"EOD analytics email {'sent' if ok else 'failed'}: {total} stocks | win rate {win_rate}%")
        return ok
    except Exception as e:
        add_log(f"EOD analytics email error: {e}")
        return False


def run_eod_analytics():
    """
    Triggered at 15:45 IST by the scheduler.
    Snapshots active + today's exited positions, fetches closing prices,
    computes performance, sends the analytics email, and saves to history.
    """
    now_ist  = datetime.now(IST)
    date_str = now_ist.strftime("%d %b %Y")
    add_log(f"=== EOD Analytics [{date_str}] starting ===")

    # Combine: still-active positions + positions exited today
    positions_snapshot = dict(state["active_positions"])   # active (may not have exited yet)

    # Also pull in positions exited today from exited_positions list
    today_label = now_ist.strftime("%d %b")
    for ep in state["exited_positions"]:
        sym_key = ep["symbol"] + ".NS"
        if sym_key not in positions_snapshot and today_label in ep.get("exited_at", ""):
            # Reconstruct minimal position dict from exited record
            positions_snapshot[sym_key] = {
                "symbol":     sym_key,
                "display":    ep["symbol"],
                "entry_price":ep["entry"],
                "sl":         ep.get("exit_price", ep["entry"]),   # SL already triggered
                "t1":         ep["entry"] * (1 + T1_PCT / 100),
                "t2":         ep["entry"] * (1 + T2_PCT / 100),
                "sector":     ep.get("sector", "—"),
                "alerted_at": ep.get("alerted_at", "—"),
            }

    if not positions_snapshot:
        add_log("EOD Analytics: no positions tracked today — skipping")
        return

    perf_list = fetch_eod_performance(positions_snapshot)
    if not perf_list:
        add_log("EOD Analytics: could not fetch prices — skipping")
        return

    sent = send_eod_analytics_email(perf_list, date_str)

    # Save to rolling history
    history = load_analytics_history()
    history.append({
        "date":     date_str,
        "total":    len(perf_list),
        "wins":     sum(1 for p in perf_list if p["pnl_pct"] > 0),
        "losses":   sum(1 for p in perf_list if p["pnl_pct"] < 0),
        "avg_pnl":  round(sum(p["pnl_pct"] for p in perf_list) / len(perf_list), 2),
        "win_rate": round(sum(1 for p in perf_list if p["pnl_pct"] > 0) / len(perf_list) * 100),
        "stocks":   [{k: p[k] for k in ("symbol","sector","pnl_pct","outcome","t1_hit","t2_hit","sl_hit")} for p in perf_list],
    })
    save_analytics_history(history)
    add_log(f"=== EOD Analytics done | email_sent={sent} ===")


def run_intraday():
    run_pipeline(scan_mode=SCAN_MODE_INTRADAY)

def start_scheduler():
    scheduler = BackgroundScheduler(timezone=IST)
    WD = "mon-fri"

    scheduler.add_job(run_intraday, CronTrigger(hour=9,  minute=45, day_of_week=WD, timezone=IST), id="scan_0945")
    scheduler.add_job(run_intraday, CronTrigger(hour=10, minute=25, day_of_week=WD, timezone=IST), id="scan_1025")
    scheduler.add_job(run_intraday, CronTrigger(hour=11, minute=30, day_of_week=WD, timezone=IST), id="scan_1130")
    scheduler.add_job(run_intraday, CronTrigger(hour=13, minute=30, day_of_week=WD, timezone=IST), id="scan_1330")
    scheduler.add_job(run_intraday, CronTrigger(hour=14, minute=45, day_of_week=WD, timezone=IST), id="scan_1445")
    # ── No post-market or night scans — market only (09:45 to 14:45) ──────────
    # Use "Run Scan Now" on the dashboard for any manual after-hours scan.

    # Analytics email at 15:45 IST — after market closes, shows today's performance
    scheduler.add_job(run_eod_analytics, CronTrigger(hour=15, minute=45, day_of_week=WD, timezone=IST), id="eod_analytics")

    scheduler.add_job(self_ping,        "interval", minutes=10, id="self_ping")
    scheduler.add_job(monitor_positions,"interval", minutes=10, id="exit_monitor")

    scheduler.start()
    add_log(
        "Scheduler ready | INTRADAY: 09:45 10:25 11:30 13:30 14:45 | "
        "EOD ANALYTICS: 15:45 | EXIT MONITOR: every 10 min | Mon-Fri"
    )
    return scheduler

# ── STARTUP ────────────────────────────────────────────────────────────────────
add_log("StockSense AI v3 starting ...")
load_positions()   # FIX 1: restore positions from disk on every startup/restart
_scheduler = start_scheduler()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT",5000)), debug=False)
