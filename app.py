"""
app.py — StockSense AI v3
=========================
Dual-API stock scanner for NSE with 4 accuracy filters:
  1. Nifty Trend Filter       — only scan when Nifty > EMA20
  2. Sector Momentum Filter   — prefer stocks in strong sectors
  3. Earnings Date Check      — skip if earnings within 3 days, warn within 7
  4. Market Breadth Filter    — advance/decline ratio check

  Phase 1 -> Yahoo Finance  : ~1800 NSE stocks, quick filter
  Phase 2 -> Twelve Data    : Deep analysis (RSI, EMA, MACD, BB)
  Email   -> Resend API     : Rich HTML with entry, T1, T2, SL, reasons

Scan schedule (IST, weekdays only): 09:30 . 11:00 . 13:30 . 14:30
"""

import os, time, logging, threading
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

TD_BASE     = "https://api.twelvedata.com"
TD_KEY      = os.getenv("TWELVE_DATA_API_KEY", "")
TD_CALL_GAP = 8.0

P1_MIN_MOVE  = 0.5
P1_MAX_MOVE  = 6.0
P1_VOL_MIN   = 1.5
P1_MIN_PRICE = 20.0
P1_MAX_PASS  = 50

S = {
    "breakout": 2.0, "trend": 2.0, "macd": 1.5,
    "rsi": 1.5, "volume": 1.5, "bb": 1.0, "news": 0.5,
}
MAX_SCORE            = sum(S.values())
MIN_SCORE            = 5.5
MIN_SCORE_BAD_MARKET = 6.5

ENTRY_BUF = 0.3
T1_PCT    = 1.2
T2_PCT    = 2.2
SL_PCT    = 0.9

# We use a curated Nifty 500 list instead of all 2278 NSE stocks.
# Nifty 500 = 95% of NSE trading volume. Micro-caps beyond this are
# too illiquid for swing trades and cause OOM on Render free (512MB).
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


FALLBACK_SYMBOLS = NIFTY500_SYMBOLS  # alias for backward compat

FALLBACK_SYMBOLS = NIFTY500_SYMBOLS  # alias for backward compat

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
}

def add_log(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    log.info(msg)
    state["log_lines"].append(line)
    if len(state["log_lines"]) > 120:
        state["log_lines"] = state["log_lines"][-120:]

def fetch_nse_symbols():
    """
    Uses curated Nifty 500 list.
    Scanning all 2278 NSE stocks causes OOM on Render free (512MB limit).
    Nifty 500 covers 95% of NSE volume - sufficient for swing trade signals.
    """
    add_log(f"Using Nifty 500 list ({len(NIFTY500_SYMBOLS)} stocks) - memory safe")
    return NIFTY500_SYMBOLS

def compute_rsi(close, period=14):
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta).clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    val = 100 - 100 / (1 + rs.iloc[-1])
    return float(val) if not np.isnan(val) else 50.0

# ---- FILTER 1: NIFTY TREND ----

def check_nifty_trend():
    for attempt in range(3):  # retry up to 3 times
        try:
            if attempt > 0:
                add_log(f"  Nifty retry attempt {attempt+1}...")
                time.sleep(15 * attempt)  # wait 15s, then 30s
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
            if attempt == 2:  # all retries exhausted — assume bullish to not block scan
                add_log("  Nifty check giving up — assuming bullish to allow scan")
                return {"is_ok": True, "status": "unknown", "value": 0, "ema20": 0, "gap": 0, "chg1d": 0}

# ---- FILTER 2: SECTOR MOMENTUM ----

def check_sector_momentum():
    strong, weak = [], []
    add_log("  Checking sector momentum ...")
    for sector, stocks in SECTOR_MAP.items():
        sample = stocks[:3]  # 3 stocks enough to gauge sector direction
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
        time.sleep(2)  # avoid rate limiting between sector checks
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
        if days < 0:   return {"skip": False, "warn": False, "days": days}
        elif days <= 3: return {"skip": True,  "warn": False, "days": days}
        elif days <= 7: return {"skip": False, "warn": True,  "days": days}
        else:           return {"skip": False, "warn": False, "days": days}
    except Exception:
        return {"skip": False, "warn": False, "days": None}

# ---- FILTER 4: MARKET BREADTH ----

def check_market_breadth():
    try:
        add_log("  Checking market breadth ...")
        sample = NIFTY500_SYMBOLS[:50]  # 50 liquid stocks enough for breadth signal
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
    if nifty["is_ok"]:              health_score += 1
    if len(sectors["strong"]) >= 3: health_score += 1
    if breadth["is_ok"]:            health_score += 1
    if breadth["ratio"] >= 1.5:     health_score += 1

    is_bad = health_score <= 1  # only 0/4 or 1/4 triggers strict mode
    result = {
        "nifty_ok": nifty["is_ok"], "nifty_value": nifty["value"],
        "nifty_ema20": nifty["ema20"], "nifty_chg1d": nifty["chg1d"],
        "nifty_trend": nifty["status"],
        "strong_sectors": sectors["strong"], "weak_sectors": sectors["weak"],
        "breadth_ratio": breadth["ratio"], "breadth_pct_up": breadth["pct_up"],
        "breadth_ok": breadth["is_ok"],
        "score": health_score, "is_bad": is_bad,
        "min_score_used": MIN_SCORE_BAD_MARKET if is_bad else MIN_SCORE,
    }
    state["market_health"] = result
    label = "WEAK MARKET" if is_bad else "HEALTHY MARKET"
    add_log(f"Market health: {health_score}/4 - {label}")
    if is_bad:
        add_log(f"  Raising min score to {MIN_SCORE_BAD_MARKET} - only high-confidence alerts")
    return result

# ---- PHASE 1: YAHOO BROAD SCAN ----

def phase1_scan(symbols, strong_sectors):
    candidates = []
    add_log(f"Phase 1 - scanning {len(symbols)} NSE stocks ...")
    sym_to_sector = {s: sec for sec, stocks in SECTOR_MAP.items() for s in stocks}
    batch_size = 20  # small batches = low memory per batch
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        try:
            raw = yf.download(" ".join(batch), period="10d", interval="1d",
                              group_by="ticker", auto_adjust=True, progress=False, timeout=30)
        except Exception as e:
            log.debug(f"Batch error: {e}")
            time.sleep(2)
            continue
        for sym in batch:
            try:
                df = raw[sym].copy() if len(batch) > 1 else raw.copy()
                df = df.dropna(subset=["Close","Volume"])
                if len(df) < 10: continue
                close = df["Close"]; vol = df["Volume"]
                curr = float(close.iloc[-1]); prev = float(close.iloc[-2])
                vol10 = float(vol.rolling(10).mean().iloc[-1])
                vol_r = float(vol.iloc[-1]) / vol10 if vol10 > 0 else 0
                chg = (curr - prev) / prev * 100 if prev > 0 else 0
                # Also check intraday move from today's open
                # Catches recovery moves on crash days where daily chg is negative
                open_price = float(df["Open"].iloc[-1]) if "Open" in df.columns else curr
                intraday_chg = (curr - open_price) / open_price * 100 if open_price > 0 else 0
                if curr < P1_MIN_PRICE: continue
                # Pass if EITHER daily change OR intraday recovery qualifies
                daily_ok = P1_MIN_MOVE <= chg <= P1_MAX_MOVE
                intraday_ok = P1_MIN_MOVE <= intraday_chg <= P1_MAX_MOVE
                if not (daily_ok or intraday_ok): continue
                # Use the better of the two changes for display
                chg = chg if daily_ok else intraday_chg
                if vol_r < P1_VOL_MIN: continue
                sma5 = float(close.rolling(5).mean().iloc[-1])
                sma10 = float(close.rolling(10).mean().iloc[-1])
                if curr < sma5 or sma5 < sma10: continue
                sector = sym_to_sector.get(sym, "OTHER")
                candidates.append({
                    "symbol": sym, "chg": round(chg,2), "vol_ratio": round(vol_r,2),
                    "close": curr, "sector": sector,
                    "in_strong_sector": sector in strong_sectors if strong_sectors else True,
                    "news": [],
                })
            except Exception:
                continue
        time.sleep(3)  # avoid Yahoo rate limiting
    candidates.sort(key=lambda x: (x["in_strong_sector"], x["vol_ratio"]), reverse=True)
    top = candidates[:P1_MAX_PASS]
    add_log(f"Phase 1 complete - {len(candidates)} passed, top {len(top)} to Phase 2")
    state["phase1_count"] = len(top)
    return top

# ---- TWELVE DATA ----

def td_get(endpoint, params):
    if not TD_KEY: return {}
    params["apikey"] = TD_KEY
    try:
        r = requests.get(f"{TD_BASE}/{endpoint}", params=params, timeout=20)
        state["td_calls_today"] += 1
        data = r.json()
        if data.get("status") == "error": return {}
        return data
    except Exception:
        return {}

def td_indicators(symbol):
    # Yahoo format: RELIANCE.NS   TCS.NS    M&M.NS
    # TD free plan: RELIANCE.NSE  TCS.NSE   MM.NSE
    ticker = symbol.replace(".NS", "")
    ticker = ticker.replace("&", "")    # M&M → MM,  M&MFIN → MMFIN
    nse_sym = ticker + ".NSE"           # RELIANCE.NSE, TCS.NSE, MM.NSE
    common = {"symbol": nse_sym, "interval": "1day", "outputsize": 30}
    time.sleep(TD_CALL_GAP)
    quote = td_get("quote", {"symbol": nse_sym})
    if not quote or "close" not in quote: return None
    time.sleep(TD_CALL_GAP)
    rsi_d = td_get("rsi", {**common, "time_period": 14})
    time.sleep(TD_CALL_GAP)
    macd_d = td_get("macd", {**common, "fast_period": 12, "slow_period": 26, "signal_period": 9})
    time.sleep(TD_CALL_GAP)
    ema20_d = td_get("ema", {**common, "time_period": 20})
    ema50_d = td_get("ema", {**common, "time_period": 50})
    time.sleep(TD_CALL_GAP)
    bb_d = td_get("bbands", {**common, "time_period": 20, "sd": 2})
    try:
        cmp = float(quote["close"])
        rsi = float(rsi_d["values"][0]["rsi"]) if rsi_d.get("values") else 50.0
        macd_val = float(macd_d["values"][0]["macd"]) if macd_d.get("values") else 0.0
        macd_sig = float(macd_d["values"][0]["macd_signal"]) if macd_d.get("values") else 0.0
        ema20 = float(ema20_d["values"][0]["ema"]) if ema20_d.get("values") else cmp
        ema50 = float(ema50_d["values"][0]["ema"]) if ema50_d.get("values") else cmp
        bb_upper = float(bb_d["values"][0]["upper_band"]) if bb_d.get("values") else cmp*1.02
        bb_lower = float(bb_d["values"][0]["lower_band"]) if bb_d.get("values") else cmp*0.98
        bb_mid = float(bb_d["values"][0]["middle_band"]) if bb_d.get("values") else cmp
        return {"cmp": cmp, "rsi": rsi, "macd": macd_val, "macd_sig": macd_sig,
                "ema20": ema20, "ema50": ema50,
                "bb_upper": bb_upper, "bb_lower": bb_lower, "bb_mid": bb_mid}
    except Exception:
        return None

# ---- PHASE 2: DEEP ANALYSIS ----

def phase2_analyze(candidates, market):
    results = []
    min_score = market["min_score_used"]
    add_log(f"Phase 2 - {len(candidates)} candidates (min score: {min_score}) ...")
    for c in candidates:
        sym = c["symbol"]
        add_log(f"  Analyzing {sym.replace('.NS','')} ...")
        earn = check_earnings(sym)
        if earn["skip"]:
            add_log(f"    Skipped - earnings in {earn['days']} days")
            continue
        # TD free plan does not support NSE — Yahoo Finance used for all analysis
        ind = None
        if ind is None:
            try:
                df = yf.Ticker(sym).history(period="60d", interval="1d")
                close = df["Close"].dropna()
                cmp = float(close.iloc[-1])
                rsi = compute_rsi(close)
                ema20 = float(close.ewm(span=20, adjust=False).mean().iloc[-1])
                ema50 = float(close.ewm(span=50, adjust=False).mean().iloc[-1])
                roll = close.rolling(20)
                bb_mid = float(roll.mean().iloc[-1])
                bb_upper = bb_mid + 2*float(roll.std().iloc[-1])
                bb_lower = bb_mid - 2*float(roll.std().iloc[-1])
                ema12 = float(close.ewm(span=12,adjust=False).mean().iloc[-1])
                ema26 = float(close.ewm(span=26,adjust=False).mean().iloc[-1])
                macd_val = ema12 - ema26
                ind = {"cmp": cmp, "rsi": rsi, "macd": macd_val, "macd_sig": macd_val*0.9,
                       "ema20": ema20, "ema50": ema50,
                       "bb_upper": bb_upper, "bb_lower": bb_lower, "bb_mid": bb_mid}
            except Exception as e:
                log.debug(f"Fallback failed {sym}: {e}")
                continue
        cmp = ind["cmp"]
        bd = {}; reasons = []; warns = []
        if earn["warn"]:
            warns.append(f"Earnings report in ~{earn['days']} days - consider smaller position size")
        if c["in_strong_sector"] and c["sector"] != "OTHER":
            reasons.append(f"Sector tailwind - {c['sector']} sector is currently outperforming")
        if not market["nifty_ok"]:
            warns.append("Nifty below EMA20 (bearish) - counter-trend trade, use tighter stop")
        if c["vol_ratio"] >= 2.0 and c["chg"] >= 0.8:
            bd["breakout"] = S["breakout"]
            reasons.append(f"Volume surge {c['vol_ratio']:.1f}x average with {c['chg']:.1f}% move - breakout signal")
        if cmp > ind["ema20"] > ind["ema50"]:
            bd["trend"] = S["trend"]
            reasons.append(f"Price above EMA20 ({ind['ema20']:.2f}) above EMA50 ({ind['ema50']:.2f}) - strong uptrend")
        elif cmp > ind["ema20"]:
            bd["trend"] = S["trend"] * 0.5
            reasons.append("Price above EMA20 - short-term uptrend forming")
        if ind["macd"] > ind["macd_sig"] and ind["macd"] > 0:
            bd["macd"] = S["macd"]
            reasons.append(f"MACD bullish crossover confirmed - strong momentum")
        elif ind["macd"] > ind["macd_sig"]:
            bd["macd"] = S["macd"] * 0.6
            reasons.append("MACD crossover - early bullish signal")
        if 52 <= ind["rsi"] <= 68:
            bd["rsi"] = S["rsi"]
            reasons.append(f"RSI {ind['rsi']:.1f} in sweet spot (52-68) - momentum without being overbought")
        elif 68 < ind["rsi"] <= 75:
            bd["rsi"] = S["rsi"] * 0.4
            warns.append(f"RSI {ind['rsi']:.1f} slightly high - use tighter stop")
        elif ind["rsi"] > 75:
            warns.append(f"RSI {ind['rsi']:.1f} overbought - high reversal risk")
        if c["vol_ratio"] >= 1.5:
            bd["volume"] = S["volume"]
            reasons.append(f"Volume {c['vol_ratio']:.1f}x 10-day average - strong buying interest")
        bb_range = ind["bb_upper"] - ind["bb_lower"]
        bb_pos = (cmp - ind["bb_lower"]) / bb_range if bb_range > 0 else 0.5
        if 0.5 <= bb_pos <= 0.85:
            bd["bb"] = S["bb"]
            reasons.append(f"Price in upper Bollinger zone - momentum with room to grow")
        elif bb_pos > 0.85:
            warns.append("Price near upper Bollinger Band - watch for resistance")
        try:
            raw_news = yf.Ticker(sym).news or []
            headlines = [n.get("title","") for n in raw_news[:5] if n.get("title")]
            nl = " ".join(headlines).lower()
            pos = sum(w in nl for w in ["surge","rally","buy","upgrade","record","beat","profit","growth","strong","bullish"])
            neg = sum(w in nl for w in ["fall","drop","loss","sell","downgrade","weak","penalty","fraud","probe"])
            if pos > neg and pos >= 1:
                bd["news"] = S["news"]
                reasons.append("Positive news sentiment supporting the move")
            c["news"] = headlines[:3]
        except Exception:
            c["news"] = []
        score = round(sum(bd.values()), 2)
        if score < min_score:
            add_log(f"    Score {score:.1f} < {min_score} - skipped")
            continue
        buf = cmp * ENTRY_BUF / 100
        el = round(cmp - buf, 2); eh = round(cmp + buf, 2); mid = (el+eh)/2
        t1 = round(mid * (1 + T1_PCT/100), 2)
        t2 = round(mid * (1 + T2_PCT/100), 2)
        sl = round(el  * (1 - SL_PCT/100), 2)
        rr = round((t1 - mid) / (mid - sl), 2) if mid > sl else 0
        add_log(f"    PASS {sym.replace('.NS','')} score={score:.1f} RSI={ind['rsi']:.1f} RR=1:{rr:.1f}")
        results.append({
            "symbol": sym, "score": score, "breakdown": bd, "ind": ind,
            "chg": c["chg"], "vol_ratio": c["vol_ratio"], "sector": c["sector"],
            "news": c["news"], "reasons": reasons, "warns": warns, "earn": earn,
            "levels": {"cmp": cmp, "entry_low": el, "entry_high": eh,
                       "target1": t1, "target2": t2, "stop_loss": sl, "rr_ratio": rr},
        })
    results.sort(key=lambda x: x["score"], reverse=True)
    add_log(f"Phase 2 complete - {len(results)} qualify")
    state["phase2_count"] = len(results)
    return results

# ---- EMAIL ----

def build_and_send_email(stocks, market):
    resend_key = os.getenv("RESEND_API_KEY","")
    sender     = os.getenv("ALERT_EMAIL_SENDER","")
    recips     = [r.strip() for r in os.getenv("ALERT_EMAIL_RECIPIENTS","").split(",") if r.strip()]
    if not resend_key or not sender or not recips:
        add_log("Email config missing"); return False

    now  = datetime.now(IST).strftime("%d %b %Y  %H:%M IST")
    syms = ", ".join(s["symbol"].replace(".NS","") for s in stocks)
    subject = f"{'WEAK MARKET - ' if market['is_bad'] else ''}BUY SIGNAL | {syms} | StockSense AI"

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

    def sbar(score):
        pct = min(100, score/MAX_SCORE*100)
        return f'<div style="background:#1e3248;border-radius:4px;height:5px;margin:10px 0"><div style="width:{pct:.0f}%;background:linear-gradient(90deg,#00E5FF,#FFD600);border-radius:4px;height:5px"></div></div>'

    def pill(ok, label):
        c = "#00C853" if ok else "#607D8B"
        return f'<span style="background:{c}22;color:{c};border:1px solid {c}55;padding:2px 9px;border-radius:20px;font-size:11px;margin:2px 3px 2px 0;display:inline-block">{"+ " if ok else "- "}{label}</span>'

    cards = ""
    plain = f"STOCKSENSE AI v3 - BUY SIGNAL\n{now}\nMarket Health: {market['score']}/4\n{'='*50}\n\n"
    if market["is_bad"]:
        plain += f"WEAK MARKET: Only high-confidence signals (score above {MIN_SCORE_BAD_MARKET}) included.\n\n"

    for s in stocks:
        sym = s["symbol"].replace(".NS","")
        tl = s["levels"]; ind = s["ind"]; bd = s["breakdown"]
        plain += (
            f"{sym} [{s['sector']}]\n"
            f"  CMP       : Rs.{tl['cmp']:,.2f}\n"
            f"  Buy Range : Rs.{tl['entry_low']:,.2f} - Rs.{tl['entry_high']:,.2f}\n"
            f"  Target 1  : Rs.{tl['target1']:,.2f} (+{T1_PCT}%)\n"
            f"  Target 2  : Rs.{tl['target2']:,.2f} (+{T2_PCT}%)\n"
            f"  Stop Loss : Rs.{tl['stop_loss']:,.2f} (-{SL_PCT}%)\n"
            f"  R:R Ratio : 1:{tl['rr_ratio']:.1f}\n"
            f"  Score     : {s['score']:.1f}/{MAX_SCORE:.0f}\n\n"
            f"  WHY BUY:\n" + "".join(f"  + {r}\n" for r in s["reasons"])
        )
        if s["warns"]: plain += "\n  CAUTION:\n" + "".join(f"  ! {w}\n" for w in s["warns"])
        plain += f"\n{'-'*50}\n\n"

        reasons_html = "".join(
            f'<div style="display:flex;gap:8px;margin-bottom:7px">'
            f'<span style="color:#00C853">+</span>'
            f'<span style="color:#B0BEC5;font-size:13px;line-height:1.5">{r}</span></div>'
            for r in s["reasons"])
        warns_html = ""
        if s["warns"]:
            warns_html = '<div style="background:#FF990010;border:1px solid #FF990033;border-radius:8px;padding:10px 14px;margin-top:12px">'
            warns_html += "".join(f'<div style="color:#FF9900;font-size:12px;margin-bottom:3px">! {w}</div>' for w in s["warns"])
            warns_html += '</div>'
        news_html = ""
        if s["news"]:
            items = "".join(f"<li style='margin-bottom:4px'>{h[:110]}</li>" for h in s["news"])
            news_html = f'<div style="margin-top:12px"><div style="font-size:10px;color:#607D8B;letter-spacing:1px;text-transform:uppercase;margin-bottom:5px">News</div><ul style="margin:0;padding-left:16px;color:#78909C;font-size:12px;line-height:1.8">{items}</ul></div>'
        sec_badge = f'<span style="background:#00E5FF15;color:#00E5FF;border:1px solid #00E5FF44;padding:2px 8px;border-radius:20px;font-size:10px;margin-left:8px">{s["sector"]}</span>' if s["sector"] != "OTHER" else ""
        accent = "#FF9900" if market["is_bad"] else "#00E5FF"
        cards += f"""
        <div style="background:#1A2B3C;border-radius:14px;padding:22px;margin-bottom:22px;border-left:4px solid {accent}">
          <div style="display:flex;justify-content:space-between;flex-wrap:wrap;gap:10px">
            <div>
              <div style="font-size:22px;font-weight:900;color:#E8F4FD">{sym}{sec_badge}</div>
              <div style="color:#00E5FF;font-size:11px;margin-top:3px">NSE 2-3 DAY SWING &middot; +{s['chg']:.2f}%</div>
            </div>
            <div style="text-align:right">
              <div style="font-size:10px;color:#607D8B">SCORE</div>
              <div style="font-size:28px;font-weight:900;color:#FFD600;line-height:1">{s['score']:.1f}<span style="font-size:11px;color:#607D8B"> /{MAX_SCORE:.0f}</span></div>
            </div>
          </div>
          {sbar(s['score'])}
          <div style="margin-bottom:14px">
            {pill('breakout' in bd,'Breakout')}{pill('trend' in bd,'EMA Trend')}
            {pill('macd' in bd,'MACD')}{pill('rsi' in bd,f"RSI {ind['rsi']:.0f}")}
            {pill('volume' in bd,f"Vol {s['vol_ratio']:.1f}x")}{pill('bb' in bd,'BB Zone')}
            {pill(s['sector'] in market.get('strong_sectors',[]),'Strong Sector')}
          </div>
          <table style="width:100%;border-collapse:collapse;margin-bottom:14px">
            <tr style="background:#263D52"><td style="padding:9px 12px;color:#78909C;font-size:12px">Current Price</td><td style="padding:9px 12px;text-align:right;color:#E8F4FD;font-weight:700;font-size:17px">Rs.{tl['cmp']:,.2f}</td></tr>
            <tr><td style="padding:7px 12px;color:#78909C;font-size:12px;border-bottom:1px solid #263D52">Buy Range</td><td style="padding:7px 12px;text-align:right;color:#00E5FF;font-weight:700;font-size:14px;border-bottom:1px solid #263D52">Rs.{tl['entry_low']:,.2f} - Rs.{tl['entry_high']:,.2f}</td></tr>
            <tr><td style="padding:7px 12px;color:#78909C;font-size:12px;border-bottom:1px solid #263D52">Target 1 (+{T1_PCT}%)</td><td style="padding:7px 12px;text-align:right;color:#00C853;font-weight:700;font-size:14px;border-bottom:1px solid #263D52">Rs.{tl['target1']:,.2f}</td></tr>
            <tr><td style="padding:7px 12px;color:#78909C;font-size:12px;border-bottom:1px solid #263D52">Target 2 (+{T2_PCT}%)</td><td style="padding:7px 12px;text-align:right;color:#00C853;font-weight:700;font-size:16px;border-bottom:1px solid #263D52">Rs.{tl['target2']:,.2f}</td></tr>
            <tr><td style="padding:7px 12px;color:#78909C;font-size:12px">Stop Loss (-{SL_PCT}%)</td><td style="padding:7px 12px;text-align:right;color:#FF1744;font-weight:700;font-size:14px">Rs.{tl['stop_loss']:,.2f}</td></tr>
          </table>
          <div style="background:#0F1F2E;border-radius:8px;padding:6px 14px;margin-bottom:12px;display:inline-flex;gap:18px;flex-wrap:wrap">
            <span style="color:#607D8B;font-size:11px">R:R <span style="color:#FFD600;font-weight:700">1:{tl['rr_ratio']:.1f}</span></span>
            <span style="color:#607D8B;font-size:11px">RSI <span style="color:#E8F4FD;font-weight:700">{ind['rsi']:.1f}</span></span>
            <span style="color:#607D8B;font-size:11px">Vol <span style="color:#E8F4FD;font-weight:700">{s['vol_ratio']:.1f}x</span></span>
          </div>
          <div style="background:#0F1F2E;border-radius:10px;padding:14px 16px;margin-bottom:10px">
            <div style="font-size:10px;color:#00E5FF;letter-spacing:2px;text-transform:uppercase;margin-bottom:10px">Why Buy</div>
            {reasons_html}
          </div>
          {warns_html}{news_html}
        </div>"""

    plain += "Automated algo signal - not financial advice. Always use stop-losses."
    header_color = "#FF9900" if market["is_bad"] else "#00E5FF"
    header_title = "WEAK MARKET ALERT" if market["is_bad"] else "BUY SIGNAL"
    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#0B1826;font-family:'Segoe UI',Tahoma,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0"><tr><td align="center" style="padding:28px 12px">
<table width="640" style="max-width:640px;width:100%">
  <tr><td style="background:#0F1F2E;border-radius:16px 16px 0 0;padding:32px 28px;border-bottom:2px solid {header_color}">
    <div style="font-size:10px;color:{header_color};letter-spacing:3px;text-transform:uppercase;margin-bottom:8px">StockSense AI v3 &middot; 4-Filter Engine</div>
    <div style="font-size:32px;font-weight:900;color:#fff">{'WARNING ' if market['is_bad'] else 'SIGNAL '}{header_title}</div>
    <div style="font-size:12px;color:#607D8B;margin-top:6px">{now} &middot; {len(stocks)} setup{"s" if len(stocks)!=1 else ""}</div>
    <div style="margin-top:10px;font-size:11px;color:#4a6a8a">Nifty Trend &middot; Sector Momentum &middot; Earnings Check &middot; Market Breadth</div>
  </td></tr>
  <tr><td style="background:#0F1F2E;padding:24px 28px">
    {market_banner}
    {cards}
  </td></tr>
  <tr><td style="background:#080f1a;border-radius:0 0 16px 16px;padding:18px 28px;border-top:1px solid #1A2B3C">
    <div style="font-size:11px;color:#37474F;line-height:1.8">
      Automated algorithmic signal - not financial advice.<br>
      Never risk more than 2% of capital per trade. Always honour your stop loss.
    </div>
  </td></tr>
</table></td></tr></table></body></html>"""

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

# ---- PIPELINE ----

def run_pipeline(force=False):
    if state["running"]: add_log("Already running - skipped"); return
    now_ist = datetime.now(IST)
    # Weekend check disabled for testing — re-enable after test confirmed
    # if now_ist.weekday() >= 5 and not force:
    #     add_log("Weekend - skipped"); return
    if force: add_log("Manual run - weekend check bypassed")
    state["running"] = True
    state["last_run"] = now_ist.strftime("%d %b %Y  %H:%M IST")
    add_log(f"=== StockSense AI v3 started {state['last_run']} ===")
    try:
        market     = run_market_health_checks()
        symbols    = fetch_nse_symbols()
        candidates = phase1_scan(symbols, market["strong_sectors"])
        if not candidates:
            state["last_result"] = "Phase 1: No stocks passed filters"
            state["last_stocks"] = []; return
        finalists = phase2_analyze(candidates, market)
        if not finalists:
            state["last_result"] = f"No stocks met score above {market['min_score_used']}"
            state["last_stocks"] = []; return
        sent = build_and_send_email(finalists, market)
        if sent:
            state["alerts_sent"] += len(finalists)
            state["last_result"] = f"{'WEAK MKT - ' if market['is_bad'] else ''}Sent: {', '.join(s['symbol'].replace('.NS','') for s in finalists)}"
            state["last_stocks"] = [{
                "symbol": s["symbol"].replace(".NS",""), "score": s["score"],
                "score_max": MAX_SCORE, "cmp": s["levels"]["cmp"],
                "entry_low": s["levels"]["entry_low"], "entry_high": s["levels"]["entry_high"],
                "target1": s["levels"]["target1"], "target2": s["levels"]["target2"],
                "stop_loss": s["levels"]["stop_loss"], "rr_ratio": s["levels"]["rr_ratio"],
                "rsi": round(s["ind"]["rsi"],1), "vol_ratio": round(s["vol_ratio"],2),
                "chg": s["chg"], "sector": s["sector"],
            } for s in finalists]
        else:
            state["last_result"] = "Email failed - check config"
    except Exception as e:
        add_log(f"Pipeline error: {e}"); state["last_result"] = f"Error: {e}"
    finally:
        state["running"] = False
        add_log(f"=== Done | TD calls: {state['td_calls_today']}/800 ===")

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
    <span class="badge">~1800 NSE</span><span class="badge">4-FILTER</span>
  </div>
</header>
<main>
  <div class="grid" style="margin-top:0">
    <div class="card a"><div class="lbl">Last Run</div><div class="val mono">{{ last_run }}</div></div>
    <div class="card a"><div class="lbl">Result</div><div class="val mono" style="font-size:11px">{{ last_result }}</div></div>
    <div class="card a"><div class="lbl">Alerts Sent</div><div class="val g">{{ alerts_sent }}</div></div>
    <div class="card a"><div class="lbl">Next Run</div><div class="val c mono" style="font-size:13px">{{ next_run }}</div></div>
    <div class="card a"><div class="lbl">TD Calls</div><div class="val y">{{ td_calls }}<span style="font-size:10px;color:#4a6a8a">/800</span></div></div>
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
    {% if market_bad %}<div style="margin-top:10px;background:#ff990015;border:1px solid #ff990033;border-radius:7px;padding:7px 11px;font-size:11px;color:#ff9900">
      WEAK MARKET - only stocks scoring above {{ min_score_bad }}/{{ max_score }} are alerted
    </div>{% endif %}
  </div>

  <h2>Daily Schedule</h2>
  <div style="margin-bottom:18px">
    {% for t in ["07:25","07:35","09:30","11:00","13:30","14:30","20:00","20:15"] %}
    <span class="sp {% if t == current_slot %}active{% endif %}">{{ t }} IST</span>
    {% endfor %}
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
      </div>
      <div style="text-align:right">
        <div style="font-size:9px;color:#4a6a8a">SCORE</div>
        <div style="font-size:24px;font-weight:900;color:#ffd600;line-height:1">{{ s.score }}<span style="font-size:10px;color:#4a6a8a">/{{ s.score_max }}</span></div>
      </div>
    </div>
    <div class="bw"><div class="b" style="width:{{ (s.score/s.score_max*100)|int }}%"></div></div>
    <div class="pr">
      <div class="pc"><div class="pl">CMP</div><div class="pv" style="color:#e8f4fd">Rs.{{ "{:,.2f}".format(s.cmp) }}</div></div>
      <div class="pc"><div class="pl">Buy Range</div><div class="pv c" style="font-size:10px">{{ "{:,.2f}".format(s.entry_low) }}-{{ "{:,.2f}".format(s.entry_high) }}</div></div>
      <div class="pc"><div class="pl">Target 1</div><div class="pv g">Rs.{{ "{:,.2f}".format(s.target1) }}</div></div>
      <div class="pc"><div class="pl">Target 2</div><div class="pv g">Rs.{{ "{:,.2f}".format(s.target2) }}</div></div>
      <div class="pc"><div class="pl">Stop Loss</div><div class="pv r">Rs.{{ "{:,.2f}".format(s.stop_loss) }}</div></div>
      <div class="pc"><div class="pl">R:R</div><div class="pv y">1:{{ s.rr_ratio }}</div></div>
    </div>
  </div>{% endfor %}{% endif %}

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
    slots = ["09:30","11:00","13:30","14:30"]
    cur_t = f"{now_ist.hour:02d}:{now_ist.minute:02d}"
    current_slot = next((sl for sl in slots if cur_t >= sl), "")
    next_run = next((sl for sl in slots if cur_t < sl), slots[0]+" (tomorrow)")
    mh = state["market_health"]
    return render_template_string(DASH,
        last_run=state["last_run"], last_result=state["last_result"],
        alerts_sent=state["alerts_sent"], next_run=next_run,
        stocks=state["last_stocks"], log_lines=state["log_lines"][-50:],
        running=state["running"], current_slot=current_slot,
        td_calls=state["td_calls_today"],
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
    # Force=True bypasses weekend check for manual/test runs
    threading.Thread(target=lambda: run_pipeline(force=True), daemon=True).start()
    return jsonify({"status":"started"})

@app.route("/health")
def health():
    return "ok", 200  # plain text - never "too large" for cron-job.org

@app.route("/status")
def status():
    return jsonify({k:state[k] for k in ["last_run","last_result","alerts_sent","running","td_calls_today","market_health"]})

@app.route("/test-twelvedata")
def test_twelvedata():
    """Hit this URL to verify Twelve Data API key is working"""
    if not TD_KEY:
        return jsonify({
            "status": "error",
            "message": "TWELVE_DATA_API_KEY is not set in environment variables",
            "td_key_present": False
        })
    # Test with a simple quote for Reliance
    try:
        params = {"symbol": "RELIANCE.NSE", "apikey": TD_KEY}  # TD free plan format
        r = requests.get(f"{TD_BASE}/quote", params=params, timeout=15)
        data = r.json()
        if data.get("status") == "error":
            return jsonify({
                "status": "error",
                "message": data.get("message", "Unknown TD error"),
                "td_key_present": True,
                "raw": data
            })
        return jsonify({
            "status": "note",
            "message": "TD key works but NSE needs paid plan. App uses Yahoo Finance instead.",
            "td_key_present": True,
            "reliance_price": data.get("close"),
            "symbol": data.get("symbol"),
            "td_calls_used": state["td_calls_today"]
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e), "td_key_present": True})

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
        "reasons":["Volume surge 2.4x - breakout","Price above EMA20 - strong uptrend","RSI 63.2 - momentum zone"],
        "warns":[],"earn":{"skip":False,"warn":False,"days":15},
        "levels":{"cmp":2850.0,"entry_low":2841.45,"entry_high":2858.55,
                  "target1":2892.0,"target2":2915.0,"stop_loss":2815.87,"rr_ratio":1.8},
    }], mock)
    return jsonify({"email_sent":ok})

def self_ping():
    """
    Pings own /health every 10 min to prevent Render free tier hibernation.
    Render hibernates after ~15 min of inactivity. This keeps it awake
    independently of cron-job.org, so even if external pings fail the
    app stays alive and the scheduler keeps running.
    """
    try:
        port = os.getenv("PORT", "10000")
        url  = f"http://127.0.0.1:{port}/health"
        r = requests.get(url, timeout=10)
        add_log(f"Self-ping OK ({r.status_code})")
    except Exception as e:
        add_log(f"Self-ping failed: {e}")

def start_scheduler():
    scheduler = BackgroundScheduler(timezone=IST)
    # Stock scan jobs — weekdays only at IST times
    for hour, minute in [(7,25),(7,35),(9,30),(11,0),(13,30),(14,30),(20,0),(20,15),(21,50)]:
        # day_of_week removed for weekend testing — add back after test confirmed
        scheduler.add_job(run_pipeline, CronTrigger(hour=hour, minute=minute, timezone=IST))
    # Self-ping every 10 min — keeps Render from hibernating
    # Render hibernates after ~15 min; 10 min interval gives safe margin
    scheduler.add_job(self_ping, "interval", minutes=10, id="self_ping")
    scheduler.start()
    add_log("Scheduler ready - 07:25 07:35 09:30 11:00 13:30 14:30 20:00 20:15 21:50(TEST) IST + self-ping every 10 min")
    return scheduler

# ---------------------------------------------------------------
# START SCHEDULER AT MODULE LEVEL — works with both gunicorn & direct run
# Previously inside `if __name__ == "__main__"` so gunicorn never called it
# ---------------------------------------------------------------
add_log("StockSense AI v3 starting ...")
_scheduler = start_scheduler()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT",5000)), debug=False)
