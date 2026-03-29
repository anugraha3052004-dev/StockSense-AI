"""
app.py — StockSense AI
======================
Flask web server + background scheduler for Render deployment.
Scans NSE stocks 4x daily and sends BUY SIGNAL email alerts.

GitHub → Render flow:
  1. Push this repo to GitHub
  2. Create Render Web Service → connect repo
  3. Set environment variables in Render dashboard
  4. Deploy — alerts run automatically every market day
"""

import os, time, smtplib, logging, threading
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from logging.handlers import RotatingFileHandler

import numpy as np
import pandas as pd
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
from flask import Flask, jsonify, render_template_string

# ─── LOGGING ─────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("stockalert")
log.setLevel(logging.INFO)

# ─── CONFIG ──────────────────────────────────────────────────────────────────

IST = pytz.timezone("Asia/Kolkata")

WATCHLIST = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
    "SBIN.NS", "BAJFINANCE.NS", "BHARTIARTL.NS", "KOTAKBANK.NS",
    "AXISBANK.NS", "INDUSINDBK.NS", "WIPRO.NS", "HCLTECH.NS", "TECHM.NS",
    "SUNPHARMA.NS", "DRREDDY.NS", "CIPLA.NS", "DIVISLAB.NS",
    "NTPC.NS", "POWERGRID.NS", "ONGC.NS", "COALINDIA.NS", "IRCTC.NS",
    "TATAMOTORS.NS", "MARUTI.NS", "ADANIPORTS.NS", "ADANIENT.NS",
    "BAJAJFINSV.NS", "HINDUNILVR.NS", "LTIM.NS",
]

MIN_MOVE    = 0.8
MAX_MOVE    = 5.0
VOL_MIN     = 1.8
RSI_LOW     = 55
RSI_HIGH    = 72
NEAR_HIGH   = 3.0
ALERT_SCORE = 7.5
MIN_SCORE   = 6.0

S = {
    "breakout": 2.0, "trend": 1.5, "volume": 2.0,
    "rsi": 1.5, "near_high": 1.0, "room": 0.5, "news": 1.0,
}

ENTRY_BUF  = 0.3
TARGET_PCT = 1.8
SL_PCT     = 0.9

# ─── RUN STATE (shown on dashboard page) ─────────────────────────────────────

state = {
    "last_run":     "Never",
    "last_result":  "No run yet",
    "alerts_sent":  0,
    "last_stocks":  [],
    "next_run":     "",
    "running":      False,
    "log_lines":    [],
}


def add_log(msg: str):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    log.info(msg)
    state["log_lines"].append(line)
    if len(state["log_lines"]) > 60:
        state["log_lines"] = state["log_lines"][-60:]


# ─── INDICATORS ──────────────────────────────────────────────────────────────

def compute_rsi(close: pd.Series, period=14) -> float:
    delta = close.diff()
    gain  = delta.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    loss  = (-delta).clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    rs    = gain / loss.replace(0, np.nan)
    val   = 100 - 100 / (1 + rs.iloc[-1])
    return float(val) if not np.isnan(val) else 50.0


def compute_indicators(df: pd.DataFrame):
    if len(df) < 15:
        return None
    close = df["Close"]
    vol   = df["Volume"]
    sma5  = close.rolling(5).mean()
    sma10 = close.rolling(10).mean()
    vol10 = vol.rolling(10).mean()

    curr   = float(close.iloc[-1])
    prev   = float(close.iloc[-2])
    high10 = float(df["High"].tail(10).max())
    v10    = float(vol10.iloc[-1])

    sl = df.tail(6)
    hh = sum(sl["High"].iloc[i] > sl["High"].iloc[i-1] for i in range(1, 6))
    hl = sum(sl["Low"].iloc[i]  > sl["Low"].iloc[i-1]  for i in range(1, 6))

    reject = False
    for _, r in df.tail(3).iterrows():
        body = abs(r["Close"] - r["Open"])
        wick = r["High"] - max(r["Close"], r["Open"])
        if body > 0 and wick > 2 * body:
            reject = True
            break

    return {
        "close":      curr,
        "change_pct": (curr - prev) / prev * 100 if prev else 0,
        "vol_ratio":  float(vol.iloc[-1]) / v10 if v10 else 0,
        "rsi":        compute_rsi(close),
        "uptrend":    float(sma5.iloc[-1]) > float(sma10.iloc[-1]) and curr > float(sma5.iloc[-1]),
        "near_high":  ((high10 - curr) / high10 * 100 <= NEAR_HIGH) if high10 else False,
        "hhhl":       hh >= 3 and hl >= 3,
        "rejection":  reject,
    }


# ─── SCAN + SCORE ─────────────────────────────────────────────────────────────

def scan_and_score():
    results = []
    add_log(f"Scanning {len(WATCHLIST)} stocks …")

    for sym in WATCHLIST:
        try:
            t  = yf.Ticker(sym)
            df = t.history(period="22d", interval="1d", timeout=15)
            if df.empty or len(df) < 12:
                continue
            df = df[["Open","High","Low","Close","Volume"]].copy()
            df.index = pd.to_datetime(df.index).tz_localize(None)
            time.sleep(0.4)
        except Exception as e:
            log.debug(f"{sym} fetch error: {e}")
            continue

        ind = compute_indicators(df)
        if ind is None:
            continue

        chg = ind["change_pct"]
        if not (MIN_MOVE <= chg <= MAX_MOVE):       continue
        if ind["vol_ratio"] < VOL_MIN:               continue
        if not ind["near_high"]:                     continue
        if not ind["uptrend"]:                       continue
        if not (RSI_LOW <= ind["rsi"] <= RSI_HIGH):  continue
        if ind["rejection"]:                         continue
        if ind["rsi"] > 80:                          continue

        bd = {}
        if ind["near_high"] and ind["vol_ratio"] >= 2.0: bd["breakout"]  = S["breakout"]
        if ind["hhhl"] and ind["uptrend"]:                bd["trend"]     = S["trend"]
        if ind["vol_ratio"] >= VOL_MIN:                   bd["volume"]    = S["volume"]
        if RSI_LOW <= ind["rsi"] <= RSI_HIGH:             bd["rsi"]       = S["rsi"]
        if ind["near_high"]:                              bd["near_high"] = S["near_high"]
        if chg < 3.0:                                     bd["room"]      = S["room"]

        news = []
        try:
            raw  = t.news or []
            news = [n.get("title","") for n in raw[:5] if n.get("title")]
            nl   = " ".join(news).lower()
            pos  = sum(w in nl for w in ["surge","rally","buy","upgrade","record","beat","profit","growth"])
            neg  = sum(w in nl for w in ["fall","drop","loss","sell","downgrade","weak","penalty"])
            if pos > neg and pos >= 1:
                bd["news"] = S["news"]
        except Exception:
            pass

        score = round(sum(bd.values()), 2)
        if score < MIN_SCORE:
            continue

        add_log(f"  ✓ {sym.replace('.NS','')}  score={score:.1f}  RSI={ind['rsi']:.1f}  vol={ind['vol_ratio']:.2f}x  Δ={chg:.2f}%")
        results.append({"symbol": sym, "score": score, "breakdown": bd, "ind": ind, "news": news})

    results.sort(key=lambda x: x["score"], reverse=True)
    return results


def fetch_cmp(symbol: str):
    try:
        fi = yf.Ticker(symbol).fast_info
        p  = fi.get("last_price") or fi.get("regularMarketPrice")
        return float(p) if p else None
    except Exception:
        return None


def trade_levels(cmp: float) -> dict:
    buf = cmp * ENTRY_BUF / 100
    el  = round(cmp - buf, 2)
    eh  = round(cmp + buf, 2)
    mid = (el + eh) / 2
    tgt = mid * (1 + TARGET_PCT / 100)
    return {
        "cmp": cmp, "entry_low": el, "entry_high": eh,
        "target_low":  round(tgt * 0.995, 2),
        "target_high": round(tgt * 1.005, 2),
        "stop_loss":   round(el * (1 - SL_PCT / 100), 2),
    }


# ─── EMAIL ────────────────────────────────────────────────────────────────────

def build_and_send_email(stocks: list) -> bool:
    sender = os.getenv("ALERT_EMAIL_SENDER", "")
    pwd    = os.getenv("ALERT_EMAIL_PASSWORD", "")
    recips = [r.strip() for r in os.getenv("ALERT_EMAIL_RECIPIENTS","").split(",") if r.strip()]

    add_log(f"Email sender: '{sender}' | pwd_len={len(pwd)} | recips={recips}")
    if not sender:
        add_log("ERROR: ALERT_EMAIL_SENDER is empty - check Render env vars")
        return False
    if not pwd:
        add_log("ERROR: ALERT_EMAIL_PASSWORD is empty - check Render env vars")
        return False
    if not recips:
        add_log("ERROR: ALERT_EMAIL_RECIPIENTS is empty - check Render env vars")
        return False

    syms    = ", ".join(s["symbol"].replace(".NS","") for s in stocks)
    subject = f"🚀 BUY SIGNAL | {syms} | StockSense AI"
    now     = datetime.now(IST).strftime("%d %b %Y  %H:%M IST")
    MAX_S   = 9.5

    def badge(cond):
        c = "#00C853" if cond else "#607D8B"
        t = "YES" if cond else "NO"
        return f'<span style="background:{c};color:#fff;padding:1px 8px;border-radius:3px;font-size:11px;font-weight:700">{t}</span>'

    cards = ""
    plain_body = f"{'='*56}\n  🚀 STOCKSENSE AI — BUY SIGNAL\n  {now}\n{'='*56}\n\n"

    for s in stocks:
        sym = s["symbol"].replace(".NS","")
        tl  = s["levels"]
        bd  = s["breakdown"]
        ind = s["ind"]
        pct = min(100, s["score"] / MAX_S * 100)

        plain_body += (
            f"📌  {sym}\n"
            f"    CMP          : ₹{tl['cmp']:,.2f}\n"
            f"    Buy Range    : ₹{tl['entry_low']:,.2f} – ₹{tl['entry_high']:,.2f}\n"
            f"    Target Range : ₹{tl['target_low']:,.2f} – ₹{tl['target_high']:,.2f}\n"
            f"    Stop Loss    : ₹{tl['stop_loss']:,.2f}\n"
            f"    Timeframe    : 2–3 Trading Days\n"
            f"    Score        : {s['score']:.1f} / {MAX_S}\n\n"
            f"    ✔ Breakout    : {'Yes' if 'breakout' in bd else 'No'}\n"
            f"    ✔ Trend       : {'Yes' if 'trend' in bd else 'No'}\n"
            f"    ✔ RSI         : {'Yes' if 'rsi' in bd else 'No'} ({ind['rsi']:.1f})\n"
            f"    ✔ Volume      : {'Yes' if 'volume' in bd else 'No'} ({ind['vol_ratio']:.2f}x)\n"
            f"    ✔ Positive News: {'Yes' if 'news' in bd else 'No'}\n"
            f"\n{'-'*56}\n\n"
        )

        news_html = ""
        if s["news"]:
            items = "".join(f"<li>{h[:110]}</li>" for h in s["news"][:2])
            news_html = f'<div style="margin-top:14px"><div style="font-size:11px;color:#78909C;margin-bottom:6px;text-transform:uppercase;letter-spacing:1px">Recent News</div><ul style="margin:0;padding-left:16px;color:#B0BEC5;font-size:12px;line-height:1.8">{items}</ul></div>'

        cards += f"""
        <div style="background:#1A2B3C;border-radius:12px;padding:24px;margin-bottom:20px;border-left:4px solid #00E5FF">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px">
            <div>
              <div style="font-size:22px;font-weight:900;color:#E8F4FD;letter-spacing:1px">{sym}</div>
              <div style="color:#00E5FF;font-size:11px;margin-top:2px">NSE · 2–3 DAY SWING</div>
            </div>
            <div style="text-align:right">
              <div style="font-size:11px;color:#607D8B">SCORE</div>
              <div style="font-size:28px;font-weight:900;color:#FFD600;line-height:1">{s['score']:.1f}<span style="font-size:12px;color:#607D8B;font-weight:400"> / {MAX_S}</span></div>
            </div>
          </div>
          <div style="margin:14px 0 4px;background:#263D52;border-radius:4px;height:4px">
            <div style="width:{pct:.0f}%;background:linear-gradient(90deg,#00E5FF,#FFD600);border-radius:4px;height:4px"></div>
          </div>
          <table style="width:100%;border-collapse:collapse;margin-top:18px">
            <tr><td style="padding:9px 0;border-bottom:1px solid #263D52;color:#78909C;font-size:13px">Current Price</td><td style="padding:9px 0;border-bottom:1px solid #263D52;text-align:right;color:#E8F4FD;font-weight:700;font-size:16px">₹{tl['cmp']:,.2f}</td></tr>
            <tr><td style="padding:9px 0;border-bottom:1px solid #263D52;color:#78909C;font-size:13px">Buy Range</td><td style="padding:9px 0;border-bottom:1px solid #263D52;text-align:right;color:#00E5FF;font-weight:700;font-size:16px">₹{tl['entry_low']:,.2f} – ₹{tl['entry_high']:,.2f}</td></tr>
            <tr><td style="padding:9px 0;border-bottom:1px solid #263D52;color:#78909C;font-size:13px">Target Range</td><td style="padding:9px 0;border-bottom:1px solid #263D52;text-align:right;color:#00C853;font-weight:700;font-size:16px">₹{tl['target_low']:,.2f} – ₹{tl['target_high']:,.2f}</td></tr>
            <tr><td style="padding:9px 0;color:#78909C;font-size:13px">Stop Loss</td><td style="padding:9px 0;text-align:right;color:#FF1744;font-weight:700;font-size:16px">₹{tl['stop_loss']:,.2f}</td></tr>
          </table>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:18px">
            <div style="background:#263D52;padding:10px 12px;border-radius:7px"><div style="font-size:10px;color:#607D8B;letter-spacing:1px;text-transform:uppercase;margin-bottom:4px">Breakout</div>{badge('breakout' in bd)}</div>
            <div style="background:#263D52;padding:10px 12px;border-radius:7px"><div style="font-size:10px;color:#607D8B;letter-spacing:1px;text-transform:uppercase;margin-bottom:4px">Strong Trend</div>{badge('trend' in bd)}</div>
            <div style="background:#263D52;padding:10px 12px;border-radius:7px"><div style="font-size:10px;color:#607D8B;letter-spacing:1px;text-transform:uppercase;margin-bottom:4px">RSI Zone</div>{badge('rsi' in bd)}<span style="color:#607D8B;font-size:11px;margin-left:5px">{ind['rsi']:.1f}</span></div>
            <div style="background:#263D52;padding:10px 12px;border-radius:7px"><div style="font-size:10px;color:#607D8B;letter-spacing:1px;text-transform:uppercase;margin-bottom:4px">Volume</div>{badge('volume' in bd)}<span style="color:#607D8B;font-size:11px;margin-left:5px">{ind['vol_ratio']:.2f}x</span></div>
          </div>
          {news_html}
        </div>"""

    plain_body += "⚠ Automated algo alert — not financial advice. Use strict stop-losses."

    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#0B1826;font-family:'Segoe UI',Tahoma,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0"><tr><td align="center" style="padding:28px 12px">
<table width="600" style="max-width:600px;width:100%">
  <tr><td style="background:linear-gradient(135deg,#0B1826,#1A2B3C);border-radius:14px 14px 0 0;padding:32px 28px;border-bottom:2px solid #00E5FF">
    <div style="font-size:10px;color:#00E5FF;letter-spacing:3px;text-transform:uppercase;margin-bottom:8px">StockSense AI · Momentum Alert</div>
    <div style="font-size:32px;font-weight:900;color:#fff">🚀 BUY SIGNAL</div>
    <div style="font-size:12px;color:#607D8B;margin-top:6px">{now}</div>
  </td></tr>
  <tr><td style="background:#0F1F2E;padding:22px 28px">
    <div style="font-size:13px;color:#607D8B;margin-bottom:18px;line-height:1.7">
      Scanner identified <strong style="color:#E8F4FD">{len(stocks)} high-probability momentum setup{"s" if len(stocks)!=1 else ""}</strong>.
      Entry is a range — do not chase above the upper bound.
    </div>{cards}
  </td></tr>
  <tr><td style="background:#0B1826;border-radius:0 0 14px 14px;padding:18px 28px;border-top:1px solid #1A2B3C">
    <div style="font-size:11px;color:#37474F;line-height:1.8">⚠ <strong>Disclaimer:</strong> Automated algorithmic signal — not financial advice. Use strict stop-losses.</div>
  </td></tr>
</table></td></tr></table></body></html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = ", ".join(recips)
    msg.attach(MIMEText(plain_body, "plain", "utf-8"))
    msg.attach(MIMEText(html,       "html",  "utf-8"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as srv:
            srv.ehlo(); srv.starttls()
            srv.login(sender, pwd)
            srv.sendmail(sender, recips, msg.as_string())
        add_log(f"✅ Email sent → {recips}  stocks={syms}")
        return True
    except smtplib.SMTPAuthenticationError:
        add_log("❌ Gmail auth failed — check App Password")
        return False
    except Exception as e:
        add_log(f"❌ Email error: {e}")
        return False


# ─── MAIN PIPELINE JOB ───────────────────────────────────────────────────────

def run_pipeline():
    if state["running"]:
        add_log("Pipeline already running — skipped")
        return

    now_ist = datetime.now(IST)

    # Skip weekends
    if now_ist.weekday() >= 5:
        add_log(f"Weekend ({now_ist.strftime('%A')}) — skipped")
        return

    state["running"] = True
    state["last_run"] = now_ist.strftime("%d %b %Y  %H:%M IST")
    add_log(f"=== Pipeline started  {state['last_run']} ===")

    try:
        candidates = scan_and_score()

        if not candidates:
            state["last_result"] = "No stocks passed filter"
            state["last_stocks"] = []
            add_log("No stocks passed the momentum filter — no alert sent")
            return

        finalists = [s for s in candidates if s["score"] >= ALERT_SCORE][:2]

        if not finalists:
            best = candidates[0]
            state["last_result"] = f"Best: {best['symbol']} @ {best['score']:.1f} — below threshold"
            state["last_stocks"] = []
            add_log(f"No stock met score ≥ {ALERT_SCORE}. Best: {best['symbol']} @ {best['score']:.1f}")
            return

        for s in finalists:
            cmp = fetch_cmp(s["symbol"]) or s["ind"]["close"]
            s["levels"] = trade_levels(cmp)

        sent = build_and_send_email(finalists)

        if sent:
            state["alerts_sent"] += len(finalists)
            state["last_result"] = f"✅ Alert sent: {', '.join(s['symbol'].replace('.NS','') for s in finalists)}"
            state["last_stocks"] = [
                {
                    "symbol":      s["symbol"].replace(".NS",""),
                    "score":       s["score"],
                    "cmp":         s["levels"]["cmp"],
                    "entry_low":   s["levels"]["entry_low"],
                    "entry_high":  s["levels"]["entry_high"],
                    "target_low":  s["levels"]["target_low"],
                    "target_high": s["levels"]["target_high"],
                    "stop_loss":   s["levels"]["stop_loss"],
                    "rsi":         round(s["ind"]["rsi"], 1),
                    "vol_ratio":   round(s["ind"]["vol_ratio"], 2),
                }
                for s in finalists
            ]
        else:
            state["last_result"] = "⚠ Alert failed — check email config"

    except Exception as e:
        add_log(f"Pipeline error: {e}")
        state["last_result"] = f"Error: {e}"
    finally:
        state["running"] = False
        add_log("=== Pipeline complete ===")


# ─── FLASK APP ────────────────────────────────────────────────────────────────

app = Flask(__name__)

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="60">
<title>StockSense AI</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Sora:wght@400;600;800&display=swap');
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:#080f1a;color:#cdd9e5;font-family:'Sora',sans-serif;min-height:100vh}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:16px;margin-bottom:24px}
  .card{background:#0f1e2e;border:1px solid #1e3248;border-radius:12px;padding:20px}
  .card.accent{border-color:#00e5ff33;border-left:3px solid #00e5ff}
  .label{font-size:10px;letter-spacing:2px;text-transform:uppercase;color:#4a6a8a;margin-bottom:6px;font-family:'JetBrains Mono',monospace}
  .value{font-size:22px;font-weight:800;color:#e8f4fd}
  .value.green{color:#00c853}
  .value.cyan{color:#00e5ff}
  .value.yellow{color:#ffd600}
  .mono{font-family:'JetBrains Mono',monospace;font-size:13px}
  header{background:#0a1520;border-bottom:1px solid #1e3248;padding:18px 32px;display:flex;align-items:center;justify-content:space-between}
  header h1{font-size:20px;font-weight:800;color:#fff;letter-spacing:.5px}
  header h1 span{color:#00e5ff}
  .badge{background:#00e5ff15;border:1px solid #00e5ff44;color:#00e5ff;font-size:10px;padding:3px 10px;border-radius:20px;font-family:'JetBrains Mono',monospace;letter-spacing:1px}
  .badge.live{background:#00c85315;border-color:#00c85344;color:#00c853}
  .badge.warn{background:#ff990015;border-color:#ff990044;color:#ff9900}
  main{max-width:1100px;margin:0 auto;padding:28px 20px}
  h2{font-size:13px;letter-spacing:2px;text-transform:uppercase;color:#4a6a8a;margin-bottom:14px;font-family:'JetBrains Mono',monospace}
  .stock-card{background:#0f1e2e;border:1px solid #1e3248;border-left:4px solid #00e5ff;border-radius:12px;padding:20px;margin-bottom:16px}
  .stock-header{display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px;margin-bottom:16px}
  .sym{font-size:24px;font-weight:800;color:#e8f4fd;letter-spacing:1px}
  .sub{font-size:11px;color:#00e5ff;margin-top:3px}
  .score-box{text-align:right}
  .score-num{font-size:30px;font-weight:900;color:#ffd600;line-height:1}
  .score-den{font-size:12px;color:#4a6a8a;font-weight:400}
  .price-row{display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:12px;margin-top:4px}
  .price-cell{background:#162030;border-radius:8px;padding:12px}
  .price-label{font-size:10px;letter-spacing:1px;text-transform:uppercase;color:#4a6a8a;margin-bottom:4px;font-family:'JetBrains Mono',monospace}
  .price-val{font-size:16px;font-weight:700;font-family:'JetBrains Mono',monospace}
  .price-val.cyan{color:#00e5ff}
  .price-val.green{color:#00c853}
  .price-val.red{color:#ff1744}
  .price-val.white{color:#e8f4fd}
  .log-box{background:#060e18;border:1px solid #1e3248;border-radius:10px;padding:16px;height:240px;overflow-y:auto;font-family:'JetBrains Mono',monospace;font-size:12px;color:#4a7a9b;line-height:1.9}
  .log-box span.ok{color:#00c853}
  .log-box span.err{color:#ff1744}
  .log-box span.info{color:#00e5ff}
  .run-btn{background:#00e5ff;color:#080f1a;border:none;padding:10px 24px;border-radius:8px;font-weight:700;font-size:13px;cursor:pointer;font-family:'Sora',sans-serif;letter-spacing:.5px;transition:opacity .2s}
  .run-btn:hover{opacity:.85}
  .run-btn:disabled{opacity:.4;cursor:not-allowed}
  .sched{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:20px}
  .sched-pill{background:#0f1e2e;border:1px solid #1e3248;border-radius:20px;padding:5px 14px;font-size:12px;color:#cdd9e5;font-family:'JetBrains Mono',monospace}
  .sched-pill.active{background:#00e5ff15;border-color:#00e5ff;color:#00e5ff}
  .bar-wrap{background:#1e3248;border-radius:4px;height:4px;margin:10px 0 14px}
  .bar{height:4px;border-radius:4px;background:linear-gradient(90deg,#00e5ff,#ffd600)}
  @media(max-width:600px){.price-row{grid-template-columns:1fr 1fr}}
</style>
</head>
<body>
<header>
  <h1>Stock<span>Sense</span> AI</h1>
  <div style="display:flex;gap:10px;align-items:center">
    {% if running %}
    <span class="badge warn">⟳ SCANNING</span>
    {% else %}
    <span class="badge live">● LIVE</span>
    {% endif %}
    <span class="badge">NSE · EMAIL ALERTS</span>
  </div>
</header>

<main>
  <!-- Stats -->
  <div class="grid" style="margin-top:0">
    <div class="card accent">
      <div class="label">Last Run</div>
      <div class="value mono" style="font-size:15px">{{ last_run }}</div>
    </div>
    <div class="card accent">
      <div class="label">Last Result</div>
      <div class="value mono" style="font-size:14px">{{ last_result }}</div>
    </div>
    <div class="card accent">
      <div class="label">Alerts Sent</div>
      <div class="value green">{{ alerts_sent }}</div>
    </div>
    <div class="card accent">
      <div class="label">Next Run (IST)</div>
      <div class="value cyan mono" style="font-size:16px">{{ next_run }}</div>
    </div>
  </div>

  <!-- Schedule -->
  <h2>Daily Schedule</h2>
  <div class="sched">
    {% for t in ["09:30","11:00","13:30","14:30"] %}
    <div class="sched-pill {% if t == current_slot %}active{% endif %}">{{ t }} IST</div>
    {% endfor %}
  </div>

  <!-- Manual trigger -->
  <div style="margin-bottom:28px;display:flex;align-items:center;gap:16px">
    <button class="run-btn" onclick="triggerRun(this)" {% if running %}disabled{% endif %}>
      ▶ Run Scan Now
    </button>
    <span style="font-size:12px;color:#4a6a8a">Page auto-refreshes every 60s</span>
  </div>

  <!-- Last alerts -->
  {% if stocks %}
  <h2>Last Alert Stocks</h2>
  {% for s in stocks %}
  <div class="stock-card">
    <div class="stock-header">
      <div>
        <div class="sym">{{ s.symbol }}</div>
        <div class="sub">NSE · 2–3 DAY SWING · RSI {{ s.rsi }} · Vol {{ s.vol_ratio }}x</div>
      </div>
      <div class="score-box">
        <div style="font-size:10px;color:#4a6a8a;letter-spacing:1px;margin-bottom:2px">SCORE</div>
        <div class="score-num">{{ s.score }}<span class="score-den"> / 9.5</span></div>
      </div>
    </div>
    <div class="bar-wrap"><div class="bar" style="width:{{ (s.score/9.5*100)|int }}%"></div></div>
    <div class="price-row">
      <div class="price-cell">
        <div class="price-label">CMP</div>
        <div class="price-val white">₹{{ "{:,.2f}".format(s.cmp) }}</div>
      </div>
      <div class="price-cell">
        <div class="price-label">Buy Range</div>
        <div class="price-val cyan" style="font-size:13px">₹{{ "{:,.2f}".format(s.entry_low) }}–{{ "{:,.2f}".format(s.entry_high) }}</div>
      </div>
      <div class="price-cell">
        <div class="price-label">Target</div>
        <div class="price-val green" style="font-size:13px">₹{{ "{:,.2f}".format(s.target_low) }}–{{ "{:,.2f}".format(s.target_high) }}</div>
      </div>
      <div class="price-cell">
        <div class="price-label">Stop Loss</div>
        <div class="price-val red">₹{{ "{:,.2f}".format(s.stop_loss) }}</div>
      </div>
    </div>
  </div>
  {% endfor %}
  {% endif %}

  <!-- Log -->
  <h2 style="margin-top:8px">Live Log</h2>
  <div class="log-box" id="logbox">
    {% for line in log_lines %}
    <div class="
      {% if '✓' in line or '✅' in line %}ok
      {% elif '❌' in line or 'error' in line|lower %}err
      {% elif '===' in line %}info
      {% endif %}
    ">{{ line }}</div>
    {% endfor %}
  </div>
</main>

<script>
  document.getElementById('logbox').scrollTop = 999999;
  function triggerRun(btn) {
    btn.disabled = true;
    btn.textContent = '⟳ Scanning…';
    fetch('/run').then(r => r.json()).then(d => {
      setTimeout(() => location.reload(), 3000);
    }).catch(() => { btn.disabled = false; btn.textContent = '▶ Run Scan Now'; });
  }
</script>
</body>
</html>"""


@app.route("/")
def dashboard():
    now_ist = datetime.now(IST)
    slots   = ["09:30","11:00","13:30","14:30"]
    h, m    = now_ist.hour, now_ist.minute
    cur_t   = f"{h:02d}:{m:02d}"
    current_slot = ""
    for sl in slots:
        if cur_t >= sl:
            current_slot = sl

    # Next run time
    next_run = ""
    for sl in slots:
        if cur_t < sl:
            next_run = sl
            break
    if not next_run:
        next_run = slots[0] + " (tomorrow)"

    state["next_run"] = next_run

    return render_template_string(
        DASHBOARD_HTML,
        last_run     = state["last_run"],
        last_result  = state["last_result"],
        alerts_sent  = state["alerts_sent"],
        next_run     = next_run,
        stocks       = state["last_stocks"],
        log_lines    = state["log_lines"][-40:],
        running      = state["running"],
        current_slot = current_slot,
    )


@app.route("/run")
def manual_run():
    if state["running"]:
        return jsonify({"status": "already_running"})
    thread = threading.Thread(target=run_pipeline, daemon=True)
    thread.start()
    return jsonify({"status": "started"})


@app.route("/status")
def status():
    return jsonify({
        "last_run":    state["last_run"],
        "last_result": state["last_result"],
        "alerts_sent": state["alerts_sent"],
        "running":     state["running"],
        "last_stocks": state["last_stocks"],
    })


@app.route("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.now(IST).strftime("%H:%M IST")})


# ─── SCHEDULER ───────────────────────────────────────────────────────────────

def start_scheduler():
    scheduler = BackgroundScheduler(timezone=IST)
    # 09:30, 11:00, 13:30, 14:30 IST — weekdays only
    for hour, minute in [(9,30),(11,0),(13,30),(14,30)]:
        scheduler.add_job(
            run_pipeline,
            CronTrigger(hour=hour, minute=minute, day_of_week="mon-fri", timezone=IST),
        )
    scheduler.start()
    add_log(f"Scheduler started — 4 daily scans (IST): 09:30 · 11:00 · 13:30 · 14:30")
    return scheduler


@app.route("/test-email")
def test_email():
    ok = build_and_send_email([{
        "symbol": "RELIANCE.NS",
        "score": 8.5,
        "breakdown": {"breakout": 1, "trend": 1, "volume": 1, "rsi": 1, "near_high": 1},
        "ind": {"rsi": 63.2, "vol_ratio": 2.4, "change_pct": 1.8},
        "news": ["Reliance reports record quarterly profit"],
        "levels": {
            "cmp": 2850.00,
            "entry_low": 2841.45,
            "entry_high": 2858.55,
            "target_low": 2891.23,
            "target_high": 2900.77,
            "stop_loss": 2815.87,
        }
    }])
    return jsonify({"email_sent": ok})


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    add_log("StockSense AI starting …")
    scheduler = start_scheduler()
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
