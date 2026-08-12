import os
import gc
import time
import sqlite3
import logging
import threading
from dataclasses import dataclass
from datetime import datetime

import requests
import ccxt
import pandas as pd
import streamlit as st

# =========================================================
# STREAMLIT CONFIG — BG STAR PRO v3
# =========================================================
st.set_page_config(
    page_title="BG STAR PRO v3 — Streamlit",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
)

COINS = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT"]
TIMEFRAMES = ["15m"]
SCAN_INTERVAL_SECONDS = 300
BTC_CONTEXT_CACHE_SECONDS = 15 * 60
HTF_CONTEXT_CACHE_SECONDS = 30 * 60
DB_FILE = "signals.db"

# Streamlit Cloud: prefer st.secrets, then environment variables.
def secret_or_env(name, default=""):
    try:
        value = st.secrets.get(name, None)
        if value is not None:
            return str(value)
    except Exception:
        pass
    return os.getenv(name, default)

TELEGRAM_BOT_TOKEN = secret_or_env("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = secret_or_env("TELEGRAM_CHAT_ID")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("BG_STAR_PRO_STREAMLIT")

# =========================================================
# GLOBAL STATE
# =========================================================
STATE_LOCK = threading.Lock()
SCAN_LOCK = threading.Lock()
DB_LOCK = threading.Lock()

CONTEXT_CACHE = {"btc_trend": "NEUTRAL", "last_update": 0}
SYSTEM_STATUS = {
    "last_scan": "Not scanned yet",
    "scan_duration_ms": 0,
    "total_signals": 0,
    "last_signal": None,
    "telegram_last": "Not sent",
}

thread_local = threading.local()

# =========================================================
# SQLITE — STREAMLIT COMPATIBLE
# =========================================================
def get_db_connection():
    conn = sqlite3.connect(DB_FILE, timeout=15.0, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn

def init_db():
    with DB_LOCK:
        try:
            with get_db_connection() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS processed_signals (
                        signal_key TEXT PRIMARY KEY,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.commit()
        except Exception as e:
            logger.error(f"DB Init Error: {e}")

def is_signal_processed(signal_key):
    with DB_LOCK:
        try:
            with get_db_connection() as conn:
                cursor = conn.execute(
                    "SELECT 1 FROM processed_signals WHERE signal_key = ?",
                    (signal_key,)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"DB Read Error: {e}")
            return False

def mark_signal_processed(signal_key):
    with DB_LOCK:
        try:
            with get_db_connection() as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO processed_signals (signal_key) VALUES (?)",
                    (signal_key,)
                )
                conn.execute("""
                    DELETE FROM processed_signals
                    WHERE signal_key NOT IN (
                        SELECT signal_key
                        FROM processed_signals
                        ORDER BY timestamp DESC
                        LIMIT 1000
                    )
                """)
                conn.commit()
        except Exception as e:
            logger.error(f"DB Write Error: {e}")

# =========================================================
# TELEGRAM — STREAMLIT SAFE SENDER
# =========================================================
telegram_lock = threading.Lock()

def send_telegram_message(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        SYSTEM_STATUS["telegram_last"] = "Not configured"
        logger.warning("Telegram credentials are not configured.")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    # Telegram 429/temporary server errors: bounded retry.
    for attempt in range(3):
        try:
            with telegram_lock:
                response = requests.post(url, json=payload, timeout=10)

            if response.status_code == 200:
                SYSTEM_STATUS["telegram_last"] = (
                    f"Sent {datetime.now().strftime('%H:%M:%S')}"
                )
                return True

            if response.status_code == 429:
                try:
                    retry_after = int(
                        response.json().get("parameters", {}).get("retry_after", 2)
                    )
                except Exception:
                    retry_after = 2
                time.sleep(min(max(retry_after, 1), 15))
                continue

            if response.status_code in (500, 502, 503, 504):
                time.sleep(2 ** attempt)
                continue

            logger.warning(
                f"Telegram HTTP {response.status_code}: {response.text[:300]}"
            )
            break

        except requests.RequestException as e:
            logger.error(f"Telegram request error: {e}")
            time.sleep(2 ** attempt)

    SYSTEM_STATUS["telegram_last"] = "Send failed"
    return False

# =========================================================
# EXCHANGE — THREAD LOCAL
# =========================================================
def get_exchange():
    if getattr(thread_local, "needs_reconnect", False):
        if hasattr(thread_local, "exchange"):
            try:
                thread_local.exchange.close()
            except Exception:
                pass
            del thread_local.exchange
        thread_local.needs_reconnect = False

    if not hasattr(thread_local, "exchange"):
        try:
            thread_local.exchange = ccxt.kucoin({
                "enableRateLimit": True,
                "timeout": 10000,
            })
        except Exception as e:
            logger.error(f"Exchange init failed: {e}")
            return None

    return thread_local.exchange


# =========================================================
# SIGNAL MODEL & SMC LOGIC (Untouched Core Logic)
# =========================================================
@dataclass
class Signal:
    coin: str; tf: str; direction: str; entry: float; sl: float; tp: float; score: int; timestamp: pd.Timestamp; reasons: list

class SMCAnalyzer:
    @staticmethod
    def identify_pivots(df, left_bars=5, right_bars=2):
        df["pivot_high"] = False
        df["pivot_low"] = False
        if len(df) < left_bars + right_bars + 5: return df

        for i in range(left_bars, len(df) - right_bars):
            high_value, low_value = df["high"].iloc[i], df["low"].iloc[i]
            is_high, is_low = True, True

            for j in range(1, left_bars + 1):
                if high_value <= df["high"].iloc[i - j]: is_high = False
                if low_value >= df["low"].iloc[i - j]: is_low = False
            for j in range(1, right_bars + 1):
                if high_value <= df["high"].iloc[i + j]: is_high = False
                if low_value >= df["low"].iloc[i + j]: is_low = False

            if is_high: df.iat[i, df.columns.get_loc("pivot_high")] = True
            if is_low: df.iat[i, df.columns.get_loc("pivot_low")] = True
        return df

    @staticmethod
    def detect_fvg(df):
        df["fvg_bull"] = False
        df["fvg_bear"] = False
        bull = ((df["low"] > df["high"].shift(2)) & (df["close"].shift(1) > df["open"].shift(1)))
        bear = ((df["high"] < df["low"].shift(2)) & (df["close"].shift(1) < df["open"].shift(1)))
        df.loc[bull, "fvg_bull"] = True
        df.loc[bear, "fvg_bear"] = True
        return df

    @staticmethod
    def map_structure_and_ob(df):
        df["structure"] = "NONE"
        df["ob_bull"], df["ob_bear"], df["liq_sweep"] = False, False, False
        last_ph, last_pl, trend = None, None, 0

        for i in range(10, len(df)):
            curr_high, curr_low, curr_close = df["high"].iloc[i], df["low"].iloc[i], df["close"].iloc[i]
            if df["pivot_high"].iloc[i]: last_ph = curr_high
            if df["pivot_low"].iloc[i]: last_pl = curr_low
            if last_ph is None or last_pl is None: continue

            if curr_high > last_ph and curr_close < last_ph: df.iat[i, df.columns.get_loc("liq_sweep")] = True
            if curr_low < last_pl and curr_close > last_pl: df.iat[i, df.columns.get_loc("liq_sweep")] = True

            if curr_close > last_ph:
                if trend == -1:
                    df.iat[i, df.columns.get_loc("structure")] = "CHOCH_BULL"
                    trend = 1
                elif trend == 1:
                    df.iat[i, df.columns.get_loc("structure")] = "BOS_BULL"
                    ob_idx = i - 1
                    while ob_idx > 0 and df["close"].iloc[ob_idx] >= df["open"].iloc[ob_idx]: ob_idx -= 1
                    if ob_idx > 0: df.iat[ob_idx, df.columns.get_loc("ob_bull")] = True
                last_ph = None
            elif curr_close < last_pl:
                if trend == 1:
                    df.iat[i, df.columns.get_loc("structure")] = "CHOCH_BEAR"
                    trend = -1
                elif trend == -1:
                    df.iat[i, df.columns.get_loc("structure")] = "BOS_BEAR"
                    ob_idx = i - 1
                    while ob_idx > 0 and df["close"].iloc[ob_idx] <= df["open"].iloc[ob_idx]: ob_idx -= 1
                    if ob_idx > 0: df.iat[ob_idx, df.columns.get_loc("ob_bear")] = True
                last_pl = None
        return df

    @staticmethod
    def calc_atr(df):
        prev_close = df["close"].shift(1)
        tr1 = df["high"] - df["low"]
        tr2 = (df["high"] - prev_close).abs()
        tr3 = (df["low"] - prev_close).abs()
        df["atr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1).rolling(14).mean()
        return df

# =========================================================
# MARKET DATA ENGINE
# =========================================================
class BGStarEngine:
    def fetch_data(self, sym, tf, limit=200):
        try:
            ex = get_exchange()
            if ex is None: return None
            ohlcv = ex.fetch_ohlcv(sym, tf, limit=limit)
            if not ohlcv: return None
            df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            return df.set_index("timestamp")
        except ccxt.RateLimitExceeded:
            logger.warning(f"KuCoin rate limit: {sym} {tf}. Backing off.")
            time.sleep(3)
            return None
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            logger.error(f"Network/Exchange Error {sym} {tf}: {e}. Triggering Reconnect.")
            thread_local.needs_reconnect = True
            return None
        except Exception as e:
            logger.error(f"Unknown Fetch Error {sym} {tf}: {e}")
            thread_local.needs_reconnect = True
            return None

    def update_global_context(self):
        now = time.time()
        with STATE_LOCK:
            if now - CONTEXT_CACHE["last_update"] < BTC_CONTEXT_CACHE_SECONDS: return

        btc = self.fetch_data("BTC/USDT", "1h", limit=150)
        if btc is None: return
        try:
            btc = SMCAnalyzer.identify_pivots(btc)
            btc = SMCAnalyzer.map_structure_and_ob(btc)
            structures = btc[btc["structure"] != "NONE"].tail(3)
            trend = "NEUTRAL"
            
            if len(structures) > 0:
                last_structure = structures.iloc[-1]["structure"]
                if "BULL" in last_structure: trend = "BULLISH"
                elif "BEAR" in last_structure: trend = "BEARISH"
                
            with STATE_LOCK: 
                CONTEXT_CACHE["btc_trend"] = trend
                CONTEXT_CACHE["last_update"] = now
        finally:
            del btc

    def get_mtf_context(self, sym):
        now = time.time()
        with STATE_LOCK:
            cached = CONTEXT_CACHE.get(sym)
            if cached and now - cached.get("last_fetch", 0) < HTF_CONTEXT_CACHE_SECONDS: return cached

        df_1h, df_4h = self.fetch_data(sym, "1h", limit=200), self.fetch_data(sym, "4h", limit=200)
        if df_1h is None or df_4h is None: return None

        try:
            ema200_1h = df_1h["close"].ewm(span=200, adjust=False).mean().iloc[-2]
            ema200_4h = df_4h["close"].ewm(span=200, adjust=False).mean().iloc[-2]
            context = {"ema200_1h": float(ema200_1h), "ema200_4h": float(ema200_4h), "last_fetch": now}
            with STATE_LOCK: CONTEXT_CACHE[sym] = context
            return context
        finally:
            del df_1h, df_4h

    def analyze_tf(self, sym, tf, mtf_ctx, btc_trend):
        df = self.fetch_data(sym, tf, limit=200)
        if df is None or len(df) < 50: return None
        
        df = df.iloc[:-1].copy() # Strict Closed Candle Analysis
        
        df = SMCAnalyzer.identify_pivots(df)
        df = SMCAnalyzer.detect_fvg(df)
        df = SMCAnalyzer.map_structure_and_ob(df)
        df = SMCAnalyzer.calc_atr(df)
        
        last_closed = df.iloc[-1]
        prev_closed = df.iloc[-2]
        
        if pd.isna(last_closed['atr']) or last_closed['atr'] == 0: 
            del df; return None

        if (last_closed['atr'] / last_closed['close']) * 100 < 0.15:
            del df; return None

        htf_bull = last_closed['close'] > mtf_ctx["ema200_1h"] and last_closed['close'] > mtf_ctx["ema200_4h"]
        htf_bear = last_closed['close'] < mtf_ctx["ema200_1h"] and last_closed['close'] < mtf_ctx["ema200_4h"]

        signal = None
        score = 0
        recent_window = df.tail(10)
        
        fvg_bull_present = recent_window['fvg_bull'].any()
        ob_bull_present = recent_window['ob_bull'].any()
        liq_sweep_recent = recent_window['liq_sweep'].any()
        fvg_bear_present = recent_window['fvg_bear'].any()
        ob_bear_present = recent_window['ob_bear'].any()

        body_size = abs(last_closed['close'] - last_closed['open'])
        has_displacement = body_size > (1.2 * last_closed['atr'])

        sl_multiplier = 1.0 if liq_sweep_recent else 1.5
        tp_multiplier = 3.0 if (ob_bull_present or ob_bear_present) else 2.5

        if htf_bull and btc_trend != "BEARISH":
            if last_closed['structure'] in ['BOS_BULL', 'CHOCH_BULL'] or prev_closed['structure'] in ['BOS_BULL', 'CHOCH_BULL']:
                score += 3
                if htf_bull: score += 2
                if btc_trend == "BULLISH": score += 1
                if fvg_bull_present: score += 2
                if ob_bull_present: score += 1
                if liq_sweep_recent: score += 1
                if has_displacement: score += 1
                
                if score >= 8:
                    entry = last_closed['close']
                    sl = entry - (last_closed['atr'] * sl_multiplier)
                    tp = entry + (last_closed['atr'] * tp_multiplier)
                    signal = Signal(sym, tf, "BUY", entry, sl, tp, score, df.index[-1])

        if not signal and htf_bear and btc_trend != "BULLISH":
            if last_closed['structure'] in ['BOS_BEAR', 'CHOCH_BEAR'] or prev_closed['structure'] in ['BOS_BEAR', 'CHOCH_BEAR']:
                score += 3
                if htf_bear: score += 2
                if btc_trend == "BEARISH": score += 1
                if fvg_bear_present: score += 2
                if ob_bear_present: score += 1
                if liq_sweep_recent: score += 1
                if has_displacement: score += 1
                
                if score >= 8:
                    entry = last_closed['close']
                    sl = entry + (last_closed['atr'] * sl_multiplier)
                    tp = entry - (last_closed['atr'] * tp_multiplier)
                    signal = Signal(sym, tf, "SELL", entry, sl, tp, score, df.index[-1])

        del df
        return signal

    def scan_coin(self, sym):
        mtf_ctx = self.get_mtf_context(sym)
        if not mtf_ctx: return []
        with STATE_LOCK: btc_trend = CONTEXT_CACHE.get("btc_trend", "NEUTRAL")
        signals = [self.analyze_tf(sym, tf, mtf_ctx, btc_trend) for tf in TIMEFRAMES]
        return [s for s in signals if s]



# =========================================================
# SCAN ENGINE — STREAMLIT RERUN MODEL
# =========================================================
engine = BGStarEngine()

def run_scan_job():
    if not SCAN_LOCK.acquire(blocking=False):
        logger.warning("Previous scan still running. Skipping this cycle.")
        return []

    start_time = time.time()
    all_signals = []

    try:
        now = datetime.now()
        with STATE_LOCK:
            SYSTEM_STATUS["last_scan"] = now.strftime("%Y-%m-%d %H:%M:%S")

        engine.update_global_context()

        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(engine.scan_coin, coin): coin
                for coin in COINS
            }
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        all_signals.extend(result)
                except Exception as e:
                    logger.error(f"Worker Error: {e}")

        new_signals = []

        for sig in all_signals:
            signal_key = f"{sig.coin}_{sig.direction}_{sig.timestamp}"

            if is_signal_processed(signal_key):
                continue

            # Mark BEFORE Telegram send to prevent duplicate sends on Streamlit reruns.
            mark_signal_processed(signal_key)

            with STATE_LOCK:
                SYSTEM_STATUS["total_signals"] += 1
                SYSTEM_STATUS["last_signal"] = {
                    "coin": sig.coin,
                    "direction": sig.direction,
                    "score": sig.score,
                    "timeframe": sig.tf,
                    "entry": float(sig.entry),
                    "sl": float(sig.sl),
                    "tp": float(sig.tp),
                    "timestamp": str(sig.timestamp),
                    "reasons": list(sig.reasons),
                }

            sig_strength = "⭐⭐⭐ STRONG" if sig.score >= 9 else "⭐⭐ VALID"
            emoji = "🟢 BUY (LONG)" if sig.direction == "BUY" else "🔴 SELL (SHORT)"

            reasons_text = "\n".join(f"• {r}" for r in sig.reasons)
            msg = (
                "🚨 <b>BG STAR PRO v3 — SIGNAL ALERT</b>\n\n"
                f"🪙 <b>Asset:</b> #{sig.coin.replace('/USDT', '')}\n"
                f"🎯 <b>Action:</b> {emoji}\n"
                f"📊 <b>Score:</b> {sig.score}/11 | {sig_strength}\n"
                f"⏳ <b>Timeframe:</b> {sig.tf}\n\n"
                f"📍 <b>Entry:</b> {sig.entry:.4f}\n"
                f"🛑 <b>SL:</b> {sig.sl:.4f}\n"
                f"✅ <b>TP:</b> {sig.tp:.4f}\n\n"
                "🧠 <b>Why this signal?</b>\n"
                f"{reasons_text}\n\n"
                "<i>✓ Closed-Candle Analysis\n"
                "✓ SQLite Duplicate Protection\n"
                "✓ Telegram Delivery</i>"
            )

            sent = send_telegram_message(msg)
            if sent:
                add_alert_to_history(sig)
                new_signals.append((sig, msg))
                logger.info(
                    f"Signal Sent: {sig.coin} | {sig.direction} | Score: {sig.score}"
                )
            else:
                # Keep the signal in the processed ledger. This avoids duplicate
                # Telegram messages. A failed send is visible in the dashboard.
                logger.warning(
                    f"Signal generated but Telegram delivery failed: {sig.coin}"
                )

        return new_signals

    except Exception as e:
        logger.error(f"Scan Job Exception: {e}")
        return []

    finally:
        duration_ms = int((time.time() - start_time) * 1000)
        with STATE_LOCK:
            SYSTEM_STATUS["scan_duration_ms"] = duration_ms
        SCAN_LOCK.release()
        gc.collect()


# =========================================================
# ALERT HISTORY
# =========================================================
if "signal_history" not in st.session_state:
    st.session_state.signal_history = []

def add_alert_to_history(sig):
    item = {
        "asset": sig.coin,
        "signal": sig.direction,
        "score": sig.score,
        "timeframe": sig.tf,
        "entry": float(sig.entry),
        "sl": float(sig.sl),
        "tp": float(sig.tp),
        "timestamp": str(sig.timestamp),
        "reasons": list(sig.reasons),
    }
    # Keep newest first and bounded for Streamlit memory.
    st.session_state.signal_history.insert(0, item)
    st.session_state.signal_history = st.session_state.signal_history[:30]

# =========================================================
# STREAMLIT UI
# =========================================================
init_db()

if "initial_scan_done" not in st.session_state:
    st.session_state.initial_scan_done = False

if not st.session_state.initial_scan_done:
    st.session_state.initial_scan_done = True
    with st.spinner("Initial market scan..."):
        run_scan_job()

st.title("🚀 BG STAR PRO v3 — SMC Signal Dashboard")
st.caption("Streamlit edition • KuCoin market scanner • Telegram signal delivery")

st.subheader("🪙 Market Scanner — Monitored Crypto")
coin_cols = st.columns(len(COINS))
for col, coin in zip(coin_cols, COINS):
    col.metric(coin, "SCANNING", "15m")

with st.sidebar:
    st.header("⚙️ Configuration")

    telegram_ok = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
    st.write(
        "Telegram:",
        "🟢 Connected" if telegram_ok else "🔴 Not configured"
    )

    st.write("Exchange: 🟢 KuCoin")
    st.write(f"Scan interval: **{SCAN_INTERVAL_SECONDS // 60} min**")
    st.write(f"Timeframe: **{', '.join(TIMEFRAMES)}")

    st.subheader("🪙 Monitored Coins")
    for coin in COINS:
        st.write(f"• **{coin}**")

    st.divider()

    if st.button("🔄 Run Scan Now", use_container_width=True):
        with st.spinner("Scanning market..."):
            results = run_scan_job()
        if results:
            st.success(f"{len(results)} new signal(s) found and sent.")
        else:
            st.info("No new signal found, or Telegram delivery failed.")

    if st.button("🧹 Refresh Dashboard", use_container_width=True):
        st.rerun()

# Auto-scan using Streamlit reruns without a background scheduler.
# This keeps the app compatible with Streamlit Cloud's execution model.
try:
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(
        interval=SCAN_INTERVAL_SECONDS * 1000,
        key="bg_star_auto_refresh"
    )
except ImportError:
    st.warning(
        "streamlit-autorefresh is not installed. "
        "The manual Run Scan Now button still works."
    )

with STATE_LOCK:
    btc_trend = CONTEXT_CACHE.get("btc_trend", "NEUTRAL")
    last_scan = SYSTEM_STATUS.get("last_scan", "Not scanned yet")
    duration = SYSTEM_STATUS.get("scan_duration_ms", 0)
    total_signals = SYSTEM_STATUS.get("total_signals", 0)
    telegram_last = SYSTEM_STATUS.get("telegram_last", "Not sent")
    last_signal = SYSTEM_STATUS.get("last_signal")

m1, m2, m3, m4 = st.columns(4)
m1.metric("BTC Context", btc_trend)
m2.metric("Total Signals", total_signals)
m3.metric("Last Scan", last_scan)
m4.metric("Scan Time", f"{duration} ms")

st.divider()

if not telegram_ok:
    st.error(
        "Telegram is not configured. Add TELEGRAM_BOT_TOKEN and "
        "TELEGRAM_CHAT_ID to Streamlit Secrets."
    )
else:
    st.success(f"Telegram delivery: {telegram_last}")

st.subheader("🚨 LIVE SIGNAL ALERT")

if last_signal:
    is_buy = last_signal["direction"] == "BUY"
    alert_title = "🟢 BUY SIGNAL — LONG" if is_buy else "🔴 SELL SIGNAL — SHORT"

    st.markdown(
        f"""
        <div style="
            border:3px solid {'#16a34a' if is_buy else '#dc2626'};
            border-radius:18px;
            padding:22px;
            margin:8px 0 18px 0;
            background:linear-gradient(135deg, {'#052e16' if is_buy else '#450a0a'}, #111827);
        ">
            <div style="font-size:30px;font-weight:800;">
                {alert_title}
            </div>
            <div style="font-size:16px;margin-top:6px;">
                {last_signal["coin"]} • {last_signal["timeframe"]} •
                Score {last_signal["score"]}/11
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Asset", last_signal["coin"])
    c2.metric("Action", last_signal["direction"])
    c3.metric("Entry", f'{last_signal["entry"]:.6f}')
    c4.metric("Score", f'{last_signal["score"]}/11')

    c5, c6, c7 = st.columns(3)
    c5.metric("Stop Loss", f'{last_signal["sl"]:.6f}')
    c6.metric("Take Profit", f'{last_signal["tp"]:.6f}')
    c7.metric("Timeframe", last_signal["timeframe"])

    st.markdown("### 🧠 Why did this signal trigger?")
    for reason in last_signal.get("reasons", []):
        st.success(f"✓ {reason}") if is_buy else st.error(f"✓ {reason}")

    st.caption(f'Closed-candle time: {last_signal["timestamp"]}')
else:
    st.info("🟡 WAITING — No valid BUY/SELL signal yet.")

st.subheader("📜 Signal Alert History")

if st.session_state.signal_history:
    for idx, item in enumerate(st.session_state.signal_history):
        buy = item["signal"] == "BUY"
        with st.expander(
            f'{"🟢 BUY" if buy else "🔴 SELL"} • {item["asset"]} • '
            f'Score {item["score"]}/11 • {item["timestamp"]}',
            expanded=(idx == 0),
        ):
            a, b, c, d = st.columns(4)
            a.metric("Entry", f'{item["entry"]:.6f}')
            b.metric("SL", f'{item["sl"]:.6f}')
            c.metric("TP", f'{item["tp"]:.6f}')
            d.metric("Timeframe", item["timeframe"])

            st.markdown("**Signal Reasons**")
            for reason in item["reasons"]:
                st.write(f"• {reason}")
else:
    st.caption("No Telegram-delivered signals in this Streamlit session yet.")

st.subheader("🧠 Engine Status")
status_df = pd.DataFrame([
    ["KuCoin", "Connected via CCXT"],
    ["Monitored Coins", ", ".join(COINS)],
    ["Scan Timeframe", ", ".join(TIMEFRAMES)],
    ["SMC Engine", "Active"],
    ["Closed Candle", "Enabled"],
    ["Duplicate Protection", "SQLite WAL"],
    ["BTC Context Filter", btc_trend],
    ["Telegram", "Configured" if telegram_ok else "Missing Secrets"],
    ["Last Telegram", telegram_last],
], columns=["Component", "Status"])
st.dataframe(status_df, use_container_width=True, hide_index=True)

st.caption(
    "Note: Streamlit Cloud may restart/sleep the app. The scanner runs on "
    "Streamlit reruns; it is not a permanent background daemon."
)
