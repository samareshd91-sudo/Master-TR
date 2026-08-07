import os
import gc
import time
import queue
import logging
import threading
import sqlite3
import requests
import ccxt
import pandas as pd
from flask import Flask, jsonify

from dataclasses import dataclass
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from apscheduler.schedulers.background import BackgroundScheduler

# =========================================================
# CONFIG — RENDER FREE PRODUCTION-SAFE
# =========================================================
COINS = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT"]
TIMEFRAMES = ["15m"]

SCAN_INTERVAL_SECONDS = 300  # 5 Minutes Interval
BTC_CONTEXT_CACHE_SECONDS = 15 * 60
HTF_CONTEXT_CACHE_SECONDS = 30 * 60
MAX_WORKERS = 3
DB_FILE = "signals.db"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("BG_STAR_PRO_v3")

# =========================================================
# FLASK WEB APP (Enhanced Health Endpoints)
# =========================================================
app = Flask(__name__)

@app.route("/")
def home():
    return "BG STAR PRO v3 SMC ENGINE ONLINE", 200

@app.route("/health")
def health_check():
    return jsonify({
        "status": "OK",
        "last_scan": SYSTEM_STATUS.get("last_scan", "Booting..."),
        "scan_duration_ms": SYSTEM_STATUS.get("scan_duration_ms", 0),
        "total_signals": SYSTEM_STATUS.get("total_signals", 0),
        "telegram_queue": telegram_queue.qsize(),
        "btc_context": CONTEXT_CACHE.get("btc_trend", "NEUTRAL"),
        "scanner_lock": "LOCKED" if SCAN_LOCK.locked() else "RELEASED"
    }), 200

# =========================================================
# GLOBAL STATE, LOCKS & SQLITE (WAL + Thread-Safe)
# =========================================================
STATE_LOCK = threading.Lock()
SCAN_LOCK = threading.Lock()
DB_LOCK = threading.Lock()

CONTEXT_CACHE = {"btc_trend": "NEUTRAL", "last_update": 0}
SYSTEM_STATUS = {"last_scan": "Booting...", "scan_duration_ms": 0, "total_signals": 0}

def get_db_connection():
    conn = sqlite3.connect(DB_FILE, timeout=15.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn

def init_db():
    with DB_LOCK:
        try:
            with get_db_connection() as conn:
                conn.execute('''CREATE TABLE IF NOT EXISTS processed_signals
                                (signal_key TEXT PRIMARY KEY, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
        except Exception as e:
            logger.error(f"DB Init Error: {e}")

def is_signal_processed(signal_key):
    with DB_LOCK:
        try:
            with get_db_connection() as conn:
                cursor = conn.execute("SELECT 1 FROM processed_signals WHERE signal_key = ?", (signal_key,))
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"DB Read Error: {e}")
            return False

def mark_signal_processed(signal_key):
    with DB_LOCK:
        try:
            with get_db_connection() as conn:
                conn.execute("INSERT OR IGNORE INTO processed_signals (signal_key) VALUES (?)", (signal_key,))
                # Keep table bounded to prevent storage leak
                conn.execute("DELETE FROM processed_signals WHERE signal_key NOT IN (SELECT signal_key FROM processed_signals ORDER BY timestamp DESC LIMIT 1000)")
        except Exception as e:
            logger.error(f"DB Write Error: {e}")

# =========================================================
# TELEGRAM QUEUE WORKER (Graceful Error Handling)
# =========================================================
telegram_queue = queue.Queue(maxsize=100)

def telegram_worker():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=2, pool_maxsize=2)
    session.mount("https://", adapter)

    while True:
        msg = telegram_queue.get()
        try:
            if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
                continue
            response = session.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
                timeout=10
            )
            if response.status_code != 200:
                logger.warning(f"Telegram HTTP {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"Telegram Error (Will Drop Msg & Continue): {e}")
        finally:
            telegram_queue.task_done()
            time.sleep(1.5)  # Rate limit protection

# =========================================================
# THREAD-LOCAL EXCHANGE (Controlled Reconnect)
# =========================================================
thread_local = threading.local()

def get_exchange():
    if getattr(thread_local, "needs_reconnect", False):
        if hasattr(thread_local, "exchange"):
            del thread_local.exchange
        thread_local.needs_reconnect = False

    if not hasattr(thread_local, "exchange"):
        try:
            thread_local.exchange = ccxt.kucoin({"enableRateLimit": True, "timeout": 10000})
        except Exception as e:
            logger.error(f"Exchange init failed: {e}")
            return None
    return thread_local.exchange

# =========================================================
# SIGNAL MODEL & SMC LOGIC (Untouched Core Logic)
# =========================================================
@dataclass
class Signal:
    coin: str; tf: str; direction: str; entry: float; sl: float; tp: float; score: int; timestamp: pd.Timestamp

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

# ==========================================
# ROBUST SCAN ENGINE (Execution Lock + Interval)
# ==========================================
engine = BGStarEngine()

def run_scan_job():
    # 1. Prevent Overlapping Executions safely
    if not SCAN_LOCK.acquire(blocking=False):
        logger.warning("Previous scan still running. Skipping this cycle.")
        return

    start_time = time.time()
    try:
        now = datetime.now()
        with STATE_LOCK: 
            SYSTEM_STATUS["last_scan"] = now.strftime("%Y-%m-%d %H:%M:%S")

        engine.update_global_context()
        all_signals = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(engine.scan_coin, coin): coin for coin in COINS}
            for future in as_completed(futures):
                try:
                    res = future.result()
                    if res: all_signals.extend(res)
                except Exception as e:
                    logger.error(f"Worker Error: {e}")
            
        for sig in all_signals:
            signal_key = f"{sig.coin}_{sig.direction}_{sig.timestamp}"
            
            if is_signal_processed(signal_key):
                continue
                
            mark_signal_processed(signal_key)
            
            with STATE_LOCK:
                SYSTEM_STATUS["total_signals"] += 1

            sig_strength = "⭐⭐⭐ STRONG" if sig.score >= 9 else "⭐⭐ VALID"
            emoji = "🟢 BUY (LONG)" if sig.direction == "BUY" else "🔴 SELL (SHORT)"
            
            msg = (
                "🚀 <b>BG STAR PRO v3 (PRODUCTION)</b>\n\n"
                f"🪙 <b>Asset:</b> #{sig.coin.replace('/USDT', '')}\n"
                f"🎯 <b>Action:</b> {emoji}\n"
                f"📊 <b>Score:</b> {sig.score}/11 | {sig_strength}\n"
                f"⏳ <b>Timeframe:</b> {sig.tf}\n\n"
                f"📍 <b>Entry:</b> {sig.entry:.4f}\n"
                f"🛑 <b>SL:</b> {sig.sl:.4f}\n"
                f"✅ <b>TP:</b> {sig.tp:.4f}\n\n"
                "<i>✓ Safe Reconnect\n✓ WAL SQLite\n✓ Pro-Audited Logic</i>"
            )
            
            try: 
                telegram_queue.put_nowait(msg)
            except queue.Full: 
                logger.warning(f"Telegram queue full! Dropped signal for {sig.coin}")

            logger.info(f"Signal Generated: {sig.coin} | {sig.direction} | Score: {sig.score}")
            
        del all_signals
            
    except Exception as e:
        logger.error(f"Scan Job Exception: {e}")
    finally:
        duration_ms = int((time.time() - start_time) * 1000)
        with STATE_LOCK:
            SYSTEM_STATUS["scan_duration_ms"] = duration_ms
        
        # 2. Release Lock and GC Cleanup
        SCAN_LOCK.release()
        gc.collect()

# ==========================================
# BOOTSTRAP DETERMINISTIC STARTUP
# ==========================================
scheduler = BackgroundScheduler()

if not hasattr(app, 'is_bootstrapped'):
    init_db()
    
    # Start Telegram Worker
    threading.Thread(target=telegram_worker, daemon=True).start()
    
    # Add Robust Interval Job (Starts immediately, then runs every 5 mins)
    scheduler.add_job(
        run_scan_job, 
        'interval', 
        seconds=SCAN_INTERVAL_SECONDS, 
        next_run_time=datetime.now(), 
        max_instances=1, 
        coalesce=True
    )
    scheduler.start()
    
    app.is_bootstrapped = True
    logger.info("BG STAR PRO v3 - Worker, SQLite & Scheduler Bootstrapped Successfully.")
