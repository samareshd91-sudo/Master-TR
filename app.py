import streamlit as st
st.set_page_config(page_title="BG STAR PRO v3", layout="wide", page_icon="⚡")

import os
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import ccxt
import pandas as pd
import numpy as np
import gc
import time
import threading
import queue
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from streamlit_autorefresh import st_autorefresh
from apscheduler.schedulers.background import BackgroundScheduler

# ==========================================
# 1. STRICT LOGGING SETUP
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("BG_STAR_PRO_v3_SMC")

# ==========================================
# 2. ENVIRONMENT VARIABLES
# ==========================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# ==========================================
# 3. GLOBAL STATE & METRICS
# ==========================================
COINS = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT"]
TIMEFRAMES = ["1m", "5m", "15m"]

COOLDOWNS = {}
STATE_LOCK = threading.Lock()
CONTEXT_CACHE = {"btc_trend": "NEUTRAL", "last_update": 0}

SYSTEM_STATUS = {
    "status": "🟢 ONLINE (SMC Institutional Engine)",
    "last_scan": "Booting...",
    "next_scan": "Waiting...",
    "scan_duration_ms": 0,
    "api_latency_ms": 0,
    "total_signals": 0,
    "active_workers": 3
}

# ==========================================
# 4. TELEGRAM QUEUE WORKER
# ==========================================
telegram_queue = queue.Queue()

def telegram_worker():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retry))
    session.mount('https://', HTTPAdapter(max_retries=retry))
    
    while True:
        msg = telegram_queue.get()
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            telegram_queue.task_done()
            continue
        try:
            session.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", 
                json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}, 
                timeout=5
            )
        except Exception as e:
            logger.error(f"Telegram Delivery Failed: {e}")
        finally:
            telegram_queue.task_done()
            time.sleep(1)

@st.cache_resource
def start_tg_worker():
    t = threading.Thread(target=telegram_worker, daemon=True)
    t.start()
    return t
start_tg_worker()

# ==========================================
# 5. THREAD-LOCAL CCXT
# ==========================================
thread_local = threading.local()

def get_exchange():
    if not hasattr(thread_local, "ex") or getattr(thread_local, "needs_reconnect", False):
        try:
            thread_local.ex = ccxt.kucoin({'enableRateLimit': True, 'timeout': 10000})
            thread_local.needs_reconnect = False
        except Exception as e:
            logger.error(f"Exchange Init Failed: {e}")
    return thread_local.ex
  # ==========================================
# 6. SMC TRADING ENGINE ALGORITHMS
# ==========================================
@dataclass
class Signal:
    coin: str; tf: str; direction: str; entry: float; sl: float; tp: float; score: int; timestamp: pd.Timestamp

class SMCAnalyzer:
    """Core algorithms for Smart Money Concepts"""
    
    @staticmethod
    def identify_pivots(df, left_bars=5, right_bars=5):
        """Identifies Swing Highs and Swing Lows (Fractals)"""
        df['pivot_high'] = False
        df['pivot_low'] = False
        
        for i in range(left_bars, len(df) - right_bars):
            # Swing High
            is_high = True
            for j in range(1, left_bars + 1):
                if df['high'].iloc[i] <= df['high'].iloc[i - j]: is_high = False
            for j in range(1, right_bars + 1):
                if df['high'].iloc[i] <= df['high'].iloc[i + j]: is_high = False
            if is_high: df.iat[i, df.columns.get_loc('pivot_high')] = True
            
            # Swing Low
            is_low = True
            for j in range(1, left_bars + 1):
                if df['low'].iloc[i] >= df['low'].iloc[i - j]: is_low = False
            for j in range(1, right_bars + 1):
                if df['low'].iloc[i] >= df['low'].iloc[i + j]: is_low = False
            if is_low: df.iat[i, df.columns.get_loc('pivot_low')] = True
            
        return df

    @staticmethod
    def detect_fvg(df):
        """Identifies Bullish and Bearish Fair Value Gaps"""
        df['fvg_bull'] = False
        df['fvg_bear'] = False
        
        # Bullish FVG: C3 Low > C1 High
        bull_fvg_cond = (df['low'] > df['high'].shift(2)) & (df['close'].shift(1) > df['open'].shift(1))
        df.loc[bull_fvg_cond, 'fvg_bull'] = True
        
        # Bearish FVG: C3 High < C1 Low
        bear_fvg_cond = (df['high'] < df['low'].shift(2)) & (df['close'].shift(1) < df['open'].shift(1))
        df.loc[bear_fvg_cond, 'fvg_bear'] = True
        
        return df

    @staticmethod
    def map_structure_and_ob(df):
        """Maps BOS, CHoCH, and Order Blocks dynamically"""
        df['structure'] = 'NONE' # BOS_BULL, BOS_BEAR, CHOCH_BULL, CHOCH_BEAR
        df['ob_bull'] = False
        df['ob_bear'] = False
        df['liq_sweep'] = False
        
        last_ph, last_pl = None, None
        trend = 0 # 1 bull, -1 bear
        
        for i in range(10, len(df)):
            curr_high = df['high'].iloc[i]
            curr_low = df['low'].iloc[i]
            curr_close = df['close'].iloc[i]
            
            if df['pivot_high'].iloc[i]: last_ph = curr_high
            if df['pivot_low'].iloc[i]: last_pl = curr_low
            
            if last_ph and last_pl:
                # Liquidity Sweep Logic (Wick breaks pivot, but closes inside)
                if curr_high > last_ph and curr_close < last_ph:
                    df.iat[i, df.columns.get_loc('liq_sweep')] = True
                if curr_low < last_pl and curr_close > last_pl:
                    df.iat[i, df.columns.get_loc('liq_sweep')] = True

                # BOS / CHoCH Logic
                if curr_close > last_ph:
                    if trend == -1:
                        df.iat[i, df.columns.get_loc('structure')] = 'CHOCH_BULL'
                        trend = 1
                    elif trend == 1:
                        df.iat[i, df.columns.get_loc('structure')] = 'BOS_BULL'
                        # Bullish OB is the last bearish candle before the impulse
                        ob_idx = i - 1
                        while ob_idx > 0 and df['close'].iloc[ob_idx] >= df['open'].iloc[ob_idx]:
                            ob_idx -= 1
                        df.iat[ob_idx, df.columns.get_loc('ob_bull')] = True
                    last_ph = None # Reset pivot after break
                    
                elif curr_close < last_pl:
                    if trend == 1:
                        df.iat[i, df.columns.get_loc('structure')] = 'CHOCH_BEAR'
                        trend = -1
                    elif trend == -1:
                        df.iat[i, df.columns.get_loc('structure')] = 'BOS_BEAR'
                        # Bearish OB is the last bullish candle before the impulse
                        ob_idx = i - 1
                        while ob_idx > 0 and df['close'].iloc[ob_idx] <= df['open'].iloc[ob_idx]:
                            ob_idx -= 1
                        df.iat[ob_idx, df.columns.get_loc('ob_bear')] = True
                    last_pl = None # Reset pivot after break

        return df

    @staticmethod
    def calc_atr(df):
        df['prev_close'] = df['close'].shift(1)
        tr1 = df['high'] - df['low']
        tr2 = abs(df['high'] - df['prev_close'])
        tr3 = abs(df['low'] - df['prev_close'])
        df['tr'] = tr1.combine(tr2, max).combine(tr3, max)
        df['atr'] = df['tr'].rolling(14).mean()
        return df
    class BGStarEngine:
    def fetch_data(self, sym, tf, limit=300):
        start_time = time.time()
        try:
            ex = get_exchange()
            ohlcv = ex.fetch_ohlcv(sym, tf, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            with STATE_LOCK: SYSTEM_STATUS["api_latency_ms"] = int((time.time() - start_time) * 1000)
            return df.set_index('timestamp')
        except ccxt.RateLimitExceeded:
            logger.warning(f"Rate Limit Hit on {sym}. Backoff 30s...")
            time.sleep(30)
            return None
        except Exception as e:
            logger.error(f"Fetch Error {sym} {tf}: {e}")
            thread_local.needs_reconnect = True 
            return None

    def update_global_context(self):
        global CONTEXT_CACHE
        now = time.time()
        with STATE_LOCK:
            if now - CONTEXT_CACHE["last_update"] < 900: return
            CONTEXT_CACHE["last_update"] = now

        btc_1h = self.fetch_data("BTC/USDT", "1h", limit=200)
        if btc_1h is not None:
            btc_1h = SMCAnalyzer.identify_pivots(btc_1h)
            btc_1h = SMCAnalyzer.map_structure_and_ob(btc_1h)
            
            # Simple HTF BTC Trend derived from recent structure
            recent_structs = btc_1h[btc_1h['structure'] != 'NONE'].tail(2)
            if len(recent_structs) > 0:
                last_s = recent_structs.iloc[-1]['structure']
                if "BULL" in last_s: CONTEXT_CACHE["btc_trend"] = "BULLISH"
                elif "BEAR" in last_s: CONTEXT_CACHE["btc_trend"] = "BEARISH"
                else: CONTEXT_CACHE["btc_trend"] = "NEUTRAL"
            del btc_1h

    def get_mtf_context(self, sym):
        global CONTEXT_CACHE
        now = time.time()
        with STATE_LOCK:
            if sym not in CONTEXT_CACHE or now - CONTEXT_CACHE[sym].get("last_fetch", 0) > 3600:
                df_1h = self.fetch_data(sym, '1h', limit=150)
                df_4h = self.fetch_data(sym, '4h', limit=150)
                
                if df_1h is not None and df_4h is not None:
                    ema200_1h = df_1h['close'].ewm(span=200, adjust=False).mean().iloc[-2]
                    ema200_4h = df_4h['close'].ewm(span=200, adjust=False).mean().iloc[-2]
                    
                    CONTEXT_CACHE[sym] = {
                        "ema200_1h": ema200_1h, "ema200_4h": ema200_4h,
                        "last_fetch": now
                    }
                    del df_1h, df_4h
                else: return None
        return CONTEXT_CACHE.get(sym)

    def analyze_tf(self, sym, tf, mtf_ctx, btc_trend):
        df = self.fetch_data(sym, tf, limit=200)
        if df is None or len(df) < 50: return None
        
        df = SMCAnalyzer.identify_pivots(df)
        df = SMCAnalyzer.detect_fvg(df)
        df = SMCAnalyzer.map_structure_and_ob(df)
        df = SMCAnalyzer.calc_atr(df)
        
        last_closed = df.iloc[-2]
        prev_closed = df.iloc[-3]
        
        if pd.isna(last_closed['atr']) or last_closed['atr'] == 0: 
            del df; return None

        # False Breakout Filter: Ignore if ATR is less than 0.15% (Dead market)
        if (last_closed['atr'] / last_closed['close']) * 100 < 0.15:
            del df; return None

        # HTF Trend Check
        htf_bull = last_closed['close'] > mtf_ctx["ema200_1h"] and last_closed['close'] > mtf_ctx["ema200_4h"]
        htf_bear = last_closed['close'] < mtf_ctx["ema200_1h"] and last_closed['close'] < mtf_ctx["ema200_4h"]

        signal = None
        
        # Scoring System Variables
        score = 0
        recent_window = df.tail(10)
        
        # SMC Confluence Scoring
        fvg_bull_present = recent_window['fvg_bull'].any()
        ob_bull_present = recent_window['ob_bull'].any()
        liq_sweep_recent = recent_window['liq_sweep'].any()
        fvg_bear_present = recent_window['fvg_bear'].any()
        ob_bear_present = recent_window['ob_bear'].any()

        # Dynamic Risk Reward based on Structure & Volatility
        sl_multiplier = 1.0 if liq_sweep_recent else 1.5
        tp_multiplier = 3.0 if ob_bull_present or ob_bear_present else 2.5

        # --- BUY LOGIC ---
        if htf_bull and btc_trend != "BEARISH":
            # Structure confirmation
            if last_closed['structure'] in ['BOS_BULL', 'CHOCH_BULL'] or prev_closed['structure'] in ['BOS_BULL', 'CHOCH_BULL']:
                score += 5
                if fvg_bull_present: score += 2
                if ob_bull_present: score += 2
                if liq_sweep_recent: score += 1
                
                # Minimum score threshold for Strong Signal
                if score >= 7:
                    entry = last_closed['close']
                    sl = entry - (last_closed['atr'] * sl_multiplier)
                    tp = entry + (last_closed['atr'] * tp_multiplier)
                    signal = Signal(sym, tf, "BUY", entry, sl, tp, score, df.index[-2])

        # --- SELL LOGIC ---
        if not signal and htf_bear and btc_trend != "BULLISH":
            if last_closed['structure'] in ['BOS_BEAR', 'CHOCH_BEAR'] or prev_closed['structure'] in ['BOS_BEAR', 'CHOCH_BEAR']:
                score += 5
                if fvg_bear_present: score += 2
                if ob_bear_present: score += 2
                if liq_sweep_recent: score += 1
                
                if score >= 7:
                    entry = last_closed['close']
                    sl = entry + (last_closed['atr'] * sl_multiplier)
                    tp = entry - (last_closed['atr'] * tp_multiplier)
                    signal = Signal(sym, tf, "SELL", entry, sl, tp, score, df.index[-2])

        del df
        return signal

    def scan_coin(self, sym):
        mtf_ctx = self.get_mtf_context(sym)
        if not mtf_ctx: return []
        
        with STATE_LOCK:
            btc_trend = CONTEXT_CACHE.get("btc_trend", "NEUTRAL")

        signals = [self.analyze_tf(sym, tf, mtf_ctx, btc_trend) for tf in TIMEFRAMES]
        return [s for s in signals if s]
              # ==========================================
# 7. APSCHEDULER JOB 
# ==========================================
engine = BGStarEngine()

def run_scan_job():
    global SYSTEM_STATUS, COOLDOWNS
    start_time = time.time()
    
    try:
        now = datetime.now()
        with STATE_LOCK:
            SYSTEM_STATUS["last_scan"] = now.strftime("%Y-%m-%d %H:%M:%S")
            SYSTEM_STATUS["next_scan"] = (now + timedelta(seconds=60)).strftime("%Y-%m-%d %H:%M:%S")
            COOLDOWNS = {k: v for k, v in COOLDOWNS.items() if now - v < timedelta(minutes=5)}

        engine.update_global_context()

        all_signals = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(engine.scan_coin, coin): coin for coin in COINS}
            for future in as_completed(futures):
                try:
                    res = future.result()
                    if res: all_signals.extend(res)
                except Exception as e:
                    logger.error(f"Worker Error: {e}")
        del futures

        for sig in all_signals:
            sig_key = f"{sig.coin}_{sig.direction}"
            with STATE_LOCK:
                if sig_key in COOLDOWNS: continue 
                COOLDOWNS[sig_key] = now
                SYSTEM_STATUS["total_signals"] += 1
            
            # Multi-Timeframe Alignment Check for High Confidence
            same_dir = [s for s in all_signals if s.coin == sig.coin and s.direction == sig.direction]
            is_mtf_aligned = len(same_dir) >= 2
            
            sig_strength = "⭐⭐⭐ STRONG (SMC Confluence)" if sig.score >= 9 else "⭐⭐ VALID (Structure Break)"
            mtf_text = "🔥 MTF ALIGNED (1m/5m/15m)" if is_mtf_aligned else f"Timeframe: {sig.tf}"
            emoji = "🟢 BUY (LONG)" if sig.direction == "BUY" else "🔴 SELL (SHORT)"
            
            msg = f"🚀 <b>BG STAR PRO v3 (INSTITUTIONAL SMC)</b>\n\n"
            msg += f"🪙 <b>Asset:</b> #{sig.coin.replace('/USDT', '')}\n"
            msg += f"🎯 <b>Action:</b> {emoji}\n"
            msg += f"📊 <b>Score:</b> {sig.score}/10 | {sig_strength}\n"
            msg += f"⏳ <b>Status:</b> {mtf_text}\n\n"
            msg += f"📍 <b>Entry:</b> {sig.entry:.4f}\n"
            msg += f"🛑 <b>SL:</b> {sig.sl:.4f} (Dynamic)\n"
            msg += f"✅ <b>TP:</b> {sig.tp:.4f} (Dynamic R:R)\n\n"
            msg += f"<i>✓ BOS/CHoCH Detected\n✓ FVG/OB Confirmed\n✓ HTF & BTC Aligned</i>"
            
            telegram_queue.put(msg) 
            logger.info(f"SMC Signal Generated: {sig.coin} | {sig.direction} | Score: {sig.score}")

        del all_signals 
        
    except Exception as e:
        logger.error(f"Scheduler Job Exception: {e}")
    finally:
        with STATE_LOCK:
            SYSTEM_STATUS["scan_duration_ms"] = int((time.time() - start_time) * 1000)
        gc.collect()

@st.cache_resource
def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_scan_job, 'interval', seconds=60, max_instances=1)
    scheduler.start()
    return scheduler

start_scheduler()

# ==========================================
# 8. STREAMLIT ENTERPRISE DASHBOARD
# ==========================================
st_autorefresh(interval=60000, limit=None, key="dashboard_autorefresh")

st.markdown("""
<style>
    .big-card { background: #0f172a; padding: 20px; border-radius: 12px; border: 1px solid #1e293b; text-align: center; }
    .status-badge { background: #059669; color: white; padding: 5px 15px; border-radius: 20px; font-weight: bold; font-size: 14px;}
    .metric-box { background: #1e293b; padding: 15px; border-radius: 8px; border: 1px solid #334155; text-align: left;}
    h1, h3, h4 { color: #f8fafc; margin: 0 0 10px 0;}
    .label { color: #94a3b8; font-size: 14px; display: block; margin-bottom: 5px;}
    .value { color: #38bdf8; font-size: 18px; font-weight: bold; }
    .alert-value { color: #f59e0b; font-size: 18px; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

st.title("⚡ BG STAR PRO v3")
st.markdown("### Institutional SMC Trading Engine (Phase 3)")

st.markdown(f"""
<div class="big-card" style="margin-bottom: 20px;">
    <span class="status-badge">{SYSTEM_STATUS['status']}</span>
    <p style="margin-top: 15px; color: #94a3b8;">Active Core: BOS/CHoCH, FVG, Order Blocks, Liquidity Sweeps, HTF Alignment</p>
</div>
""", unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(f"""
    <div class="metric-box">
        <span class="label">Last Scan</span>
        <span class="value">{SYSTEM_STATUS['last_scan']}</span>
    </div>
    <div class="metric-box" style="margin-top: 15px;">
        <span class="label">Next Scan</span>
        <span class="value">{SYSTEM_STATUS['next_scan']}</span>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="metric-box">
        <span class="label">API Latency (Heartbeat)</span>
        <span class="value">{SYSTEM_STATUS['api_latency_ms']} ms</span>
    </div>
    <div class="metric-box" style="margin-top: 15px;">
        <span class="label">Scan Duration</span>
        <span class="value">{SYSTEM_STATUS['scan_duration_ms']} ms</span>
    </div>
    """, unsafe_allow_html=True)

with col3:
    tg_q_size = telegram_queue.qsize()
    q_color = "value" if tg_q_size == 0 else "alert-value"
    st.markdown(f"""
    <div class="metric-box">
        <span class="label">Total Signals Generated</span>
        <span class="value">{SYSTEM_STATUS['total_signals']}</span>
    </div>
    <div class="metric-box" style="margin-top: 15px;">
        <span class="label">Telegram Queue / Workers</span>
        <span class="{q_color}">{tg_q_size} pending / {SYSTEM_STATUS['active_workers']} active</span>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br><hr style='border-color: #1e293b;'><p style='text-align: center; color: #64748b; font-size: 12px;'>Target Assets: BTC, ETH, BNB, SOL, XRP | Engine: Institutional SMC | GC: Active</p>", unsafe_allow_html=True)

