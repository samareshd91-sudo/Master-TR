import streamlit as st
st.set_page_config(page_title="BG STAR PRO v3",layout="wide",page_icon="⚡")

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
from datetime import datetime,timedelta
from concurrent.futures import ThreadPoolExecutor,as_completed
from dataclasses import dataclass
from streamlit_autorefresh import st_autorefresh
from apscheduler.schedulers.background import BackgroundScheduler

SCAN_DELAY_SECONDS=300
COOLDOWN_MINUTES=5
BTC_CONTEXT_CACHE_SECONDS=900
MTF_CONTEXT_CACHE_SECONDS=3600

COINS=[
    "BTC/USDT",
    "ETH/USDT",
    "BNB/USDT",
    "SOL/USDT",
    "XRP/USDT"
]

TIMEFRAMES=["15m"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger=logging.getLogger("BG_STAR_PRO_v3_SMC")

TELEGRAM_BOT_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN","")
TELEGRAM_CHAT_ID=os.getenv("TELEGRAM_CHAT_ID","")

COOLDOWNS={}
STATE_LOCK=threading.Lock()

CONTEXT_CACHE={
    "btc_trend":"NEUTRAL",
    "last_update":0
}

SYSTEM_STATUS={
    "status":"🟢 ONLINE (Institutional SMC)",
    "last_scan":"Booting...",
    "next_scan":"Waiting...",
    "scan_duration_ms":0,
    "api_latency_ms":0,
    "total_signals":0,
    "active_workers":3
}

telegram_queue=queue.Queue()

def telegram_worker():
    session=requests.Session()

    retry=Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429,500,502,503,504]
    )

    session.mount(
        "http://",
        HTTPAdapter(max_retries=retry)
    )
    session.mount(
        "https://",
        HTTPAdapter(max_retries=retry)
    )

    while True:
        msg=telegram_queue.get()

        try:
            if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
                continue

            response=session.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id":TELEGRAM_CHAT_ID,
                    "text":msg,
                    "parse_mode":"HTML"
                },
                timeout=5
            )

            if response.status_code!=200:
                logger.warning(
                    f"Telegram API returned {response.status_code}"
                )

        except Exception as e:
            logger.error(f"Telegram Delivery Failed: {e}")

        finally:
            telegram_queue.task_done()
            time.sleep(1)

@st.cache_resource
def start_tg_worker():
    worker=threading.Thread(
        target=telegram_worker,
        daemon=True
    )
    worker.start()
    return worker

start_tg_worker()

thread_local=threading.local()

def get_exchange():
    if (
        not hasattr(thread_local,"ex")
        or getattr(thread_local,"needs_reconnect",False)
    ):
        try:
            thread_local.ex=ccxt.kucoin({
                "enableRateLimit":True,
                "timeout":10000
            })
            thread_local.needs_reconnect=False

        except Exception as e:
            logger.error(f"Exchange Init Failed: {e}")
            return None

    return thread_local.ex

@dataclass
class Signal:
    coin:str
    tf:str
    direction:str
    entry:float
    sl:float
    tp:float
    score:int
    timestamp:pd.Timestamp

class SMCAnalyzer:

    @staticmethod
    def identify_pivots(df,left_bars=5,right_bars=5):
        df["pivot_high"]=False
        df["pivot_low"]=False

        if len(df)<=left_bars+right_bars:
            return df

        for i in range(
            left_bars,
            len(df)-right_bars
        ):
            high_value=df["high"].iloc[i]
            low_value=df["low"].iloc[i]

            is_high=True
            is_low=True

            for j in range(1,left_bars+1):
                if high_value<=df["high"].iloc[i-j]:
                    is_high=False
                if low_value>=df["low"].iloc[i-j]:
                    is_low=False

            for j in range(1,right_bars+1):
                if high_value<=df["high"].iloc[i+j]:
                    is_high=False
                if low_value>=df["low"].iloc[i+j]:
                    is_low=False

            if is_high:
                df.iat[
                    i,
                    df.columns.get_loc("pivot_high")
                ]=True

            if is_low:
                df.iat[
                    i,
                    df.columns.get_loc("pivot_low")
                ]=True

        return df

    @staticmethod
    def detect_fvg(df):
        df["fvg_bull"]=False
        df["fvg_bear"]=False

        bull_condition=(
            (df["low"]>df["high"].shift(2))&
            (df["close"].shift(1)>df["open"].shift(1))
        )

        bear_condition=(
            (df["high"]<df["low"].shift(2))&
            (df["close"].shift(1)<df["open"].shift(1))
        )

        df.loc[bull_condition,"fvg_bull"]=True
        df.loc[bear_condition,"fvg_bear"]=True

        return df

    @staticmethod
    def map_structure_and_ob(df):
        df["structure"]="NONE"
        df["ob_bull"]=False
        df["ob_bear"]=False
        df["liq_sweep"]=False

        last_ph=None
        last_pl=None
        trend=0

        for i in range(10,len(df)):
            curr_high=df["high"].iloc[i]
            curr_low=df["low"].iloc[i]
            curr_close=df["close"].iloc[i]

            if df["pivot_high"].iloc[i]:
                last_ph=curr_high

            if df["pivot_low"].iloc[i]:
                last_pl=curr_low

            if last_ph is not None and last_pl is not None:

                if (
                    curr_high>last_ph
                    and curr_close<last_ph
                ):
                    df.iat[
                        i,
                        df.columns.get_loc("liq_sweep")
                    ]=True

                if (
                    curr_low<last_pl
                    and curr_close>last_pl
                ):
                    df.iat[
                        i,
                        df.columns.get_loc("liq_sweep")
                    ]=True

                if curr_close>last_ph:

                    if trend==-1:
                        df.iat[
                            i,
                            df.columns.get_loc("structure")
                        ]="CHOCH_BULL"
                        trend=1

                    elif trend==1:
                        df.iat[
                            i,
                            df.columns.get_loc("structure")
                        ]="BOS_BULL"

                        ob_idx=i-1

                        while (
                            ob_idx>0
                            and
                            df["close"].iloc[ob_idx]>=df["open"].iloc[ob_idx]
                        ):
                            ob_idx-=1

                        if ob_idx>0:
                            df.iat[
                                ob_idx,
                                df.columns.get_loc("ob_bull")
                            ]=True

                    last_ph=None

                elif curr_close<last_pl:

                    if trend==1:
                        df.iat[
                            i,
                            df.columns.get_loc("structure")
                        ]="CHOCH_BEAR"
                        trend=-1

                    elif trend==-1:
                        df.iat[
                            i,
                            df.columns.get_loc("structure")
                        ]="BOS_BEAR"

                        ob_idx=i-1

                        while (
                            ob_idx>0
                            and
                            df["close"].iloc[ob_idx]<=df["open"].iloc[ob_idx]
                        ):
                            ob_idx-=1

                        if ob_idx>0:
                            df.iat[
                                ob_idx,
                                df.columns.get_loc("ob_bear")
                            ]=True

                    last_pl=None

        return df

    @staticmethod
    def calc_atr(df):
        df["prev_close"]=df["close"].shift(1)

        tr1=df["high"]-df["low"]
        tr2=(df["high"]-df["prev_close"]).abs()
        tr3=(df["low"]-df["prev_close"]).abs()

        df["tr"]=pd.concat(
            [tr1,tr2,tr3],
            axis=1
        ).max(axis=1)

        df["atr"]=df["tr"].rolling(14).mean()

        return df
class BGStarEngine:

    def fetch_data(self,sym,tf,limit=300):
        start_time=time.time()

        try:
            ex=get_exchange()

            if ex is None:
                return None

            ohlcv=ex.fetch_ohlcv(
                sym,
                tf,
                limit=limit
            )

            if not ohlcv:
                return None

            df=pd.DataFrame(
                ohlcv,
                columns=[
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume"
                ]
            )

            df["timestamp"]=pd.to_datetime(
                df["timestamp"],
                unit="ms"
            )

            with STATE_LOCK:
                SYSTEM_STATUS["api_latency_ms"]=int(
                    (time.time()-start_time)*1000
                )

            return df.set_index("timestamp")

        except ccxt.RateLimitExceeded:
            logger.warning(
                f"Rate Limit Hit on {sym} {tf}"
            )
            thread_local.needs_reconnect=True
            return None

        except Exception as e:
            logger.error(
                f"Fetch Error {sym} {tf}: {e}"
            )
            thread_local.needs_reconnect=True
            return None

    def update_global_context(self):
        global CONTEXT_CACHE

        now=time.time()

        with STATE_LOCK:
            if (
                now-CONTEXT_CACHE["last_update"]
                <
                BTC_CONTEXT_CACHE_SECONDS
            ):
                return

            CONTEXT_CACHE["last_update"]=now

        btc_1h=self.fetch_data(
            "BTC/USDT",
            "1h",
            limit=200
        )

        if btc_1h is None:
            return

        try:
            btc_1h=SMCAnalyzer.identify_pivots(
                btc_1h
            )

            btc_1h=SMCAnalyzer.map_structure_and_ob(
                btc_1h
            )

            recent_structures=btc_1h[
                btc_1h["structure"]!="NONE"
            ].tail(3)

            trend="NEUTRAL"

            if len(recent_structures)>0:
                last_structure=(
                    recent_structures
                    .iloc[-1]["structure"]
                )

                if "BULL" in last_structure:
                    trend="BULLISH"

                elif "BEAR" in last_structure:
                    trend="BEARISH"

            with STATE_LOCK:
                CONTEXT_CACHE["btc_trend"]=trend

            logger.info(
                f"BTC Global Trend: {trend}"
            )

        except Exception as e:
            logger.error(
                f"BTC Context Error: {e}"
            )

        finally:
            del btc_1h

    def get_mtf_context(self,sym):
        global CONTEXT_CACHE

        now=time.time()

        with STATE_LOCK:
            cached=CONTEXT_CACHE.get(sym)

            if (
                cached
                and
                now-cached.get("last_fetch",0)
                <=
                MTF_CONTEXT_CACHE_SECONDS
            ):
                return cached

        df_1h=self.fetch_data(
            sym,
            "1h",
            limit=220
        )

        df_4h=self.fetch_data(
            sym,
            "4h",
            limit=220
        )

        if df_1h is None or df_4h is None:
            return None

        try:
            ema200_1h=(
                df_1h["close"]
                .ewm(
                    span=200,
                    adjust=False
                )
                .mean()
                .iloc[-2]
            )

            ema200_4h=(
                df_4h["close"]
                .ewm(
                    span=200,
                    adjust=False
                )
                .mean()
                .iloc[-2]
            )

            context={
                "ema200_1h":ema200_1h,
                "ema200_4h":ema200_4h,
                "last_fetch":now
            }

            with STATE_LOCK:
                CONTEXT_CACHE[sym]=context

            return context

        except Exception as e:
            logger.error(
                f"MTF Context Error {sym}: {e}"
            )
            return None

        finally:
            del df_1h
            del df_4h

    def analyze_tf(
        self,
        sym,
        tf,
        mtf_ctx,
        btc_trend
    ):
        df=self.fetch_data(
            sym,
            tf,
            limit=250
        )

        if df is None or len(df)<60:
            return None

        try:
            df=SMCAnalyzer.identify_pivots(df)
            df=SMCAnalyzer.detect_fvg(df)
            df=SMCAnalyzer.map_structure_and_ob(df)
            df=SMCAnalyzer.calc_atr(df)

            # Ignore current running candle
            last_closed=df.iloc[-2]
            prev_closed=df.iloc[-3]

            if (
                pd.isna(last_closed["atr"])
                or
                last_closed["atr"]<=0
            ):
                return None

            atr_percentage=(
                last_closed["atr"]
                /
                last_closed["close"]
            )*100

            if atr_percentage<0.15:
                return None

            # HTF FILTER
            htf_bull=(
                last_closed["close"]
                >
                mtf_ctx["ema200_1h"]
                and
                last_closed["close"]
                >
                mtf_ctx["ema200_4h"]
            )

            htf_bear=(
                last_closed["close"]
                <
                mtf_ctx["ema200_1h"]
                and
                last_closed["close"]
                <
                mtf_ctx["ema200_4h"]
            )

            # RECENT SMC CONDITIONS
            recent_window=df.tail(12)

            fvg_bull_present=(
                recent_window["fvg_bull"].any()
            )

            fvg_bear_present=(
                recent_window["fvg_bear"].any()
            )

            ob_bull_present=(
                recent_window["ob_bull"].any()
            )

            ob_bear_present=(
                recent_window["ob_bear"].any()
            )

            liq_sweep_recent=(
                recent_window["liq_sweep"].any()
            )

            score_buy=0
            score_sell=0

            bullish_structure=(
                last_closed["structure"]
                in ["BOS_BULL","CHOCH_BULL"]
                or
                prev_closed["structure"]
                in ["BOS_BULL","CHOCH_BULL"]
            )

            bearish_structure=(
                last_closed["structure"]
                in ["BOS_BEAR","CHOCH_BEAR"]
                or
                prev_closed["structure"]
                in ["BOS_BEAR","CHOCH_BEAR"]
            )

            # BUY SCORE
            if htf_bull and btc_trend!="BEARISH":
                if bullish_structure:
                    score_buy+=5

                    if fvg_bull_present:
                        score_buy+=2

                    if ob_bull_present:
                        score_buy+=2

                    if liq_sweep_recent:
                        score_buy+=1

                    if score_buy>=7:
                        entry=last_closed["close"]

                        sl_multiplier=(
                            1.0
                            if liq_sweep_recent
                            else 1.5
                        )

                        tp_multiplier=(
                            3.0
                            if ob_bull_present
                            else 2.5
                        )

                        sl=(
                            entry-
                            last_closed["atr"]*
                            sl_multiplier
                        )

                        tp=(
                            entry+
                            last_closed["atr"]*
                            tp_multiplier
                        )

                        return Signal(
                            sym,
                            tf,
                            "BUY",
                            entry,
                            sl,
                            tp,
                            score_buy,
                            df.index[-2]
                        )

            # SELL SCORE
            if htf_bear and btc_trend!="BULLISH":
                if bearish_structure:
                    score_sell+=5

                    if fvg_bear_present:
                        score_sell+=2

                    if ob_bear_present:
                        score_sell+=2

                    if liq_sweep_recent:
                        score_sell+=1

                    if score_sell>=7:
                        entry=last_closed["close"]

                        sl_multiplier=(
                            1.0
                            if liq_sweep_recent
                            else 1.5
                        )

                        tp_multiplier=(
                            3.0
                            if ob_bear_present
                            else 2.5
                        )

                        sl=(
                            entry+
                            last_closed["atr"]*
                            sl_multiplier
                        )

                        tp=(
                            entry-
                            last_closed["atr"]*
                            tp_multiplier
                        )

                        return Signal(
                            sym,
                            tf,
                            "SELL",
                            entry,
                            sl,
                            tp,
                            score_sell,
                            df.index[-2]
                        )

            return None

        except Exception as e:
            logger.error(
                f"Analysis Error {sym} {tf}: {e}"
            )
            return None

        finally:
            del df
    def scan_coin(self,sym):

        mtf_ctx=self.get_mtf_context(sym)

        if not mtf_ctx:
            return []

        with STATE_LOCK:
            btc_trend=CONTEXT_CACHE.get(
                "btc_trend",
                "NEUTRAL"
            )

        signals=[]

        for tf in TIMEFRAMES:

            signal=self.analyze_tf(
                sym,
                tf,
                mtf_ctx,
                btc_trend
            )

            if signal:
                signals.append(signal)

        return signals


# =========================================================
# SCAN JOB
# =========================================================

engine=BGStarEngine()


def run_scan_job():

    global COOLDOWNS

    start_time=time.time()

    try:

        now=datetime.now()

        with STATE_LOCK:

            COOLDOWNS={
                k:v
                for k,v in COOLDOWNS.items()
                if now-v<timedelta(
                    minutes=COOLDOWN_MINUTES
                )
            }

            SYSTEM_STATUS["last_scan"]=(
                now.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            )

        # BTC context
        engine.update_global_context()

        all_signals=[]

        with ThreadPoolExecutor(
            max_workers=3
        ) as executor:

            futures={
                executor.submit(
                    engine.scan_coin,
                    coin
                ):coin
                for coin in COINS
            }

            for future in as_completed(futures):

                try:

                    result=future.result()

                    if result:
                        all_signals.extend(result)

                except Exception as e:

                    logger.error(
                        f"Worker Error: {e}"
                    )

        # Process signals
        for sig in all_signals:

            sig_key=(
                f"{sig.coin}_{sig.direction}"
            )

            with STATE_LOCK:

                if sig_key in COOLDOWNS:
                    continue

                COOLDOWNS[sig_key]=now

                SYSTEM_STATUS[
                    "total_signals"
                ]+=1

            sig_strength=(
                "⭐⭐⭐ STRONG (SMC Confluence)"
                if sig.score>=9
                else
                "⭐⭐ VALID (Structure Break)"
            )

            emoji=(
                "🟢 BUY (LONG)"
                if sig.direction=="BUY"
                else
                "🔴 SELL (SHORT)"
            )

            msg=(
                "🚀 <b>BG STAR PRO v3</b>\n\n"
            )

            msg+=(
                f"🪙 <b>Asset:</b> "
                f"#{sig.coin.replace('/USDT','')}\n"
            )

            msg+=(
                f"🎯 <b>Action:</b> {emoji}\n"
            )

            msg+=(
                f"📊 <b>Score:</b> "
                f"{sig.score}/10 | "
                f"{sig_strength}\n"
            )

            msg+=(
                "⏳ <b>Timeframe:</b> 15m\n\n"
            )

            msg+=(
                f"📍 <b>Entry:</b> "
                f"{sig.entry:.4f}\n"
            )

            msg+=(
                f"🛑 <b>SL:</b> "
                f"{sig.sl:.4f}\n"
            )

            msg+=(
                f"✅ <b>TP:</b> "
                f"{sig.tp:.4f}\n\n"
            )

            msg+=(
                "<i>✓ BOS/CHoCH Detected\n"
                "✓ FVG/OB Confirmed\n"
                "✓ HTF & BTC Aligned</i>"
            )

            telegram_queue.put(msg)

            logger.info(
                f"SMC Signal: "
                f"{sig.coin} | "
                f"{sig.direction} | "
                f"Score {sig.score}"
            )

    except Exception as e:

        logger.error(
            f"Scheduler Job Exception: {e}"
        )

    finally:

        duration_ms=int(
            (time.time()-start_time)*1000
        )

        next_time=(
            datetime.now()
            +timedelta(
                seconds=SCAN_DELAY_SECONDS
            )
        )

        with STATE_LOCK:

            SYSTEM_STATUS[
                "scan_duration_ms"
            ]=duration_ms

            SYSTEM_STATUS[
                "next_scan"
            ]=next_time.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        try:

            scheduler=get_scheduler()

            scheduler.add_job(
                run_scan_job,
                "date",
                run_date=next_time,
                id="run_scan_job",
                replace_existing=True
            )

        except Exception as e:

            logger.error(
                f"Schedule Error: {e}"
            )

        gc.collect()
        # =========================================================
# START SCHEDULER
# =========================================================

get_scheduler()


# =========================================================
# STREAMLIT DASHBOARD
# =========================================================

st_autorefresh(
    interval=60000,
    limit=None,
    key="dashboard_refresh"
)

st.markdown("""
<style>
.big-card{
background:#0f172a;
padding:20px;
border-radius:12px;
border:1px solid #1e293b;
text-align:center;
}
.status{
background:#059669;
color:white;
padding:6px 15px;
border-radius:20px;
font-weight:bold;
}
.box{
background:#1e293b;
padding:15px;
border-radius:8px;
border:1px solid #334155;
margin-bottom:12px;
}
.label{
color:#94a3b8;
font-size:13px;
}
.value{
color:#38bdf8;
font-size:18px;
font-weight:bold;
}
</style>
""", unsafe_allow_html=True)


st.title("⚡ BG STAR PRO v3")

st.markdown(
    "### Institutional SMC Trading Engine — 15M ONLY"
)


st.markdown(
    f"""
    <div class="big-card">
        <span class="status">
            {SYSTEM_STATUS["status"]}
        </span>
        <p style="color:#94a3b8;margin-top:15px;">
        Active Core: BOS/CHoCH | FVG | Order Block |
        Liquidity Sweep | HTF Alignment | BTC Filter
        </p>
    </div>
    """,
    unsafe_allow_html=True
)


col1,col2,col3 = st.columns(3)


with col1:

    st.markdown(
        f"""
        <div class="box">
        <span class="label">Last Scan</span><br>
        <span class="value">
        {SYSTEM_STATUS["last_scan"]}
        </span>
        </div>

        <div class="box">
        <span class="label">Next Scan</span><br>
        <span class="value">
        {SYSTEM_STATUS["next_scan"]}
        </span>
        </div>
        """,
        unsafe_allow_html=True
    )


with col2:

    st.markdown(
        f"""
        <div class="box">
        <span class="label">API Latency</span><br>
        <span class="value">
        {SYSTEM_STATUS["api_latency_ms"]} ms
        </span>
        </div>

        <div class="box">
        <span class="label">Scan Duration</span><br>
        <span class="value">
        {SYSTEM_STATUS["scan_duration_ms"]} ms
        </span>
        </div>
        """,
        unsafe_allow_html=True
    )


with col3:

    queue_size = telegram_queue.qsize()

    st.markdown(
        f"""
        <div class="box">
        <span class="label">Total Signals</span><br>
        <span class="value">
        {SYSTEM_STATUS["total_signals"]}
        </span>
        </div>

        <div class="box">
        <span class="label">Telegram Queue</span><br>
        <span class="value">
        {queue_size} pending
        </span>
        </div>
        """,
        unsafe_allow_html=True
    )


st.markdown("<hr>", unsafe_allow_html=True)

st.markdown(
    """
    <div style="text-align:center;color:#64748b;">
    <b>15M ONLY SIGNAL ENGINE</b><br>
    BTC • ETH • BNB • SOL • XRP<br>
    Scan Cycle: Every 5 Minutes
    </div>
    """,
    unsafe_allow_html=True
        )
