import streamlit as st

st.set_page_config(
    page_title="BG STAR PRO v3",
    layout="wide",
    page_icon="⚡"
)

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
# CONFIG
# ==========================================

SCAN_DELAY_SECONDS = 300
COOLDOWN_MINUTES = 15
BTC_CONTEXT_CACHE_SECONDS = 300
MTF_CONTEXT_CACHE_SECONDS = 1800

COINS = [
    "BTC/USDT",
    "ETH/USDT",
    "BNB/USDT",
    "SOL/USDT",
    "XRP/USDT"
]

TIMEFRAMES = ["15m"]


# ==========================================
# LOGGING
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("BG_STAR_PRO_v3_SMC")


# ==========================================
# ENVIRONMENT
# ==========================================

TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    ""
)

TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    ""
)


# ==========================================
# GLOBAL STATE
# ==========================================

COOLDOWNS = {}

STATE_LOCK = threading.Lock()

CONTEXT_CACHE = {
    "btc_trend": "NEUTRAL",
    "last_update": 0
}

SYSTEM_STATUS = {
    "status": "🟢 ONLINE",
    "last_scan": "Booting...",
    "next_scan": "Starting...",
    "scan_duration_ms": 0,
    "api_latency_ms": 0,
    "total_signals": 0,
    "active_workers": 3
}


# ==========================================
# TELEGRAM QUEUE
# ==========================================

telegram_queue = queue.Queue()


def telegram_worker():

    session = requests.Session()

    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[
            429,
            500,
            502,
            503,
            504
        ],
        allowed_methods=[
            "POST"
        ]
    )

    adapter = HTTPAdapter(
        max_retries=retry
    )

    session.mount(
        "http://",
        adapter
    )

    session.mount(
        "https://",
        adapter
    )

    while True:

        msg = telegram_queue.get()

        try:

            if (
                not TELEGRAM_BOT_TOKEN
                or
                not TELEGRAM_CHAT_ID
            ):
                continue

            response = session.post(
                f"https://api.telegram.org/bot"
                f"{TELEGRAM_BOT_TOKEN}/sendMessage",

                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": msg,
                    "parse_mode": "HTML"
                },

                timeout=5
            )

            if response.status_code != 200:

                logger.warning(
                    "Telegram API returned "
                    f"{response.status_code}"
                )

        except Exception as e:

            logger.error(
                f"Telegram Delivery Failed: {e}"
            )

        finally:

            telegram_queue.task_done()

        time.sleep(1)


@st.cache_resource
def start_tg_worker():

    worker = threading.Thread(
        target=telegram_worker,
        daemon=True,
        name="TelegramWorker"
    )

    worker.start()

    return worker


start_tg_worker()


# ==========================================
# THREAD LOCAL EXCHANGE
# ==========================================

thread_local = threading.local()


def get_exchange():

    if (
        not hasattr(
            thread_local,
            "ex"
        )
        or
        getattr(
            thread_local,
            "needs_reconnect",
            False
        )
    ):

        try:

            thread_local.ex = ccxt.kucoin({
                "enableRateLimit": True,
                "timeout": 10000
            })

            thread_local.needs_reconnect = False

        except Exception as e:

            logger.error(
                f"Exchange Init Failed: {e}"
            )

            thread_local.ex = None
            return None

    return thread_local.ex


# ==========================================
# SIGNAL MODEL
# ==========================================

@dataclass
class Signal:

    coin: str
    tf: str
    direction: str

    entry: float
    sl: float
    tp: float

    score: int
    timestamp: pd.Timestamp


# ==========================================
# SMC ANALYZER
# ==========================================

class SMCAnalyzer:


    @staticmethod
    def identify_pivots(
        df,
        left_bars=5,
        right_bars=5
    ):

        df["pivot_high"] = False
        df["pivot_low"] = False

        if len(df) <= (
            left_bars + right_bars
        ):
            return df

        for i in range(
            left_bars,
            len(df) - right_bars
        ):

            high_value = df["high"].iloc[i]
            low_value = df["low"].iloc[i]

            is_high = True
            is_low = True

            for j in range(
                1,
                left_bars + 1
            ):

                if (
                    high_value
                    <=
                    df["high"].iloc[i - j]
                ):
                    is_high = False

                if (
                    low_value
                    >=
                    df["low"].iloc[i - j]
                ):
                    is_low = False

            for j in range(
                1,
                right_bars + 1
            ):

                if (
                    high_value
                    <=
                    df["high"].iloc[i + j]
                ):
                    is_high = False

                if (
                    low_value
                    >=
                    df["low"].iloc[i + j]
                ):
                    is_low = False

            if is_high:

                df.iat[
                    i,
                    df.columns.get_loc(
                        "pivot_high"
                    )
                ] = True

            if is_low:

                df.iat[
                    i,
                    df.columns.get_loc(
                        "pivot_low"
                    )
                ] = True

        return df


    @staticmethod
    def detect_fvg(df):

        df["fvg_bull"] = False
        df["fvg_bear"] = False

        bull_condition = (
            (df["low"] > df["high"].shift(2))
            &
            (
                df["close"].shift(1)
                >
                df["open"].shift(1)
            )
        )

        bear_condition = (
            (df["high"] < df["low"].shift(2))
            &
            (
                df["close"].shift(1)
                <
                df["open"].shift(1)
            )
        )

        df.loc[
            bull_condition,
            "fvg_bull"
        ] = True

        df.loc[
            bear_condition,
            "fvg_bear"
        ] = True

        return df
# ==========================================
# 6. SMC ANALYZER
# ==========================================

@dataclass
class Signal:
    coin: str
    tf: str
    direction: str
    entry: float
    sl: float
    tp: float
    score: int
    timestamp: pd.Timestamp


class SMCAnalyzer:

    @staticmethod
    def identify_pivots(df, left=5, right=5):
        df["pivot_high"] = False
        df["pivot_low"] = False

        if len(df) <= left + right:
            return df

        for i in range(left, len(df) - right):
            h = df["high"].iloc[i]
            l = df["low"].iloc[i]

            high_ok = all(
                h > df["high"].iloc[i-j]
                for j in range(1, left + 1)
            ) and all(
                h > df["high"].iloc[i+j]
                for j in range(1, right + 1)
            )

            low_ok = all(
                l < df["low"].iloc[i-j]
                for j in range(1, left + 1)
            ) and all(
                l < df["low"].iloc[i+j]
                for j in range(1, right + 1)
            )

            df.iat[
                i,
                df.columns.get_loc("pivot_high")
            ] = high_ok

            df.iat[
                i,
                df.columns.get_loc("pivot_low")
            ] = low_ok

        return df

    @staticmethod
    def detect_fvg(df):
        df["fvg_bull"] = (
            (df["low"] > df["high"].shift(2)) &
            (df["close"].shift(1) > df["open"].shift(1))
        )

        df["fvg_bear"] = (
            (df["high"] < df["low"].shift(2)) &
            (df["close"].shift(1) < df["open"].shift(1))
        )

        return df

    @staticmethod
    def map_structure_and_ob(df):
        df["structure"] = "NONE"
        df["ob_bull"] = False
        df["ob_bear"] = False
        df["liq_sweep"] = False

        last_ph = None
        last_pl = None
        trend = 0

        for i in range(10, len(df)):
            high = df["high"].iloc[i]
            low = df["low"].iloc[i]
            close = df["close"].iloc[i]

            if df["pivot_high"].iloc[i]:
                last_ph = high

            if df["pivot_low"].iloc[i]:
                last_pl = low

            if last_ph is None or last_pl is None:
                continue

            # Liquidity sweep
            if high > last_ph and close < last_ph:
                df.iat[
                    i,
                    df.columns.get_loc("liq_sweep")
                ] = True

            if low < last_pl and close > last_pl:
                df.iat[
                    i,
                    df.columns.get_loc("liq_sweep")
                ] = True

            # Bullish BOS / CHOCH
            if close > last_ph:

                if trend == -1:
                    df.iat[
                        i,
                        df.columns.get_loc("structure")
                    ] = "CHOCH_BULL"

                elif trend == 1:
                    df.iat[
                        i,
                        df.columns.get_loc("structure")
                    ] = "BOS_BULL"

                    ob_idx = i - 1

                    while (
                        ob_idx > 0 and
                        df["close"].iloc[ob_idx] >=
                        df["open"].iloc[ob_idx]
                    ):
                        ob_idx -= 1

                    if ob_idx > 0:
                        df.iat[
                            ob_idx,
                            df.columns.get_loc("ob_bull")
                        ] = True

                trend = 1
                last_ph = None

            # Bearish BOS / CHOCH
            elif close < last_pl:

                if trend == 1:
                    df.iat[
                        i,
                        df.columns.get_loc("structure")
                    ] = "CHOCH_BEAR"

                elif trend == -1:
                    df.iat[
                        i,
                        df.columns.get_loc("structure")
                    ] = "BOS_BEAR"

                    ob_idx = i - 1

                    while (
                        ob_idx > 0 and
                        df["close"].iloc[ob_idx] <=
                        df["open"].iloc[ob_idx]
                    ):
                        ob_idx -= 1

                    if ob_idx > 0:
                        df.iat[
                            ob_idx,
                            df.columns.get_loc("ob_bear")
                        ] = True

                trend = -1
                last_pl = None

        return df

    @staticmethod
    def calc_atr(df):
        prev_close = df["close"].shift(1)

        tr1 = df["high"] - df["low"]
        tr2 = (df["high"] - prev_close).abs()
        tr3 = (df["low"] - prev_close).abs()

        df["tr"] = pd.concat(
            [tr1, tr2, tr3],
            axis=1
        ).max(axis=1)

        df["atr"] = df["tr"].rolling(14).mean()

        return df


# ==========================================
# 7. BG STAR ENGINE
# ==========================================

class BGStarEngine:

    def fetch_data(self, symbol, timeframe, limit=200):
        start = time.time()

        try:
            exchange = get_exchange()

            if exchange is None:
                return None

            data = exchange.fetch_ohlcv(
                symbol,
                timeframe,
                limit=limit
            )

            if not data:
                return None

            df = pd.DataFrame(
                data,
                columns=[
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume"
                ]
            )

            df["timestamp"] = pd.to_datetime(
                df["timestamp"],
                unit="ms"
            )

            with STATE_LOCK:
                SYSTEM_STATUS["api_latency_ms"] = int(
                    (time.time() - start) * 1000
                )

            return df.set_index("timestamp")

        except ccxt.RateLimitExceeded:
            logger.warning(
                f"Rate limit: {symbol} {timeframe}"
            )
            thread_local.needs_reconnect = True
            return None

        except Exception as e:
            logger.error(
                f"Fetch error {symbol} {timeframe}: {e}"
            )
            thread_local.needs_reconnect = True
            return None

    def update_global_context(self):
        now = time.time()

        with STATE_LOCK:
            last_update = CONTEXT_CACHE.get(
                "last_update",
                0
            )

        if now - last_update < 900:
            return

        df = self.fetch_data(
            "BTC/USDT",
            "1h",
            200
        )

        if df is None or len(df) < 50:
            return

        try:
            df = SMCAnalyzer.identify_pivots(df)
            df = SMCAnalyzer.map_structure_and_ob(df)

            recent = df[
                df["structure"] != "NONE"
            ].tail(3)

            trend = "NEUTRAL"

            if not recent.empty:
                structure = recent.iloc[-1]["structure"]

                if "BULL" in structure:
                    trend = "BULLISH"
                elif "BEAR" in structure:
                    trend = "BEARISH"

            with STATE_LOCK:
                CONTEXT_CACHE["btc_trend"] = trend
                CONTEXT_CACHE["last_update"] = now

            logger.info(
                f"BTC Trend: {trend}"
            )

        except Exception as e:
            logger.error(
                f"BTC Context Error: {e}"
            )

        finally:
            del df

    def get_mtf_context(self, symbol):
        now = time.time()

        with STATE_LOCK:
            cached = CONTEXT_CACHE.get(symbol)

        if (
            cached and
            now - cached.get("last_fetch", 0) < 3600
        ):
            return cached

        df_1h = self.fetch_data(
            symbol,
            "1h",
            200
        )

        df_4h = self.fetch_data(
            symbol,
            "4h",
            200
        )

        if df_1h is None or df_4h is None:
            return None

        try:
            ema_1h = (
                df_1h["close"]
                .ewm(
                    span=200,
                    adjust=False
                )
                .mean()
                .iloc[-2]
            )

            ema_4h = (
                df_4h["close"]
                .ewm(
                    span=200,
                    adjust=False
                )
                .mean()
                .iloc[-2]
            )

            context = {
                "ema200_1h": ema_1h,
                "ema200_4h": ema_4h,
                "last_fetch": now
            }

            with STATE_LOCK:
                CONTEXT_CACHE[symbol] = context

            return context

        except Exception as e:
            logger.error(
                f"MTF Context Error {symbol}: {e}"
            )
            return None

        finally:
            del df_1h
            del df_4h
# ==========================================
# PART 3 - BG STAR ENGINE
# ==========================================

class BGStarEngine:

    def fetch_data(self, sym, tf, limit=250):
        start = time.time()

        try:
            ex = get_exchange()

            if ex is None:
                return None

            data = ex.fetch_ohlcv(
                sym,
                timeframe=tf,
                limit=limit
            )

            if not data:
                return None

            df = pd.DataFrame(
                data,
                columns=[
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume"
                ]
            )

            df["timestamp"] = pd.to_datetime(
                df["timestamp"],
                unit="ms"
            )

            df = df.set_index("timestamp")

            with STATE_LOCK:
                SYSTEM_STATUS["api_latency_ms"] = int(
                    (time.time() - start) * 1000
                )

            return df

        except ccxt.RateLimitExceeded:
            logger.warning(
                f"Rate limit: {sym} {tf}"
            )
            thread_local.needs_reconnect = True
            return None

        except Exception as e:
            logger.error(
                f"Fetch error {sym} {tf}: {e}"
            )
            thread_local.needs_reconnect = True
            return None


    # ==========================================
    # BTC GLOBAL TREND
    # ==========================================

    def update_global_context(self):
        now = time.time()

        with STATE_LOCK:
            last_update = CONTEXT_CACHE.get(
                "last_update",
                0
            )

        # BTC context প্রতি 5 মিনিটে একবার update
        if now - last_update < 300:
            return

        df = self.fetch_data(
            "BTC/USDT",
            "1h",
            200
        )

        if df is None or len(df) < 60:
            return

        try:
            df = SMCAnalyzer.identify_pivots(df)
            df = SMCAnalyzer.map_structure_and_ob(df)

            recent = df[
                df["structure"] != "NONE"
            ].tail(3)

            trend = "NEUTRAL"

            if not recent.empty:
                structure = recent.iloc[-1]["structure"]

                if "BULL" in structure:
                    trend = "BULLISH"

                elif "BEAR" in structure:
                    trend = "BEARISH"

            with STATE_LOCK:
                CONTEXT_CACHE["btc_trend"] = trend
                CONTEXT_CACHE["last_update"] = now

            logger.info(
                f"BTC Context: {trend}"
            )

        except Exception as e:
            logger.error(
                f"BTC Context Error: {e}"
            )

        finally:
            del df


    # ==========================================
    # HIGHER TIMEFRAME CONTEXT
    # ==========================================

    def get_mtf_context(self, sym):

        now = time.time()

        with STATE_LOCK:
            cached = CONTEXT_CACHE.get(sym)

        if (
            cached
            and
            now - cached.get("last_fetch", 0) < 900
        ):
            return cached

        df_1h = self.fetch_data(
            sym,
            "1h",
            200
        )

        df_4h = self.fetch_data(
            sym,
            "4h",
            200
        )

        if df_1h is None or df_4h is None:
            return None

        try:
            ema200_1h = (
                df_1h["close"]
                .ewm(
                    span=200,
                    adjust=False
                )
                .mean()
                .iloc[-2]
            )

            ema200_4h = (
                df_4h["close"]
                .ewm(
                    span=200,
                    adjust=False
                )
                .mean()
                .iloc[-2]
            )

            context = {
                "ema200_1h": float(ema200_1h),
                "ema200_4h": float(ema200_4h),
                "last_fetch": now
            }

            with STATE_LOCK:
                CONTEXT_CACHE[sym] = context

            return context

        except Exception as e:
            logger.error(
                f"MTF Context Error {sym}: {e}"
            )
            return None

        finally:
            del df_1h
            del df_4h


    # ==========================================
    # 15 MINUTE ANALYSIS
    # ==========================================

    def analyze_tf(
        self,
        sym,
        tf,
        mtf_ctx,
        btc_trend
    ):

        df = self.fetch_data(
            sym,
            tf,
            250
        )

        if df is None or len(df) < 80:
            return None

        try:
            # ------------------------------
            # SMC CALCULATIONS
            # ------------------------------

            df = SMCAnalyzer.identify_pivots(df)
            df = SMCAnalyzer.detect_fvg(df)
            df = SMCAnalyzer.map_structure_and_ob(df)
            df = SMCAnalyzer.calc_atr(df)

            # শেষ candle চলমান হতে পারে
            last = df.iloc[-2]
            prev = df.iloc[-3]

            if pd.isna(last["atr"]) or last["atr"] <= 0:
                return None

            # ------------------------------
            # DEAD MARKET FILTER
            # ------------------------------

            atr_percent = (
                last["atr"] /
                last["close"]
            ) * 100

            if atr_percent < 0.15:
                return None

            # ------------------------------
            # HTF FILTER
            # ------------------------------

            htf_bull = (
                last["close"] >
                mtf_ctx["ema200_1h"]
                and
                last["close"] >
                mtf_ctx["ema200_4h"]
            )

            htf_bear = (
                last["close"] <
                mtf_ctx["ema200_1h"]
                and
                last["close"] <
                mtf_ctx["ema200_4h"]
            )

            # ------------------------------
            # RECENT SMC DATA
            # ------------------------------

            recent = df.tail(12)

            fvg_bull = recent[
                "fvg_bull"
            ].any()

            fvg_bear = recent[
                "fvg_bear"
            ].any()

            ob_bull = recent[
                "ob_bull"
            ].any()

            ob_bear = recent[
                "ob_bear"
            ].any()

            liquidity = recent[
                "liq_sweep"
            ].any()

            bull_structure = (
                last["structure"]
                in [
                    "BOS_BULL",
                    "CHOCH_BULL"
                ]
                or
                prev["structure"]
                in [
                    "BOS_BULL",
                    "CHOCH_BULL"
                ]
            )

            bear_structure = (
                last["structure"]
                in [
                    "BOS_BEAR",
                    "CHOCH_BEAR"
                ]
                or
                prev["structure"]
                in [
                    "BOS_BEAR",
                    "CHOCH_BEAR"
                ]
            )

            # ------------------------------
            # BUY SCORE
            # ------------------------------

            if (
                htf_bull
                and
                btc_trend != "BEARISH"
                and
                bull_structure
            ):

                score = 5

                if fvg_bull:
                    score += 2

                if ob_bull:
                    score += 2

                if liquidity:
                    score += 1

                if score >= 7:

                    entry = float(
                        last["close"]
                    )

                    sl_mult = (
                        1.0
                        if liquidity
                        else 1.5
                    )

                    tp_mult = (
                        3.0
                        if ob_bull
                        else 2.5
                    )

                    sl = (
                        entry -
                        last["atr"] *
                        sl_mult
                    )

                    tp = (
                        entry +
                        last["atr"] *
                        tp_mult
                    )

                    return Signal(
                        sym,
                        tf,
                        "BUY",
                        entry,
                        float(sl),
                        float(tp),
                        score,
                        df.index[-2]
                    )

            # ------------------------------
            # SELL SCORE
            # ------------------------------

            if (
                htf_bear
                and
                btc_trend != "BULLISH"
                and
                bear_structure
            ):

                score = 5

                if fvg_bear:
                    score += 2

                if ob_bear:
                    score += 2

                if liquidity:
                    score += 1

                if score >= 7:

                    entry = float(
                        last["close"]
                    )

                    sl_mult = (
                        1.0
                        if liquidity
                        else 1.5
                    )

                    tp_mult = (
                        3.0
                        if ob_bear
                        else 2.5
                    )

                    sl = (
                        entry +
                        last["atr"] *
                        sl_mult
                    )

                    tp = (
                        entry -
                        last["atr"] *
                        tp_mult
                    )

                    return Signal(
                        sym,
                        tf,
                        "SELL",
                        entry,
                        float(sl),
                        float(tp),
                        score,
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


    # ==========================================
    # SCAN ONE COIN
    # ==========================================

    def scan_coin(self, sym):

        mtf_ctx = self.get_mtf_context(sym)

        if mtf_ctx is None:
            return []

        with STATE_LOCK:
            btc_trend = CONTEXT_CACHE.get(
                "btc_trend",
                "NEUTRAL"
            )

        results = []

        for tf in TIMEFRAMES:

            signal = self.analyze_tf(
                sym,
                tf,
                mtf_ctx,
                btc_trend
            )

            if signal:
                results.append(signal)

        return results


engine = BGStarEngine()
   # ==========================================
# PART 4 - SCANNER + SCHEDULER + DASHBOARD
# ==========================================

def run_scan_job():

    global COOLDOWNS

    start_time = time.time()

    try:
        now = datetime.now()

        with STATE_LOCK:

            SYSTEM_STATUS["last_scan"] = (
                now.strftime("%Y-%m-%d %H:%M:%S")
            )

            COOLDOWNS = {
                key: value
                for key, value in COOLDOWNS.items()
                if (
                    now - value
                ).total_seconds()
                <
                COOLDOWN_MINUTES * 60
            }

        logger.info(
            "===== SCAN STARTED ====="
        )

        # --------------------------------------
        # UPDATE BTC GLOBAL CONTEXT
        # --------------------------------------

        engine.update_global_context()

        # --------------------------------------
        # SCAN ALL COINS
        # --------------------------------------

        all_signals = []

        with ThreadPoolExecutor(
            max_workers=3
        ) as executor:

            futures = {
                executor.submit(
                    engine.scan_coin,
                    coin
                ): coin
                for coin in COINS
            }

            for future in as_completed(futures):

                coin = futures[future]

                try:

                    result = future.result()

                    if result:
                        all_signals.extend(result)

                except Exception as e:

                    logger.error(
                        f"Worker Error {coin}: {e}"
                    )

        # --------------------------------------
        # PROCESS SIGNALS
        # --------------------------------------

        for sig in all_signals:

            signal_key = (
                f"{sig.coin}_{sig.direction}"
            )

            with STATE_LOCK:

                if signal_key in COOLDOWNS:
                    continue

                COOLDOWNS[signal_key] = now

                SYSTEM_STATUS[
                    "total_signals"
                ] += 1

            # ----------------------------------
            # SIGNAL STRENGTH
            # ----------------------------------

            if sig.score >= 9:

                strength = (
                    "⭐⭐⭐ STRONG"
                )

            else:

                strength = (
                    "⭐⭐ VALID"
                )

            emoji = (
                "🟢 BUY (LONG)"
                if sig.direction == "BUY"
                else
                "🔴 SELL (SHORT)"
            )

            # ----------------------------------
            # TELEGRAM MESSAGE
            # ----------------------------------

            msg = (
                "🚀 <b>"
                "BG STAR PRO v3"
                "</b>\n\n"
            )

            msg += (
                f"🪙 <b>Asset:</b> "
                f"#{sig.coin.replace('/USDT', '')}\n"
            )

            msg += (
                f"🎯 <b>Action:</b> "
                f"{emoji}\n"
            )

            msg += (
                f"📊 <b>Score:</b> "
                f"{sig.score}/10 | "
                f"{strength}\n"
            )

            msg += (
                f"⏳ <b>Timeframe:</b> "
                f"{sig.tf}\n\n"
            )

            msg += (
                f"📍 <b>Entry:</b> "
                f"{sig.entry:.4f}\n"
            )

            msg += (
                f"🛑 <b>SL:</b> "
                f"{sig.sl:.4f}\n"
            )

            msg += (
                f"✅ <b>TP:</b> "
                f"{sig.tp:.4f}\n\n"
            )

            msg += (
                "<i>"
                "✓ 15m Signal\n"
                "✓ BOS/CHoCH\n"
                "✓ FVG/Order Block\n"
                "✓ Liquidity Check\n"
                "✓ 1H/4H HTF Alignment\n"
                "✓ BTC Context Filter"
                "</i>"
            )

            telegram_queue.put(msg)

            logger.info(
                f"SIGNAL | {sig.coin} | "
                f"{sig.direction} | "
                f"Score {sig.score}"
            )

        duration = int(
            (time.time() - start_time) * 1000
        )

        next_scan = (
            datetime.now()
            +
            timedelta(
                seconds=SCAN_DELAY_SECONDS
            )
        )

        with STATE_LOCK:

            SYSTEM_STATUS[
                "scan_duration_ms"
            ] = duration

            SYSTEM_STATUS[
                "next_scan"
            ] = next_scan.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        logger.info(
            f"===== SCAN FINISHED | "
            f"{duration} ms ====="
        )

    except Exception as e:

        logger.exception(
            f"Scheduler Job Exception: {e}"
        )

    finally:

        gc.collect()


# ==========================================
# SCHEDULER
# ==========================================

@st.cache_resource
def start_scheduler():

    scheduler = BackgroundScheduler(
        timezone="Asia/Kolkata"
    )

    # প্রথম scan অ্যাপ চালুর 5 সেকেন্ড পরে
    scheduler.add_job(
        run_scan_job,
        "date",
        run_date=datetime.now() + timedelta(seconds=5),
        id="initial_scan",
        replace_existing=True
    )

    # পরবর্তী scan প্রতি 5 মিনিটে
    scheduler.add_job(
        run_scan_job,
        "interval",
        seconds=SCAN_DELAY_SECONDS,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
        id="five_minute_scan",
        replace_existing=True
    )

    scheduler.start()

    logger.info(
        "Scheduler started | "
        "Scan interval = 5 minutes"
    )

    return scheduler


start_scheduler()


# ==========================================
# STREAMLIT DASHBOARD REFRESH
# ==========================================

st_autorefresh(
    interval=60000,
    limit=None,
    key="dashboard_refresh"
)


# ==========================================
# DASHBOARD STYLE
# ==========================================

st.markdown(
    """
    <style>

    .big-card {
        background: #0f172a;
        padding: 20px;
        border-radius: 12px;
        border: 1px solid #1e293b;
        text-align: center;
    }

    .status-badge {
        background: #059669;
        color: white;
        padding: 5px 15px;
        border-radius: 20px;
        font-weight: bold;
        font-size: 14px;
    }

    .metric-box {
        background: #1e293b;
        padding: 15px;
        border-radius: 8px;
        border: 1px solid #334155;
        text-align: left;
    }

    .label {
        color: #94a3b8;
        font-size: 14px;
        display: block;
        margin-bottom: 5px;
    }

    .value {
        color: #38bdf8;
        font-size: 18px;
        font-weight: bold;
    }

    .alert-value {
        color: #f59e0b;
        font-size: 18px;
        font-weight: bold;
    }

    </style>
    """,
    unsafe_allow_html=True
)


# ==========================================
# DASHBOARD HEADER
# ==========================================

st.title(
    "⚡ BG STAR PRO v3"
)

st.markdown(
    "### Institutional SMC Trading Engine"
)


st.markdown(
    f"""
    <div class="big-card">

        <span class="status-badge">
            {SYSTEM_STATUS["status"]}
        </span>

        <p style="
            margin-top:15px;
            color:#94a3b8;
        ">

        15m Signals |
        1H + 4H HTF |
        BTC Filter |
        BOS/CHoCH |
        FVG |
        Order Blocks |
        Liquidity

        </p>

    </div>
    """,
    unsafe_allow_html=True
)


# ==========================================
# METRICS
# ==========================================

col1, col2, col3 = st.columns(3)


# ------------------------------------------
# COLUMN 1
# ------------------------------------------

with col1:

    st.markdown(
        f"""
        <div class="metric-box">

            <span class="label">
                Last Scan
            </span>

            <span class="value">
                {SYSTEM_STATUS["last_scan"]}
            </span>

        </div>

        <div class="metric-box"
             style="margin-top:15px;">

            <span class="label">
                Next Scan
            </span>

            <span class="value">
                {SYSTEM_STATUS["next_scan"]}
            </span>

        </div>
        """,
        unsafe_allow_html=True
    )


# ------------------------------------------
# COLUMN 2
# ------------------------------------------

with col2:

    st.markdown(
        f"""
        <div class="metric-box">

            <span class="label">
                API Latency
            </span>

            <span class="value">
                {SYSTEM_STATUS["api_latency_ms"]} ms
            </span>

        </div>

        <div class="metric-box"
             style="margin-top:15px;">

            <span class="label">
                Scan Duration
            </span>

            <span class="value">
                {SYSTEM_STATUS["scan_duration_ms"]} ms
            </span>

        </div>
        """,
        unsafe_allow_html=True
    )


# ------------------------------------------
# COLUMN 3
# ------------------------------------------

with col3:

    queue_size = telegram_queue.qsize()

    queue_class = (
        "value"
        if queue_size == 0
        else
        "alert-value"
    )

    st.markdown(
        f"""
        <div class="metric-box">

            <span class="label">
                Total Signals
            </span>

            <span class="value">
                {SYSTEM_STATUS["total_signals"]}
            </span>

        </div>

        <div class="metric-box"
             style="margin-top:15px;">

            <span class="label">
                Telegram Queue
            </span>

            <span class="{queue_class}">
                {queue_size} pending
            </span>

        </div>
        """,
        unsafe_allow_html=True
    )


# ==========================================
# FOOTER
# ==========================================

st.markdown(
    """
    <br>
    <hr style="border-color:#1e293b;">

    <p style="
        text-align:center;
        color:#64748b;
        font-size:12px;
    ">

    BTC | ETH | BNB | SOL | XRP
    |
    Signal TF: 15m
    |
    Scan Cycle: 5 Minutes
    |
    Dashboard Refresh: 60 Seconds

    </p>
    """,
    unsafe_allow_html=True
            )
