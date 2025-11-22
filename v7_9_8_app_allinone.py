"""
v7.9.8 選股雷達 - 整合版 (規則引擎 + Streamlit UI 單檔)

執行方式:
    streamlit run v7_9_8_app_allinone.py

說明:
    - 本檔案同時包含 v7.9.8 核心規則引擎 (Universe / Firm / Score / 分層 / E 層候選)
      與 Pmoney 成交量掃描 + Streamlit 介面。
    - 適合先在雲端/本機快速驗證，不強制拆成 rules_v798.py + v798_app.py。
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any, List

import pandas as pd
import requests
import streamlit as st
import yfinance as yf

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None


# ============================================================
# 1. v7.9.8 規則引擎 (Universe / Firm / Score / 分層)
# ============================================================

class Layer(str, Enum):
    """§6 分層結果（選股用）"""
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    ELIMINATED = "X"   # 未通過 Universe 或完全不合層級條件


@dataclass
class StockSnapshot:
    """
    單一股票在評估當日的「快照」。

    注意：
    - 這裡只放「選股當下需要」的欄位（不含持有成本、最高價等出場相關欄位）。
    - 數值一律以「小數」表示（例如 ROE = 0.15 表示 15%）。
    """

    # 識別
    symbol: str
    name: str

    # 類別
    is_financial: bool       # 是否屬於金融股（銀行/保險等）
    is_cyclical: bool        # 是否屬於循環股（鋼鐵/塑化/航運/面板/DRAM 等）

    # 價量與均線（評估日當日）
    close: float             # 收盤價
    volume: float            # 成交量（股數）
    ma20: float
    ma60: float
    ma240: float

    # 流動性與市值（§3.5 + §3.1）
    avg_turnover_20: float   # 近 20 日平均成交金額 (TWD)
    turnover_ratio_20: float # 近 20 日平均換手率（0–1）
    market_cap: float        # 市值 (TWD)

    # 非金融基本面（§3.1）
    roe_ttm: float           # ROE(TTM)，例如 0.12 = 12%
    opm_ttm: float           # OPM(TTM)
    debt_ratio: float        # 負債比（總負債 / 總資產）
    revenue_yoy_m1: float    # 最近 1 月營收 YoY
    revenue_yoy_m2: float    # 最近 2 月前營收 YoY
    revenue_yoy_m3: float    # 最近 3 月前營收 YoY

    # 成長與品質延伸（§8）
    eps_growth_4q: float         # 近 4 季 EPS 成長率
    net_income_growth_3m: float  # 近 3 月淨利成長率

    # 金融股專用（§3.1-F）
    npl_ratio: Optional[float] = None          # NPL，不良貸款比
    coverage_ratio: Optional[float] = None     # 覆蓋率

    # 動能／相對強弱
    rs60: float = 50.0                         # RS(60) 百分位（0–100）

    # 族群與法人（Firm / E 層使用）
    industry: Optional[str] = None
    industry_index_price: Optional[float] = None
    industry_index_ma60: Optional[float] = None
    industry_up_ratio_5d: Optional[float] = None   # 近 5 日產業上漲家數占比（0–1）
    inst_net_buy_20: Optional[float] = None        # 近 20 日法人合計淨買超金額 (TWD)
    industry_rank_by_size: Optional[int] = None    # 產業內市值或營收排名（1=最大）
    last_quarter_growth: Optional[float] = None    # 最近一季營收或 EPS YoY（E 層用）


@dataclass
class UniverseResult:
    passed: bool
    reason: str
    checks: Dict[str, bool]


@dataclass
class FirmResult:
    f_price: bool
    f_volume: bool
    f_trend: bool
    f_group: bool
    count: int          # N_F
    is_firm: bool       # 四面齊


@dataclass
class ScoreResult:
    total: int
    growth: int
    quality: int
    momentum: int
    valuation: int


@dataclass
class ClassificationResult:
    symbol: str
    name: str
    layer: Layer
    is_e_candidate: bool
    universe: UniverseResult
    firm: FirmResult
    score: ScoreResult
    extra_info: Dict[str, Any]


# 規則常數（依 v7.9.8）
PRICE_CAP_DEFAULT = 80.0
MIN_TURNOVER_20 = 50_000_000     # 近 20 日均額 ≥ 5,000 萬
MIN_TURNOVER_RATIO_20 = 0.003    # 近 20 日換手率 ≥ 0.3%
MIN_MARKET_CAP = 1_000_000_000   # 市值 ≥ 10 億


def _check_universe_non_financial(
    s: StockSnapshot,
    price_cap: float = PRICE_CAP_DEFAULT
) -> UniverseResult:
    """§3.1 非金融 Universe 濾網"""
    checks: Dict[str, bool] = {}

    checks["price"] = s.close <= price_cap
    checks["market_cap"] = s.market_cap >= MIN_MARKET_CAP

    checks["revenue_3m_yoy"] = (
        s.revenue_yoy_m1 is not None
        and s.revenue_yoy_m2 is not None
        and s.revenue_yoy_m3 is not None
        and s.revenue_yoy_m1 >= 0.05
        and s.revenue_yoy_m2 >= 0.05
        and s.revenue_yoy_m3 >= 0.05
    )

    checks["roe"] = s.roe_ttm is not None and s.roe_ttm > 0.10
    checks["opm"] = s.opm_ttm is not None and s.opm_ttm >= 0.05
    checks["debt_ratio"] = (s.debt_ratio is not None) and (s.debt_ratio < 0.60)

    checks["turnover_20"] = (s.avg_turnover_20 is not None) and (s.avg_turnover_20 >= MIN_TURNOVER_20)
    checks["turnover_ratio_20"] = (s.turnover_ratio_20 is not None) and (s.turnover_ratio_20 >= MIN_TURNOVER_RATIO_20)

    passed = all(checks.values())
    reason = "OK" if passed else "Non-financial universe filter failed"
    return UniverseResult(passed=passed, reason=reason, checks=checks)


def _check_universe_financial(
    s: StockSnapshot,
    price_cap: float = PRICE_CAP_DEFAULT
) -> UniverseResult:
    """§3.1-F 金融股 Universe 濾網"""
    checks: Dict[str, bool] = {}

    checks["price"] = s.close <= price_cap
    checks["market_cap"] = s.market_cap >= MIN_MARKET_CAP
    checks["roe"] = s.roe_ttm is not None and s.roe_ttm > 0.10

    if s.npl_ratio is None:
        checks["npl"] = False
    else:
        checks["npl"] = s.npl_ratio < 0.01

    if s.coverage_ratio is None:
        checks["coverage"] = False
    else:
        checks["coverage"] = s.coverage_ratio > 1.0

    checks["growth"] = (
        (s.eps_growth_4q is not None and s.eps_growth_4q >= 0.05)
        or (s.net_income_growth_3m is not None and s.net_income_growth_3m >= 0.05)
    )

    checks["turnover_20"] = (s.avg_turnover_20 is not None) and (s.avg_turnover_20 >= MIN_TURNOVER_20)
    checks["turnover_ratio_20"] = (s.turnover_ratio_20 is not None) and (s.turnover_ratio_20 >= MIN_TURNOVER_RATIO_20)

    passed = all(checks.values())
    reason = "OK" if passed else "Financial universe filter failed"
    return UniverseResult(passed=passed, reason=reason, checks=checks)


def check_universe(
    s: StockSnapshot,
    price_cap: float = PRICE_CAP_DEFAULT
) -> UniverseResult:
    """依是否金融股，呼叫對應 Universe 濾網"""
    if s.is_financial:
        return _check_universe_financial(s, price_cap=price_cap)
    else:
        return _check_universe_non_financial(s, price_cap=price_cap)


def check_firm(s: StockSnapshot) -> FirmResult:
    """§3.2 Firm 動能四面齊"""
    # 價格：收盤 > 60MA 且 > 240MA
    f_price = (
        (s.close is not None) and (s.ma60 is not None) and (s.ma240 is not None)
        and (s.close > s.ma60) and (s.close > s.ma240)
    )

    # 量能：當日金額 >= 1.5 × 20 日均額
    if s.avg_turnover_20 is not None and s.close is not None:
        today_turnover = s.close * s.volume
        f_volume = today_turnover >= 1.5 * s.avg_turnover_20
    else:
        f_volume = False

    # 趨勢溢價：收盤 ≥ 年線 × 1.02
    f_trend = (s.close is not None) and (s.ma240 is not None) and (s.close >= s.ma240 * 1.02)

    # 族群同步：產業 5 日上漲家數占比 ≥0.6 或 產業指數 > 60MA
    if (s.industry_up_ratio_5d is not None) or (s.industry_index_price is not None and s.industry_index_ma60 is not None):
        cond_a = (s.industry_up_ratio_5d is not None) and (s.industry_up_ratio_5d >= 0.6)
        cond_b = (
            s.industry_index_price is not None and s.industry_index_ma60 is not None
            and s.industry_index_price > s.industry_index_ma60
        )
        f_group = cond_a or cond_b
    else:
        # 資料不足時 demo 預設放行（實務上你可改成 False）
        f_group = True

    cond_list = [f_price, f_volume, f_trend, f_group]
    count = sum(1 for c in cond_list if c)
    is_firm = (count == 4)

    return FirmResult(
        f_price=f_price,
        f_volume=f_volume,
        f_trend=f_trend,
        f_group=f_group,
        count=count,
        is_firm=is_firm,
    )


def calculate_score(s: StockSnapshot, firm: FirmResult) -> ScoreResult:
    """§8 評分（最大 100 分）"""
    growth = 0
    quality = 0
    momentum = 0
    valuation = 0

    # Growth 30
    if not s.is_financial:
        rev_avg = 0.0
        cnt = 0
        for x in (s.revenue_yoy_m1, s.revenue_yoy_m2, s.revenue_yoy_m3):
            if x is not None:
                rev_avg += x
                cnt += 1
        rev_avg = rev_avg / cnt if cnt > 0 else 0.0

        g1 = max(0.0, min(0.3, rev_avg)) / 0.3 * 15.0
        eps_g = s.eps_growth_4q or 0.0
        g2 = max(0.0, min(0.3, eps_g)) / 0.3 * 15.0
        growth = int(round(g1 + g2))
    else:
        eps_g = s.eps_growth_4q or 0.0
        ni_g = s.net_income_growth_3m or 0.0
        g1 = max(0.0, min(0.3, eps_g)) / 0.3 * 15.0
        g2 = max(0.0, min(0.3, ni_g)) / 0.3 * 15.0
        growth = int(round(g1 + g2))

    # Quality 30
    if not s.is_financial:
        roe = s.roe_ttm or 0.0
        opm = s.opm_ttm or 0.0
        q1 = max(0.0, min(0.3, roe)) / 0.3 * 15.0
        q2 = max(0.0, min(0.3, opm)) / 0.3 * 15.0
        quality = int(round(q1 + q2))
    else:
        roe = s.roe_ttm or 0.0
        q1 = max(0.0, min(0.3, roe)) / 0.3 * 15.0
        npl = s.npl_ratio if s.npl_ratio is not None else 0.02
        coverage = s.coverage_ratio if s.coverage_ratio is not None else 0.5
        raw_q2 = max(0.0, 1.5 - npl * 10.0 + (coverage - 1.0))
        q2 = max(0.0, min(2.0, raw_q2)) / 2.0 * 15.0
        quality = int(round(q1 + q2))

    # Momentum 25
    m = 0
    if s.close > s.ma60:
        m += 5
    if s.close > s.ma240:
        m += 5
    if s.ma20 > s.ma60:
        m += 5
    if s.avg_turnover_20 is not None:
        today_turnover = s.close * s.volume
        if today_turnover > s.avg_turnover_20:
            m += 5
    if firm.f_group:
        m += 5
    momentum = m

    # Valuation 15（暫給 10 分，未實作完整估值）
    valuation = 10

    total = growth + quality + momentum + valuation
    total = int(max(0, min(100, total)))
    return ScoreResult(total=total, growth=growth, quality=quality, momentum=momentum, valuation=valuation)


def check_e_candidate(s: StockSnapshot) -> bool:
    """§4 E 層候選條件"""
    if s.rs60 < 75:
        return False
    if s.inst_net_buy_20 is None or s.inst_net_buy_20 < 0:
        return False
    if s.industry_rank_by_size is None or s.industry_rank_by_size > 3:
        return False
    if s.last_quarter_growth is None or s.last_quarter_growth < 0.10:
        return False
    return True


def classify_stock(
    s: StockSnapshot,
    universe: UniverseResult,
    firm: FirmResult,
    score: ScoreResult
) -> ClassificationResult:
    """§6 分層邏輯：A / B / C / D / X"""
    extra: Dict[str, Any] = {}

    if not universe.passed:
        return ClassificationResult(
            symbol=s.symbol,
            name=s.name,
            layer=Layer.ELIMINATED,
            is_e_candidate=False,
            universe=universe,
            firm=firm,
            score=score,
            extra_info={"reason": "Universe not passed"},
        )

    # A 層
    if firm.is_firm and score.total >= 70:
        layer = Layer.A
        extra["reason"] = "Firm (4/4) and score>=70"
    else:
        # B 層
        if firm.count == 3 or (60 <= score.total <= 69):
            layer = Layer.B
            extra["reason"] = "Firm missing 1 or score 60-69"
        else:
            # C 層
            rev_avg = 0.0
            cnt = 0
            for x in (s.revenue_yoy_m1, s.revenue_yoy_m2, s.revenue_yoy_m3):
                if x is not None:
                    rev_avg += x
                    cnt += 1
            rev_avg = rev_avg / cnt if cnt > 0 else 0.0
            growth_relaxed = (rev_avg >= 0.0) or (s.revenue_yoy_m1 is not None and s.revenue_yoy_m1 >= 0.05)

            if (
                firm.count >= 3
                and firm.f_price
                and (s.roe_ttm is not None and s.roe_ttm > 0.10)
                and (s.opm_ttm is not None and s.opm_ttm >= 0.03)
                and growth_relaxed
            ):
                layer = Layer.C
                extra["reason"] = "Three-of-four Firm + relaxed growth"
            else:
                # D 層
                cond_bottom = 0
                if s.roe_ttm is not None and s.roe_ttm >= 0.08:
                    cond_bottom += 1
                if s.opm_ttm is not None and s.opm_ttm >= 0.02:
                    cond_bottom += 1
                if rev_avg >= -0.03:
                    cond_bottom += 1

                if firm.count >= 2 and cond_bottom >= 2:
                    layer = Layer.D
                    extra["reason"] = "Momentum>=2 and bottom-line>=2"
                else:
                    layer = Layer.ELIMINATED
                    extra["reason"] = "Does not match any layer A/B/C/D"

    is_e = check_e_candidate(s)

    return ClassificationResult(
        symbol=s.symbol,
        name=s.name,
        layer=layer,
        is_e_candidate=is_e,
        universe=universe,
        firm=firm,
        score=score,
        extra_info=extra,
    )


# ============================================================
# 2. 成交量掃描爬蟲 (TWSE + TPEX + Yahoo 備援)
# ============================================================

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
}
TWSE_REF = {"Referer": "https://www.twse.com.tw/zh/trading/historical/mi-index.html"}
TPEX_REF = {"Referer": "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st41.php"}


def _fmt_int(x):
    try:
        s = str(x).replace(",", "").replace("+", "").strip()
        if s in ("", "-"):
            return None
        return int(float(s))
    except Exception:
        return None


def _taipei_anchor_date() -> dt.date:
    """決定抓取資料的基準日 (下午3點前抓昨天)"""
    now = dt.datetime.now()
    if ZoneInfo:
        try:
            now = dt.datetime.now(ZoneInfo("Asia/Taipei"))
        except Exception:
            pass
    d = now.date()
    if now.hour < 15:
        d = d - dt.timedelta(days=1)
    return d


def fetch_twse_json(yyyymmdd: str):
    urls = [
        (
            "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX",
            {"date": yyyymmdd, "type": "ALLBUT0999", "response": "json"},
        ),
        (
            "https://www.twse.com.tw/exchangeReport/MI_INDEX",
            {"date": yyyymmdd, "type": "ALLBUT0999", "response": "json"},
        ),
    ]
    s = requests.Session()
    s.headers.update(UA)
    s.headers.update(TWSE_REF)

    for url, params in urls:
        try:
            r = s.get(url, params=params, timeout=10)
            if r.status_code == 200:
                j = r.json()
                if isinstance(j, dict) and (j.get("stat") == "OK" or "tables" in j):
                    return j
        except Exception:
            continue
    return None


def parse_twse_top_by_volume(j: dict) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if isinstance(j, dict):
        tables = j.get("tables", [])
        target_table = None
        for t in tables:
            fields = t.get("fields", [])
            if "證券代號" in fields and "成交股數" in fields:
                target_table = t
                break

        if target_table:
            fields = target_table["fields"]
            data = target_table["data"]
            id_i = fields.index("證券代號")
            name_i = fields.index("證券名稱")
            vol_i = fields.index("成交股數")

            for row in data:
                sid = str(row[id_i]).strip()
                if len(sid) != 4:
                    continue
                vol = _fmt_int(row[vol_i])
                if vol is None:
                    continue

                rows.append(
                    {
                        "symbol": f"{sid}.TW",
                        "name": row[name_i],
                        "volume": vol,
                        "market": "上市",
                    }
                )
    return rows


def fetch_tpex_json(roc_date: str):
    urls = [
        (
            "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php",
            {"l": "zh-tw", "d": roc_date, "s": "0,asc,0"},
        ),
        (
            "https://www.tpex.org.tw/www/stock/aftertrading/daily_close_quotes/stk_quote_result.php",
            {"l": "zh-tw", "d": roc_date, "s": "0,asc,0"},
        ),
    ]
    s = requests.Session()
    s.headers.update(UA)
    s.headers.update(TPEX_REF)

    for url, params in urls:
        try:
            r = s.get(url, params=params, timeout=10)
            if r.status_code == 200:
                j = r.json()
                if isinstance(j, dict) and j.get("aaData"):
                    return j
        except Exception:
            continue
    return None


def parse_tpex_top_by_volume(j: dict) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(j, dict):
        return rows
    data = j.get("aaData", [])

    for row in data:
        try:
            sid = str(row[0]).strip()
            if len(sid) != 4:
                continue
            vol = _fmt_int(row[8])
            if vol is None:
                continue

            rows.append(
                {
                    "symbol": f"{sid}.TWO",
                    "name": row[1],
                    "volume": vol,
                    "market": "上櫃",
                }
            )
        except Exception:
            continue
    return rows


def yahoo_fallback(topn: int) -> List[Dict[str, Any]]:
    """Yahoo Finance 備援爬蟲"""
    try:
        url = f"https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?count={topn*2}&scrIds=most_actives_tw"
        r = requests.get(url, headers=UA, timeout=10)
        j = r.json()
        quotes = j["finance"]["result"][0]["quotes"]
        rows: List[Dict[str, Any]] = []
        for q in quotes:
            sym = q.get("symbol", "")
            if not (sym.endswith(".TW") or sym.endswith(".TWO")):
                continue
            sid = sym.split(".")[0]
            if len(sid) != 4:
                continue
            rows.append(
                {
                    "symbol": sym,
                    "name": q.get("shortName", sid),
                    "volume": q.get("regularMarketVolume", 0),
                    "market": "Yahoo熱門",
                }
            )
        return rows
    except Exception:
        return []


@st.cache_data(ttl=1800)
def get_market_scan_list(limit: int):
    """
    整合上市櫃抓取邏輯，回傳成交量排行清單。
    回傳: (List[dict], date_str)
    """
    d = _taipei_anchor_date()

    # 回溯 5 天找資料
    for _ in range(5):
        while d.weekday() >= 5:  # 跳過週末
            d = d - dt.timedelta(days=1)

        date_str = d.strftime("%Y-%m-%d")
        roc_date = f"{d.year-1911}/{d.month:02d}/{d.day:02d}"

        # 1. 抓上市
        j_tw = fetch_twse_json(d.strftime("%Y%m%d"))
        rows_tw = parse_twse_top_by_volume(j_tw) if j_tw else []

        # 2. 抓上櫃
        j_tp = fetch_tpex_json(roc_date)
        rows_tp = parse_tpex_top_by_volume(j_tp) if j_tp else []

        if rows_tw or rows_tp:
            all_data = rows_tw + rows_tp
            # 依成交量排序
            all_data.sort(key=lambda x: x["volume"], reverse=True)
            return all_data[: limit * 2], date_str

        d = d - dt.timedelta(days=1)

    return yahoo_fallback(limit), "Yahoo即時(備援)"


# ============================================================
# 3. yfinance → StockSnapshot 轉換
# ============================================================

def build_snapshot_from_yfinance(
    symbol: str, name: str, info: Dict[str, Any], history: pd.DataFrame
) -> Optional[StockSnapshot]:
    """
    將 yfinance 的 info + history 轉成 StockSnapshot。
    注意：部分指標 (月營收 YoY、RS60、法人等) 以近似值或 None 處理，
    主要用於雲端 demo，不等於完整 v7.9.8 正式數據管線。
    """
    if history is None or history.empty or len(history) < 240:
        return None

    hist = history.dropna(subset=["Close", "Volume"])
    if hist.empty or len(hist) < 240:
        return None

    last = hist.iloc[-1]
    close = float(last["Close"])
    volume = float(last["Volume"])

    ma20 = float(hist["Close"].rolling(20).mean().iloc[-1])
    ma60 = float(hist["Close"].rolling(60).mean().iloc[-1])
    ma240 = float(hist["Close"].rolling(240).mean().iloc[-1])

    # 近 20 日平均成交金額
    turnover_20 = float((hist["Close"] * hist["Volume"]).tail(20).mean())

    # 近 20 日換手率 (用 volume / sharesOutstanding 近似)
    shares_out = info.get("sharesOutstanding") or None
    if isinstance(shares_out, (int, float)) and shares_out > 0:
        avg_vol20 = float(hist["Volume"].tail(20).mean())
        turnover_ratio_20 = avg_vol20 / shares_out
    else:
        turnover_ratio_20 = None

    market_cap = float(info.get("marketCap") or 0.0)

    roe_ttm = info.get("returnOnEquity")
    opm_ttm = info.get("operatingMargins")

    # 負債比
    debt_ratio = None
    total_debt = info.get("totalDebt")
    total_assets = info.get("totalAssets")
    total_equity = info.get("totalStockholderEquity")
    debt_to_equity = info.get("debtToEquity")

    if isinstance(total_debt, (int, float)) and isinstance(total_assets, (int, float)) and total_assets > 0:
        debt_ratio = float(total_debt) / float(total_assets)
    elif isinstance(total_debt, (int, float)) and isinstance(total_equity, (int, float)) and (total_debt + total_equity) > 0:
        debt_ratio = float(total_debt) / float(total_debt + total_equity)
    elif isinstance(debt_to_equity, (int, float)):
        # 將 D/E% 轉為 D/(D+E) 的近似
        de = float(debt_to_equity) / 100.0
        debt_ratio = de / (1.0 + de)

    # 年度營收成長當作 3 個月 YoY 的近似
    rev_growth = info.get("revenueGrowth")
    if isinstance(rev_growth, (int, float)):
        revenue_yoy_m1 = revenue_yoy_m2 = revenue_yoy_m3 = float(rev_growth)
    else:
        revenue_yoy_m1 = revenue_yoy_m2 = revenue_yoy_m3 = None

    eps_growth = info.get("earningsGrowth")
    ni_growth = info.get("earningsQuarterlyGrowth")
    eps_growth_4q = float(eps_growth) if isinstance(eps_growth, (int, float)) else 0.0
    net_income_growth_3m = float(ni_growth) if isinstance(ni_growth, (int, float)) else 0.0

    # 產業資訊
    industry = info.get("industry") or info.get("sector") or ""
    industry_lower = industry.lower() if isinstance(industry, str) else ""

    # 金融股判斷 (簡易)
    is_financial = any(
        key in industry_lower
        for key in ["bank", "insurance", "financial", "證券", "投信", "投顧", "銀行", "保險"]
    )
    # 循環股判斷 (簡易)
    is_cyclical = any(
        key in industry_lower
        for key in ["steel", "metal", "shipping", "ship", "plastic", "petrochemical", "panel", "display", "dram", "memory", "鋼", "航運", "塑膠", "面板"]
    )

    snapshot = StockSnapshot(
        symbol=symbol,
        name=name,
        is_financial=is_financial,
        is_cyclical=is_cyclical,
        close=close,
        volume=volume,
        ma20=ma20,
        ma60=ma60,
        ma240=ma240,
        avg_turnover_20=turnover_20,
        turnover_ratio_20=turnover_ratio_20,
        market_cap=market_cap,
        roe_ttm=float(roe_ttm) if isinstance(roe_ttm, (int, float)) else 0.0,
        opm_ttm=float(opm_ttm) if isinstance(opm_ttm, (int, float)) else 0.0,
        debt_ratio=float(debt_ratio) if isinstance(debt_ratio, (int, float)) else 0.0,
        revenue_yoy_m1=revenue_yoy_m1,
        revenue_yoy_m2=revenue_yoy_m2,
        revenue_yoy_m3=revenue_yoy_m3,
        eps_growth_4q=eps_growth_4q,
        net_income_growth_3m=net_income_growth_3m,
        npl_ratio=None,
        coverage_ratio=None,
        rs60=50.0,  # demo 先給中性值
        industry=industry,
        industry_index_price=None,
        industry_index_ma60=None,
        industry_up_ratio_5d=None,
        inst_net_buy_20=None,
        industry_rank_by_size=None,
        last_quarter_growth=None,
    )
    return snapshot


# ============================================================
# 4. Streamlit UI
# ============================================================

st.set_page_config(page_title="v7.9.8 選股雷達", page_icon="🎯", layout="wide")

st.title("🎯 v7.9.8 投資規則 - 嚴格篩選雷達（單檔整合版）")
st.markdown(
    """
**核心精神：** 整合 Pmoney 成交量掃描與 v7.9.8 規則核心引擎，對熱門標的進行 Universe / Firm / 分層檢查。  
- **§3.1 / §3.1-F 基本面：** ROE > 10%、OPM ≥ 5%、營收成長、負債比、(金融股: NPL / Coverage / EPS/淨利成長)  
- **§3.5 流動性：** 20 日均額 ≥ 5,000 萬、20 日換手率 ≥ 0.3%  
- **§3.2 Firm：** 站上季線與年線、量能放大、趨勢溢價、族群同步  
- **§6 分層：** A / B / C / D 分級＋E 層候選旗標  
"""
)

st.sidebar.header("⚙️ 參數設定")
scan_limit = st.sidebar.slider("掃描成交量前 N 大", 30, 200, 100, 10)
max_price = st.sidebar.number_input("股價上限 (§3.1)", value=80.0, step=5.0)
min_vol = st.sidebar.number_input("當日成交量下限 (張)", value=1000)

st.sidebar.divider()
st.sidebar.info("💡 全揭露模式：所有掃描過的股票都會列出，並顯示 Universe / Firm / 分層原因，方便檢視『死在哪一關』。")


if st.button("🚀 啟動雷達 (v7.9.8)", type="primary"):
    # 1. 取得成交量排行清單
    with st.spinner("Pmoney 引擎正在抓取成交量排行..."):
        target_list, data_date = get_market_scan_list(scan_limit)

    if not target_list:
        st.error("無法取得市場資料，請稍後再試。")
        st.stop()

    st.success(f"已取得 {len(target_list)} 檔熱門股 (資料日期: {data_date})，開始逐檔健檢...")

    results: List[Dict[str, Any]] = []
    progress_bar = st.progress(0)
    status_text = st.empty()

    scan_targets = target_list[:scan_limit]

    for i, meta in enumerate(scan_targets):
        symbol = meta["symbol"]
        name = meta["name"]

        progress = (i + 1) / len(scan_targets)
        progress_bar.progress(progress)
        status_text.text(f"正在分析 [{i+1}/{len(scan_targets)}]: {name} ({symbol}) ...")

        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="2y")
            if hist is None or hist.empty:
                continue

            last = hist.iloc[-1]
            vol_lots_today = float(last["Volume"]) / 1000.0

            # 粗略當日量濾網（Universe 裡仍有正式的 20 日均額＋換手率）
            volume_filter_ok = vol_lots_today >= float(min_vol)

            info = ticker.info or {}
            snapshot = build_snapshot_from_yfinance(symbol, name, info, hist)
            if snapshot is None:
                continue

            universe = check_universe(snapshot, price_cap=max_price)
            firm = check_firm(snapshot)
            score = calculate_score(snapshot, firm)
            cls = classify_stock(snapshot, universe, firm)

            grade = cls.layer.value
            price = round(snapshot.close, 2)

            basic_ok = universe.checks.get("roe", False) and universe.checks.get("opm", False)
            tech_ok = firm.f_price and firm.f_volume and firm.f_trend
            price_ok = universe.checks.get("price", False)

            roe_percent = f"{snapshot.roe_ttm * 100:.1f}%" if snapshot.roe_ttm is not None else "-"
            opm_percent = f"{snapshot.opm_ttm * 100:.1f}%" if snapshot.opm_ttm is not None else "-"

            results.append(
                {
                    "代號": symbol,
                    "名稱": name,
                    "評級": grade,
                    "收盤價": price,
                    "成交量": int(vol_lots_today),
                    "基本面": "✅" if basic_ok else "❌",
                    "技術面": "✅" if tech_ok else "❌",
                    "價格符合": "✅" if price_ok else "❌",
                    "ROE": roe_percent,
                    "OPM": opm_percent,
                    "Score": score.total,
                    "Univers
