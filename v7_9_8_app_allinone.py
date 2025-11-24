from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any, List

import pandas as pd
import requests
import yfinance as yf
import streamlit as st

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None


# =========================
# 一、v7.9.8 規則核心資料結構
# =========================

class Layer(str, Enum):
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    X = "X"  # 淘汰 / 資料不足


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
    count: int
    is_firm: bool


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
    universe: UniverseResult
    firm: FirmResult
    score: ScoreResult
    is_e_candidate: bool
    extra_reason: str


@dataclass
class StockSnapshot:
    symbol: str
    name: str
    close: float
    volume: float
    ma20: float
    ma60: float
    ma240: Optional[float]
    market_cap: float
    roe: float
    opm: float
    rev_growth: float
    debt_ratio: float
    is_financial: bool


MIN_MKT_CAP = 1_000_000_000  # §3.1 市值下限


# =========================
# 二、Universe / Firm / Score
# =========================

def check_universe(s: StockSnapshot, price_cap: float) -> UniverseResult:
    """§3.1 / §3.1-F Universe 篩選"""
    checks: Dict[str, bool] = {}

    # 價格上限：非金融才管；金融股不限價（跟你原始條文一致）
    if s.is_financial:
        checks["price"] = True
    else:
        checks["price"] = s.close <= price_cap

    # 市值
    checks["mkt_cap"] = s.market_cap >= MIN_MKT_CAP
    # ROE
    checks["roe"] = s.roe > 0.10
    # OPM
    checks["opm"] = s.opm >= 0.05
    # 營收成長
    checks["rev"] = s.rev_growth >= 0.05
    # 負債比
    checks["debt"] = s.debt_ratio < 0.60 if s.debt_ratio >= 0 else True

    passed = all(checks.values())
    reason = "OK" if passed else "Universe not passed"
    return UniverseResult(passed=passed, reason=reason, checks=checks)


def check_firm(s: StockSnapshot) -> FirmResult:
    """§3.2 Firm 動能就緒（簡化版）"""

    # 價格：站上季線＋年線；如果資料不足年線，只看季線
    if s.ma240 is not None and s.ma240 > 0:
        f_price = (s.close > s.ma60) and (s.close > s.ma240)
    else:
        f_price = s.close > s.ma60

    # 量能條件：這裡因為前面已經是成交量前 N 大，所以不再硬性設 1.5 倍爆量
    f_volume = True

    # 趨勢溢價：> 年線 * 1.02，有年線才算
    if s.ma240 is not None and s.ma240 > 0:
        f_trend = s.close >= 1.02 * s.ma240
    else:
        f_trend = False

    # 族群同步：目前資料源沒有產業指數，先一律視為通過
    f_group = True

    conds = [f_price, f_volume, f_trend, f_group]
    count = sum(1 for c in conds if c)
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
    """§8 簡化版評分：100 分制"""

    # 成長 (0–30)：用營收成長近似
    g_src = max(0.0, min(0.30, s.rev_growth))
    growth = int(round(g_src / 0.30 * 30))

    # 品質 (0–30)：ROE + OPM
    q1 = max(0.0, min(0.30, s.roe)) / 0.30 * 15
    q2 = max(0.0, min(0.30, s.opm)) / 0.30 * 15
    quality = int(round(q1 + q2))

    # 動能 (0–25)
    momentum = 0
    if s.close > s.ma60:
        momentum += 5
    if s.ma240 is not None and s.close > s.ma240:
        momentum += 5
    if firm.f_volume:
        momentum += 5
    if firm.f_trend:
        momentum += 5
    if firm.f_group:
        momentum += 5

    # 估值 (固定 10 分，先不在這版裡細拆)
    valuation = 10

    total = max(0, min(100, growth + quality + momentum + valuation))
    return ScoreResult(
        total=int(total),
        growth=growth,
        quality=quality,
        momentum=momentum,
        valuation=valuation,
    )


def classify_stock(
    s: StockSnapshot,
    uni: UniverseResult,
    firm: FirmResult,
    score: ScoreResult,
) -> ClassificationResult:
    """§6 分層：A/B/C/D/X"""

    if not uni.passed:
        return ClassificationResult(
            symbol=s.symbol,
            name=s.name,
            layer=Layer.X,
            universe=uni,
            firm=firm,
            score=score,
            is_e_candidate=False,
            extra_reason="Universe not passed",
        )

    if firm.is_firm and score.total >= 70:
        layer = Layer.A
        reason = "Firm(4/4) + Score>=70"
    elif firm.count == 3 or (60 <= score.total <= 69):
        layer = Layer.B
        reason = "Firm缺一或Score在60–69"
    elif firm.f_price and uni.checks.get("roe", False) and (s.opm >= 0.03):
        layer = Layer.C
        reason = "站上均線 + 基本面佳"
    else:
        layer = Layer.D
        reason = "Universe 通過但動能較弱"

    # E 候選：這版簡化為「高分標的」
    is_e = (score.total >= 75)

    return ClassificationResult(
        symbol=s.symbol,
        name=s.name,
        layer=layer,
        universe=uni,
        firm=firm,
        score=score,
        is_e_candidate=is_e,
        extra_reason=reason,
    )


# =========================
# 三、抓 TWSE + TPEx 成交量排行
# =========================

UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}
TWSE_REF = {
    "Referer": "https://www.twse.com.tw/zh/trading/historical/mi-index.html"
}
TPEX_REF = {
    "Referer": "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php"
}


def _fmt_int(x):
    try:
        s = str(x).replace(",", "").replace("+", "").strip()
        if s in ("", "-"):
            return None
        return int(float(s))
    except Exception:
        return None


def _smart_trade_date() -> dt.date:
    """決定抓哪一天的盤後資料：盤前抓前一天、週一早上抓上週五"""
    now = dt.datetime.now()
    if ZoneInfo:
        try:
            now = dt.datetime.now(ZoneInfo("Asia/Taipei"))
        except Exception:
            pass

    d = now.date()
    wd = d.weekday()

    if wd == 5:      # Sat -> Fri
        d -= dt.timedelta(days=1)
    elif wd == 6:    # Sun -> Fri
        d -= dt.timedelta(days=2)
    elif wd == 0 and now.hour < 15:  # Mon 盤前 -> Fri
        d -= dt.timedelta(days=3)
    elif now.hour < 15 and wd <= 4:  # 平日盤前 -> 前一日
        d -= dt.timedelta(days=1)

    return d


def fetch_twse_json(yyyymmdd: str):
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
    params = {"response": "json", "date": yyyymmdd, "type": "ALLBUT0999"}
    s = requests.Session()
    s.headers.update(UA)
    s.headers.update(TWSE_REF)
    try:
        r = s.get(url, params=params, timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception:
        return None
    return None


def parse_twse_top_by_volume(j: dict) -> List[Dict[str, Any]]:
    """解析上市 JSON，回傳 [{symbol, name, volume, market}]"""
    rows: List[Dict[str, Any]] = []
    if not isinstance(j, dict):
        return rows

    # 新版格式：tables[]
    tables = j.get("tables")
    if isinstance(tables, list):
        for t in tables:
            fields = t.get("fields", [])
            data = t.get("data", [])
            if "證券代號" in fields and "成交股數" in fields:
                id_i = fields.index("證券代號")
                name_i = fields.index("證券名稱")
                vol_i = fields.index("成交股數")
                for row in data:
                    if not isinstance(row, list):
                        continue
                    sid = str(row[id_i]).strip()
                    # 這裡改成「4–6 碼純數字」，跟你 VBA 一樣
                    if (not sid.isdigit()) or not (4 <= len(sid) <= 6):
                        continue
                    vol = _fmt_int(row[vol_i])
                    if vol is None or vol <= 0:
                        continue
                    rows.append(
                        {
                            "symbol": f"{sid}.TW",
                            "name": row[name_i],
                            "volume": vol,
                            "market": "上市",
                        }
                    )
        if rows:
            return rows

    # 舊版 fallback：data9 / fields9 ...
    for key, value in j.items():
        if not (isinstance(key, str) and key.startswith("data")):
            continue
        if not isinstance(value, list):
            continue
        idx = key[4:]
        fields = j.get(f"fields{idx}", [])
        if "證券代號" not in fields or "成交股數" not in fields:
            continue
        id_i = fields.index("證券代號")
        name_i = fields.index("證券名稱")
        vol_i = fields.index("成交股數")
        for row in value:
            if not isinstance(row, list):
                continue
            if len(row) <= max(id_i, name_i, vol_i):
                continue
            sid = str(row[id_i]).strip()
            if (not sid.isdigit()) or not (4 <= len(sid) <= 6):
                continue
            vol = _fmt_int(row[vol_i])
            if vol is None or vol <= 0:
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
    """抓上櫃 JSON，參數完全比照你 Excel VBA 寫法"""
    urls = [
        (
            "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php",
            {"l": "zh-tw", "o": "json", "d": roc_date},
        ),
        (
            "https://www.tpex.org.tw/www/stock/aftertrading/daily_close_quotes/stk_quote_result.php",
            {"l": "zh-tw", "o": "json", "d": roc_date},
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
                if isinstance(j, dict):
                    return j
        except Exception:
            continue
    return None


def parse_tpex_top_by_volume(j: dict) -> List[Dict[str, Any]]:
    """解析上櫃 JSON（aaData 格式）"""
    rows: List[Dict[str, Any]] = []
    if not isinstance(j, dict):
        return rows
    data = j.get("aaData", [])
    for row in data:
        try:
            sid = str(row[0]).strip()
            # 一樣改成 4–6 碼純數字
            if (not sid.isdigit()) or not (4 <= len(sid) <= 6):
                continue
            vol = _fmt_int(row[8])
            if vol is None or vol <= 0:
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
    """全部官方來源都掛掉時的 Yahoo 備援"""
    try:
        url = (
            "https://query1.finance.yahoo.com/v1/finance/screener/predefined/"
            f"saved?count={topn*2}&scrIds=most_actives_tw"
        )
        r = requests.get(url, headers=UA, timeout=10)
        j = r.json()
        quotes = j["finance"]["result"][0]["quotes"]
        rows: List[Dict[str, Any]] = []
        for q in quotes:
            sym = q.get("symbol", "")
            if not (sym.endswith(".TW") or sym.endswith(".TWO")):
                continue
            sid = sym.split(".")[0]
            if not (sid.isdigit() and 4 <= len(sid) <= 6):
                continue
            rows.append(
                {
                    "symbol": sym,
                    "name": q.get("shortName", sid),
                    "volume": int(q.get("regularMarketVolume", 0)),
                    "market": "Yahoo熱門",
                }
            )
        return rows
    except Exception:
        return []


@st.cache_data(ttl=1800)
def get_market_scan_list(limit: int):
    """整合上市＋上櫃成交量排行；抓不到才用 Yahoo 備援"""
    d = _smart_trade_date()
    for _ in range(5):
        yyyymmdd = d.strftime("%Y%m%d")
        roc_date = f"{d.year - 1911}/{d.month:02d}/{d.day:02d}"

        rows_tw: List[Dict[str, Any]] = []
        rows_tp: List[Dict[str, Any]] = []

        j_tw = fetch_twse_json(yyyymmdd)
        if j_tw:
            rows_tw = parse_twse_top_by_volume(j_tw)

        j_tp = fetch_tpex_json(roc_date)
        if j_tp:
            rows_tp = parse_tpex_top_by_volume(j_tp)

        if rows_tw or rows_tp:
            all_data = rows_tw + rows_tp
            all_data.sort(key=lambda x: x["volume"], reverse=True)
            return all_data[: limit * 2], d.strftime("%Y-%m-%d")

        # 若當天沒資料就往前一天回溯，跳過週末
        d -= dt.timedelta(days=1)
        while d.weekday() >= 5:
            d -= dt.timedelta(days=1)

    fb = yahoo_fallback(limit)
    if fb:
        return fb, "Yahoo即時(備援)"
    return [], d.strftime("%Y-%m-%d")


# =========================
# 四、yfinance → StockSnapshot
# =========================

def build_snapshot(
    symbol: str,
    name: str,
    info: Dict[str, Any],
    history: pd.DataFrame,
) -> Optional[StockSnapshot]:
    """把 yfinance 的 info + history 整理成一筆 Snapshot"""
    if history is None or history.empty:
        return None

    hist = history.dropna(subset=["Close", "Volume"])
    if hist.empty:
        return None

    # 這裡延續你之前的設定：至少 60 根 K 才算「有歷史」
    if len(hist) < 60:
        return None

    last = hist.iloc[-1]
    close = float(last["Close"])
    volume = float(last["Volume"])

    ma20 = float(hist["Close"].rolling(20).mean().iloc[-1])
    ma60 = float(hist["Close"].rolling(60).mean().iloc[-1])
    if len(hist) >= 240:
        ma240 = float(hist["Close"].rolling(240).mean().iloc[-1])
    else:
        ma240 = None

    market_cap = float(info.get("marketCap") or 0.0)
    roe = float(info.get("returnOnEquity") or 0.0)
    opm = float(info.get("operatingMargins") or 0.0)
    rev_growth = float(info.get("revenueGrowth") or 0.0)

    # 用 D/E 推算負債比
    de_ratio = info.get("debtToEquity")
    if isinstance(de_ratio, (int, float)):
        de = float(de_ratio) / 100.0
        debt_ratio = de / (1.0 + de)
    else:
        debt_ratio = 0.0

    industry = (info.get("industry") or info.get("sector") or "").lower()
    is_fin = any(k in industry for k in ["bank", "insurance", "financial", "證券", "銀行", "保險"])

    return StockSnapshot(
        symbol=symbol,
        name=name,
        close=close,
        volume=volume,
        ma20=ma20,
        ma60=ma60,
        ma240=ma240,
        market_cap=market_cap,
        roe=roe,
        opm=opm,
        rev_growth=rev_growth,
        debt_ratio=debt_ratio,
        is_financial=is_fin,
    )


# =========================
# 五、Streamlit 介面
# =========================

st.set_page_config(
    page_title="v7.9.8 選股雷達",
    page_icon="🎯",
    layout="wide",
)

st.title("🎯 v7.9.8 投資規則 - 嚴格篩選雷達（單檔整合版）")
st.markdown(
    """
    **核心精神：** 以 v7.9.8 規則為主軸，整合成交量掃描與 Universe / Firm / 分層邏輯，對熱門標的進行初篩。  
    - §3.1 / §3.1-F 基本面：ROE>10%、OPM≥5%、營收成長、負債比  
    - §3.5 流動性：由「成交量排行前 N 名」保證，程式不再額外踢掉低量  
    - §3.2 Firm：站上季線與年線、趨勢溢價、族群同步（簡化版）  
    """
)

st.sidebar.header("⚙️ 參數設定")
scan_limit = st.sidebar.slider("掃描成交量前 N 大", 30, 200, 100, 10)
max_price = st.sidebar.number_input("股價上限 (§3.1 非金融)", value=80.0, step=5.0)

st.sidebar.markdown("---")
st.sidebar.info(
    "💡 全揭露模式：所有掃描過的股票都會列出，並顯示 Universe / Firm / 分層原因，方便檢視死在哪一關。"
)

if st.button("🚀 啟動雷達 (v7.9.8)", type="primary"):
    # 1. 抓成交量排行
    with st.spinner("正在抓取上市＋上櫃成交量排行..."):
        target_list, data_date = get_market_scan_list(scan_limit)

    if not target_list:
        st.error("無法取得市場資料（TWSE/TPEx/Yahoo 皆連線失敗），請稍後再試。")
        st.stop()

    st.success(f"資料來源：{data_date}｜掃描標的數：{len(target_list)} 檔。開始逐檔健檢...")

    results: List[Dict[str, Any]] = []
    progress_bar = st.progress(0)
    status_text = st.empty()

    empty_hist = 0   # history 完全空白
    short_hist = 0   # <60 根 K
    ok_snap = 0      # 成功算出 Snapshot

    scan_targets = target_list[:scan_limit]

    # 2. 逐檔跑 v7.9.8 規則
    for i, meta in enumerate(scan_targets):
        symbol = meta["symbol"]
        name = meta["name"]

        progress_bar.progress((i + 1) / len(scan_targets))
        status_text.text(f"正在分析 [{i+1}/{len(scan_targets)}]: {name} ({symbol}) ...")

        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="2y")

            if hist is None or hist.empty:
                empty_hist += 1
                results.append(
                    {
                        "代號": symbol,
                        "名稱": name,
                        "評級": "X",
                        "E候選": "",
                        "收盤價": None,
                        "成交量": int(meta.get("volume", 0) / 1000),
                        "基本面": "❌",
                        "技術面": "❌",
                        "價格符合": "❌",
                        "ROE": "-",
                        "OPM": "-",
                        "Score": 0,
                        "LayerReason": "history 空白，無法計算指標",
                    }
                )
                continue

            last = hist.iloc[-1]
            close_today = float(last["Close"])
            vol_lots_today = int(float(last["Volume"]) / 1000)

            info = ticker.info or {}
            snap = build_snapshot(symbol, name, info, hist)

            if snap is None:
                short_hist += 1
                results.append(
                    {
                        "代號": symbol,
                        "名稱": name,
                        "評級": "X",
                        "E候選": "",
                        "收盤價": round(close_today, 2),
                        "成交量": vol_lots_today,
                        "基本面": "❌",
                        "技術面": "❌",
                        "價格符合": "❌",
                        "ROE": "-",
                        "OPM": "-",
                        "Score": 0,
                        "LayerReason": "歷史不足(<60根K)，暫不評分",
                    }
                )
                continue

            ok_snap += 1

            uni = check_universe(snap, price_cap=max_price)
            firm = check_firm(snap)
            score = calculate_score(snap, firm)
            cls = classify_stock(snap, uni, firm, score)

            results.append(
                {
                    "代號": snap.symbol,
                    "名稱": snap.name,
                    "評級": cls.layer.value,
                    "E候選": "⭐" if cls.is_e_candidate else "",
                    "收盤價": round(snap.close, 2),
                    "成交量": int(snap.volume / 1000),
                    "基本面": "✅"
                    if (uni.checks.get("roe") and uni.checks.get("opm"))
                    else "❌",
                    "技術面": "✅"
                    if (firm.f_price and firm.f_trend)
                    else "❌",
                    "價格符合": "✅" if uni.checks.get("price") else "❌",
                    "ROE": f"{snap.roe*100:.1f}%",
                    "OPM": f"{snap.opm*100:.1f}%",
                    "Score": score.total,
                    "LayerReason": cls.extra_reason,
                }
            )
        except Exception:
            results.append(
                {
                    "代號": symbol,
                    "名稱": name,
                    "評級": "X",
                    "E候選": "",
                    "收盤價": None,
                    "成交量": int(meta.get("volume", 0) / 1000),
                    "基本面": "❌",
                    "技術面": "❌",
                    "價格符合": "❌",
                    "ROE": "-",
                    "OPM": "-",
                    "Score": 0,
                    "LayerReason": "抓取 yfinance 資料時發生錯誤",
                }
            )

    progress_bar.empty()
    status_text.empty()

    st.warning(
        f"掃描統計：history 空 {empty_hist} 檔、"
        f"歷史不足(<60根K) {short_hist} 檔、"
        f"成功評分 {ok_snap} 檔"
    )

    # 3. 顯示結果
    if results:
        df = pd.DataFrame(results)
        grade_order = {"A": 0, "B": 1, "C": 2, "D": 3, "X": 4}
        df["grade_sort"] = df["評級"].map(grade_order).fillna(4)
        df = df.sort_values(
            by=["grade_sort", "Score", "成交量"],
            ascending=[True, False, False],
        )

        a_count = int((df["評級"] == "A").sum())
        b_count = int((df["評級"] == "B").sum())

        st.info(f"掃描完成：A 級 {a_count} 檔，B 級 {b_count} 檔。")

        st.dataframe(
            df[
                [
                    "代號",
                    "名稱",
                    "評級",
                    "E候選",
                    "收盤價",
                    "成交量",
                    "基本面",
                    "技術面",
                    "價格符合",
                    "ROE",
                    "OPM",
                    "Score",
                    "LayerReason",
                ]
            ],
            use_container_width=True,
        )

        st.markdown("### 📋 v7.9.8 建議操作（示意）")
        valid_stocks = df[df["評級"].isin(["A", "B"])]
        if not valid_stocks.empty:
            for _, row in valid_stocks.iterrows():
                action = "整張買進" if row["評級"] == "A" else "半單位買進"
                st.success(
                    f"**[{row['評級']}級] {row['名稱']} ({row['代號']})** | "
                    f"收盤 {row['收盤價']} | ROE {row['ROE']} | "
                    f"Score {row['Score']} → 建議：隔日開盤 {action}"
                    "（實際下單仍依主程式規則）。"
                )
        else:
            st.warning("今日無 A/B 級標的。")
    else:
        st.error("掃描結果為空。")
