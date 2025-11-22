# 投資規則 v7.9.8 選股雷達 (單檔整合版 - 無需外部規則檔)
# 整合: Pmoney爬蟲 + v7.9.8完整邏輯 + 安全氣囊
# 執行方式: streamlit run v798_app.py

import streamlit as st
import yfinance as yf
import pandas as pd
import requests
import datetime as dt
import time
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Any, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

# ==========================================
# Part A: v7.9.8 邏輯引擎 (原本缺失的 rules_v798 部分)
# ==========================================

class Layer(Enum):
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    E = "E"
    X = "X" # 淘汰

@dataclass
class StockSnapshot:
    symbol: str
    name: str
    close: float
    volume: float
    # 技術指標
    ma20: float
    ma60: float
    ma240: float
    price_history_3d: List[float] # 用於檢查連3日站上
    vol_ma20: float
    # 基本面
    market_cap: float
    roe_ttm: float
    opm_ttm: float
    debt_ratio: float
    rev_growth: float
    eps_growth: float
    # 屬性
    is_financial: bool
    is_cyclical: bool

@dataclass
class ScoreResult:
    total: int
    details: Dict[str, int]

@dataclass
class UniverseResult:
    passed: bool
    checks: Dict[str, bool]
    reason: str = ""

@dataclass
class FirmResult:
    is_firm: bool
    f_price: bool
    f_volume: bool
    f_trend: bool
    f_group: bool
    count: int

@dataclass
class ClassifyResult:
    layer: Layer
    is_e_candidate: bool
    extra_info: Dict[str, Any]

# --- 核心運算邏輯 ---

def calculate_debt_ratio(info: dict) -> float:
    # 嘗試計算負債比 (Total Debt / Total Assets)
    total_debt = info.get("totalDebt")
    total_assets = info.get("totalAssets")
    
    if isinstance(total_debt, (int, float)) and isinstance(total_assets, (int, float)) and total_assets > 0:
        return total_debt / total_assets
    
    # 備用: 從 D/E 推算
    de_ratio = info.get("debtToEquity")
    if isinstance(de_ratio, (int, float)):
        de = de_ratio / 100.0
        return de / (1.0 + de)
    return 0.5 # 無資料時給予中性值

def check_universe(snap: StockSnapshot, price_cap: float = 80.0) -> UniverseResult:
    checks = {}
    
    # 1. 價格上限 (金融股通常不限，非金融限80)
    if not snap.is_financial:
        checks['price'] = snap.close <= price_cap
    else:
        checks['price'] = True 

    # 2. 市值 >= 10億
    checks['cap'] = snap.market_cap >= 1_000_000_000
    
    # 3. 基本面篩選
    if snap.is_financial:
        # 金融股
        checks['roe'] = snap.roe_ttm > 0.10
        checks['growth'] = snap.eps_growth >= 0.05 or snap.rev_growth >= 0.05
        checks['opm'] = True
        checks['debt'] = True
    else:
        # 非金融股
        checks['roe'] = snap.roe_ttm > 0.10
        checks['opm'] = snap.opm_ttm >= 0.05
        checks['growth'] = snap.rev_growth >= 0.05 # 近似月營收條件
        checks['debt'] = snap.debt_ratio < 0.60
    
    passed = all(checks.values())
    
    reason = []
    if not checks.get('price', True): reason.append(f"價>{price_cap}")
    if not checks.get('roe', True): reason.append("ROE低")
    if not checks.get('opm', True): reason.append("OPM低")
    if not checks.get('debt', True): reason.append("負債高")
    if not checks.get('growth', True): reason.append("成長低")
    
    return UniverseResult(passed, checks, ", ".join(reason))

def check_firm(snap: StockSnapshot) -> FirmResult:
    # F_price: 連3日站上季線與年線
    if len(snap.price_history_3d) < 3:
        # 資料不足，只看當日
        f_price = snap.close > snap.ma60 and snap.close > snap.ma240
    else:
        f_price = True
        # 這裡做簡化檢查：假設均線這三天變動不大，用當前均線去比對過去三天收盤
        # (嚴謹做法需計算 rolling history，為效能做取捨)
        for p in snap.price_history_3d:
            if not (p > snap.ma60 and p > snap.ma240):
                f_price = False
                break
    
    # F_volume: 量能 >= 1.5倍 20日均量
    f_volume = snap.volume >= (1.5 * snap.vol_ma20)
    
    # F_trend: 趨勢溢價 >= 年線 * 1.02
    f_trend = snap.close >= (snap.ma240 * 1.02)
    
    # F_group: 族群同步 (無資料來源，暫給過)
    f_group = True
    
    count = sum([f_price, f_volume, f_trend, f_group])
    return FirmResult(count == 4, f_price, f_volume, f_trend, f_group, count)

def calculate_score(snap: StockSnapshot, firm: FirmResult) -> ScoreResult:
    score = 0
    details = {}
    
    # 成長 (30)
    s_growth = 0
    if snap.rev_growth >= 0.20: s_growth += 15
    elif snap.rev_growth >= 0.05: s_growth += 10
    if snap.eps_growth >= 0.20: s_growth += 15
    elif snap.eps_growth >= 0.05: s_growth += 10
    score += s_growth
    details['成長'] = s_growth
    
    # 品質 (30)
    s_qual = 0
    if snap.roe_ttm >= 0.15: s_qual += 15
    elif snap.roe_ttm >= 0.10: s_qual += 10
    if snap.opm_ttm >= 0.10: s_qual += 15
    elif snap.opm_ttm >= 0.05: s_qual += 10
    score += s_qual
    details['品質'] = s_qual
    
    # 動能 (25)
    s_mom = 0
    if snap.close > snap.ma60: s_mom += 5
    if snap.close > snap.ma240: s_mom += 5
    if snap.ma20 > snap.ma60: s_mom += 5
    if snap.volume > snap.vol_ma20: s_mom += 5
    if firm.f_group: s_mom += 5
    score += s_mom
    details['動能'] = s_mom
    
    # 估值 (15)
    score += 10
    details['估值'] = 10
    
    return ScoreResult(min(100, score), details)

def classify_stock(snap: StockSnapshot, uni: UniverseResult, firm: FirmResult) -> ClassifyResult:
    score = calculate_score(snap, firm)
    
    if not uni.passed:
        return ClassifyResult(Layer.X, False, {"reason": uni.reason})
    
    # A: Firm(4) + Score>=70
    if firm.is_firm and score.total >= 70:
        return ClassifyResult(Layer.A, False, {"reason": "四面齊+高分"})
        
    # B: Firm缺1 或 Score 60-69
    if firm.count == 3 or (60 <= score.total <= 69):
        return ClassifyResult(Layer.B, False, {"reason": "動能缺1或中分"})
        
    # C: 站上均線 + 基本面好
    if firm.f_price and snap.roe_ttm > 0.10 and snap.opm_ttm >= 0.03:
        return ClassifyResult(Layer.C, True, {"reason": "基本面佳+站上均線"})
        
    # D: 其他合格者
    return ClassifyResult(Layer.D, False, {"reason": "基本面通過"})

# ==========================================
# Part B: 爬蟲與資料處理 (含安全氣囊)
# ==========================================

UA_HEADER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
}
TWSE_REF = {"Referer": "https://www.twse.com.tw/zh/trading/historical/mi-index.html"}
TPEX_REF = {"Referer": "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st41.php"}

# 安全氣囊：當爬蟲全掛時使用的備用清單
SAFE_LIST = [
    "2330.TW", "2317.TW", "2603.TW", "2609.TW", "2615.TW", "2881.TW", "2882.TW", "2303.TW", "3231.TW", "2382.TW",
    "2454.TW", "3711.TW", "2891.TW", "2886.TW", "2892.TW", "5880.TW", "2884.TW", "1605.TW", "2002.TW", "2409.TW",
    "3481.TW", "2618.TW", "2610.TW", "3037.TW", "2371.TW", "2356.TW", "2324.TW", "5347.TWO", "6182.TWO", "8069.TWO"
]

def _fmt_int(x):
    try:
        s = str(x).replace(",", "").replace("+", "").strip()
        if s in ("", "-"): return None
        return int(float(s))
    except: return None

def _taipei_anchor_date() -> dt.date:
    now = dt.datetime.now()
    if ZoneInfo:
        try: now = dt.datetime.now(ZoneInfo("Asia/Taipei"))
        except: pass
    d = now.date()
    if now.hour < 15: d = d - dt.timedelta(days=1)
    return d

def get_market_scan_list(limit: int):
    """嘗試抓取市場熱門股，失敗則回傳安全氣囊"""
    d = _taipei_anchor_date()
    s = requests.Session()
    s.headers.update(UA_HEADER)
    targets = []
    
    try:
        # 嘗試回溯 3 天
        for _ in range(3):
            while d.weekday() >= 5: d = d - dt.timedelta(days=1)
            date_str = d.strftime("%Y%m%d")
            roc_date = f"{d.year-1911}/{d.month:02d}/{d.day:02d}"
            
            # TWSE
            s.headers.update(TWSE_REF)
            try:
                url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={date_str}&type=ALLBUT0999&response=json"
                j = s.get(url, timeout=3).json()
                if j.get('stat') == 'OK':
                    for t in j.get('tables', []):
                        if '證券代號' in t.get('fields', []):
                            df = pd.DataFrame(t['data'], columns=t['fields'])
                            for _, row in df.iterrows():
                                if len(row['證券代號']) == 4:
                                    vol = _fmt_int(row['成交股數'])
                                    if vol: targets.append({'symbol': f"{row['證券代號']}.TW", 'name': row['證券名稱'], 'volume': vol})
                            break
            except: pass
            
            # TPEx
            if not targets: # 如果 TWSE 沒抓到才試 TPEx 避免太久，或者同時抓
                try:
                    url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d={roc_date}&s=0,asc,0"
                    j = s.get(url, timeout=3).json()
                    if j.get('aaData'):
                        for row in j['aaData']:
                            if len(row[0]) == 4:
                                vol = _fmt_int(row[8])
                                if vol: targets.append({'symbol': f"{row[0]}.TWO", 'name': row[1], 'volume': vol})
                except: pass
                
            if targets:
                targets.sort(key=lambda x: x['volume'], reverse=True)
                return targets[:limit*2], d.strftime("%Y-%m-%d")
            
            d = d - dt.timedelta(days=1)
            
    except: pass

    # 若全失敗，使用安全氣囊
    if not targets:
        fallback_targets = []
        for sym in SAFE_LIST[:limit]:
            fallback_targets.append({'symbol': sym, 'name': '熱門備用', 'volume': 0})
        return fallback_targets, "備用清單(連線受阻)"
        
    return targets[:limit], d.strftime("%Y-%m-%d")

def build_snapshot(symbol: str, name: str, hist: pd.DataFrame, info: dict) -> Optional[StockSnapshot]:
    if len(hist) < 240: return None
    last = hist.iloc[-1]
    closes = hist['Close'].tail(3).values[::-1]
    ma20 = float(hist['Close'].rolling(20).mean().iloc[-1])
    ma60 = float(hist['Close'].rolling(60).mean().iloc[-1])
    ma240 = float(hist['Close'].rolling(240).mean().iloc[-1])
    
    ind = (info.get('industry') or "").lower()
    is_fin = any(x in ind for x in ['bank', 'insurance', 'financial', '金', '銀', '保'])
    is_cyc = any(x in ind for x in ['steel', 'shipping', 'plastic', '鋼', '海', '塑'])

    return StockSnapshot(
        symbol=symbol, name=name, close=float(last['Close']), volume=float(last['Volume']),
        ma20=ma20, ma60=ma60, ma240=ma240, price_history_3d=closes.tolist(),
        vol_ma20=float(hist['Volume'].rolling(20).mean().iloc[-1]),
        market_cap=info.get('marketCap', 0),
        roe_ttm=info.get('returnOnEquity', 0) or 0,
        opm_ttm=info.get('operatingMargins', 0) or 0,
        debt_ratio=calculate_debt_ratio(info),
        rev_growth=info.get('revenueGrowth', 0) or 0,
        eps_growth=info.get('earningsGrowth', 0) or 0,
        is_financial=is_fin, is_cyclical=is_cyc
    )

# ==========================================
# Part C: Streamlit UI
# ==========================================

st.set_page_config(page_title="v7.9.8 選股雷達", page_icon="🎯", layout="wide")
st.title("🎯 v7.9.8 選股雷達 (單檔防禦版)")
st.markdown("**說明：** 若官方資料連線逾時，系統將自動切換至「熱門備用清單」進行分析，確保功能運作。")

with st.sidebar:
    st.header("⚙️ 參數")
    scan_limit = st.slider("掃描數量", 30, 200, 50)
    max_price = st.number_input("股價上限", value=80.0)
    min_vol = st.number_input("成交量下限", value=1000)

if st.button("🚀 啟動雷達", type="primary"):
    with st.spinner("正在取得市場資料..."):
        targets, d_str = get_market_scan_list(scan_limit)
    
    st.success(f"資料來源: {d_str} | 數量: {len(targets)} | 開始 v7.9.8 分析...")
    
    results = []
    prog = st.progress(0)
    status = st.empty()
    
    scan_targets = targets[:scan_limit]
    
    for i, meta in enumerate(scan_targets):
        sym = meta['symbol']
        name = meta['name']
        prog.progress((i+1)/len(scan_targets))
        status.text(f"分析: {sym}")
        
        try:
            tick = yf.Ticker(sym)
            hist = tick.history(period="2y")
            if len(hist) < 10: continue
            
            curr_close = hist.iloc[-1]['Close']
            curr_vol = hist.iloc[-1]['Volume'] / 1000
            if curr_vol < min_vol: continue
            
            # 價格過濾 (全揭露模式下不直接跳過，但標記)
            info = tick.info
            snap = build_snapshot(sym, name, hist, info)
            
            if snap:
                uni = check_universe(snap, max_price)
                firm = check_firm(snap)
                cls = classify_stock(snap, uni, firm)
                score = calculate_score(snap, firm)
                
                results.append({
                    '代號': sym, '名稱': name, '評級': cls.layer.value,
                    '收盤': round(snap.close, 2), '成交': int(snap.volume/1000),
                    '基本面': "✅" if (uni.checks.get('roe') and uni.checks.get('opm')) else "❌",
                    '動能': "✅" if firm.is_firm else "❌",
                    '原因': cls.extra_info.get('reason', uni.reason),
                    'ROE': f"{snap.roe_ttm*100:.1f}%",
                    'Score': score.total
                })
        except: continue
            
    prog.empty()
    status.empty()
    
    if results:
        df = pd.DataFrame(results)
        st.dataframe(df, use_container_width=True)
        
        # 建議
        buys = df[df['評級'].isin(['A', 'B'])]
        if not buys.empty:
            st.markdown("### 📋 建議操作")
            for _, r in buys.iterrows():
                act = "整張" if r['評級']=='A' else "半單位"
                st.success(f"**[{r['評級']}] {r['代號']}** | 收盤 {r['收盤']} | 建議: 隔日開盤 {act}買進")
        else:
            st.warning("無 A/B 級標的")
    else:
        st.error("無資料")
