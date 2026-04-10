import logging
from datetime import datetime
from typing import Dict, List

import yfinance as yf

logger = logging.getLogger(__name__)

# 台股產業分類（含 hashtag 與龍頭）
STOCK_GROUPS: Dict[str, Dict[str, List[str]]] = {
    "金融股": {
        "hashtags": ["#金控", "#銀行", "#證券", "#保險", "#金融科技"],
        "leaders": ["2881 富邦金", "2891 中信金", "2882 國泰金"],
    },
    "營建股": {
        "hashtags": ["#建設", "#營造", "#不動產", "#資產開發"],
        "leaders": ["2533 皇昌", "2542 興富發", "2520 冠德"],
    },
    "生技股": {
        "hashtags": ["#新藥", "#醫材", "#CDMO", "#疫苗", "#精準醫療"],
        "leaders": ["6446 藥華藥", "4147 中裕", "1795 美時"],
    },
    "航運股": {
        "hashtags": ["#貨櫃", "#散裝", "#航空", "#物流"],
        "leaders": ["2603 長榮", "2609 陽明", "2615 萬海"],
    },
    "傳統產業": {
        "hashtags": ["#塑化", "#鋼鐵", "#水泥", "#食品", "#紡織", "#汽車零組件"],
        "leaders": ["1301 台塑", "2002 中鋼", "1216 統一"],
    },
    "科技股-半導體": {
        "hashtags": ["#晶圓代工", "#IC設計", "#封測", "#EDA", "#矽智財", "#成熟製程"],
        "leaders": ["2330 台積電", "2454 聯發科", "2303 聯電"],
    },
    "科技股-AI伺服器": {
        "hashtags": ["#AI伺服器", "#ODM", "#散熱", "#機殼", "#高速傳輸"],
        "leaders": ["2382 廣達", "3231 緯創", "6669 緯穎"],
    },
    "科技股-電子代工": {
        "hashtags": ["#EMS", "#OEM", "#組裝", "#供應鏈管理"],
        "leaders": ["2317 鴻海", "2301 光寶科", "4938 和碩"],
    },
    "科技股-網通": {
        "hashtags": ["#網通", "#交換器", "#路由器", "#WiFi", "#5G", "#光通訊"],
        "leaders": ["2412 中華電", "3596 智易", "4904 遠傳"],
    },
    "科技股-PCB與載板": {
        "hashtags": ["#PCB", "#ABF載板", "#HDI", "#CCL", "#高頻高速材料"],
        "leaders": ["8046 南電", "2383 台光電", "3037 欣興"],
    },
    "科技股-光電": {
        "hashtags": ["#面板", "#LED", "#光學鏡頭", "#ARVR", "#感測"],
        "leaders": ["2409 友達", "3481 群創", "3008 大立光"],
    },
    "科技股-電源與儲能": {
        "hashtags": ["#電源供應器", "#BBU", "#UPS", "#儲能", "#車用電源"],
        "leaders": ["2308 台達電", "6409 旭隼", "2376 技嘉"],
    },
}


def _to_tw_ticker(symbol: str) -> str:
    raw = symbol.strip().upper().replace(".TW", "").replace(".TWO", "")
    if not raw.isdigit():
        return symbol.strip()
    if raw.startswith("6") or raw.startswith("8"):
        return f"{raw}.TWO"
    return f"{raw}.TW"


def format_stock_groups() -> str:
    lines = ["📊 台股股群分類（含龍頭公司）"]
    for group, config in STOCK_GROUPS.items():
        hashtags = " ".join(config["hashtags"])
        leaders = "、".join(config["leaders"])
        lines.append(f"\n【{group}】")
        lines.append(f"hashtag: {hashtags}")
        lines.append(f"龍頭: {leaders}")
    return "\n".join(lines)


def get_stock_price_summary(symbol: str) -> str:
    ticker = _to_tw_ticker(symbol)
    stock = yf.Ticker(ticker)

    history = stock.history(period="5d", interval="1d")
    if history.empty:
        return f"查無 {symbol} 的股價資料，請確認代號是否正確。"

    latest = history.iloc[-1]
    previous_close = history.iloc[-2]["Close"] if len(history) > 1 else latest["Close"]
    change = latest["Close"] - previous_close
    change_pct = (change / previous_close * 100) if previous_close else 0

    info = stock.fast_info
    market_cap = info.get("market_cap", 0)
    currency = info.get("currency", "TWD")

    return (
        f"📈 {ticker} 即時摘要\n"
        f"收盤價: {latest['Close']:.2f} {currency}\n"
        f"漲跌: {change:+.2f} ({change_pct:+.2f}%)\n"
        f"最高/最低: {latest['High']:.2f} / {latest['Low']:.2f}\n"
        f"成交量: {int(latest['Volume']):,}\n"
        f"市值: {int(market_cap):,}"
    )


def get_financial_summary(symbol: str) -> str:
    ticker = _to_tw_ticker(symbol)
    stock = yf.Ticker(ticker)

    quarterly_income = stock.quarterly_income_stmt
    quarterly_balance = stock.quarterly_balance_sheet

    if quarterly_income.empty:
        return f"目前無法取得 {ticker} 的財報資料。"

    latest_col = quarterly_income.columns[0]
    if hasattr(latest_col, "year") and hasattr(latest_col, "month"):
        quarter = (latest_col.month - 1) // 3 + 1
        quarter_label = f"{latest_col.year}-Q{quarter}"
    else:
        quarter_label = str(latest_col)

    revenue = quarterly_income.loc["Total Revenue", latest_col] if "Total Revenue" in quarterly_income.index else None
    gross_profit = quarterly_income.loc["Gross Profit", latest_col] if "Gross Profit" in quarterly_income.index else None
    net_income = quarterly_income.loc["Net Income", latest_col] if "Net Income" in quarterly_income.index else None

    debt_ratio_text = "無資料"
    if not quarterly_balance.empty:
        b_col = quarterly_balance.columns[0]
        liabilities = quarterly_balance.loc["Total Liabilities Net Minority Interest", b_col] if "Total Liabilities Net Minority Interest" in quarterly_balance.index else None
        equity = quarterly_balance.loc["Stockholders Equity", b_col] if "Stockholders Equity" in quarterly_balance.index else None
        if liabilities and equity:
            debt_ratio_text = f"{(liabilities / (liabilities + equity) * 100):.2f}%"

    def _fmt(value):
        return f"{int(value):,}" if value is not None else "無資料"

    return (
        f"📑 {ticker} 財報摘要（{quarter_label}）\n"
        f"營收: {_fmt(revenue)}\n"
        f"毛利: {_fmt(gross_profit)}\n"
        f"淨利: {_fmt(net_income)}\n"
        f"負債比: {debt_ratio_text}"
    )


def get_stock_news(symbol: str, limit: int = 3) -> str:
    ticker = _to_tw_ticker(symbol)
    stock = yf.Ticker(ticker)

    try:
        news = stock.news[:limit]
    except Exception as exc:
        logger.error("取得新聞失敗: %s", exc)
        return f"目前無法取得 {ticker} 的新聞資料。"

    if not news:
        return f"{ticker} 目前查無近期新聞。"

    lines = [f"📰 {ticker} 最新新聞"]
    for item in news:
        title = item.get("title", "無標題")
        publisher = item.get("publisher", "未知來源")
        link = item.get("link", "")
        publish_time = item.get("providerPublishTime")
        date_text = ""
        if publish_time:
            date_text = datetime.utcfromtimestamp(publish_time).strftime("%Y-%m-%d")
        lines.append(f"- {title} ({publisher} {date_text})")
        if link:
            lines.append(f"  {link}")

    return "\n".join(lines)
