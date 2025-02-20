from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import re
import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# 全局變數
cleaned_df = None

# 載入環境變數
CHANNEL_ACCESS_TOKEN = os.getenv('CHANNEL_ACCESS_TOKEN')
CHANNEL_SECRET = os.getenv('CHANNEL_SECRET')

# 初始化 LINE Bot API
line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

def create_session():
    """創建具有重試機制的會話"""
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504]
    )
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def scrape_marathon_data():
    """爬取馬拉松資料"""
    url = 'http://www.taipeimarathon.org.tw/contest.aspx'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        session = create_session()
        response = session.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        
        races = []
        race_rows = soup.find_all('tr', class_='rowbackgroundcolor')
        
        for row in race_rows:
            cells = row.find_all('td')
            if len(cells) >= 7:
                name_cell = cells[1]
                link_tag = name_cell.find('a')
                name = link_tag.text.strip() if link_tag else name_cell.text.strip()
                link = link_tag.get('href', "無資料") if link_tag else "無資料"
                
                # 提取報名日期
                registration_date = cells[7].text.strip() if len(cells) > 7 else "無資料"
                
                race_info = {
                    'date': cells[3].text.strip(),
                    'name': name,
                    'location': cells[4].text.strip(),
                    'distance': cells[5].text.strip(),
                    'link': link,
                    'registration_date': registration_date
                }
                races.append(race_info)
        
        return pd.DataFrame(races)
    
    except Exception as e:
        logger.error(f"爬取過程發生錯誤: {str(e)}")
        return pd.DataFrame(columns=['date', 'name', 'location', 'distance', 'link', 'registration_date'])

def clean_data(df):
    """清理並處理數據"""
    try:
        if df.empty:
            return df
        
        # 地區代碼映射
        location_mapping = {
            '台北': 1, '臺北': 1, '新北': 1, '基隆': 1, '桃園': 1, '新竹': 1, '宜蘭': 1,
            '台中': 2, '臺中': 2, '苗栗': 2, '彰化': 2, '南投': 2, '雲林': 2,
            '高雄': 3, '台南': 3, '臺南': 3, '嘉義': 3, '屏東': 3,
            '花蓮': 4, '台東': 4, '臺東': 4,
            '金門': 5, '澎湖': 5, '馬祖': 5
        }
        
        # 添加地區代碼
        df['region_code'] = df['location'].apply(
            lambda x: next((code for loc, code in location_mapping.items() if loc in x), 0)
        )
        
        # 從日期欄位提取月份 (格式為 MM/DD)
        df['month'] = df['date'].str.extract(r'(\d{2})/\d{2}')[0]
        
        return df
    
    except Exception as e:
        logger.error(f"數據清理過程發生錯誤: {str(e)}")
        return pd.DataFrame(columns=['date', 'name', 'location', 'distance', 'link', 'region_code', 'month', 'registration_date'])

def update_data():
    """更新數據"""
    global cleaned_df
    try:
        raw_df = scrape_marathon_data()
        if not raw_df.empty:
            cleaned_df = clean_data(raw_df)
            logger.info("數據更新成功")
        else:
            logger.warning("數據更新失敗：獲取到空的數據框架")
    except Exception as e:
        logger.error(f"數據更新失敗: {str(e)}")

def format_response(results):
    """格式化回應訊息"""
    if results.empty:
        return "找不到符合的賽事"
    
    response = "找到以下賽事：\n\n"
    for _, row in results.iterrows():
        response += f"📅 {row['date']}\n"
        response += f"🏃 {row['name']}\n"
        response += f"📍 {row['location']}\n"
        response += f"🏃‍♂️ {row['distance']}\n"
        if row['link'] != "無資料":
            response += f"🔗 {row['link']}\n"
        if row['registration_date'] != "無資料":
            response += f"⏰ {row['registration_date']}\n"
        response += "\n"
    
    return response

def search_races(df, search_type, value):
    """統一的搜尋函數"""
    if df is None or df.empty:
        return "目前沒有可用的賽事資料"
    
    try:
        if search_type == 'date':
            # 從輸入值中提取月份
            input_month = value[-2:]  # 取得後兩位數字作為月份
            results = df[df['month'] == input_month]
        elif search_type == 'region':
            results = df[df['region_code'] == int(value)]
        elif search_type == 'keyword':
            results = df[
                df['name'].str.contains(value, na=False, case=False) |
                df['location'].str.contains(value, na=False, case=False)
            ]
        else:
            return "無效的搜尋類型"
        
        return format_response(results)
    
    except Exception as e:
        logger.error(f"搜尋過程發生錯誤: {str(e)}")
        return "搜尋過程發生錯誤，請稍後再試"

@app.route("/callback", methods=['POST'])
def callback():
    """處理 LINE Webhook"""
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    
    return 'OK'

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    """處理收到的訊息"""
    text = event.message.text.strip()
    
    # 檢查是否為不需回應的特定文字
    no_response_texts = [
        "依時間查詢賽事",
        "依地區查詢賽事",
        "依賽事名稱查詢"
    ]
    if text in no_response_texts:
        return
    
    try:
        # 處理搜尋指令
        if text.startswith('/'):
            keyword = text[1:]
            if not keyword:
                result = "請在/後輸入搜尋關鍵字"
            else:
                result = search_races(cleaned_df, 'keyword', keyword)
        
        # 處理日期搜尋 (YYYYMM 格式)
        elif re.match(r'^\d{6}$', text):
            result = search_races(cleaned_df, 'date', text)
        
        # 處理地區代碼搜尋 (1-5)
        elif re.match(r'^[1-5]$', text):
            result = search_races(cleaned_df, 'region', text)
        
        # 說明訊息
        else:
            result = (
                "請使用以下方式搜尋：\n"
                "1️⃣. YYYYMM - 搜尋特定月份\n"
                "2️⃣. 1-5 - 搜尋特定地區\n\n"
                "地區代碼：\n"
                "1: 北部地區\n"
                "2: 中部地區\n"
                "3: 南部地區\n"
                "4: 東部地區\n"
                "5: 離島地區"
                "3️⃣. /關鍵字 - 搜尋賽事名稱\n"
            )
        
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=result)
        )
    
    except Exception as e:
        logger.error(f"處理訊息時發生錯誤: {str(e)}")
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="系統發生錯誤，請稍後再試")
        )

def init_data():
    """初始化數據"""
    global cleaned_df
    try:
        raw_df = scrape_marathon_data()
        if not raw_df.empty:
            cleaned_df = clean_data(raw_df)
            logger.info("數據初始化成功")
        else:
            cleaned_df = pd.DataFrame(columns=['date', 'name', 'location', 'distance', 'link', 'region_code', 'month', 'registration_date'])
            logger.warning("使用空的數據框架初始化")
        
        # 設置定時更新
        scheduler = BackgroundScheduler()
        scheduler.add_job(update_data, 'interval', hours=24)
        scheduler.start()
    
    except Exception as e:
        logger.error(f"數據初始化失敗: {str(e)}")
        cleaned_df = pd.DataFrame(columns=['date', 'name', 'location', 'distance', 'link', 'region_code', 'month', 'registration_date'])

@app.route("/")
def hello():
    return 'Hello, World!'

if __name__ == "__main__":
    init_data()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
else:
    # 確保在 production 環境中也能初始化數據
    init_data()
