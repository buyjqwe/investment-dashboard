import streamlit as st
import pandas as pd
import requests
import re
import random
import time
import json
from datetime import datetime, timedelta
import secrets
import plotly.graph_objects as go
import plotly.express as px  # Import for colors
import hashlib
from copy import deepcopy
import yfinance as yf

# --- 页面基础设置 ---
st.set_page_config(page_title="专业投资分析仪表盘", page_icon="🚀", layout="wide")

# --- 全局常量 ---
SUPPORTED_CURRENCIES = ["USD", "CNY", "EUR", "HKD", "JPY", "GBP"]
CURRENCY_SYMBOLS = {"USD": "$", "CNY": "¥", "EUR": "€", "HKD": "HK$", "JPY": "¥", "GBP": "£"}
SESSION_EXPIRATION_DAYS = 7
DATA_REFRESH_INTERVAL_SECONDS = 3600  # 1 hour
BASE_ONEDRIVE_PATH = "root:/Apps/StreamlitDashboard"
OUNCES_TO_GRAMS = 31.1035
SECTOR_TRANSLATION = {
    'Technology': '科技',
    'Financial Services': '金融服务',
    'Healthcare': '医疗健康',
    'Industrials': '工业',
    'Consumer Cyclical': '周期性消费',
    'Consumer Defensive': '防御性消费',
    'Basic Materials': '基础材料',
    'Communication Services': '通信服务',
    'Energy': '能源',
    'Real Estate': '房地产',
    'Utilities': '公用事业',
    'N/A': '未分类'
}

# --- 初始化 Session State ---
if 'logged_in' not in st.session_state: st.session_state.logged_in = False
if 'user_email' not in st.session_state: st.session_state.user_email = ""
if 'login_step' not in st.session_state: st.session_state.login_step = "enter_email"
if 'display_currency' not in st.session_state: st.session_state.display_currency = "USD"
if 'last_market_data_fetch' not in st.session_state: st.session_state.last_market_data_fetch = 0
if 'migration_done' not in st.session_state: st.session_state.migration_done = False

# --- API 配置 ---
# Ensure you have these secrets configured in your Streamlit deployment environment
MS_GRAPH_CONFIG = st.secrets["microsoft_graph"]
ADMIN_EMAIL = MS_GRAPH_CONFIG["admin_email"]
ONEDRIVE_SENDER_EMAIL = MS_GRAPH_CONFIG['sender_email']
CF_CONFIG = st.secrets["cloudflare"]

# --- 核心功能函数定义 ---
def get_email_hash(email): return hashlib.sha256(email.encode('utf-8')).hexdigest()

@st.cache_data(ttl=3500)
def get_ms_graph_token():
    url = f"https://login.microsoftonline.com/{MS_GRAPH_CONFIG['tenant_id']}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": MS_GRAPH_CONFIG['client_id'],
        "client_secret": MS_GRAPH_CONFIG['client_secret'],
        "scope": "https://graph.microsoft.com/.default"
    }
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

def onedrive_api_request(method, path, headers, data=None):
    base_url = f"https://graph.microsoft.com/v1.0/users/{ONEDRIVE_SENDER_EMAIL}/drive"
    url = f"{base_url}/{path}"
    if method.lower() == 'get': return requests.get(url, headers=headers)
    if method.lower() == 'put': return requests.put(url, headers=headers, data=data)
    return None

def get_onedrive_data(path, is_json=True):
    try:
        token = get_ms_graph_token()
        headers = {"Authorization": f"Bearer {token}"}
        resp = onedrive_api_request('get', f"{path}:/content", headers)
        if resp.status_code == 404:
            # This is not an error, just means the file doesn't exist yet.
            return None
        resp.raise_for_status()
        return resp.json() if is_json else resp.text
    except requests.exceptions.RequestException as e:
        st.error(f"从 OneDrive 加载数据失败 ({path}): {e}")
        return None

def save_onedrive_data(path, data):
    try:
        token = get_ms_graph_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        json_data = json.dumps(data, indent=2, ensure_ascii=False)
        onedrive_api_request('put', f"{path}:/content", headers, data=json_data.encode('utf-8'))
        return True
    except Exception as e:
        st.error(f"保存数据到 OneDrive 失败 ({path}): {e}")
        return False

def get_user_profile(email):
    return get_onedrive_data(f"{BASE_ONEDRIVE_PATH}/users/{get_email_hash(email)}.json")

def save_user_profile(email, data):
    return save_onedrive_data(f"{BASE_ONEDRIVE_PATH}/users/{get_email_hash(email)}.json", data)

def get_global_data(file_name):
    data = get_onedrive_data(f"{BASE_ONEDRIVE_PATH}/{file_name}.json")
    return data if data else {}

def save_global_data(file_name, data):
    return save_onedrive_data(f"{BASE_ONEDRIVE_PATH}/{file_name}.json", data)

def send_verification_code(email, code):
    try:
        token = get_ms_graph_token()
        url = f"https://graph.microsoft.com/v1.0/users/{ONEDRIVE_SENDER_EMAIL}/sendMail"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {
            "message": {
                "subject": f"[{code}] 您的登录/注册验证码",
                "body": {"contentType": "Text", "content": f"您的验证码是：{code}，5分钟内有效。"},
                "toRecipients": [{"emailAddress": {"address": email}}]
            },
            "saveToSentItems": "true"
        }
        requests.post(url, headers=headers, json=payload, timeout=10).raise_for_status()
        return True
    except Exception as e:
        st.error(f"邮件发送失败: {e}")
        return False

def handle_send_code(email):
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        st.sidebar.error("请输入有效的邮箱地址。")
        return
    codes = get_global_data("codes")
    code = str(random.randint(100000, 999999))
    codes[email] = {"code": code, "expires_at": time.time() + 300} # 5-minute expiration
    if not save_global_data("codes", codes) or not send_verification_code(email, code):
        return
    st.sidebar.success("验证码已发送，请查收。")
    st.session_state.login_step = "enter_code"
    st.session_state.temp_email = email
    st.rerun()

def handle_verify_code(email, code):
    codes = get_global_data("codes")
    code_info = codes.get(email)
    if not code_info or time.time() > code_info["expires_at"]:
        st.sidebar.error("验证码已过期或不存在。")
        return
    if code_info["code"] == code:
        if not get_user_profile(email):
            new_profile = {"role": "user", "portfolio": {"stocks": [], "cash_accounts": [], "crypto": [], "liabilities": [], "transactions": [], "gold": []}}
            save_user_profile(email, new_profile)
            st.toast("🎉 欢迎新用户！已为您创建账户。")
        
        sessions = get_global_data("sessions")
        token = secrets.token_hex(16)
        sessions[token] = {"email": email, "expires_at": time.time() + (SESSION_EXPIRATION_DAYS * 24 * 60 * 60)}
        save_global_data("sessions", sessions)
        
        del codes[email]
        save_global_data("codes", codes)
        
        st.session_state.logged_in = True
        st.session_state.user_email = email
        st.session_state.login_step = "logged_in"
        st.query_params["session_token"] = token
        st.rerun()
    else:
        st.sidebar.error("验证码错误。")

def check_session_from_query_params():
    if st.session_state.get('logged_in'):
        return
    token = st.query_params.get("session_token")
    if not token:
        return
    sessions = get_global_data("sessions")
    session_info = sessions.get(token)
    if session_info and time.time() < session_info.get("expires_at", 0):
        st.session_state.logged_in = True
        st.session_state.user_email = session_info["email"]
        st.session_state.login_step = "logged_in"
    elif "session_token" in st.query_params:
        st.query_params.clear()

@st.cache_data(ttl=86400)  # Cache for 24 hours
def get_exchange_rates():
    try:
        resp = requests.get(f"https://open.er-api.com/v6/latest/USD")
        resp.raise_for_status()
        data = resp.json()
        return data.get("rates") if data.get("result") == "success" else None
    except Exception as e:
        st.error(f"获取汇率失败: {e}")
        return None

def get_prices_from_market_data(market_data, tickers):
    prices = {}
    for t in tickers:
        original_ticker = t.replace('-USD', '')
        prices[original_ticker] = market_data.get(t, {}).get("latest_price", 0)
    return prices

@st.cache_data(ttl=86400)
def get_stock_profile_yf(symbol):
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        if info and info.get('shortName'):
            return info
    except Exception:
        return None
    return None

@st.cache_data(ttl=300) # Cache for 5 minutes
def get_market_data_yf(symbols):
    """
    Fetches the latest market data for a list of symbols using yfinance.
    """
    if not symbols:
        return {}
    
    data = {}
    try:
        tickers = yf.Tickers(symbols)
        
        if len(symbols) == 1:
            # Handle the single ticker case, which has a different structure
            ticker_obj = tickers.tickers[symbols[0]]
            hist = ticker_obj.history(period="2d")
            if not hist.empty:
                data[symbols[0]] = {
                    "latest_price": hist['Close'].iloc[-1],
                    "previous_close": hist['Close'].iloc[-2] if len(hist) > 1 else hist['Close'].iloc[-1]
                }
        else:
            # Handle the multiple tickers case
            for symbol, ticker_obj in tickers.tickers.items():
                hist = ticker_obj.history(period="2d")
                if not hist.empty:
                    data[symbol] = {
                        "latest_price": hist['Close'].iloc[-1],
                        "previous_close": hist['Close'].iloc[-2] if len(hist) > 1 else hist['Close'].iloc[-1]
                    }
    except Exception as e:
        st.warning(f"yfinance data fetch failed for some tickers: {e}")
    
    return data

def get_asset_history(email):
    history = []
    try:
        token = get_ms_graph_token()
        headers = {"Authorization": f"Bearer {token}"}
        email_hash = get_email_hash(email)
        path = f"{BASE_ONEDRIVE_PATH}/history/{email_hash}:/children"
        resp = onedrive_api_request('get', path, headers)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        files = resp.json().get('value', [])
        for file in files:
            file_path = f"{BASE_ONEDRIVE_PATH}/history/{email_hash}/{file['name']}"
            snapshot = get_onedrive_data(file_path)
            if snapshot:
                history.append(snapshot)
    except Exception:
        return []
    return sorted(history, key=lambda x: x['date'])

def get_closest_snapshot(target_date, asset_history):
    if not asset_history: return None
    target_date_str = target_date.strftime('%Y-%m-%d')
    relevant_snapshots = [s for s in asset_history if s['date'] <= target_date_str]
    if not relevant_snapshots: return None
    return max(relevant_snapshots, key=lambda x: x['date'])

def update_asset_snapshot(email, user_profile, total_assets_usd, total_liabilities_usd, total_stock_value_usd, total_cash_balance_usd, total_crypto_value_usd, total_gold_value_usd, current_rates):
    today_str = datetime.now().strftime("%Y-%m-%d")
    if not get_onedrive_data(f"{BASE_ONEDRIVE_PATH}/history/{get_email_hash(email)}/{today_str}.json"):
        st.toast("今日资产快照已生成！")
        snapshot = {
            "date": today_str,
            "total_assets_usd": total_assets_usd,
            "total_liabilities_usd": total_liabilities_usd,
            "net_worth_usd": total_assets_usd - total_liabilities_usd,
            "total_stock_value_usd": total_stock_value_usd,
            "total_cash_balance_usd": total_cash_balance_usd,
            "total_crypto_value_usd": total_crypto_value_usd,
            "total_gold_value_usd": total_gold_value_usd,
            "exchange_rates": current_rates,
            "portfolio": user_profile["portfolio"]
        }
        save_onedrive_data(f"{BASE_ONEDRIVE_PATH}/history/{get_email_hash(email)}/{today_str}.json", snapshot)

@st.cache_data(ttl=3600)
def get_detailed_ai_analysis(prompt):
    try:
        account_id, api_token, model = CF_CONFIG['account_id'], CF_CONFIG['api_token'], "@cf/meta/llama-3-8b-instruct"
        url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/{model}"
        headers = {"Authorization": f"Bearer {api_token}"}
        payload = {"prompt": prompt, "stream": False, "max_tokens": 2048}
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        return response.json().get("result", {}).get("response", "AI 分析时出现错误或超时。")
    except Exception as e:
        return f"无法连接到 AI 服务进行分析: {e}"

@st.cache_data(ttl=1800)
def get_detailed_history_df(_asset_history_tuples, start_date, end_date):
    """
    Calculates detailed historical asset values.
    Accepts a tuple of tuples for caching and converts it back to a list of dicts.
    """
    if not _asset_history_tuples:
        return pd.DataFrame()

    # --- FIX: Convert the hashable tuple back to a list of dicts ---
    _asset_history = [dict(s) for s in _asset_history_tuples]
    
    all_historical_tickers = set()
    for snapshot in _asset_history:
        portfolio = snapshot.get('portfolio', {})
        for s in portfolio.get("stocks", []): all_historical_tickers.add(s['ticker'])
        for c in portfolio.get("crypto", []): all_historical_tickers.add(f"{c['symbol'].upper()}-USD")
    all_historical_tickers.add("GC=F")
    
    hist_prices_df = yf.download(list(all_historical_tickers), start=start_date, end=end_date + timedelta(days=1), progress=False)
    if hist_prices_df.empty:
        return pd.DataFrame()

    daily_values_data = []
    all_dates = pd.date_range(start=start_date, end=end_date, freq='D')

    for date in all_dates:
        # get_closest_snapshot now receives the corrected list of dicts
        snapshot = get_closest_snapshot(date.date(), _asset_history)
        if not snapshot: continue

        portfolio = snapshot.get('portfolio', {})
        exchange_rates = snapshot.get('exchange_rates', {})

        try:
            prices_series = hist_prices_df['Close'].loc[date.strftime('%Y-%m-%d')]
        except KeyError:
            temp_df = hist_prices_df[hist_prices_df.index < date]
            if not temp_df.empty:
                prices_series = temp_df['Close'].iloc[-1]
            else:
                continue

        stock_holdings = portfolio.get("stocks", [])
        crypto_holdings = portfolio.get("crypto", [])
        gold_holdings = portfolio.get("gold", [])
        cash_accounts = portfolio.get("cash_accounts", [])
        liabilities = portfolio.get("liabilities", [])

        gold_price_per_ounce = prices_series.get("GC=F", 0)
        gold_price_per_gram = (gold_price_per_ounce / OUNCES_TO_GRAMS) if pd.notna(gold_price_per_ounce) and gold_price_per_ounce > 0 else 0

        stock_value_usd = sum(s.get('quantity',0) * prices_series.get(s['ticker'], 0) / exchange_rates.get(s.get('currency', 'USD'), 1) for s in stock_holdings if pd.notna(prices_series.get(s['ticker'])))
        crypto_value_usd = sum(c.get('quantity',0) * prices_series.get(f"{c['symbol'].upper()}-USD", 0) for c in crypto_holdings if pd.notna(prices_series.get(f"{c['symbol'].upper()}-USD")))
        gold_value_usd = sum(g.get('grams', 0) * gold_price_per_gram for g in gold_holdings)
        cash_value_usd = sum(acc.get('balance',0) / exchange_rates.get(acc.get('currency', 'USD'), 1) for acc in cash_accounts)
        liabilities_usd = sum(liab.get('balance',0) / exchange_rates.get(liab.get('currency', 'USD'), 1) for liab in liabilities)
        
        assets_usd = stock_value_usd + crypto_value_usd + gold_value_usd + cash_value_usd
        net_worth_usd = assets_usd - liabilities_usd

        daily_values_data.append({
            'date': date,
            'net_worth_usd': net_worth_usd,
            'stock_value_usd': stock_value_usd,
            'crypto_value_usd': crypto_value_usd,
            'gold_value_usd': gold_value_usd,
            'cash_value_usd': cash_value_usd,
        })
    
    if not daily_values_data:
        return pd.DataFrame()
        
    df = pd.DataFrame(daily_values_data)
    df['date'] = pd.to_datetime(df['date'])
    return df.set_index('date').sort_index()

def display_login_form():
    with st.sidebar:
        st.header("🔐 邮箱登录/注册")
        if st.session_state.login_step == "enter_email":
            email = st.text_input("邮箱地址", key="email_input")
            if st.button("发送验证码"): handle_send_code(email)
        elif st.session_state.login_step == "enter_code":
            email_display = st.session_state.get("temp_email", "")
            st.info(f"验证码已发送至: {email_display}")
            code = st.text_input("验证码", key="code_input")
            if st.button("登录或注册"): handle_verify_code(email_display, code)
            if st.button("返回"):
                st.session_state.login_step = "enter_email"
                st.rerun()

def display_asset_allocation_chart(stock_usd, cash_usd, crypto_usd, gold_usd, display_curr, display_rate, display_symbol):
    labels, values_usd = ['股票', '现金', '加密货币', '黄金'], [stock_usd, cash_usd, crypto_usd, gold_usd]
    non_zero_labels, non_zero_values = [l for l, v in zip(labels, values_usd) if v > 0.01], [v for v in values_usd if v > 0.01]
    if not non_zero_values:
        st.info("暂无资产可供分析。")
        return
    fig = go.Figure(data=[go.Pie(
        labels=non_zero_labels,
        values=[v * display_rate for v in non_zero_values],
        hole=.4,
        textinfo='percent+label',
        hovertemplate=f"<b>%{{label}}</b><br>价值: {display_symbol}%{{value:,.2f}} {display_curr}<br>占比: %{{percent}}<extra></extra>"
    )])
    fig.update_layout(title_text='资产配置', showlegend=False, height=300, margin=dict(l=10, r=10, t=40, b=10))
    st.plotly_chart(fig, use_container_width=True)

def display_dashboard():
    st.title(f"🚀 {st.session_state.user_email} 的专业仪表盘")
    asset_history = get_asset_history(st.session_state.user_email)
    user_profile = get_user_profile(st.session_state.user_email)
    if user_profile is None:
        st.error("无法加载用户数据。")
        st.stop()
    
    user_portfolio = user_profile.setdefault("portfolio", {})
    for key in ["stocks", "cash_accounts", "crypto", "liabilities", "transactions", "gold"]:
        user_portfolio.setdefault(key, [])
    
    stock_tickers = [s['ticker'] for s in user_portfolio.get("stocks", [])]
    crypto_symbols = [c['symbol'] for c in user_portfolio.get("crypto", [])]
    
    last_fetched_tickers = st.session_state.get('last_fetched_tickers', set())
    current_tickers = set(stock_tickers + crypto_symbols)
    tickers_changed = current_tickers != last_fetched_tickers
    
    if st.sidebar.button('🔄 刷新市场数据'):
        st.session_state.last_market_data_fetch = 0
    
    now = time.time()
    
    if tickers_changed or (now - st.session_state.last_market_data_fetch > DATA_REFRESH_INTERVAL_SECONDS):
        with st.spinner("正在获取最新市场数据..."):
            y_crypto_tickers = [f"{s.upper()}-USD" for s in crypto_symbols]
            all_yf_tickers = list(set(stock_tickers + y_crypto_tickers + ["GC=F"]))
            st.session_state.market_data = get_market_data_yf(all_yf_tickers)
            st.session_state.exchange_rates = get_exchange_rates()
            st.session_state.last_market_data_fetch = now
            st.session_state.last_fetched_tickers = current_tickers
            st.rerun()

    market_data = st.session_state.get('market_data', {})
    exchange_rates = st.session_state.get('exchange_rates', {})
    if not exchange_rates:
        st.error("无法加载汇率，资产总值不准确。")
        st.stop()

    prices = get_prices_from_market_data(market_data, stock_tickers + crypto_symbols + ["GC=F"])
    
    failed_tickers = [ticker for ticker in (stock_tickers + [f"{c}-USD" for c in crypto_symbols] + ["GC=F"]) if prices.get(ticker.replace('-USD', ''), 0) == 0]
    if failed_tickers:
        st.warning(f"警告：未能获取以下资产的价格，其市值可能显示为0: {', '.join(failed_tickers)}")
    
    gold_price_per_ounce = prices.get("GC=F", 0)
    gold_price_per_gram = gold_price_per_ounce / OUNCES_TO_GRAMS if gold_price_per_ounce > 0 else 0

    stock_holdings = user_portfolio.get("stocks", [])
    cash_accounts = user_portfolio.get("cash_accounts", [])
    crypto_holdings = user_portfolio.get("crypto", [])
    liabilities = user_portfolio.get("liabilities", [])
    gold_holdings = user_portfolio.get("gold", [])

    total_stock_value_usd = sum(s.get('quantity',0) * prices.get(s['ticker'], 0) / exchange_rates.get(s.get('currency', 'USD'), 1) for s in stock_holdings)
    total_cash_balance_usd = sum(acc.get('balance',0) / exchange_rates.get(acc.get('currency', 'USD'), 1) for acc in cash_accounts)
    total_crypto_value_usd = sum(c.get('quantity',0) * prices.get(c['symbol'], 0) for c in crypto_holdings)
    total_gold_value_usd = sum(g.get('grams', 0) * gold_price_per_gram for g in gold_holdings)
    total_assets_usd = total_stock_value_usd + total_cash_balance_usd + total_crypto_value_usd + total_gold_value_usd
    total_liabilities_usd = sum(liab.get('balance',0) / exchange_rates.get(liab.get('currency', 'USD'), 1) for liab in liabilities)
    net_worth_usd = total_assets_usd - total_liabilities_usd
    
    update_asset_snapshot(st.session_state.user_email, user_profile, total_assets_usd, total_liabilities_usd, total_stock_value_usd, total_cash_balance_usd, total_crypto_value_usd, total_gold_value_usd, exchange_rates)

    display_curr = st.sidebar.selectbox("选择显示货币", options=SUPPORTED_CURRENCIES, index=SUPPORTED_CURRENCIES.index(st.session_state.display_currency))
    st.session_state.display_currency = display_curr # Remember choice
    display_rate, display_symbol = exchange_rates.get(display_curr, 1), CURRENCY_SYMBOLS.get(display_curr, "")

    st.sidebar.header("分析周期")
    min_date = datetime.strptime(asset_history[0]['date'], '%Y-%m-%d').date() if asset_history else datetime.now().date() - timedelta(days=30)
    max_date = datetime.now().date()
    default_start_date = max_date - timedelta(days=7)
    if default_start_date < min_date: default_start_date = min_date
    start_date = st.sidebar.date_input("开始日期", value=default_start_date, min_value=min_date, max_value=max_date)

    # Convert asset_history to a hashable type for caching
    asset_history_tuples = tuple(map(tuple, (s.items() for s in asset_history)))
    history_df = get_detailed_history_df(asset_history_tuples, start_date, max_date - timedelta(days=1))
    
    # Append today's data to the history for a complete chart
    if not history_df.empty:
        today_data = pd.DataFrame([{
            'date': pd.to_datetime(max_date),
            'net_worth_usd': net_worth_usd,
            'stock_value_usd': total_stock_value_usd,
            'crypto_value_usd': total_crypto_value_usd,
            'gold_value_usd': total_gold_value_usd,
            'cash_value_usd': total_cash_balance_usd,
        }]).set_index('date')
        history_df = pd.concat([history_df, today_data])


    st.header("所选周期表现 (核心指标)")
    if history_df.empty or len(history_df.index) < 2:
        st.info("历史数据不足（少于2天），无法生成周期表现。显示当前指标。")
        col1, col2, col3 = st.columns(3)
        col1.metric("🏦 净资产", f"{display_symbol}{net_worth_usd * display_rate:,.2f} {display_curr}")
        col2.metric("💰 总资产", f"{display_symbol}{total_assets_usd * display_rate:,.2f} {display_curr}")
        col3.metric("💳 总负债", f"{display_symbol}{total_liabilities_usd * display_rate:,.2f} {display_curr}")
    else:
        try:
            start_val_usd = history_df['net_worth_usd'].iloc[0]
            end_val_usd = history_df['net_worth_usd'].iloc[-1]
            
            total_pl_usd = end_val_usd - start_val_usd
            total_pl_pct = (total_pl_usd / start_val_usd) * 100 if start_val_usd != 0 else 0
            
            daily_change_usd = history_df['net_worth_usd'].diff()
            best_day_usd = daily_change_usd.max()
            worst_day_usd = daily_change_usd.min()

            m_col1, m_col2, m_col3 = st.columns(3)
            m_col1.metric("周期初净资产", f"{display_symbol}{start_val_usd * display_rate:,.2f} {display_curr}")
            m_col2.metric("周期末净资产", f"{display_symbol}{end_val_usd * display_rate:,.2f} {display_curr}")
            m_col3.metric(f"周期总盈亏 ({display_curr})", 
                          f"{display_symbol}{total_pl_usd * display_rate:,.2f}",
                          delta=f"{total_pl_pct:,.2f}%")

            m_col4, m_col5, m_col6 = st.columns(3)
            m_col4.metric("最佳单日盈利", f"{display_symbol}{best_day_usd * display_rate:,.2f} {display_curr}")
            m_col5.metric("最大单日亏损", f"{display_symbol}{worst_day_usd * display_rate:,.2f} {display_curr}")
            # Add back the total liabilities as it's a useful core metric
            m_col6.metric("💳 当前总负债", f"{display_symbol}{total_liabilities_usd * display_rate:,.2f} {display_curr}")

        except Exception as e:
            st.warning(f"无法计算周期表现总结: {e}")
            # Fallback to original display if calculation fails
            col1, col2, col3 = st.columns(3)
            col1.metric("🏦 净资产", f"{display_symbol}{net_worth_usd * display_rate:,.2f} {display_curr}")
            col2.metric("💰 总资产", f"{display_symbol}{total_assets_usd * display_rate:,.2f} {display_curr}")
            col3.metric("💳 总负债", f"{display_symbol}{total_liabilities_usd * display_rate:,.2f} {display_curr}")

    stock_df_data = [{"代码": s['ticker'], "数量": s['quantity'], "货币": s['currency'], "成本价": f"{CURRENCY_SYMBOLS.get(s.get('currency', 'USD'), '')}{s.get('average_cost', 0):,.2f}", "现价": f"{CURRENCY_SYMBOLS.get(s.get('currency', 'USD'), '')}{prices.get(s['ticker'], 0):,.2f}", "市值": f"{CURRENCY_SYMBOLS.get(s.get('currency', 'USD'), '')}{s.get('quantity', 0) * prices.get(s['ticker'], 0):,.2f}", "未实现盈亏": f"{CURRENCY_SYMBOLS.get(s.get('currency', 'USD'), '')}{(s.get('quantity', 0) * prices.get(s['ticker'], 0)) - (s.get('quantity', 0) * s.get('average_cost', 0)):,.2f}", "回报率(%)": f"{(((s.get('quantity', 0) * prices.get(s['ticker'], 0)) - (s.get('quantity', 0) * s.get('average_cost', 0))) / (s.get('quantity', 0) * s.get('average_cost', 0)) * 100) if (s.get('quantity', 0) * s.get('average_cost', 0)) > 0 else 0:.2f}%"} for s in stock_holdings]
    crypto_df_data = [{"代码": c['symbol'], "数量": f"{c.get('quantity',0):.6f}", "成本价": f"${c.get('average_cost', 0):,.2f}", "现价": f"${prices.get(c['symbol'], 0):,.2f}", "市值": f"${c.get('quantity', 0) * prices.get(c['symbol'], 0):,.2f}", "未实现盈亏": f"${(c.get('quantity', 0) * prices.get(c['symbol'], 0)) - (c.get('quantity', 0) * c.get('average_cost', 0)):,.2f}", "回报率(%)": f"{(((c.get('quantity', 0) * prices.get(c['symbol'], 0)) - (c.get('quantity', 0) * c.get('average_cost', 0))) / (c.get('quantity', 0) * c.get('average_cost', 0)) * 100) if (c.get('quantity', 0) * c.get('average_cost', 0)) > 0 else 0:.2f}%"} for c in crypto_holdings]
    gold_df_data = [{"资产": "黄金", "克数 (g)": g.get('grams', 0), "成本价 ($/g)": f"${g.get('average_cost_per_gram', 0):,.2f}", "现价 ($/g)": f"${gold_price_per_gram:,.2f}", "市值": f"${g.get('grams', 0) * gold_price_per_gram:,.2f}", "未实现盈亏": f"${(g.get('grams', 0) * gold_price_per_gram) - (g.get('grams', 0) * g.get('average_cost_per_gram', 0)):,.2f}", "回报率(%)": f"{(((g.get('grams', 0) * gold_price_per_gram) - (g.get('grams', 0) * g.get('average_cost_per_gram', 0))) / (g.get('grams', 0) * g.get('average_cost_per_gram', 0)) * 100) if (g.get('grams', 0) * g.get('average_cost_per_gram', 0)) > 0 else 0:.2f}%"} for g in gold_holdings]

    # --- MODIFICATION: Removed manual transaction tab (tab2) ---
    tab1, tab2, tab3, tab4 = st.tabs(["📊 资产总览", "✍️ 资产编辑与交易", "📈 历史趋势", "🤖 AI深度分析"])

    with tab1:
        st.subheader("资产配置概览")
        
        # --- MODIFICATION: Moved Sector chart logic from old tab5 to here ---
        col1_alloc, col2_alloc = st.columns(2)
        with col1_alloc:
             display_asset_allocation_chart(total_stock_value_usd, total_cash_balance_usd, total_crypto_value_usd, total_gold_value_usd, display_curr, display_rate, display_symbol)
        
        with col2_alloc:
            sector_values = {}
            with st.spinner("正在获取持仓股票的行业信息..."):
                for s in stock_holdings:
                    profile = get_stock_profile_yf(s['ticker'])
                    sector_english = profile.get('sector', 'N/A') if profile else 'N/A'
                    sector_chinese = SECTOR_TRANSLATION.get(sector_english, sector_english)
                    value_usd = s.get('quantity',0) * prices.get(s['ticker'], 0) / exchange_rates.get(s.get('currency', 'USD'), 1)
                    sector_values[sector_chinese] = sector_values.get(sector_chinese, 0) + value_usd

            plot_values = {k: v for k, v in sector_values.items() if v > 0.01}

            if not plot_values:
                st.info("未能获取到股票的行业分类信息，或您尚未持有任何股票。")
            else:
                sector_df = pd.DataFrame(list(plot_values.items()), columns=['sector', 'value_usd']).sort_values(by='value_usd', ascending=False)
                fig = go.Figure(data=[go.Pie(labels=sector_df['sector'], values=sector_df['value_usd'] * display_rate, hole=.4, textinfo='percent+label', hovertemplate=f"<b>%{{label}}</b><br>市值: {display_symbol}%{{value:,.2f}}<br>占比: %{{percent}}<extra></extra>")])
                fig.update_layout(title_text='股票持仓行业分布', showlegend=False, height=300, margin=dict(l=10, r=10, t=40, b=10))
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("资产与盈亏明细")
        st.write("📈 **股票持仓**")
        # --- MODIFICATION: Use st.table to remove vertical scrollbar ---
        st.table(pd.DataFrame(stock_df_data))
        st.write("🥇 **黄金持仓**")
        # --- MODIFICATION: Use st.table to remove vertical scrollbar ---
        st.table(pd.DataFrame(gold_df_data))
        
        c1, c2, c3 = st.columns(3)
        with c1:
            st.write("💵 **现金账户**")
            # --- MODIFICATION: Switched to st.table to remove internal scrollbar ---
            st.table(pd.DataFrame([{"账户名称": acc['name'],"货币": acc['currency'], "余额": f"{CURRENCY_SYMBOLS.get(acc['currency'], '')}{acc['balance']:,.2f}"} for acc in cash_accounts]))
        with c2:
            st.write("🪙 **加密货币持仓**")
            # --- MODIFICATION: Use st.table to remove vertical scrollbar ---
            st.table(pd.DataFrame(crypto_df_data))
        with c3:
            st.write("💳 **负债账户**")
            # --- MODIFICATION: Switched to st.table to remove internal scrollbar ---
            st.table(pd.DataFrame([{"名称": liab['name'],"货币": liab['currency'], "金额": f"{CURRENCY_SYMBOLS.get(liab['currency'], '')}{liab['balance']:,.2f}"} for liab in liabilities]))

    # --- MODIFICATION: This is the new tab2 (formerly tab3), with all logic combined ---
    with tab2:
        st.subheader("✍️ 资产编辑与交易")
        st.info("请在此处直接编辑您的资产。系统将自动对比差异，并为您生成交易流水。\n- **卖出**：系统将按**当前市场价**计算卖出金额和盈亏。\n- **买入**：系统将按您修改后的**平均成本**反推买入金额。")
        
        # Helper function needed for this tab
        def to_df_with_schema(data, schema):
            df = pd.DataFrame(data)
            for col, col_type in schema.items():
                if col not in df.columns:
                    df[col] = pd.Series(dtype=col_type)
            return df
        
        # --- MODIFICATION: Added function to find cash account by name ---
        def get_cash_account(name):
            return next((acc for acc in cash_accounts if acc["name"] == name), None)

        # --- MODIFICATION: Added function to add transaction ---
        def add_transaction(description, type, amount, currency, account_name, symbol=None, quantity=None, realized_pl=None, pl_currency=None):
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
            new_tx = {
                "date": now_str, "type": type, "description": description, 
                "amount": amount, "currency": currency, "account": account_name,
                "symbol": symbol, "quantity": quantity, 
                "realized_pl": realized_pl, "pl_currency": pl_currency
            }
            user_profile.setdefault("transactions", []).insert(0, new_tx)

        # --- MODIFICATION: Cash account list for selection menus ---
        cash_account_names = [acc.get("name", "") for acc in cash_accounts]
        if not cash_account_names:
            st.error("您必须至少创建一个现金账户才能进行交易。")
            # Create a dummy list to prevent errors, though buttons will be disabled
            cash_account_names = ["-"]


        edit_tabs = st.tabs(["💵 现金", "💳 负债", "📈 股票", "🪙 加密货币", "🥇 黄金"])

        with edit_tabs[0]:
            schema = {'name': 'object', 'currency': 'object', 'balance': 'float64'}
            df = to_df_with_schema(user_portfolio.get("cash_accounts",[]), schema)
            
            # Store 'before' state
            cash_before_df = df.copy().set_index('name')
            
            calc_height = max(200, (len(df) + 6) * 35 + 3) # Dynamic height
            edited_df = st.data_editor(df, num_rows="dynamic", key="cash_editor_adv", column_config={"name": "账户名称", "currency": st.column_config.SelectboxColumn("货币", options=SUPPORTED_CURRENCIES, required=True), "balance": st.column_config.NumberColumn("余额", format="%.2f", required=True)}, use_container_width=True, hide_index=True, height=calc_height)
            
            if st.button("💾 保存现金账户修改", key="save_cash"):
                edited_list = edited_df.dropna(subset=['name']).to_dict('records')
                cash_after_df = pd.DataFrame(edited_list).set_index('name')

                # Diff logic
                diff_df = cash_before_df.merge(cash_after_df, on='name', how='outer', suffixes=('_old', '_new'))
                
                for name, row in diff_df.iterrows():
                    balance_old = row.get('balance_old', 0)
                    balance_new = row.get('balance_new', 0)
                    currency = row.get('currency_new', row.get('currency_old', 'USD')) # Get currency

                    if pd.isna(balance_old): # New Account
                        add_transaction("[自动] 账户创建", "存款", balance_new, currency, name)
                    elif pd.isna(balance_new): # Deleted Account
                        add_transaction("[自动] 账户删除", "取款", balance_old, currency, name)
                    elif balance_new != balance_old:
                        diff = balance_new - balance_old
                        tx_type = "存款" if diff > 0 else "取款"
                        add_transaction("[自动] 余额修正", tx_type, abs(diff), currency, name)

                user_portfolio["cash_accounts"] = edited_list
                if save_user_profile(st.session_state.user_email, user_profile): st.success("现金账户已更新！"); time.sleep(1); st.rerun()
        
        with edit_tabs[1]:
            schema = {'name': 'object', 'currency': 'object', 'balance': 'float64'}
            df = to_df_with_schema(user_portfolio.get("liabilities",[]), schema)
            calc_height = max(200, (len(df) + 6) * 35 + 3)
            edited_df = st.data_editor(df, num_rows="dynamic", key="liabilities_editor_adv", column_config={"name": "名称", "currency": st.column_config.SelectboxColumn("货币", options=SUPPORTED_CURRENCIES, required=True), "balance": st.column_config.NumberColumn("金额", format="%.2f", required=True)}, use_container_width=True, hide_index=True, height=calc_height)
            
            if st.button("💾 保存负债账户修改", key="save_liabilities"):
                # Liabilities are simple, no transaction linking needed
                user_portfolio["liabilities"] = edited_df.dropna(subset=['name']).to_dict('records')
                if save_user_profile(st.session_state.user_email, user_profile): st.success("负债账户已更新！"); time.sleep(1); st.rerun()

        with edit_tabs[2]:
            schema = {'ticker': 'object', 'quantity': 'float64', 'average_cost': 'float64', 'currency': 'object'}
            df = to_df_with_schema(user_portfolio.get("stocks",[]), schema)
            
            # Store 'before' state
            stock_before_df = df.copy().set_index('ticker')

            calc_height = max(200, (len(df) + 6) * 35 + 3)
            edited_df = st.data_editor(df, num_rows="dynamic", key="stock_editor_adv", column_config={"ticker": st.column_config.TextColumn("代码", help="请输入Yahoo Finance格式的代码", required=True), "quantity": st.column_config.NumberColumn("数量", format="%.4f", required=True), "average_cost": st.column_config.NumberColumn("平均成本", help="请以该股票的交易货币计价", format="%.2f", required=True), "currency": st.column_config.TextColumn("货币", help="将自动获取，无需填写", disabled=True)}, use_container_width=True, hide_index=True, height=calc_height)
            
            # --- MODIFICATION: Added cash account selector ---
            cash_account_stock = st.selectbox("选择关联的现金账户（用于自动流水）", cash_account_names, key="cash_stock_link", disabled=(not cash_account_names[0] != "-"))

            if st.button("💾 保存股票持仓修改", key="save_stocks", disabled=(not cash_account_names[0] != "-")):
                edited_list = edited_df.dropna(subset=['ticker', 'quantity', 'average_cost']).to_dict('records')
                
                # Auto-fetch currency for new tickers
                original_map = {s['ticker']: s for s in deepcopy(user_portfolio.get("stocks", []))}
                invalid_new_tickers = []
                for holding in edited_list:
                    holding['ticker'] = holding['ticker'].upper()
                    if (holding['ticker'] not in original_map) or (not holding.get('currency')):
                        with st.spinner(f"正在验证 {holding['ticker']}..."):
                            profile = get_stock_profile_yf(holding['ticker'])
                        if profile and profile.get('currency'):
                            holding['currency'] = profile['currency'].upper()
                        else:
                            invalid_new_tickers.append(holding['ticker'])
                if invalid_new_tickers:
                    st.error(f"以下新增的代码无效或无法获取信息: {', '.join(invalid_new_tickers)}")
                    st.stop()
                
                # Diff logic
                stock_after_df = pd.DataFrame(edited_list).set_index('ticker')
                diff_df = stock_before_df.merge(stock_after_df, on='ticker', how='outer', suffixes=('_old', '_new'))
                cash_acct = get_cash_account(cash_account_stock)
                
                for ticker, row in diff_df.iterrows():
                    qty_old = row.get('quantity_old', 0)
                    qty_new = row.get('quantity_new', 0)
                    cost_old = row.get('average_cost_old', 0)
                    cost_new = row.get('average_cost_new', 0)
                    currency = row.get('currency_new', row.get('currency_old', 'USD'))

                    qty_diff = qty_new - qty_old
                    
                    if pd.isna(qty_old): # New holding (Buy)
                        amount = qty_new * cost_new
                        cash_acct['balance'] -= (amount / exchange_rates.get(currency, 1)) * exchange_rates.get(cash_acct['currency'], 1)
                        add_transaction(f"[自动] 买入 {ticker}", "买入股票", amount, cash_acct['currency'], cash_acct['name'], ticker, qty_new)
                    
                    elif pd.isna(qty_new): # Sold all (Sell)
                        current_price = prices.get(ticker, 0)
                        amount = qty_old * current_price # Sell at market price
                        realized_pl = (current_price - cost_old) * qty_old
                        cash_acct['balance'] += (amount / exchange_rates.get(currency, 1)) * exchange_rates.get(cash_acct['currency'], 1)
                        add_transaction(f"[自动] 卖出 {ticker}", "卖出股票", amount, cash_acct['currency'], cash_acct['name'], ticker, qty_old, realized_pl, currency)

                    elif qty_diff > 0: # Bought more
                        cost_basis_old = qty_old * cost_old
                        cost_basis_new = qty_new * cost_new
                        amount = cost_basis_new - cost_basis_old # Inferred cost
                        cash_acct['balance'] -= (amount / exchange_rates.get(currency, 1)) * exchange_rates.get(cash_acct['currency'], 1)
                        add_transaction(f"[自动] 买入 {ticker}", "买入股票", amount, cash_acct['currency'], cash_acct['name'], ticker, qty_diff)

                    elif qty_diff < 0: # Sold some
                        qty_sold = abs(qty_diff)
                        current_price = prices.get(ticker, 0)
                        amount = qty_sold * current_price # Sell at market price
                        realized_pl = (current_price - cost_old) * qty_sold # P/L based on original avg cost
                        cash_acct['balance'] += (amount / exchange_rates.get(currency, 1)) * exchange_rates.get(cash_acct['currency'], 1)
                        add_transaction(f"[自动] 卖出 {ticker}", "卖出股票", amount, cash_acct['currency'], cash_acct['name'], ticker, qty_sold, realized_pl, currency)

                user_portfolio["stocks"] = edited_list
                if save_user_profile(st.session_state.user_email, user_profile): st.success("股票持仓已更新，并已自动生成流水！"); time.sleep(1); st.rerun()

        with edit_tabs[3]:
            schema = {'symbol': 'object', 'quantity': 'float64', 'average_cost': 'float64'}
            df = to_df_with_schema(user_portfolio.get("crypto",[]), schema)
            
            # Store 'before' state
            crypto_before_df = df.copy().set_index('symbol')

            calc_height = max(200, (len(df) + 6) * 35 + 3)
            edited_df = st.data_editor(df, num_rows="dynamic", key="crypto_editor_adv", column_config={"symbol": st.column_config.TextColumn("代码", required=True), "quantity": st.column_config.NumberColumn("数量", format="%.8f", required=True), "average_cost": st.column_config.NumberColumn("平均成本 (USD)", format="%.2f", required=True)}, use_container_width=True, hide_index=True, height=calc_height)

            # --- MODIFICATION: Added cash account selector ---
            cash_account_crypto = st.selectbox("选择关联的现金账户（用于自动流水）", cash_account_names, key="cash_crypto_link", disabled=(not cash_account_names[0] != "-"))

            if st.button("💾 保存加密货币修改", key="save_crypto", disabled=(not cash_account_names[0] != "-")):
                edited_list = edited_df.dropna(subset=['symbol', 'quantity', 'average_cost']).to_dict('records')
                for holding in edited_list: holding['symbol'] = holding['symbol'].upper()
                
                # Diff logic
                crypto_after_df = pd.DataFrame(edited_list).set_index('symbol')
                diff_df = crypto_before_df.merge(crypto_after_df, on='symbol', how='outer', suffixes=('_old', '_new'))
                cash_acct = get_cash_account(cash_account_crypto)
                # Crypto is simpler, avg_cost and prices are all USD
                
                for symbol, row in diff_df.iterrows():
                    qty_old = row.get('quantity_old', 0)
                    qty_new = row.get('quantity_new', 0)
                    cost_old = row.get('average_cost_old', 0)
                    cost_new = row.get('average_cost_new', 0)
                    currency = "USD" # Crypto cost basis is USD

                    qty_diff = qty_new - qty_old
                    
                    if pd.isna(qty_old): # New holding (Buy)
                        amount = qty_new * cost_new
                        cash_acct['balance'] -= (amount / exchange_rates.get(currency, 1)) * exchange_rates.get(cash_acct['currency'], 1)
                        add_transaction(f"[自动] 买入 {symbol}", "买入加密货币", amount, cash_acct['currency'], cash_acct['name'], symbol, qty_new)
                    
                    elif pd.isna(qty_new): # Sold all (Sell)
                        current_price = prices.get(symbol, 0)
                        amount = qty_old * current_price # Sell at market price
                        realized_pl = (current_price - cost_old) * qty_old
                        cash_acct['balance'] += (amount / exchange_rates.get(currency, 1)) * exchange_rates.get(cash_acct['currency'], 1)
                        add_transaction(f"[自动] 卖出 {symbol}", "卖出加密货币", amount, cash_acct['currency'], cash_acct['name'], symbol, qty_old, realized_pl, currency)

                    elif qty_diff > 0: # Bought more
                        cost_basis_old = qty_old * cost_old
                        cost_basis_new = qty_new * cost_new
                        amount = cost_basis_new - cost_basis_old # Inferred cost
                        cash_acct['balance'] -= (amount / exchange_rates.get(currency, 1)) * exchange_rates.get(cash_acct['currency'], 1)
                        add_transaction(f"[自动] 买入 {symbol}", "买入加密货币", amount, cash_acct['currency'], cash_acct['name'], symbol, qty_diff)

                    elif qty_diff < 0: # Sold some
                        qty_sold = abs(qty_diff)
                        current_price = prices.get(symbol, 0)
                        amount = qty_sold * current_price # Sell at market price
                        realized_pl = (current_price - cost_old) * qty_sold # P/L based on original avg cost
                        cash_acct['balance'] += (amount / exchange_rates.get(currency, 1)) * exchange_rates.get(cash_acct['currency'], 1)
                        add_transaction(f"[自动] 卖出 {symbol}", "卖出加密货币", amount, cash_acct['currency'], cash_acct['name'], symbol, qty_sold, realized_pl, currency)

                user_portfolio["crypto"] = edited_list
                if save_user_profile(st.session_state.user_email, user_profile): st.success("加密货币持仓已更新，并已自动生成流水！"); time.sleep(1); st.rerun()
        
        with edit_tabs[4]:
            st.info("记录您持有的实物或纸黄金。成本价请以美元/克计价。")
            schema = {'grams': 'float64', 'average_cost_per_gram': 'float64'}
            df = to_df_with_schema(user_portfolio.get("gold",[]), schema)
            
            # Store 'before' state
            gold_before_df = pd.DataFrame(user_portfolio.get("gold",[])) # Gold is a list of dicts, no unique index

            calc_height = max(200, (len(df) + 6) * 35 + 3)
            edited_df = st.data_editor(df, num_rows="dynamic", key="gold_editor_adv", column_config={"grams": st.column_config.NumberColumn("克数 (g)", format="%.3f", required=True), "average_cost_per_gram": st.column_config.NumberColumn("平均成本 ($/g)", format="%.2f", required=True)}, use_container_width=True, hide_index=True, height=calc_height)
            
            # --- MODIFICATION: Added cash account selector ---
            cash_account_gold = st.selectbox("选择关联的现金账户（用于自动流水）", cash_account_names, key="cash_gold_link", disabled=(not cash_account_names[0] != "-"))
            
            if st.button("💾 保存黄金持仓修改", key="save_gold", disabled=(not cash_account_names[0] != "-")):
                edited_list = edited_df.dropna(subset=['grams', 'average_cost_per_gram']).to_dict('records')
                
                # Diff logic for Gold (sum based)
                grams_old = gold_before_df['grams'].sum() if not gold_before_df.empty else 0
                cost_basis_old = (gold_before_df['grams'] * gold_before_df['average_cost_per_gram']).sum() if not gold_before_df.empty else 0
                avg_cost_old = (cost_basis_old / grams_old) if grams_old > 0 else 0

                gold_after_df = pd.DataFrame(edited_list)
                grams_new = gold_after_df['grams'].sum() if not gold_after_df.empty else 0
                cost_basis_new = (gold_after_df['grams'] * gold_after_df['average_cost_per_gram']).sum() if not gold_after_df.empty else 0
                avg_cost_new = (cost_basis_new / grams_new) if grams_new > 0 else 0

                cash_acct = get_cash_account(cash_account_gold)
                currency = "USD" # Gold cost basis is USD
                qty_diff = grams_new - grams_old

                if qty_diff > 0: # Bought Gold
                    amount = cost_basis_new - cost_basis_old
                    cash_acct['balance'] -= (amount / exchange_rates.get(currency, 1)) * exchange_rates.get(cash_acct['currency'], 1)
                    add_transaction(f"[自动] 买入黄金", "买入黄金", amount, cash_acct['currency'], cash_acct['name'], "GOLD (g)", qty_diff)
                
                elif qty_diff < 0: # Sold Gold
                    qty_sold = abs(qty_diff)
                    current_price = gold_price_per_gram
                    amount = qty_sold * current_price # Sell at market price
                    realized_pl = (current_price - avg_cost_old) * qty_sold
                    cash_acct['balance'] += (amount / exchange_rates.get(currency, 1)) * exchange_rates.get(cash_acct['currency'], 1)
                    add_transaction(f"[自动] 卖出黄金", "卖出黄金", amount, cash_acct['currency'], cash_acct['name'], "GOLD (g)", qty_sold, realized_pl, currency)

                user_portfolio["gold"] = edited_list
                if save_user_profile(st.session_state.user_email, user_profile): st.success("黄金持仓已更新，并已自动生成流水！"); time.sleep(1); st.rerun()

        st.subheader("📑 交易流水")
        transactions = user_profile.get("transactions", [])
        if transactions:
            transactions_df = pd.DataFrame(transactions).sort_values(by="date", ascending=False)
            # Format columns for better display
            if 'symbol' not in transactions_df.columns: transactions_df['symbol'] = None
            if 'quantity' not in transactions_df.columns: transactions_df['quantity'] = None
            if 'realized_pl' not in transactions_df.columns: transactions_df['realized_pl'] = None
            
            display_cols = ["date", "type", "description", "amount", "currency", "account", "symbol", "quantity", "realized_pl"]
            # Filter out columns that are entirely empty
            display_cols = [col for col in display_cols if col in transactions_df.columns and not transactions_df[col].isnull().all()]
            
            st.table(transactions_df[display_cols])
        else:
            st.write("暂无交易记录。")


    # --- MODIFICATION: This is now tab3 (formerly tab4) ---
    with tab3:
        st.subheader("📈 资产历史趋势")

        chart_type = st.radio(
            "选择图表类型",
            ('市值', '回报率 (%)'),
            horizontal=True,
            key='history_chart_type'
        )

        if history_df.empty or len(history_df.index) < 2:
            st.info("历史数据不足（少于2天），无法生成图表。")
        else:
            with st.spinner("正在生成历史趋势图..."):
                plot_df = history_df.copy()
                
                if chart_type == '回报率 (%)':
                    # Normalize data to show percentage change
                    plot_df = (plot_df / plot_df.iloc[0]) * 100
                    yaxis_title = "回报率 (%)"
                    hovertemplate_prefix = ""
                    hovertemplate_suffix = "%"
                else: # Default is '市值'
                    plot_df = plot_df.mul(display_rate)
                    yaxis_title = f"市值 ({display_symbol})"
                    hovertemplate_prefix = display_symbol
                    hovertemplate_suffix = f" {display_curr}"

                fig = go.Figure()
                categories = {
                    'net_worth_usd': '总净资产',
                    'stock_value_usd': '股票',
                    'crypto_value_usd': '加密货币',
                    'gold_value_usd': '黄金',
                    'cash_value_usd': '现金'
                }
                
                # Store colors to match text with lines
                colors = px.colors.qualitative.Plotly
                
                for i, (key, name) in enumerate(categories.items()):
                    color = colors[i % len(colors)]
                    fig.add_trace(go.Scatter(
                        x=plot_df.index,
                        y=plot_df[key],
                        mode='lines',
                        name=name,
                        line=dict(color=color), # Assign color
                        hovertemplate=f"日期: %{{x|%Y-%m-%d}}<br>{name}: {hovertemplate_prefix}%{{y:,.2f}}{hovertemplate_suffix}<extra></extra>"
                    ))

                    # Add text label for the last point
                    last_val = plot_df[key].iloc[-1]
                    text_label = f"{hovertemplate_prefix}{last_val:,.2f}{hovertemplate_suffix}"
                    if chart_type == '回报率 (%)':
                         text_label = f"{last_val:,.2f}{hovertemplate_suffix}"

                    fig.add_trace(go.Scatter(
                        x=[plot_df.index[-1]],
                        y=[last_val],
                        text=[text_label],
                        mode='text',
                        textposition='middle right',
                        textfont=dict(color=color, size=12),
                        showlegend=False,
                        hoverinfo='none'
                    ))
                
                fig.update_layout(
                    title_text=f"资产{chart_type}历史趋势",
                    yaxis_title=yaxis_title,
                    hovermode="x unified",
                    margin=dict(r=100) # Add right margin to make space for text
                )
                st.plotly_chart(fig, use_container_width=True)
                
    # --- MODIFICATION: This is now tab4 (formerly tab5) ---
    with tab4:
        st.subheader("🤖 AI 深度分析")
        st.info("此功能会将您匿名的持仓明细发送给AI进行全面分析，以提供更具洞察力的建议。")
        
        stock_table = pd.DataFrame(stock_df_data).to_markdown(index=False)
        gold_table = pd.DataFrame(gold_df_data).to_markdown(index=False)
        crypto_table = pd.DataFrame(crypto_df_data).to_markdown(index=False)
        cash_table = pd.DataFrame([{"账户名称": acc['name'], "货币": acc['currency'], "余额": f"{acc['balance']:,.2f}"} for acc in cash_accounts]).to_markdown(index=False)
        liabilities_table = pd.DataFrame([{"名称": liab['name'], "货币": liab['currency'], "金额": f"{liab['balance']:,.2f}"} for liab in liabilities]).to_markdown(index=False)

        prompt = f"""# 角色
你是一位资深、专业的中文投资组合分析师。你的任务是为客户提供详细、专业且易于理解的投资组合诊断报告。

# 输出要求
- **语言**: 全程必须使用**简体中文**进行分析和回答。
- **格式**: 使用Markdown格式，分点阐述，条理清晰。
- **语气**: 专业、客观、鼓励，并提供可执行的建议。
- **详细程度**: 对每个分析要点进行详细阐述，不要只给出结论，要解释原因。

# 核心分析任务
请根据下面提供的匿名投资组合数据，完成一份详细的诊断报告，报告需包含以下部分：
1.  **总体概览**: 对当前资产规模、净资产、负债水平和资产构成进行简要总结。
2.  **投资组合优点 (Strengths)**: 找出当前持仓中值得肯定的地方（例如，良好的多元化、持有了优质资产等）。
3.  **潜在风险与弱点 (Weaknesses & Risks)**: 识别并详细说明当前投资组合存在的问题，例如：
    * **集中度风险**: 是否有单一资产（股票或加密货币）或单一行业占比过高？
    * **流动性分析**: 现金及高流动性资产的比例是否合理？
    * **资产质量**: 持仓中是否有表现不佳或基本面存在问题的资产？
4.  **具体优化建议**: 提供3-5条具体的、可立即执行的调整建议。例如：“建议考虑减持部分 [某股票]，因为它在您的投资组合中占比已超过XX%，风险过于集中。可以将资金再平衡到 [某行业/ETF] 以提高多元化。”

---

# 客户的匿名投资组合数据
(所有金额单位均为 {display_curr})

## 财务摘要
- **总资产**: {display_symbol}{total_assets_usd * display_rate:,.2f}
- **总负债**: {display_symbol}{total_liabilities_usd * display_rate:,.2f}
- **净资产**: {display_symbol}{net_worth_usd * display_rate:,.2f}

## 详细持仓

### 股票持仓
{stock_table}

### 黄金持仓
{gold_table}

### 加密货币持仓
{crypto_table}

### 现金账户
{cash_table}

### 负债情况
{liabilities_table}
"""
        if st.button("开始 AI 分析"):
            with st.spinner("正在调用 AI 进行深度分析，请稍候..."):
                analysis_result = get_detailed_ai_analysis(prompt)
                st.markdown(analysis_result)

# --- Main App Logic ---
check_session_from_query_params()

if not st.session_state.get('logged_in', False):
    display_login_form()
    st.info("👋 欢迎使用专业投资分析仪表盘，请使用您的邮箱登录或注册。")
else:
    display_dashboard()


