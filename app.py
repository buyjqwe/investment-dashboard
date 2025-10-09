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

# --- 页面基础设置 ---
st.set_page_config(
    page_title="个人资产仪表盘",
    page_icon="💰",
    layout="wide"
)

# --- 全局常量 ---
SUPPORTED_CURRENCIES = ["USD", "CNY", "EUR", "HKD", "JPY", "GBP"]
CURRENCY_SYMBOLS = {"USD": "$", "CNY": "¥", "EUR": "€", "HKD": "HK$", "JPY": "¥", "GBP": "£"}
SESSION_EXPIRATION_DAYS = 7
DATA_REFRESH_INTERVAL_SECONDS = 3600 # 1 hour

# --- 初始化 Session State ---
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'user_email' not in st.session_state:
    st.session_state.user_email = ""
if 'login_step' not in st.session_state:
    st.session_state.login_step = "enter_email"
if 'display_currency' not in st.session_state:
    st.session_state.display_currency = "USD"
if 'last_market_data_fetch' not in st.session_state:
    st.session_state.last_market_data_fetch = 0

# --- 微软 Graph API 配置 ---
MS_GRAPH_CONFIG = st.secrets["microsoft_graph"]
ADMIN_EMAIL = MS_GRAPH_CONFIG["admin_email"]
ONEDRIVE_FILE_PATH = MS_GRAPH_CONFIG["onedrive_user_file_path"]
ONEDRIVE_API_URL = f"https://graph.microsoft.com/v1.0/users/{MS_GRAPH_CONFIG['sender_email']}/drive/{ONEDRIVE_FILE_PATH}"


# --- 核心功能函数定义 ---

@st.cache_data(ttl=3500)
def get_ms_graph_token():
    url = f"https://login.microsoftonline.com/{MS_GRAPH_CONFIG['tenant_id']}/oauth2/v2.0/token"
    data = {"grant_type": "client_credentials", "client_id": MS_GRAPH_CONFIG['client_id'], "client_secret": MS_GRAPH_CONFIG['client_secret'], "scope": "https://graph.microsoft.com/.default"}
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

@st.cache_data(ttl=60)
def get_user_data_from_onedrive():
    try:
        token = get_ms_graph_token()
        headers = {"Authorization": f"Bearer {token}"}
        content_url = f"{ONEDRIVE_API_URL}:/content"
        resp = requests.get(content_url, headers=headers)
        if resp.status_code == 404:
            initial_data = {
                "users": {
                    ADMIN_EMAIL: {
                        "role": "admin", 
                        "portfolio": {
                            "stocks": [{"ticker": "TSLA", "quantity": 10, "currency": "USD"}], 
                            "cash_accounts": [{"name": "默认现金", "balance": 50000, "currency": "USD"}],
                            "crypto": [{"symbol": "BTC", "quantity": 0.5}, {"symbol": "ETH", "quantity": 10}]
                        }, 
                        "transactions": [], 
                        "asset_history": []
                    }
                }, 
                "codes": {}, 
                "sessions": {}
            }
            save_user_data_to_onedrive(initial_data)
            return initial_data
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        st.error(f"从 OneDrive 加载用户数据失败: {e}")
        return None

def save_user_data_to_onedrive(data):
    try:
        token = get_ms_graph_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        content_url = f"{ONEDRIVE_API_URL}:/content"
        resp = requests.put(content_url, headers=headers, data=json.dumps(data, indent=2))
        resp.raise_for_status()
        st.cache_data.clear() # Clear cache after saving
        return True
    except Exception as e:
        st.error(f"保存用户数据到 OneDrive 失败: {e}")
        return False

def send_verification_code(email, code):
    try:
        token = get_ms_graph_token()
        url = f"https://graph.microsoft.com/v1.0/users/{MS_GRAPH_CONFIG['sender_email']}/sendMail"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {"message": {"subject": f"[{code}] 您的登录/注册验证码", "body": {"contentType": "Text", "content": f"您的验证码是：{code}，5分钟内有效。"}, "toRecipients": [{"emailAddress": {"address": email}}]}, "saveToSentItems": "true"}
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        return True
    except Exception as e:
        st.error(f"邮件发送失败: {e}")
        return False

def handle_send_code(email):
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        st.sidebar.error("请输入有效的邮箱地址。")
        return
    user_data = get_user_data_from_onedrive()
    if user_data is None: return
    code = str(random.randint(100000, 999999))
    user_data["codes"][email] = {"code": code, "expires_at": time.time() + 300}
    if not save_user_data_to_onedrive(user_data): return
    if not send_verification_code(email, code): return
    st.sidebar.success("验证码已发送，请查收。")
    st.session_state.login_step = "enter_code"
    st.session_state.temp_email = email
    st.rerun()

def handle_verify_code(email, code):
    user_data = get_user_data_from_onedrive()
    if user_data is None: return
    code_info = user_data.get("codes", {}).get(email)
    if not code_info or time.time() > code_info["expires_at"]:
        st.sidebar.error("验证码已过期或不存在。")
        return
    if code_info["code"] == code:
        if email not in user_data["users"]:
            user_data["users"][email] = {"role": "user", "portfolio": {"stocks": [{"ticker": "AAPL", "quantity": 10, "currency": "USD"}, {"ticker": "GOOG", "quantity": 5, "currency": "USD"}], "cash_accounts": [{"name": "美元银行卡", "balance": 10000, "currency": "USD"}, {"name": "人民币支付宝", "balance": 2000, "currency": "CNY"}], "crypto": [{"symbol": "BTC", "quantity": 1}]}, "transactions": [], "asset_history": []}
            st.toast("🎉 注册成功！已为您创建新账户。")
        
        token = secrets.token_hex(16)
        expires_at = time.time() + (SESSION_EXPIRATION_DAYS * 24 * 60 * 60)
        user_data.setdefault("sessions", {})[token] = {"email": email, "expires_at": expires_at}
        
        del user_data["codes"][email]
        save_user_data_to_onedrive(user_data)
        
        st.session_state.logged_in = True
        st.session_state.user_email = email
        st.session_state.login_step = "logged_in"
        st.query_params["session_token"] = token
        st.rerun() # 强制刷新以加载仪表盘
    else:
        st.sidebar.error("验证码错误。")

def check_session_from_query_params():
    """Checks for a session token in URL params to restore login state on refresh."""
    if st.session_state.get('logged_in'):
        return
    token = st.query_params.get("session_token")
    if not token:
        return
    user_data = get_user_data_from_onedrive()
    if not user_data:
        return
    sessions = user_data.setdefault("sessions", {})
    session_info = sessions.get(token)
    if session_info and time.time() < session_info.get("expires_at", 0):
        st.session_state.logged_in = True
        st.session_state.user_email = session_info["email"]
        st.session_state.login_step = "logged_in"
    elif "session_token" in st.query_params:
        st.query_params.clear()

def get_all_market_data(stock_tickers, crypto_symbols):
    """Fetches latest prices for stocks and crypto using FMP API."""
    market_data = {}
    api_key = st.secrets["financialmodelingprep"]["api_key"]

    # Fetch Stocks
    if stock_tickers:
        ticker_string = ",".join(stock_tickers)
        url = f"https://financialmodelingprep.com/api/v3/quote/{ticker_string}?apikey={api_key}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            price_map = {item['symbol']: item for item in data}
            for ticker in stock_tickers:
                if ticker in price_map:
                    market_data[ticker] = {"latest_price": price_map[ticker].get('price', 0)}
                else:
                    st.warning(f"获取 {ticker} 股价失败: API未返回该代码的数据。")
                    market_data[ticker] = None
        except Exception as e:
            st.error(f"获取股价时发生网络错误: {e}")
            for ticker in stock_tickers:
                market_data[ticker] = None

    # Fetch Crypto
    if crypto_symbols:
        # FMP uses SYMBOLUSD format for crypto
        crypto_ticker_string = ",".join([f"{symbol}USD" for symbol in crypto_symbols])
        url = f"https://financialmodelingprep.com/api/v3/quote/{crypto_ticker_string}?apikey={api_key}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            price_map = {item['symbol'].replace('USD', ''): item for item in data}
            for symbol in crypto_symbols:
                if symbol in price_map:
                    market_data[symbol] = {"latest_price": price_map[symbol].get('price', 0)}
                else:
                    st.warning(f"获取 {symbol} 价格失败: API未返回该代码的数据。")
                    market_data[symbol] = None
        except Exception as e:
            st.error(f"获取加密货币价格时发生网络错误: {e}")
            for symbol in crypto_symbols:
                market_data[symbol] = None
            
    return market_data

def get_prices_from_cache(market_data):
    """Extracts latest prices from the cached market data."""
    prices = {}
    for ticker, data in market_data.items():
        prices[ticker] = data["latest_price"] if data else 0
    return prices

@st.cache_data(ttl=3600)
def get_historical_asset_price(symbol, date_str, asset_type='stock'):
    """Fetches historical price for a given date using FMP for stock or crypto."""
    api_key = st.secrets["financialmodelingprep"]["api_key"]
    
    api_symbol = f"{symbol}USD" if asset_type == 'crypto' else symbol
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{api_symbol}?apikey={api_key}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if 'historical' in data and data['historical']:
            # Find the closest date if the exact date is not available
            for item in data['historical']:
                if item['date'] <= date_str:
                    return item.get('close', 0)
            return data['historical'][-1].get('close', 0) # return oldest if all are newer
        return 0
    except Exception:
        return 0

def get_exchange_rates(base_currency='USD'):
    """Fetches latest exchange rates."""
    try:
        url = f"https://open.er-api.com/v6/latest/{base_currency}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get("rates") if data.get("result") == "success" else None
    except Exception as e:
        st.error(f"获取汇率失败: {e}")
        return None

def update_asset_snapshot(user_data, email, total_assets_usd, total_stock_value_usd, total_cash_balance_usd, total_crypto_value_usd, current_rates):
    today_str = datetime.now().strftime("%Y-%m-%d")
    user_profile = user_data["users"][email]
    asset_history = user_profile.setdefault("asset_history", [])
    
    # Update today's snapshot if it exists, otherwise create a new one
    if asset_history and asset_history[-1]["date"] == today_str:
        asset_history[-1].update({
            "total_assets_usd": total_assets_usd,
            "total_stock_value_usd": total_stock_value_usd,
            "total_cash_balance_usd": total_cash_balance_usd,
            "total_crypto_value_usd": total_crypto_value_usd,
            "exchange_rates": current_rates,
            "portfolio": user_profile["portfolio"]
        })
    else:
        snapshot = {
            "date": today_str,
            "total_assets_usd": total_assets_usd,
            "total_stock_value_usd": total_stock_value_usd,
            "total_cash_balance_usd": total_cash_balance_usd,
            "total_crypto_value_usd": total_crypto_value_usd,
            "exchange_rates": current_rates,
            "portfolio": user_profile["portfolio"]
        }
        asset_history.append(snapshot)
        st.toast("今日资产快照已生成！")

    save_user_data_to_onedrive(user_data)


# --- UI 渲染函数 ---

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
                st.session_state.login_step = "enter_email"; st.rerun()

def display_admin_panel():
    with st.sidebar:
        st.header("👑 管理员面板")
        user_data = get_user_data_from_onedrive()
        if user_data is None: return
        with st.expander("管理所有用户"):
            all_users = list(user_data.get("users", {}).keys())
            st.write(f"当前总用户数: {len(all_users)}")
            for user_email in all_users:
                if user_email != ADMIN_EMAIL:
                    col1, col2 = st.columns([3, 1])
                    col1.write(user_email)
                    if col2.button("删除", key=f"del_{user_email}"):
                        del user_data["users"][user_email]
                        if save_user_data_to_onedrive(user_data):
                            st.toast(f"用户 {user_email} 已删除。"); st.rerun()

def display_asset_allocation_chart(stock_usd, cash_usd, crypto_usd, display_curr, display_rate, display_symbol):
    labels = ['股票', '现金', '加密货币']
    values_usd = [stock_usd, cash_usd, crypto_usd]
    
    non_zero_labels = [label for label, value in zip(labels, values_usd) if value > 0.01]
    non_zero_values = [value for value in values_usd if value > 0.01]
    
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

def display_analysis_tab(user_data, email, display_curr, display_symbol, display_rate):
    st.subheader("🔍 资产变动归因分析")
    asset_history = user_data["users"][email].get("asset_history", [])
    if len(asset_history) < 2:
        st.info("历史数据不足（少于2天），暂无法进行分析。"); return
    
    options = [7, 15, 30, 60]
    period_days = st.selectbox("选择分析周期（天）", options, index=0)
    end_snapshot = asset_history[-1]
    start_date = (datetime.strptime(end_snapshot["date"], "%Y-%m-%d") - timedelta(days=period_days)).strftime("%Y-%m-%d")
    
    start_snapshot = next((s for s in reversed(asset_history) if s["date"] <= start_date), None)
    if not start_snapshot:
        st.warning(f"未找到 {period_days} 天前的资产快照，无法进行精确比较。"); return

    total_change_usd = end_snapshot["total_assets_usd"] - start_snapshot["total_assets_usd"]
    end_prices = get_prices_from_cache(st.session_state.get('market_data', {}))
    
    # 1. Market Fluctuation
    market_change_usd = 0
    start_portfolio = start_snapshot.get("portfolio", {})
    end_portfolio = end_snapshot.get("portfolio", {})
    
    # Stocks
    for ticker in set(s['ticker'] for s in start_portfolio.get("stocks", []) + end_portfolio.get("stocks", [])):
        start_h = next((s for s in start_portfolio.get("stocks", []) if s["ticker"] == ticker), {"quantity": 0})
        end_h = next((s for s in end_portfolio.get("stocks", []) if s["ticker"] == ticker), {"quantity": 0})
        common_qty = min(start_h["quantity"], end_h["quantity"])
        if common_qty > 0:
            start_price = get_historical_asset_price(ticker, start_snapshot["date"], 'stock')
            price_change_local = common_qty * (end_prices.get(ticker, 0) - start_price)
            market_change_usd += price_change_local / start_snapshot.get("exchange_rates", {}).get(start_h.get("currency", "USD"), 1)

    # Crypto
    for symbol in set(c['symbol'] for c in start_portfolio.get("crypto", []) + end_portfolio.get("crypto", [])):
        start_h = next((c for c in start_portfolio.get("crypto", []) if c["symbol"] == symbol), {"quantity": 0})
        end_h = next((c for c in end_portfolio.get("crypto", []) if c["symbol"] == symbol), {"quantity": 0})
        common_qty = min(start_h["quantity"], end_h["quantity"])
        if common_qty > 0:
            start_price = get_historical_asset_price(symbol, start_snapshot["date"], 'crypto')
            market_change_usd += common_qty * (end_prices.get(symbol, 0) - start_price)
            
    # 2. Cash Flow
    cash_flow_usd = 0
    transactions = user_data["users"][email].get("transactions", [])
    for trans in transactions:
        trans_date = datetime.strptime(trans["date"].split(" ")[0], "%Y-%m-%d")
        if start_snapshot["date"] < trans_date.strftime("%Y-%m-%d") <= end_snapshot["date"]:
            amount = trans.get("amount", 0)
            rate = start_snapshot.get("exchange_rates", {}).get(trans.get("currency", "USD"), 1)
            if trans["type"] in ["收入"]: cash_flow_usd += amount / rate
            elif trans["type"] in ["支出"]: cash_flow_usd -= amount / rate
    
    # 3. FX Fluctuation
    fx_change_usd = 0
    for acc in start_portfolio.get("cash_accounts", []):
        currency = acc.get("currency", "USD")
        if currency != 'USD':
            start_rate = start_snapshot.get("exchange_rates", {}).get(currency, 1)
            end_rate = end_snapshot.get("exchange_rates", {}).get(currency, 1)
            if start_rate and end_rate:
                balance_change_due_to_fx = acc.get("balance", 0) * ((1/end_rate) - (1/start_rate))
                fx_change_usd += balance_change_due_to_fx

    st.metric(f"期间总资产变化 ({display_curr})", f"{display_symbol}{total_change_usd * display_rate:,.2f}", f"{display_symbol}{(total_change_usd - market_change_usd - cash_flow_usd - fx_change_usd) * display_rate:,.2f} (交易盈亏与其他)")
    col1, col2, col3 = st.columns(3)
    col1.metric("📈 市场波动", f"{display_symbol}{market_change_usd * display_rate:,.2f}")
    col2.metric("💸 资金流动", f"{display_symbol}{cash_flow_usd * display_rate:,.2f}")
    col3.metric("💱 汇率影响", f"{display_symbol}{fx_change_usd * display_rate:,.2f}")

def display_asset_charts_tab(user_data, email, display_curr, display_symbol, display_rate):
    asset_history = user_data["users"][email].get("asset_history", [])
    if not asset_history:
        st.info("暂无历史数据，无法生成图表。"); return

    history_df = pd.DataFrame(asset_history)
    history_df['date'] = pd.to_datetime(history_df['date'])
    history_df = history_df.set_index('date')
    
    chart_cols = ["total_assets_usd", "total_stock_value_usd", "total_cash_balance_usd", "total_crypto_value_usd"]
    for col in chart_cols:
        if col in history_df.columns:
            history_df[col.replace("_usd", f"_{display_curr.lower()}")] = history_df[col] * display_rate

    st.subheader(f"总资产历史趋势 ({display_curr})")
    st.area_chart(history_df[f"total_assets_{display_curr.lower()}"])
    st.subheader(f"股票市值历史趋势 ({display_curr})")
    st.area_chart(history_df[f"total_stock_value_{display_curr.lower()}"])
    st.subheader(f"加密货币市值历史趋势 ({display_curr})")
    st.area_chart(history_df[f"total_crypto_value_{display_curr.lower()}"])
    st.subheader(f"现金总额历史趋势 ({display_curr})")
    st.area_chart(history_df[f"total_cash_balance_{display_curr.lower()}"])

def display_dashboard():
    st.title(f"💰 {st.session_state.user_email} 的资产仪表盘")
    user_data = get_user_data_from_onedrive()
    if user_data is None: st.stop()

    current_user_email = st.session_state.user_email
    user_portfolio = user_data["users"][current_user_email].setdefault("portfolio", {})
    
    if "crypto" not in user_portfolio:
        user_portfolio["crypto"] = []; save_user_data_to_onedrive(user_data); st.rerun()
    
    stock_holdings = user_portfolio.get("stocks", [])
    cash_accounts = user_portfolio.get("cash_accounts", [])
    crypto_holdings = user_portfolio.get("crypto", [])
    
    stock_tickers = [s['ticker'] for s in stock_holdings if s.get('ticker')]
    crypto_symbols = [c['symbol'] for c in crypto_holdings if c.get('symbol')]
    
    if st.sidebar.button('🔄 刷新市场数据'):
        st.session_state.last_market_data_fetch = 0 # Force refresh
    
    now = time.time()
    if now - st.session_state.last_market_data_fetch > DATA_REFRESH_INTERVAL_SECONDS:
        with st.spinner("正在获取最新市场数据..."):
            st.session_state.market_data = get_all_market_data(stock_tickers, crypto_symbols)
            st.session_state.exchange_rates = get_exchange_rates()
            st.session_state.last_market_data_fetch = now
            st.rerun()
    
    market_data = st.session_state.get('market_data', {})
    prices = get_prices_from_cache(market_data)
    exchange_rates = st.session_state.get('exchange_rates', {})
    if not exchange_rates:
        st.error("无法加载汇率，资产总值不准确。"); st.stop()

    total_stock_value_usd = sum(s['quantity'] * prices.get(s['ticker'], 0) / exchange_rates.get(s.get('currency', 'USD'), 1) for s in stock_holdings)
    total_cash_balance_usd = sum(acc['balance'] / exchange_rates.get(acc.get('currency', 'USD'), 1) for acc in cash_accounts)
    total_crypto_value_usd = sum(c['quantity'] * prices.get(c['symbol'], 0) for c in crypto_holdings)
    total_assets_usd = total_stock_value_usd + total_cash_balance_usd + total_crypto_value_usd

    update_asset_snapshot(user_data, current_user_email, total_assets_usd, total_stock_value_usd, total_cash_balance_usd, total_crypto_value_usd, exchange_rates)

    display_curr = st.sidebar.selectbox("选择显示货币", options=SUPPORTED_CURRENCIES, key="display_currency")
    display_rate = exchange_rates.get(display_curr, 1)
    display_symbol = CURRENCY_SYMBOLS.get(display_curr, "")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💰 资产总值", f"{display_symbol}{total_assets_usd * display_rate:,.2f} {display_curr}")
    col2.metric("📈 股票市值", f"{display_symbol}{total_stock_value_usd * display_rate:,.2f} {display_curr}")
    col3.metric("🪙 加密货币市值", f"{display_symbol}{total_crypto_value_usd * display_rate:,.2f} {display_curr}")
    col4.metric("💵 现金总额", f"{display_symbol}{total_cash_balance_usd * display_rate:,.2f} {display_curr}")

    tab1, tab2, tab3, tab4 = st.tabs(["📊 持仓与流水", "📈 资产图表", "🔍 归因分析", "⚙️ 管理与交易"])

    with tab1:
        display_asset_allocation_chart(total_stock_value_usd, total_cash_balance_usd, total_crypto_value_usd, display_curr, display_rate, display_symbol)
        
        c1, c2, c3 = st.columns(3)
        with c1:
            st.subheader("📊 股票持仓")
            st.dataframe(pd.DataFrame([{"代码": s['ticker'], "数量": s['quantity'], "货币": s['currency'], "当前价格": f"{CURRENCY_SYMBOLS.get(s['currency'], '')}{prices.get(s['ticker'], 0):,.2f}"} for s in stock_holdings]), use_container_width=True, hide_index=True)
        with c2:
            st.subheader("💵 现金账户")
            st.dataframe(pd.DataFrame([{"账户名称": acc['name'],"货币": acc['currency'], "余额": f"{CURRENCY_SYMBOLS.get(acc['currency'], '')}{acc['balance']:,.2f}"} for acc in cash_accounts]), use_container_width=True, hide_index=True)
        with c3:
            st.subheader("🪙 加密货币持仓")
            st.dataframe(pd.DataFrame([{"代码": c['symbol'], "数量": c['quantity'], "当前价格": f"${prices.get(c['symbol'], 0):,.2f}"} for c in crypto_holdings]), use_container_width=True, hide_index=True)

        st.subheader("📑 最近流水")
        # 增加健壮性：只处理包含'date'键的交易记录，以兼容旧数据格式
        transactions = user_data["users"][current_user_email].setdefault("transactions", [])
        valid_transactions = [t for t in transactions if 'date' in t]
        if valid_transactions:
            st.dataframe(pd.DataFrame(valid_transactions).sort_values(by="date", ascending=False), use_container_width=True, hide_index=True)
        else:
            st.info("您还没有任何有效的流水记录。")


    with tab2:
        display_asset_charts_tab(user_data, current_user_email, display_curr, display_symbol, display_rate)
    with tab3:
        display_analysis_tab(user_data, current_user_email, display_curr, display_symbol, display_rate)
    with tab4:
        st.subheader("⚙️ 资产管理与交易记录")

        m_tab1, m_tab2, m_tab3 = st.tabs(["💵 现金账户", "📈 股票持仓", "🪙 加密货币"])
        with m_tab1:
            edited_cash = st.data_editor(cash_accounts, num_rows="dynamic", key="cash_editor", column_config={"name": "账户名称", "currency": st.column_config.SelectboxColumn("货币", options=SUPPORTED_CURRENCIES, required=True), "balance": st.column_config.NumberColumn("余额", format="%.2f", required=True)}, use_container_width=True)
            if st.button("💾 保存现金账户修改"):
                user_portfolio["cash_accounts"] = [a for a in edited_cash if a.get("name") and a.get("currency")]
                if save_user_data_to_onedrive(user_data): st.success("现金账户已更新！"); time.sleep(1); st.rerun()

        with m_tab2:
            edited_stocks = st.data_editor(stock_holdings, num_rows="dynamic", key="stock_editor", column_config={"ticker": "股票代码", "quantity": st.column_config.NumberColumn("数量", format="%.2f"),"currency": st.column_config.SelectboxColumn("货币", options=SUPPORTED_CURRENCIES, required=True)}, use_container_width=True)
            if st.button("💾 保存股票持仓修改"):
                user_portfolio["stocks"] = [s for s in edited_stocks if s.get("ticker") and s.get("currency")]
                if save_user_data_to_onedrive(user_data): st.success("股票持仓已更新！"); time.sleep(1); st.rerun()
        
        with m_tab3:
            edited_crypto = st.data_editor(crypto_holdings, num_rows="dynamic", key="crypto_editor", column_config={"symbol": "代码", "quantity": st.column_config.NumberColumn("数量", format="%.8f")}, use_container_width=True)
            if st.button("💾 保存加密货币持仓修改"):
                user_portfolio["crypto"] = [c for c in edited_crypto if c.get("symbol")]
                if save_user_data_to_onedrive(user_data): st.success("加密货币持仓已更新！"); time.sleep(1); st.rerun()

        st.write("---")
        st.subheader("✍️ 记录一笔新流水")
        with st.form("transaction_form", clear_on_submit=True):
            trans_type = st.selectbox("类型", ["收入", "支出", "买入股票", "卖出股票", "买入加密货币", "卖出加密货币", "转账"])
            
            col1, col2 = st.columns(2)
            with col1:
                description = st.text_input("描述")
                amount = st.number_input("金额", min_value=0.0, format="%.2f")
                account_names = [acc.get("name", "") for acc in cash_accounts]
                from_account_name = st.selectbox("选择现金账户", options=account_names, key="from_acc") if account_names else None
            with col2:
                symbol, quantity, to_account_name = "", 0.0, None
                if "股票" in trans_type:
                    symbol = st.text_input("股票代码").upper()
                    quantity = st.number_input("数量", min_value=0.0, format="%.2f")
                elif "加密货币" in trans_type:
                    symbol = st.text_input("加密货币代码").upper()
                    quantity = st.number_input("数量", min_value=0.0, format="%.8f")
                elif trans_type == "转账":
                    to_account_name = st.selectbox("转入账户", options=[n for n in account_names if n != from_account_name], key="to_acc")

            if st.form_submit_button("记录流水"):
                if from_account_name is None:
                    st.error("操作失败：请先至少创建一个现金账户。"); st.stop()
                
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
                from_account = next(acc for acc in cash_accounts if acc["name"] == from_account_name)
                
                # Update balances and create transaction record
                if trans_type == "收入":
                    from_account["balance"] += amount
                elif trans_type == "支出":
                    from_account["balance"] -= amount
                elif trans_type == "转账":
                    if to_account_name:
                        to_account = next(acc for acc in cash_accounts if acc["name"] == to_account_name)
                        from_account["balance"] -= amount
                        to_account["balance"] += amount
                elif "买入" in trans_type:
                    from_account["balance"] -= amount
                    if "股票" in trans_type:
                        holding = next((s for s in stock_holdings if s["ticker"] == symbol), None)
                        if holding: holding["quantity"] += quantity
                        else: st.error(f"买入失败: {symbol} 不在您的持仓中。请先在上方添加。"); st.stop()
                    elif "加密货币" in trans_type:
                        holding = next((c for c in crypto_holdings if c["symbol"] == symbol), None)
                        if holding: holding["quantity"] += quantity
                        else: user_portfolio["crypto"].append({"symbol": symbol, "quantity": quantity})
                elif "卖出" in trans_type:
                    from_account["balance"] += amount
                    if "股票" in trans_type:
                        holding = next((s for s in stock_holdings if s["ticker"] == symbol), None)
                        if not holding or holding["quantity"] < quantity: st.error("卖出失败: 数量不足。"); st.stop()
                        holding["quantity"] -= quantity
                    elif "加密货币" in trans_type:
                        holding = next((c for c in crypto_holdings if c["symbol"] == symbol), None)
                        if not holding or holding["quantity"] < quantity: st.error("卖出失败: 数量不足。"); st.stop()
                        holding["quantity"] -= quantity
                
                user_portfolio["stocks"] = [s for s in stock_holdings if s["quantity"] > 0]
                user_portfolio["crypto"] = [c for c in crypto_holdings if c["quantity"] > 0]
                
                new_trans = {"date": now_str, "type": trans_type, "description": description, "amount": amount, "currency": from_account["currency"], "account": from_account_name}
                if symbol: new_trans.update({"symbol": symbol, "quantity": quantity})
                if to_account_name: new_trans.update({"to_account": to_account_name})
                user_data["users"][current_user_email]["transactions"].append(new_trans)

                if save_user_data_to_onedrive(user_data):
                    st.success("流水记录成功！"); time.sleep(1); st.rerun()

# --- 主程序渲染 ---
check_session_from_query_params()
if st.session_state.logged_in:
    with st.sidebar:
        st.success(f"欢迎, {st.session_state.user_email}")
        if st.button("退出登录"):
            token_to_remove = st.query_params.get("session_token")
            user_data = get_user_data_from_onedrive()
            if user_data and token_to_remove in user_data.get("sessions", {}):
                del user_data["sessions"][token_to_remove]
                save_user_data_to_onedrive(user_data)
            for key in list(st.session_state.keys()): del st.session_state[key]
            st.query_params.clear(); st.rerun()
    display_dashboard()
    if st.session_state.user_email == ADMIN_EMAIL:
        display_admin_panel()
else:
    display_login_form()


