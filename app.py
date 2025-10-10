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
import hashlib
from copy import deepcopy
import yfinance as yf # 导入新的库

# --- 页面基础设置 ---
st.set_page_config(
    page_title="专业投资分析仪表盘",
    page_icon="🚀",
    layout="wide"
)

# --- 全局常量 ---
SUPPORTED_CURRENCIES = ["USD", "CNY", "EUR", "HKD", "JPY", "GBP"]
CURRENCY_SYMBOLS = {"USD": "$", "CNY": "¥", "EUR": "€", "HKD": "HK$", "JPY": "¥", "GBP": "£"}
SESSION_EXPIRATION_DAYS = 7
DATA_REFRESH_INTERVAL_SECONDS = 3600 # 1 hour
BASE_ONEDRIVE_PATH = "root:/Apps/StreamlitDashboard" # Base path for structured data

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
if 'migration_done' not in st.session_state:
    st.session_state.migration_done = False


# --- API 配置 ---
MS_GRAPH_CONFIG = st.secrets["microsoft_graph"]
ADMIN_EMAIL = MS_GRAPH_CONFIG["admin_email"]
ONEDRIVE_SENDER_EMAIL = MS_GRAPH_CONFIG['sender_email']
CF_CONFIG = st.secrets["cloudflare"]


# --- 核心功能函数定义 (OneDrive 和用户认证部分保持不变) ---

def get_email_hash(email):
    return hashlib.sha256(email.encode('utf-8')).hexdigest()

@st.cache_data(ttl=3500)
def get_ms_graph_token():
    url = f"https://login.microsoftonline.com/{MS_GRAPH_CONFIG['tenant_id']}/oauth2/v2.0/token"
    data = {"grant_type": "client_credentials", "client_id": MS_GRAPH_CONFIG['client_id'], "client_secret": MS_GRAPH_CONFIG['client_secret'], "scope": "https://graph.microsoft.com/.default"}
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

def onedrive_api_request(method, path, headers, data=None):
    base_url = f"https://graph.microsoft.com/v1.0/users/{ONEDRIVE_SENDER_EMAIL}/drive"
    url = f"{base_url}/{path}"
    if method.lower() == 'get': return requests.get(url, headers=headers)
    if method.lower() == 'put': return requests.put(url, headers=headers, data=data)
    if method.lower() == 'post': return requests.post(url, headers=headers, data=data)
    if method.lower() == 'patch': return requests.patch(url, headers=headers, data=data)
    return None

def get_onedrive_data(path, is_json=True):
    try:
        token = get_ms_graph_token()
        headers = {"Authorization": f"Bearer {token}"}
        resp = onedrive_api_request('get', f"{path}:/content", headers)
        if resp.status_code == 404: return None
        resp.raise_for_status()
        return resp.json() if is_json else resp.text
    except Exception as e:
        if "404" not in str(e): st.error(f"从 OneDrive 加载数据失败 ({path}): {e}")
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

# (认证邮件相关函数保持不变)
def send_verification_code(email, code):
    try:
        token = get_ms_graph_token()
        url = f"https://graph.microsoft.com/v1.0/users/{ONEDRIVE_SENDER_EMAIL}/sendMail"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {"message": {"subject": f"[{code}] 您的登录/注册验证码", "body": {"contentType": "Text", "content": f"您的验证码是：{code}，5分钟内有效。"}, "toRecipients": [{"emailAddress": {"address": email}}]}, "saveToSentItems": "true"}
        requests.post(url, headers=headers, json=payload, timeout=10).raise_for_status()
        return True
    except Exception as e:
        st.error(f"邮件发送失败: {e}"); return False

def handle_send_code(email):
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        st.sidebar.error("请输入有效的邮箱地址。"); return
    codes = get_global_data("codes")
    code = str(random.randint(100000, 999999))
    codes[email] = {"code": code, "expires_at": time.time() + 300}
    if not save_global_data("codes", codes): return
    if not send_verification_code(email, code): return
    st.sidebar.success("验证码已发送，请查收。")
    st.session_state.login_step = "enter_code"; st.session_state.temp_email = email
    st.rerun()

def handle_verify_code(email, code):
    codes = get_global_data("codes")
    code_info = codes.get(email)
    if not code_info or time.time() > code_info["expires_at"]:
        st.sidebar.error("验证码已过期或不存在。"); return
    if code_info["code"] == code:
        user_profile = get_user_profile(email)
        if not user_profile:
            user_profile = {
                "role": "user", "portfolio": {
                    "stocks": [{"ticker": "AAPL", "quantity": 10, "currency": "USD", "average_cost": 150.0}], 
                    "cash_accounts": [{"name": "美元银行卡", "balance": 10000, "currency": "USD"}], 
                    "crypto": [{"symbol": "BTC", "quantity": 0.1, "average_cost": 40000.0}],
                    "liabilities": [{"name": "信用卡", "balance": 500, "currency": "USD"}]
                }, "transactions": []}
            save_user_profile(email, user_profile)
            st.toast("🎉 注册成功！已为您创建新账户。")
        sessions, token = get_global_data("sessions"), secrets.token_hex(16)
        sessions[token] = {"email": email, "expires_at": time.time() + (SESSION_EXPIRATION_DAYS * 24 * 60 * 60)}
        save_global_data("sessions", sessions)
        del codes[email]; save_global_data("codes", codes)
        st.session_state.logged_in, st.session_state.user_email = True, email
        st.session_state.login_step, st.query_params["session_token"] = "logged_in", token
        st.rerun()
    else: st.sidebar.error("验证码错误。")

def check_session_from_query_params():
    if st.session_state.get('logged_in'): return
    token = st.query_params.get("session_token")
    if not token: return
    sessions, session_info = get_global_data("sessions"), sessions.get(token)
    if session_info and time.time() < session_info.get("expires_at", 0):
        st.session_state.logged_in, st.session_state.user_email, st.session_state.login_step = True, session_info["email"], "logged_in"
    elif "session_token" in st.query_params: st.query_params.clear()

# --- NEW: 数据获取函数 (使用 yfinance) ---

@st.cache_data(ttl=3600)
def get_all_market_data_yf(stock_tickers, crypto_symbols):
    """
    使用 yfinance 获取所有资产的最新市场数据。
    """
    market_data = {}
    
    # 转换加密货币代码以适配 yfinance (e.g., BTC -> BTC-USD)
    y_crypto_symbols = [f"{s.upper()}-USD" for s in crypto_symbols]
    
    all_tickers = stock_tickers + y_crypto_symbols
    if not all_tickers:
        return market_data

    try:
        # 一次性获取所有资产的最新价格信息
        data = yf.download(tickers=all_tickers, period="2d", progress=False)
        if data.empty:
            st.warning("无法通过yfinance获取任何市场数据，可能是代码格式问题或网络原因。")
            return {}

        # 获取最新的收盘价
        latest_prices = data['Close'].iloc[-1]

        # 批量获取其他信息（如行业、国家）
        tickers_info = yf.Tickers(all_tickers)
        
        # 处理股票数据
        for ticker in stock_tickers:
            price = latest_prices.get(ticker)
            info = tickers_info.tickers.get(ticker.upper(), {}).info
            market_data[ticker] = {
                "latest_price": price if pd.notna(price) else 0,
                "sector": info.get('sector', 'N/A'),
                "country": info.get('country', 'N/A')
            }

        # 处理加密货币数据
        for original_symbol, y_symbol in zip(crypto_symbols, y_crypto_symbols):
            price = latest_prices.get(y_symbol)
            market_data[original_symbol] = {
                "latest_price": price if pd.notna(price) else 0,
                "sector": "加密货币",
                "country": "N/A"
            }
            
    except Exception as e:
        st.warning(f"使用yfinance获取市场数据时出错: {e}")
        
    return market_data

def get_prices_from_cache(market_data):
    return {ticker: (data["latest_price"] if data and data.get("latest_price") is not None else 0) for ticker, data in market_data.items()}

@st.cache_data(ttl=86400)
def get_stock_profile_yf(symbol):
    """使用 yfinance 获取单个股票的档案信息，主要用于获取货币。"""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        # yfinance 在找不到 ticker 时 info 会为空或缺少关键信息
        if info and 'currency' in info:
            return info
    except Exception:
        return None
    return None

@st.cache_data(ttl=3600)
def get_historical_data_yf(symbol, days=365):
    """使用 yfinance 获取历史数据。"""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=f"{days}d")
        if not hist.empty:
            return hist['Close']
    except Exception:
        return pd.Series()
    return pd.Series()

def get_exchange_rates(base_currency='USD'):
    try:
        resp = requests.get(f"https://open.er-api.com/v6/latest/{base_currency}")
        resp.raise_for_status(); data = resp.json()
        return data.get("rates") if data.get("result") == "success" else None
    except Exception as e:
        st.error(f"获取汇率失败: {e}"); return None
        
# (历史快照和AI分析函数保持不变)
def get_asset_history(email, period_days):
    history = []
    today = datetime.now()
    for i in range(period_days + 1):
        date, date_str = today - timedelta(days=i), date.strftime("%Y-%m-%d")
        snapshot = get_onedrive_data(f"{BASE_ONEDRIVE_PATH}/history/{get_email_hash(email)}/{date_str}.json")
        if snapshot: history.append(snapshot)
    return sorted(history, key=lambda x: x['date'])

def update_asset_snapshot(email, user_profile, total_assets_usd, total_liabilities_usd, total_stock_value_usd, total_cash_balance_usd, total_crypto_value_usd, current_rates):
    today_str = datetime.now().strftime("%Y-%m-%d")
    snapshot = {"date": today_str, "total_assets_usd": total_assets_usd, "total_liabilities_usd": total_liabilities_usd, "net_worth_usd": total_assets_usd - total_liabilities_usd, "total_stock_value_usd": total_stock_value_usd, "total_cash_balance_usd": total_cash_balance_usd, "total_crypto_value_usd": total_crypto_value_usd, "exchange_rates": current_rates, "portfolio": user_profile["portfolio"]}
    if not get_onedrive_data(f"{BASE_ONEDRIVE_PATH}/history/{get_email_hash(email)}/{today_str}.json"):
        st.toast("今日资产快照已生成！")
    save_onedrive_data(f"{BASE_ONEDRIVE_PATH}/history/{get_email_hash(email)}/{today_str}.json", snapshot)

@st.cache_data(ttl=3600)
def get_ai_analysis(period_days, total_change, market_change, cash_flow, fx_change, display_curr, display_symbol):
    prompt = f"You are a professional and friendly financial advisor. Based on the following data for the user's portfolio over the last {period_days} days, provide a concise, insightful analysis in Chinese. The user is viewing their report in {display_curr}. Data: Total Net Worth Change: {display_symbol}{total_change:,.2f}, Investment Value Change (stocks, crypto): {display_symbol}{market_change:,.2f}, Net Cash & Liability Change (income, expenses, debt): {display_symbol}{cash_flow:,.2f}, Currency Exchange & Other Factors Impact: {display_symbol}{fx_change:,.2f}. Instructions: 1. Start with a clear, one-sentence summary. 2. Identify the BIGGEST driver of the change. 3. Briefly mention other factors. 4. Keep the tone encouraging and professional."
    try:
        account_id, api_token, model = CF_CONFIG['account_id'], CF_CONFIG['api_token'], "@cf/meta/llama-3-8b-instruct"
        url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/{model}"
        headers = {"Authorization": f"Bearer {api_token}"}
        response = requests.post(url, headers=headers, json={"prompt": prompt}, timeout=20)
        response.raise_for_status()
        return response.json().get("result", {}).get("response", "AI 分析时出现错误。")
    except Exception as e: return f"无法连接到 AI 服务进行分析: {e}"


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
            if st.button("返回"): st.session_state.login_step = "enter_email"; st.rerun()

def display_admin_panel():
    with st.sidebar: st.header("👑 管理员面板"); st.info("管理员功能待适配新数据结构。")

def display_asset_allocation_chart(stock_usd, cash_usd, crypto_usd, display_curr, display_rate, display_symbol):
    labels, values_usd = ['股票', '现金', '加密货币'], [stock_usd, cash_usd, crypto_usd]
    non_zero_labels = [label for label, value in zip(labels, values_usd) if value > 0.01]
    non_zero_values = [value for value in values_usd if value > 0.01]
    if not non_zero_values: st.info("暂无资产可供分析。"); return
    fig = go.Figure(data=[go.Pie(labels=non_zero_labels, values=[v * display_rate for v in non_zero_values], hole=.4, textinfo='percent+label', hovertemplate=f"<b>%{{label}}</b><br>价值: {display_symbol}%{{value:,.2f}} {display_curr}<br>占比: %{{percent}}<extra></extra>")])
    fig.update_layout(title_text='资产配置', showlegend=False, height=300, margin=dict(l=10, r=10, t=40, b=10))
    st.plotly_chart(fig, use_container_width=True)

def display_dashboard():
    st.title(f"🚀 {st.session_state.user_email} 的专业仪表盘")
    user_profile = get_user_profile(st.session_state.user_email)
    if user_profile is None: st.error("无法加载用户数据。"); st.stop()
    
    user_portfolio = user_profile.setdefault("portfolio", {})
    for key in ["stocks", "cash_accounts", "crypto", "liabilities"]: user_portfolio.setdefault(key, [])
    stock_holdings, cash_accounts, crypto_holdings, liabilities = user_portfolio["stocks"], user_portfolio["cash_accounts"], user_portfolio["crypto"], user_portfolio["liabilities"]

    stock_tickers = [s['ticker'] for s in stock_holdings if s.get('ticker')]; crypto_symbols = [c['symbol'] for c in crypto_holdings if c.get('symbol')]
    
    # Smart Refresh Logic
    last_fetched_tickers = st.session_state.get('last_fetched_tickers', set())
    current_tickers = set(stock_tickers + crypto_symbols)
    tickers_changed = current_tickers != last_fetched_tickers
    
    if st.sidebar.button('🔄 刷新市场数据'): st.session_state.last_market_data_fetch = 0 
    
    now = time.time()
    if tickers_changed or (now - st.session_state.last_market_data_fetch > DATA_REFRESH_INTERVAL_SECONDS):
        with st.spinner("正在获取最新市场数据 (yfinance)..."):
            # MODIFIED: Call the new yfinance data function
            st.session_state.market_data = get_all_market_data_yf(stock_tickers, crypto_symbols)
            st.session_state.exchange_rates = get_exchange_rates()
            st.session_state.last_market_data_fetch = now
            st.session_state.last_fetched_tickers = current_tickers
            st.rerun()
    
    market_data, prices, exchange_rates = st.session_state.get('market_data', {}), get_prices_from_cache(st.session_state.get('market_data', {})), st.session_state.get('exchange_rates', {})
    if not exchange_rates: st.error("无法加载汇率，资产总值不准确。"); st.stop()

    total_stock_value_usd = sum(s.get('quantity',0) * prices.get(s['ticker'], 0) / exchange_rates.get(s.get('currency', 'USD'), 1) for s in stock_holdings)
    total_cash_balance_usd = sum(acc.get('balance',0) / exchange_rates.get(acc.get('currency', 'USD'), 1) for acc in cash_accounts)
    total_crypto_value_usd = sum(c.get('quantity',0) * prices.get(c['symbol'], 0) for c in crypto_holdings)
    total_assets_usd = total_stock_value_usd + total_cash_balance_usd + total_crypto_value_usd
    total_liabilities_usd = sum(liab.get('balance',0) / exchange_rates.get(liab.get('currency', 'USD'), 1) for liab in liabilities)
    net_worth_usd = total_assets_usd - total_liabilities_usd

    update_asset_snapshot(st.session_state.user_email, user_profile, total_assets_usd, total_liabilities_usd, total_stock_value_usd, total_cash_balance_usd, total_crypto_value_usd, exchange_rates)

    display_curr = st.sidebar.selectbox("选择显示货币", options=SUPPORTED_CURRENCIES, key="display_currency")
    display_rate, display_symbol = exchange_rates.get(display_curr, 1), CURRENCY_SYMBOLS.get(display_curr, "")

    st.header("财务状况核心指标")
    col1, col2, col3 = st.columns(3)
    col1.metric("🏦 净资产", f"{display_symbol}{net_worth_usd * display_rate:,.2f} {display_curr}")
    col2.metric("💰 总资产", f"{display_symbol}{total_assets_usd * display_rate:,.2f} {display_curr}")
    col3.metric("💳 总负债", f"{display_symbol}{total_liabilities_usd * display_rate:,.2f} {display_curr}")

    tab1, tab2, tab3 = st.tabs(["📊 资产总览", "✍️ 交易管理", "📈 分析洞察"])

    with tab1:
        st.subheader("资产配置概览")
        display_asset_allocation_chart(total_stock_value_usd, total_cash_balance_usd, total_crypto_value_usd, display_curr, display_rate, display_symbol)
        st.subheader("资产与盈亏明细")
        stock_df_data = []
        for s in stock_holdings:
            current_price, market_value, avg_cost = prices.get(s['ticker'], 0), s.get('quantity', 0) * prices.get(s['ticker'], 0), s.get('average_cost', 0)
            cost_basis, unrealized_pl = s.get('quantity', 0) * avg_cost, market_value - (s.get('quantity', 0) * avg_cost)
            return_pct = (unrealized_pl / cost_basis * 100) if cost_basis > 0 else 0
            currency_symbol = CURRENCY_SYMBOLS.get(s.get('currency', 'USD'), '')
            stock_df_data.append({"代码": s['ticker'], "数量": s['quantity'], "货币": s['currency'], "成本价": f"{currency_symbol}{avg_cost:,.2f}", "现价": f"{currency_symbol}{current_price:,.2f}", "市值": f"{currency_symbol}{market_value:,.2f}", "未实现盈亏": f"{currency_symbol}{unrealized_pl:,.2f}", "回报率(%)": f"{return_pct:.2f}%"})
        st.write("📈 **股票持仓**"); st.dataframe(pd.DataFrame(stock_df_data), use_container_width=True, hide_index=True)
        
        c1, c2, c3 = st.columns(3)
        with c1: st.write("💵 **现金账户**"); st.dataframe(pd.DataFrame([{"账户名称": acc['name'],"货币": acc['currency'], "余额": f"{CURRENCY_SYMBOLS.get(acc['currency'], '')}{acc['balance']:,.2f}"} for acc in cash_accounts]), use_container_width=True, hide_index=True)
        with c2:
            st.write("🪙 **加密货币持仓**")
            crypto_df_data = []
            for c in crypto_holdings:
                current_price, market_value, avg_cost = prices.get(c['symbol'], 0), c.get('quantity', 0) * prices.get(c['symbol'], 0), c.get('average_cost', 0)
                cost_basis, unrealized_pl = c.get('quantity', 0) * avg_cost, market_value - (c.get('quantity', 0) * avg_cost)
                return_pct = (unrealized_pl / cost_basis * 100) if cost_basis > 0 else 0
                currency_symbol = CURRENCY_SYMBOLS.get("USD", "$")
                crypto_df_data.append({"代码": c['symbol'], "数量": f"{c.get('quantity',0):.6f}", "成本价": f"{currency_symbol}{avg_cost:,.2f}", "现价": f"{currency_symbol}{current_price:,.2f}", "市值": f"{currency_symbol}{market_value:,.2f}", "未实现盈亏": f"{currency_symbol}{unrealized_pl:,.2f}", "回报率(%)": f"{return_pct:.2f}%"})
            st.dataframe(pd.DataFrame(crypto_df_data), use_container_width=True, hide_index=True)
        with c3: st.write("💳 **负债账户**"); st.dataframe(pd.DataFrame([{"名称": liab['name'],"货币": liab['currency'], "金额": f"{CURRENCY_SYMBOLS.get(liab['currency'], '')}{liab['balance']:,.2f}"} for liab in liabilities]), use_container_width=True, hide_index=True)

    with tab2:
        st.subheader("✍️ 记录一笔新流水")
        with st.form("transaction_form", clear_on_submit=True):
            trans_type = st.selectbox("类型", ["收入", "支出", "买入股票", "卖出股票", "买入加密货币", "卖出加密货币", "转账"])
            col1, col2 = st.columns(2)
            with col1:
                description = st.text_input("描述"); amount = st.number_input("总金额", min_value=0.01, format="%.2f")
                account_names = [acc.get("name", "") for acc in cash_accounts]
                from_account_name = st.selectbox("选择现金账户", options=account_names, key="from_acc") if account_names else None
            with col2:
                symbol, quantity, to_account_name = "", 0.0, None
                if "股票" in trans_type or "加密货币" in trans_type:
                    symbol = st.text_input("资产代码").upper()
                    if "股票" in trans_type: quantity = st.number_input("数量", min_value=1e-4, format="%.4f")
                    else: quantity = st.number_input("数量", min_value=1e-8, format="%.8f")
                elif trans_type == "转账": to_account_name = st.selectbox("转入账户", [n for n in account_names if n != from_account_name], key="to_acc")

            if st.form_submit_button("记录流水"):
                if not from_account_name: st.error("操作失败：请先创建现金账户。"); st.stop()
                now_str, from_account = datetime.now().strftime("%Y-%m-%d %H:%M"), next((acc for acc in cash_accounts if acc["name"] == from_account_name), None)
                new_transaction = {"date": now_str, "type": trans_type, "description": description, "amount": amount, "currency": from_account["currency"], "account": from_account_name}

                if trans_type == "收入": from_account["balance"] += amount
                elif trans_type == "支出":
                    if from_account["balance"] < amount: st.error("现金账户余额不足！"); st.stop()
                    from_account["balance"] -= amount
                elif trans_type == "转账":
                    if from_account["balance"] < amount: st.error("转出账户余额不足！"); st.stop()
                    to_account = next((acc for acc in cash_accounts if acc["name"] == to_account_name), None)
                    if not to_account: st.error("转入账户未找到！"); st.stop()
                    if from_account['currency'] != to_account['currency']: st.error("跨币种转账暂不支持。"); st.stop()
                    from_account["balance"] -= amount; to_account["balance"] += amount
                    new_transaction["details"] = f"从 {from_account_name} 转至 {to_account_name}"
                elif trans_type == "买入股票":
                    if from_account["balance"] < amount: st.error("现金账户余额不足！"); st.stop()
                    if quantity <= 0: st.error("数量必须大于0"); st.stop()
                    profile = get_stock_profile_yf(symbol)
                    if not profile or not profile.get("currency"): st.error(f"无法获取股票 {symbol} 的信息，请检查代码是否有效。"); st.stop()
                    stock_currency, cash_currency = profile["currency"].upper(), from_account["currency"]
                    amount_in_usd = amount / exchange_rates.get(cash_currency, 1)
                    cost_in_stock_currency = amount_in_usd * exchange_rates.get(stock_currency, 1)
                    price_per_unit = cost_in_stock_currency / quantity
                    from_account["balance"] -= amount
                    holding = next((h for h in stock_holdings if h.get("ticker") == symbol), None)
                    if holding:
                        old_cost_basis = holding.get('average_cost', 0) * holding.get('quantity', 0)
                        new_quantity = holding.get('quantity', 0) + quantity
                        holding['quantity'], holding['average_cost'] = new_quantity, (old_cost_basis + cost_in_stock_currency) / new_quantity
                    else: stock_holdings.append({"ticker": symbol, "quantity": quantity, "average_cost": price_per_unit, "currency": stock_currency})
                    new_transaction.update({"symbol": symbol, "quantity": quantity, "price": price_per_unit})
                # (买入卖出加密货币和卖出股票逻辑保持不变)
                elif "卖出" in trans_type or trans_type == "买入加密货币":
                    # ... 
                
                user_profile.setdefault("transactions", []).insert(0, new_transaction)
                if save_user_profile(st.session_state.user_email, user_profile): st.success("流水记录成功！"); time.sleep(1); st.rerun()

        st.subheader("📑 交易流水")
        # (交易流水显示逻辑保持不变)
        # ...

        with st.expander("⚙️ 编辑现有资产与负债 (危险操作)"):
            edit_tabs = st.tabs(["💵 现金", "💳 负债", "📈 股票", "🪙 加密货币"])
            def to_df_with_schema(data, schema):
                df = pd.DataFrame(data)
                for col, col_type in schema.items():
                    if col not in df.columns: df[col] = pd.Series(dtype=col_type)
                return df

            # (现金和负债编辑逻辑保持不变)
            # ...
            with edit_tabs[2]:
                schema = {'ticker': 'object', 'quantity': 'float64', 'average_cost': 'float64', 'currency': 'object'}
                df = to_df_with_schema(user_portfolio.get("stocks",[]), schema)
                edited_df = st.data_editor(df, num_rows="dynamic", key="stock_editor_adv", column_config={
                    "ticker": st.column_config.TextColumn("代码", help="请输入Yahoo Finance格式的代码，例如：AAPL, 0700.HK, 600519.SS", required=True), 
                    "quantity": st.column_config.NumberColumn("数量", format="%.4f", required=True), 
                    "average_cost": st.column_config.NumberColumn("平均成本", help="请以该股票的交易货币计价", format="%.2f", required=True), 
                    "currency": st.column_config.TextColumn("货币", disabled=True)
                })
                if st.button("💾 保存股票持仓修改", key="save_stocks"):
                    edited_list = edited_df.dropna(subset=['ticker', 'quantity', 'average_cost']).to_dict('records')
                    original_tickers = {s['ticker'] for s in deepcopy(user_portfolio.get("stocks", []))}
                    
                    for holding in edited_list:
                        holding['ticker'] = holding['ticker'].upper()
                        if (holding['ticker'] not in original_tickers) or (not holding.get('currency') or pd.isna(holding.get('currency'))):
                            with st.spinner(f"正在验证并获取 {holding['ticker']} 的信息..."):
                                profile = get_stock_profile_yf(holding['ticker'])
                            if profile and profile.get('currency'):
                                holding['currency'] = profile['currency'].upper()
                            else:
                                st.error(f"新增的代码 {holding['ticker']} 无效或无法获取信息，保存失败。"); st.stop()

                    user_portfolio["stocks"] = edited_list
                    if save_user_profile(st.session_state.user_email, user_profile):
                        st.success("股票持仓已更新！"); time.sleep(1); st.rerun()

            # (加密货币编辑逻辑保持不变)
            # ...
            
    with tab3:
        st.subheader("📈 分析与洞察")
        sub_tab1, sub_tab2, sub_tab3 = st.tabs(["历史趋势与基准", "投资组合透视", "AI 变动归因"])
        with sub_tab1:
            benchmark_ticker = st.text_input("添加市场基准对比 (例如 SPY, IVV)", "").upper()
            asset_history = get_asset_history(st.session_state.user_email, 365)
            if len(asset_history) < 2:
                st.info("历史数据不足，无法生成图表。")
            else:
                history_df = pd.DataFrame(asset_history)
                history_df['date'] = pd.to_datetime(history_df['date'])
                history_df = history_df.set_index('date')
                history_df['net_worth_normalized'] = (history_df['net_worth_usd'] / history_df['net_worth_usd'].iloc[0]) * 100
                chart_data = history_df[['net_worth_normalized']].rename(columns={'net_worth_normalized': '我的投资组合'})

                if benchmark_ticker:
                    # MODIFIED: Call the new yfinance historical function
                    benchmark_data = get_historical_data_yf(benchmark_ticker, 365)
                    if not benchmark_data.empty:
                        benchmark_data_reindexed = benchmark_data.reindex(chart_data.index, method='ffill')
                        benchmark_data_normalized = (benchmark_data_reindexed / benchmark_data_reindexed.iloc[0]) * 100
                        chart_data[benchmark_ticker] = benchmark_data_normalized
                st.line_chart(chart_data)
        # (其他分析Tab逻辑保持不变)
        # ...

def run_migration():
    st.session_state.migration_done = True
    return

# --- 主程序渲染 ---
if not st.session_state.migration_done: run_migration()

check_session_from_query_params()
if st.session_state.logged_in:
    with st.sidebar:
        st.success(f"欢迎, {st.session_state.user_email}")
        if st.button("退出登录"):
            token_to_remove = st.query_params.get("session_token")
            if token_to_remove:
                sessions = get_global_data("sessions")
                if token_to_remove in sessions:
                    del sessions[token_to_remove]; save_global_data("sessions", sessions)
            for key in list(st.session_state.keys()): del st.session_state[key]
            st.query_params.clear(); st.rerun()
    display_dashboard()
    if st.session_state.user_email == ADMIN_EMAIL: display_admin_panel()
else: display_login_form()
