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


# --- 微软 Graph API 配置 ---
MS_GRAPH_CONFIG = st.secrets["microsoft_graph"]
ADMIN_EMAIL = MS_GRAPH_CONFIG["admin_email"]
ONEDRIVE_SENDER_EMAIL = MS_GRAPH_CONFIG['sender_email']


# --- 核心功能函数定义 ---

def get_email_hash(email):
    """Generates a stable, filename-safe hash for an email address."""
    return hashlib.sha256(email.encode('utf-8')).hexdigest()

@st.cache_data(ttl=3500)
def get_ms_graph_token():
    url = f"https://login.microsoftonline.com/{MS_GRAPH_CONFIG['tenant_id']}/oauth2/v2.0/token"
    data = {"grant_type": "client_credentials", "client_id": MS_GRAPH_CONFIG['client_id'], "client_secret": MS_GRAPH_CONFIG['client_secret'], "scope": "https://graph.microsoft.com/.default"}
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

def onedrive_api_request(method, path, headers, data=None):
    """Generic function to handle OneDrive API requests."""
    base_url = f"https://graph.microsoft.com/v1.0/users/{ONEDRIVE_SENDER_EMAIL}/drive"
    url = f"{base_url}/{path}"
    if method.lower() == 'get':
        return requests.get(url, headers=headers)
    if method.lower() == 'put':
        return requests.put(url, headers=headers, data=data)
    if method.lower() == 'post':
         return requests.post(url, headers=headers, data=data)
    if method.lower() == 'patch':
         return requests.patch(url, headers=headers, data=data)
    return None

def get_onedrive_data(path, is_json=True):
    try:
        token = get_ms_graph_token()
        headers = {"Authorization": f"Bearer {token}"}
        resp = onedrive_api_request('get', f"{path}:/content", headers)
        if resp.status_code == 404:
            return None # File not found is a normal case
        resp.raise_for_status()
        return resp.json() if is_json else resp.text
    except Exception as e:
        # Avoid showing error for 404s, as they are expected
        if "404" not in str(e):
             st.error(f"从 OneDrive 加载数据失败 ({path}): {e}")
        return None

def save_onedrive_data(path, data):
    try:
        token = get_ms_graph_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        json_data = json.dumps(data, indent=2)
        onedrive_api_request('put', f"{path}:/content", headers, data=json_data)
        return True
    except Exception as e:
        st.error(f"保存数据到 OneDrive 失败 ({path}): {e}")
        return False

def get_user_profile(email):
    email_hash = get_email_hash(email)
    return get_onedrive_data(f"{BASE_ONEDRIVE_PATH}/users/{email_hash}.json")

def save_user_profile(email, data):
    email_hash = get_email_hash(email)
    return save_onedrive_data(f"{BASE_ONEDRIVE_PATH}/users/{email_hash}.json", data)

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
            # Create new user profile
            user_profile = {"role": "user", "portfolio": {"stocks": [{"ticker": "AAPL", "quantity": 10, "currency": "USD"}, {"ticker": "GOOG", "quantity": 5, "currency": "USD"}], "cash_accounts": [{"name": "美元银行卡", "balance": 10000, "currency": "USD"}, {"name": "人民币支付宝", "balance": 2000, "currency": "CNY"}], "crypto": [{"symbol": "BTC", "quantity": 1}]}, "transactions": []}
            save_user_profile(email, user_profile)
            st.toast("🎉 注册成功！已为您创建新账户。")
        
        sessions = get_global_data("sessions")
        token = secrets.token_hex(16)
        expires_at = time.time() + (SESSION_EXPIRATION_DAYS * 24 * 60 * 60)
        sessions[token] = {"email": email, "expires_at": expires_at}
        save_global_data("sessions", sessions)
        
        del codes[email]
        save_global_data("codes", codes)
        
        st.session_state.logged_in = True; st.session_state.user_email = email
        st.session_state.login_step = "logged_in"; st.query_params["session_token"] = token
        st.rerun()
    else:
        st.sidebar.error("验证码错误。")

def check_session_from_query_params():
    if st.session_state.get('logged_in'): return
    token = st.query_params.get("session_token")
    if not token: return
    
    sessions = get_global_data("sessions")
    session_info = sessions.get(token)

    if session_info and time.time() < session_info.get("expires_at", 0):
        st.session_state.logged_in = True
        st.session_state.user_email = session_info["email"]
        st.session_state.login_step = "logged_in"
    elif "session_token" in st.query_params:
        st.query_params.clear()

def get_all_market_data(stock_tickers, crypto_symbols):
    market_data = {}; api_key = st.secrets["financialmodelingprep"]["api_key"]
    if stock_tickers:
        try:
            response = requests.get(f"https://financialmodelingprep.com/api/v3/quote/{','.join(stock_tickers)}?apikey={api_key}")
            response.raise_for_status(); data = response.json()
            price_map = {item['symbol']: item for item in data}
            for ticker in stock_tickers:
                market_data[ticker] = {"latest_price": price_map[ticker]['price']} if ticker in price_map else None
        except Exception: pass # Error already handled or warned inside
    if crypto_symbols:
        try:
            crypto_ticker_string = ",".join([f"{s}USD" for s in crypto_symbols])
            response = requests.get(f"https://financialmodelingprep.com/api/v3/quote/{crypto_ticker_string}?apikey={api_key}")
            response.raise_for_status(); data = response.json()
            price_map = {item['symbol'].replace('USD', ''): item for item in data}
            for symbol in crypto_symbols:
                market_data[symbol] = {"latest_price": price_map[symbol]['price']} if symbol in price_map else None
        except Exception: pass
    return market_data

def get_prices_from_cache(market_data):
    return {ticker: (data["latest_price"] if data else 0) for ticker, data in market_data.items()}

@st.cache_data(ttl=3600)
def get_historical_asset_price(symbol, date_str, asset_type='stock'):
    api_key = st.secrets["financialmodelingprep"]["api_key"]
    api_symbol = f"{symbol}USD" if asset_type == 'crypto' else symbol
    try:
        resp = requests.get(f"https://financialmodelingprep.com/api/v3/historical-price-full/{api_symbol}?apikey={api_key}")
        resp.raise_for_status(); data = resp.json()
        if 'historical' in data and data['historical']:
            for item in data['historical']:
                if item['date'] <= date_str: return item.get('close', 0)
            return data['historical'][-1].get('close', 0)
    except Exception: return 0
    return 0

def get_exchange_rates(base_currency='USD'):
    try:
        resp = requests.get(f"https://open.er-api.com/v6/latest/{base_currency}")
        resp.raise_for_status(); data = resp.json()
        return data.get("rates") if data.get("result") == "success" else None
    except Exception as e:
        st.error(f"获取汇率失败: {e}"); return None

def get_asset_history(email, period_days):
    history = []
    today = datetime.now()
    for i in range(period_days + 1):
        date = today - timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")
        email_hash = get_email_hash(email)
        snapshot = get_onedrive_data(f"{BASE_ONEDRIVE_PATH}/history/{email_hash}/{date_str}.json")
        if snapshot:
            history.append(snapshot)
    return sorted(history, key=lambda x: x['date'])


def update_asset_snapshot(email, user_profile, total_assets_usd, total_stock_value_usd, total_cash_balance_usd, total_crypto_value_usd, current_rates):
    today_str = datetime.now().strftime("%Y-%m-%d")
    email_hash = get_email_hash(email)
    
    snapshot = {
        "date": today_str,
        "total_assets_usd": total_assets_usd, "total_stock_value_usd": total_stock_value_usd,
        "total_cash_balance_usd": total_cash_balance_usd, "total_crypto_value_usd": total_crypto_value_usd,
        "exchange_rates": current_rates, "portfolio": user_profile["portfolio"]
    }
    
    # Check if today's snapshot already exists to avoid toast on every refresh
    existing_snapshot = get_onedrive_data(f"{BASE_ONEDRIVE_PATH}/history/{email_hash}/{today_str}.json")
    if not existing_snapshot:
        st.toast("今日资产快照已生成！")

    save_onedrive_data(f"{BASE_ONEDRIVE_PATH}/history/{email_hash}/{today_str}.json", snapshot)

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
        # Admin functions would need to be adapted to the new file structure
        # For simplicity, this is left as a placeholder for now.
        st.info("管理员功能待适配新数据结构。")

def display_asset_allocation_chart(stock_usd, cash_usd, crypto_usd, display_curr, display_rate, display_symbol):
    labels = ['股票', '现金', '加密货币']
    values_usd = [stock_usd, cash_usd, crypto_usd]
    non_zero_labels = [label for label, value in zip(labels, values_usd) if value > 0.01]
    non_zero_values = [value for value in values_usd if value > 0.01]
    if not non_zero_values:
        st.info("暂无资产可供分析。"); return
    fig = go.Figure(data=[go.Pie(labels=non_zero_labels, values=[v * display_rate for v in non_zero_values], hole=.4, textinfo='percent+label', hovertemplate=f"<b>%{{label}}</b><br>价值: {display_symbol}%{{value:,.2f}} {display_curr}<br>占比: %{{percent}}<extra></extra>")])
    fig.update_layout(title_text='资产配置', showlegend=False, height=300, margin=dict(l=10, r=10, t=40, b=10))
    st.plotly_chart(fig, use_container_width=True)

def display_dashboard():
    st.title(f"💰 {st.session_state.user_email} 的资产仪表盘")
    user_profile = get_user_profile(st.session_state.user_email)
    if user_profile is None: st.error("无法加载用户数据。"); st.stop()

    user_portfolio = user_profile.setdefault("portfolio", {})
    stock_holdings = user_portfolio.get("stocks", []); cash_accounts = user_portfolio.get("cash_accounts", []); crypto_holdings = user_portfolio.get("crypto", [])
    stock_tickers = [s['ticker'] for s in stock_holdings if s.get('ticker')]; crypto_symbols = [c['symbol'] for c in crypto_holdings if c.get('symbol')]
    
    if st.sidebar.button('🔄 刷新市场数据'): st.session_state.last_market_data_fetch = 0
    
    if time.time() - st.session_state.last_market_data_fetch > DATA_REFRESH_INTERVAL_SECONDS:
        with st.spinner("正在获取最新市场数据..."):
            st.session_state.market_data = get_all_market_data(stock_tickers, crypto_symbols)
            st.session_state.exchange_rates = get_exchange_rates()
            st.session_state.last_market_data_fetch = time.time(); st.rerun()
    
    market_data = st.session_state.get('market_data', {}); prices = get_prices_from_cache(market_data)
    exchange_rates = st.session_state.get('exchange_rates', {});
    if not exchange_rates: st.error("无法加载汇率，资产总值不准确。"); st.stop()

    total_stock_value_usd = sum(s['quantity'] * prices.get(s['ticker'], 0) / exchange_rates.get(s.get('currency', 'USD'), 1) for s in stock_holdings)
    total_cash_balance_usd = sum(acc['balance'] / exchange_rates.get(acc.get('currency', 'USD'), 1) for acc in cash_accounts)
    total_crypto_value_usd = sum(c['quantity'] * prices.get(c['symbol'], 0) for c in crypto_holdings)
    total_assets_usd = total_stock_value_usd + total_cash_balance_usd + total_crypto_value_usd

    update_asset_snapshot(st.session_state.user_email, user_profile, total_assets_usd, total_stock_value_usd, total_cash_balance_usd, total_crypto_value_usd, exchange_rates)

    display_curr = st.sidebar.selectbox("选择显示货币", options=SUPPORTED_CURRENCIES, key="display_currency")
    display_rate = exchange_rates.get(display_curr, 1); display_symbol = CURRENCY_SYMBOLS.get(display_curr, "")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💰 资产总值", f"{display_symbol}{total_assets_usd * display_rate:,.2f} {display_curr}")
    col2.metric("📈 股票市值", f"{display_symbol}{total_stock_value_usd * display_rate:,.2f} {display_curr}")
    col3.metric("🪙 加密货币市值", f"{display_symbol}{total_crypto_value_usd * display_rate:,.2f} {display_curr}")
    col4.metric("💵 现金总额", f"{display_symbol}{total_cash_balance_usd * display_rate:,.2f} {display_curr}")

    tab1, tab2, tab3 = st.tabs(["📊 资产总览", "✍️ 交易管理", "📈 分析洞察"])

    with tab1:
        st.subheader("资产配置概览")
        display_asset_allocation_chart(total_stock_value_usd, total_cash_balance_usd, total_crypto_value_usd, display_curr, display_rate, display_symbol)
        
        st.subheader("资产明细")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.write("📈 **股票持仓**")
            st.dataframe(pd.DataFrame([{"代码": s['ticker'], "数量": s['quantity'], "货币": s['currency'], "当前价格": f"{CURRENCY_SYMBOLS.get(s['currency'], '')}{prices.get(s['ticker'], 0):,.2f}"} for s in stock_holdings]), use_container_width=True, hide_index=True)
        with c2:
            st.write("💵 **现金账户**")
            st.dataframe(pd.DataFrame([{"账户名称": acc['name'],"货币": acc['currency'], "余额": f"{CURRENCY_SYMBOLS.get(acc['currency'], '')}{acc['balance']:,.2f}"} for acc in cash_accounts]), use_container_width=True, hide_index=True)
        with c3:
            st.write("🪙 **加密货币持仓**")
            st.dataframe(pd.DataFrame([{"代码": c['symbol'], "数量": c['quantity'], "当前价格": f"${prices.get(c['symbol'], 0):,.2f}"} for c in crypto_holdings]), use_container_width=True, hide_index=True)

    with tab2:
        st.subheader("✍️ 记录一笔新流水")
        with st.form("transaction_form", clear_on_submit=True):
            trans_type = st.selectbox("类型", ["收入", "支出", "买入股票", "卖出股票", "买入加密货币", "卖出加密货币", "转账"])
            col1, col2 = st.columns(2)
            with col1:
                description = st.text_input("描述"); amount = st.number_input("金额", min_value=0.0, format="%.2f")
                account_names = [acc.get("name", "") for acc in cash_accounts]
                from_account_name = st.selectbox("选择现金账户", options=account_names, key="from_acc") if account_names else None
            with col2:
                symbol, quantity, to_account_name = "", 0.0, None
                if "股票" in trans_type: symbol = st.text_input("股票代码").upper(); quantity = st.number_input("数量", min_value=0.0, format="%.2f")
                elif "加密货币" in trans_type: symbol = st.text_input("加密货币代码").upper(); quantity = st.number_input("数量", min_value=0.0, format="%.8f")
                elif trans_type == "转账": to_account_name = st.selectbox("转入账户", options=[n for n in account_names if n != from_account_name], key="to_acc")

            if st.form_submit_button("记录流水"):
                if from_account_name is None: st.error("操作失败：请先至少创建一个现金账户。"); st.stop()
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M"); from_account = next(acc for acc in cash_accounts if acc["name"] == from_account_name)
                
                if trans_type == "收入": from_account["balance"] += amount
                elif trans_type == "支出": from_account["balance"] -= amount
                elif trans_type == "转账":
                    if to_account_name: to_account = next(acc for acc in cash_accounts if acc["name"] == to_account_name); from_account["balance"] -= amount; to_account["balance"] += amount
                elif "买入" in trans_type:
                    from_account["balance"] -= amount
                    if "股票" in trans_type:
                        holding = next((s for s in stock_holdings if s["ticker"] == symbol), None)
                        if holding: holding["quantity"] += quantity
                        else: user_portfolio["stocks"].append({"ticker": symbol, "quantity": quantity, "currency": from_account['currency']}); st.toast(f"新持仓 {symbol} 已添加！")
                    elif "加密货币" in trans_type:
                        holding = next((c for c in crypto_holdings if c["symbol"] == symbol), None)
                        if holding: holding["quantity"] += quantity
                        else: user_portfolio["crypto"].append({"symbol": symbol, "quantity": quantity}); st.toast(f"新持仓 {symbol} 已添加！")
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
                
                user_portfolio["stocks"] = [s for s in stock_holdings if s["quantity"] > 0.000001]
                user_portfolio["crypto"] = [c for c in crypto_holdings if c["quantity"] > 0.000001]
                
                new_trans = {"date": now_str, "type": trans_type, "description": description, "amount": amount, "currency": from_account["currency"], "account": from_account_name}
                if symbol: new_trans.update({"symbol": symbol, "quantity": quantity})
                if to_account_name: new_trans.update({"to_account": to_account_name})
                user_profile.setdefault("transactions", []).append(new_trans)

                if save_user_profile(st.session_state.user_email, user_profile): st.success("流水记录成功！"); time.sleep(1); st.rerun()

        st.subheader("📑 最近流水")
        st.dataframe(pd.DataFrame(user_profile.get("transactions", [])).sort_values(by="date", ascending=False) if user_profile.get("transactions") else pd.DataFrame(), use_container_width=True, hide_index=True)

        with st.expander("⚙️ 编辑现有资产"):
            m_tab1, m_tab2, m_tab3 = st.tabs(["💵 现金账户", "📈 股票持仓", "🪙 加密货币"])
            with m_tab1:
                original_cash_map = {acc['name']: acc.copy() for acc in cash_accounts}
                edited_cash = st.data_editor(cash_accounts, num_rows="dynamic", key="cash_editor", column_config={"name": "账户名称", "currency": st.column_config.SelectboxColumn("货币", options=SUPPORTED_CURRENCIES, required=True), "balance": st.column_config.NumberColumn("余额", format="%.2f", required=True)}, use_container_width=True)
                
                if st.button("💾 保存现金账户修改"):
                    for edited_account in edited_cash:
                        account_name = edited_account.get('name')
                        original_account = original_cash_map.get(account_name)

                        if original_account:
                            original_balance = original_account.get('balance', 0)
                            new_balance = edited_account.get('balance', 0)
                            delta = new_balance - original_balance

                            if abs(delta) > 0.001:
                                trans_type = "收入" if delta > 0 else "支出"
                                amount = abs(delta)
                                now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
                                
                                new_trans = {
                                    "date": now_str, "type": trans_type, "description": "手动调整余额",
                                    "amount": amount, "currency": edited_account.get("currency"),
                                    "account": account_name
                                }
                                user_profile.setdefault("transactions", []).append(new_trans)
                    
                    user_portfolio["cash_accounts"] = [a for a in edited_cash if a.get("name") and a.get("currency")]
                    if save_user_profile(st.session_state.user_email, user_profile): 
                        st.success("现金账户已更新，并已自动记录收支流水！")
                        time.sleep(1)
                        st.rerun()

            with m_tab2:
                original_stock_map = {s['ticker']: s.copy() for s in stock_holdings}
                edited_stocks = st.data_editor(stock_holdings, num_rows="dynamic", key="stock_editor", column_config={"ticker": "股票代码", "quantity": st.column_config.NumberColumn("数量", format="%.2f"),"currency": st.column_config.SelectboxColumn("货币", options=SUPPORTED_CURRENCIES, required=True)}, use_container_width=True)
                
                if st.button("💾 保存股票持仓修改"):
                    for edited_stock in edited_stocks:
                        ticker = edited_stock.get('ticker')
                        original_stock = original_stock_map.get(ticker)
                        
                        if original_stock: 
                            original_quantity = original_stock.get('quantity', 0)
                            new_quantity = edited_stock.get('quantity', 0)
                            delta = new_quantity - original_quantity

                            if abs(delta) > 0.000001:
                                current_price = prices.get(ticker, 0)
                                amount = abs(delta) * current_price
                                
                                if amount > 0.01:
                                    target_currency = edited_stock.get("currency", "USD")
                                    suitable_account = next((acc for acc in cash_accounts if acc.get("currency") == target_currency), None)
                                    if not suitable_account: suitable_account = next((acc for acc in cash_accounts if acc.get("currency") == "USD"), None)
                                    if not suitable_account and cash_accounts: suitable_account = cash_accounts[0]
                                    
                                    if suitable_account:
                                        trans_type = "买入股票" if delta > 0 else "卖出股票"
                                        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
                                        
                                        cash_account_to_update = next(acc for acc in user_portfolio["cash_accounts"] if acc["name"] == suitable_account["name"])
                                        rate_adjustment = exchange_rates.get(suitable_account['currency'], 1) / exchange_rates.get(target_currency, 1)
                                        
                                        if trans_type == "买入股票":
                                            cash_account_to_update["balance"] -= amount * rate_adjustment
                                        else: 
                                            cash_account_to_update["balance"] += amount * rate_adjustment

                                        new_trans = {
                                            "date": now_str, "type": trans_type, "description": f"手动调整持仓 ({ticker})",
                                            "amount": amount * rate_adjustment, "currency": suitable_account["currency"],
                                            "account": suitable_account["name"], "symbol": ticker, "quantity": abs(delta)
                                        }
                                        user_profile.setdefault("transactions", []).append(new_trans)
                                    else:
                                        st.warning(f"无法为 {ticker} 的调整自动生成流水，因为没有找到合适的现金账户。")

                    user_portfolio["stocks"] = [s for s in edited_stocks if s.get("ticker") and s.get("currency")]
                    if save_user_profile(st.session_state.user_email, user_profile): 
                        st.success("股票持仓已更新，并已自动记录相关流水！")
                        time.sleep(1)
                        st.rerun()

            with m_tab3:
                original_crypto_map = {c['symbol']: c.copy() for c in crypto_holdings}
                edited_crypto = st.data_editor(crypto_holdings, num_rows="dynamic", key="crypto_editor", column_config={"symbol": "代码", "quantity": st.column_config.NumberColumn("数量", format="%.8f")}, use_container_width=True)

                if st.button("💾 保存加密货币持仓修改"):
                    for edited_c in edited_crypto:
                        symbol = edited_c.get('symbol')
                        original_c = original_crypto_map.get(symbol)

                        if original_c:
                            original_quantity = original_c.get('quantity', 0)
                            new_quantity = edited_c.get('quantity', 0)
                            delta = new_quantity - original_quantity

                            if abs(delta) > 0.00000001:
                                current_price = prices.get(symbol, 0)
                                amount_usd = abs(delta) * current_price
                                
                                if amount_usd > 0.01:
                                    suitable_account = next((acc for acc in cash_accounts if acc.get("currency") == "USD"), None)
                                    if not suitable_account and cash_accounts: suitable_account = cash_accounts[0]
                                    
                                    if suitable_account:
                                        trans_type = "买入加密货币" if delta > 0 else "卖出加密货币"
                                        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
                                        
                                        cash_account_to_update = next(acc for acc in user_portfolio["cash_accounts"] if acc["name"] == suitable_account["name"])
                                        rate_adjustment = exchange_rates.get(suitable_account['currency'], 1)
                                        adjusted_amount = amount_usd * rate_adjustment
                                        
                                        if trans_type == "买入加密货币":
                                            cash_account_to_update["balance"] -= adjusted_amount
                                        else:
                                            cash_account_to_update["balance"] += adjusted_amount

                                        new_trans = {
                                            "date": now_str, "type": trans_type, "description": f"手动调整持仓 ({symbol})",
                                            "amount": adjusted_amount, "currency": suitable_account["currency"],
                                            "account": suitable_account["name"], "symbol": symbol, "quantity": abs(delta)
                                        }
                                        user_profile.setdefault("transactions", []).append(new_trans)
                                    else:
                                        st.warning(f"无法为 {symbol} 的调整自动生成流水，因为没有找到合适的现金账户。")

                    user_portfolio["crypto"] = [c for c in edited_crypto if c.get("symbol")]
                    if save_user_profile(st.session_state.user_email, user_profile): 
                        st.success("加密货币持仓已更新，并已自动记录相关流水！")
                        time.sleep(1)
                        st.rerun()

    with tab3:
        sub_tab1, sub_tab2 = st.tabs(["历史趋势", "变动归因"])
        with sub_tab1:
            st.subheader(f"总资产历史趋势 ({display_curr})")
            asset_history = get_asset_history(st.session_state.user_email, 90)
            if not asset_history:
                st.info("暂无历史数据，无法生成图表。")
            else:
                history_df = pd.DataFrame(asset_history)
                history_df['date'] = pd.to_datetime(history_df['date'])
                history_df = history_df.set_index('date')
                history_df[f"total_assets_{display_curr.lower()}"] = history_df["total_assets_usd"] * display_rate
                st.area_chart(history_df[f"total_assets_{display_curr.lower()}"])

                st.subheader(f"股票市值历史趋势 ({display_curr})")
                history_df[f"total_stock_value_{display_curr.lower()}"] = history_df["total_stock_value_usd"] * display_rate
                st.area_chart(history_df[f"total_stock_value_{display_curr.lower()}"])

                st.subheader(f"加密货币市值历史趋势 ({display_curr})")
                history_df[f"total_crypto_value_{display_curr.lower()}"] = history_df["total_crypto_value_usd"] * display_rate
                st.area_chart(history_df[f"total_crypto_value_{display_curr.lower()}"])

                st.subheader(f"现金总额历史趋势 ({display_curr})")
                history_df[f"total_cash_balance_{display_curr.lower()}"] = history_df["total_cash_balance_usd"] * display_rate
                st.area_chart(history_df[f"total_cash_balance_{display_curr.lower()}"])

        with sub_tab2:
            st.subheader("🔍 资产变动归因分析")
            period_days = st.selectbox("选择分析周期（天）", [7, 15, 30, 60], index=0, key="analysis_period")
            asset_history = get_asset_history(st.session_state.user_email, period_days)
            if len(asset_history) < 2:
                st.info("历史数据不足（少于2天），暂无法进行分析。")
            else:
                end_snapshot = asset_history[-1]
                start_snapshot = asset_history[0]
                total_change_usd = end_snapshot["total_assets_usd"] - start_snapshot["total_assets_usd"]
                end_prices = get_prices_from_cache(st.session_state.get('market_data', {}))
                
                market_change_usd, cash_flow_usd, fx_change_usd = 0, 0, 0
                start_portfolio = start_snapshot.get("portfolio", {})
                end_portfolio = end_snapshot.get("portfolio", {})
                
                for ticker in set(s['ticker'] for s in start_portfolio.get("stocks", []) + end_portfolio.get("stocks", [])):
                    start_h = next((s for s in start_portfolio.get("stocks", []) if s["ticker"] == ticker), {"quantity": 0}); end_h = next((s for s in end_portfolio.get("stocks", []) if s["ticker"] == ticker), {"quantity": 0})
                    common_qty = min(start_h["quantity"], end_h["quantity"])
                    if common_qty > 0:
                        start_price = get_historical_asset_price(ticker, start_snapshot["date"], 'stock')
                        price_change_local = common_qty * (end_prices.get(ticker, 0) - start_price)
                        market_change_usd += price_change_local / start_snapshot.get("exchange_rates", {}).get(start_h.get("currency", "USD"), 1)

                for symbol in set(c['symbol'] for c in start_portfolio.get("crypto", []) + end_portfolio.get("crypto", [])):
                    start_h = next((c for c in start_portfolio.get("crypto", []) if c["symbol"] == symbol), {"quantity": 0}); end_h = next((c for c in end_portfolio.get("crypto", []) if c["symbol"] == symbol), {"quantity": 0})
                    common_qty = min(start_h["quantity"], end_h["quantity"])
                    if common_qty > 0:
                        start_price = get_historical_asset_price(symbol, start_snapshot["date"], 'crypto')
                        market_change_usd += common_qty * (end_prices.get(symbol, 0) - start_price)
                        
                transactions = user_profile.get("transactions", [])
                for trans in transactions:
                    if 'date' in trans and start_snapshot["date"] < trans["date"].split(" ")[0] <= end_snapshot["date"]:
                        amount, rate = trans.get("amount", 0), start_snapshot.get("exchange_rates", {}).get(trans.get("currency", "USD"), 1)
                        if trans["type"] == "收入": cash_flow_usd += amount / rate
                        elif trans["type"] == "支出": cash_flow_usd -= amount / rate
                for acc in start_portfolio.get("cash_accounts", []):
                    currency = acc.get("currency", "USD")
                    if currency != 'USD':
                        start_rate, end_rate = start_snapshot.get("exchange_rates", {}).get(currency, 1), end_snapshot.get("exchange_rates", {}).get(currency, 1)
                        if start_rate and end_rate: fx_change_usd += acc.get("balance", 0) * ((1/end_rate) - (1/start_rate))

                st.metric(f"期间总资产变化 ({display_curr})", f"{display_symbol}{total_change_usd * display_rate:,.2f}", f"{display_symbol}{(total_change_usd - market_change_usd - cash_flow_usd - fx_change_usd) * display_rate:,.2f} (交易盈亏与其他)")
                col1, col2, col3 = st.columns(3); col1.metric("📈 市场波动", f"{display_symbol}{market_change_usd * display_rate:,.2f}"); col2.metric("💸 资金流动", f"{display_symbol}{cash_flow_usd * display_rate:,.2f}"); col3.metric("💱 汇率影响", f"{display_symbol}{fx_change_usd * display_rate:,.2f}")

def run_migration():
    """One-time migration from single users.json to new structured format."""
    st.info("正在检查数据结构版本...")
    old_data_path = "root:/Apps/StreamlitDashboard/users.json"
    old_data = get_onedrive_data(old_data_path)
    
    if not old_data:
        st.success("数据结构已是最新版本。")
        st.session_state.migration_done = True
        time.sleep(1)
        return

    with st.spinner("检测到旧版数据文件，正在执行一次性升级..."):
        try:
            # Migrate users, history, sessions, codes
            users = old_data.get("users", {})
            for email, data in users.items():
                profile_data = {k: v for k, v in data.items() if k != 'asset_history'}
                save_user_profile(email, profile_data)
                
                history = data.get("asset_history", [])
                email_hash = get_email_hash(email)
                for snapshot in history:
                    date_str = snapshot['date']
                    save_onedrive_data(f"{BASE_ONEDRIVE_PATH}/history/{email_hash}/{date_str}.json", snapshot)

            save_global_data("sessions", old_data.get("sessions", {}))
            save_global_data("codes", old_data.get("codes", {}))

            # Rename old file to prevent re-migration
            token = get_ms_graph_token()
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            new_name_data = json.dumps({"name": "users.json.migrated"})
            onedrive_api_request('patch', old_data_path, headers, data=new_name_data)

            st.success("数据结构升级成功！应用将重新加载。")
            st.session_state.migration_done = True
            time.sleep(2)
            st.rerun()
        except Exception as e:
            st.error(f"数据迁移失败: {e}")
            st.warning("请手动备份并删除 OneDrive 中的 users.json 文件后重试。")
            st.stop()


# --- 主程序渲染 ---
if not st.session_state.migration_done:
    run_migration()

check_session_from_query_params()
if st.session_state.logged_in:
    with st.sidebar:
        st.success(f"欢迎, {st.session_state.user_email}")
        if st.button("退出登录"):
            token_to_remove = st.query_params.get("session_token")
            sessions = get_global_data("sessions")
            if token_to_remove in sessions:
                del sessions[token_to_remove]
                save_global_data("sessions", sessions)
            for key in list(st.session_state.keys()): del st.session_state[key]
            st.query_params.clear(); st.rerun()
    display_dashboard()
    if st.session_state.user_email == ADMIN_EMAIL:
        display_admin_panel()
else:
    display_login_form()

