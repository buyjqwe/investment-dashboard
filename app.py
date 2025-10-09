import streamlit as st
import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import requests
import re
import random
import time
import json
from datetime import datetime

# --- 页面基础设置 ---
st.set_page_config(
    page_title="个人资产仪表盘",
    page_icon="💰",
    layout="wide"
)

# --- 全局常量 ---
SUPPORTED_CURRENCIES = ["USD", "CNY", "EUR", "HKD", "JPY", "GBP"]
CURRENCY_SYMBOLS = {"USD": "$", "CNY": "¥", "EUR": "€", "HKD": "HK$", "JPY": "¥", "GBP": "£"}


# --- 初始化 Session State ---
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'user_email' not in st.session_state:
    st.session_state.user_email = ""
if 'login_step' not in st.session_state:
    st.session_state.login_step = "enter_email"
if 'display_currency' not in st.session_state:
    st.session_state.display_currency = "USD"


# --- 微软 Graph API 配置 ---
MS_GRAPH_CONFIG = st.secrets["microsoft_graph"]
ADMIN_EMAIL = MS_GRAPH_CONFIG["admin_email"]
ONEDRIVE_FILE_PATH = MS_GRAPH_CONFIG["onedrive_user_file_path"]
ONEDRIVE_API_URL = f"https://graph.microsoft.com/v1.0/users/{MS_GRAPH_CONFIG['sender_email']}/drive/{ONEDRIVE_FILE_PATH}"


# --- Graph API 核心函数 ---
@st.cache_data(ttl=3500)
def get_ms_graph_token():
    url = f"https://login.microsoftonline.com/{MS_GRAPH_CONFIG['tenant_id']}/oauth2/v2.0/token"
    data = {"grant_type": "client_credentials", "client_id": MS_GRAPH_CONFIG['client_id'], "client_secret": MS_GRAPH_CONFIG['client_secret'], "scope": "https://graph.microsoft.com/.default"}
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

def get_user_data_from_onedrive():
    try:
        token = get_ms_graph_token()
        headers = {"Authorization": f"Bearer {token}"}
        content_url = f"{ONEDRIVE_API_URL}:/content"
        resp = requests.get(content_url, headers=headers)
        if resp.status_code == 404:
            initial_data = {"users": {ADMIN_EMAIL: {"role": "admin", "portfolio": {"stocks": [{"ticker": "TSLA", "quantity": 10}], "cash_accounts": [{"name": "默认现金", "balance": 50000, "currency": "USD"}]}, "transactions": []}}, "codes": {}}
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

# --- 登录和用户管理逻辑 ---
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
            user_data["users"][email] = {"role": "user", "portfolio": {"stocks": [{"ticker": "AAPL", "quantity": 10}, {"ticker": "GOOG", "quantity": 5}], "cash_accounts": [{"name": "美元银行卡", "balance": 10000, "currency": "USD"}, {"name": "人民币支付宝", "balance": 2000, "currency": "CNY"}]}, "transactions": []}
            st.toast("🎉 注册成功！已为您创建新账户。")
        st.session_state.logged_in = True
        st.session_state.user_email = email
        st.session_state.login_step = "logged_in"
        del user_data["codes"][email]
        save_user_data_to_onedrive(user_data)
        st.rerun()
    else:
        st.sidebar.error("验证码错误。")

# --- UI 界面函数 ---
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
                            st.toast(f"用户 {user_email} 已删除。")
                            st.rerun()

# --- 数据获取函数 ---
@st.cache_data(ttl=600)
def get_stock_prices(tickers):
    prices = {}
    ts = TimeSeries(key=st.secrets["alpha_vantage"]["api_key"], output_format='pandas')
    for ticker in tickers:
        try:
            data, _ = ts.get_daily(symbol=ticker, outputsize='compact')
            prices[ticker] = data['4. close'].iloc[0]
        except Exception as e:
            st.warning(f"获取 {ticker} 股价失败: {e}")
            prices[ticker] = 0
    return prices

@st.cache_data(ttl=600)
def get_exchange_rates(base_currency='USD'):
    try:
        url = f"https://open.er-api.com/v6/latest/{base_currency}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if data.get("result") == "success":
            return data["rates"]
        else:
            st.error("获取汇率API返回错误。")
            return None
    except Exception as e:
        st.error(f"获取汇率失败: {e}")
        return None

def display_dashboard():
    st.title(f"💰 {st.session_state.user_email} 的资产仪表盘")
    user_data = get_user_data_from_onedrive()
    if user_data is None: st.stop()

    current_user_email = st.session_state.user_email
    user_portfolio = user_data["users"][current_user_email].setdefault("portfolio", {"stocks": [], "cash_accounts": [], "transactions": []})
    
    # --- 数据结构迁移和兼容性处理 ---
    data_migrated = False
    if "cash" in user_portfolio:
        cash_value = user_portfolio.pop("cash")
        user_portfolio["cash_accounts"] = [{"name": "默认现金", "balance": cash_value, "currency": "USD"}]
        data_migrated = True
    for account in user_portfolio.get("cash_accounts", []):
        if "currency" not in account:
            account["currency"] = "USD"
            data_migrated = True
    if data_migrated:
        if save_user_data_to_onedrive(user_data):
            st.toast("数据结构已自动更新以支持多货币！")
            st.rerun()
    
    # --- 获取数据 ---
    user_transactions = user_data["users"][current_user_email].setdefault("transactions", [])
    cash_accounts = user_portfolio.get("cash_accounts", [])
    stock_holdings = user_portfolio.get("stocks", [])
    
    tickers_to_fetch = [s['ticker'] for s in stock_holdings if s.get('ticker')]
    if 'stock_prices' not in st.session_state or st.button('🔄 刷新市场数据'):
        st.session_state.stock_prices = get_stock_prices(tickers_to_fetch)
        st.session_state.exchange_rates = get_exchange_rates()
    
    stock_prices = st.session_state.get('stock_prices', get_stock_prices(tickers_to_fetch))
    exchange_rates = st.session_state.get('exchange_rates', get_exchange_rates())

    if not exchange_rates:
        st.error("无法加载汇率，资产总值可能不准确。")
        st.stop()

    # --- 资产计算 ---
    total_stock_value_usd = sum(s['quantity'] * stock_prices.get(s['ticker'], 0) for s in stock_holdings)
    
    total_cash_balance_usd = 0
    for acc in cash_accounts:
        balance = acc.get('balance', 0)
        currency = acc.get('currency', 'USD').upper()
        # 将所有现金转换为USD进行加总
        total_cash_balance_usd += balance / exchange_rates.get(currency, 1)
        
    total_assets_usd = total_stock_value_usd + total_cash_balance_usd

    # --- 顶部UI显示 ---
    st.sidebar.selectbox(
        "选择显示货币", 
        options=SUPPORTED_CURRENCIES, 
        key="display_currency"
    )
    display_curr = st.session_state.display_currency
    display_rate = exchange_rates.get(display_curr, 1)
    display_symbol = CURRENCY_SYMBOLS.get(display_curr, "")

    col1, col2, col3 = st.columns(3)
    col1.metric("💰 资产总值", f"{display_symbol}{total_assets_usd * display_rate:,.2f} {display_curr}")
    col2.metric("📈 股票市值", f"{display_symbol}{total_stock_value_usd * display_rate:,.2f} {display_curr}")
    col3.metric("💵 现金总额", f"{display_symbol}{total_cash_balance_usd * display_rate:,.2f} {display_curr}")

    # --- 标签页 ---
    tab1, tab2, tab3 = st.tabs(["📊 持仓与流水", "📈 股价图表", "⚙️ 管理资产"])

    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("📊 股票持仓 (USD)")
            if stock_holdings:
                portfolio_df_data = [{"代码": s['ticker'], "数量": s['quantity'], "当前价格": f"${stock_prices.get(s['ticker'], 0):,.2f}", "总值": f"${s['quantity'] * stock_prices.get(s['ticker'], 0):,.2f}"} for s in stock_holdings]
                st.dataframe(pd.DataFrame(portfolio_df_data), use_container_width=True)
            else: st.info("您目前没有股票持仓。")
        with col2:
            st.subheader("💵 现金账户")
            if cash_accounts:
                cash_df_data = [{"账户名称": acc.get("name", ""),"货币": acc.get("currency", "N/A"), "余额": f"{CURRENCY_SYMBOLS.get(acc.get('currency'), '')}{acc.get('balance', 0):,.2f}"} for acc in cash_accounts]
                st.dataframe(pd.DataFrame(cash_df_data), use_container_width=True)
            else: st.info("您还没有现金账户。")

        st.subheader("📑 最近流水")
        if user_transactions:
            st.dataframe(pd.DataFrame(user_transactions).sort_values(by="date", ascending=False), use_container_width=True)
        else: st.info("您还没有任何流水记录。")

    with tab2:
        st.subheader("📈 股价图表 (USD)")
        if tickers_to_fetch:
            ts = TimeSeries(key=st.secrets["alpha_vantage"]["api_key"], output_format='pandas')
            all_data, failed_tickers = [], []
            for ticker in tickers_to_fetch:
                try:
                    data, _ = ts.get_daily(symbol=ticker, outputsize='compact')
                    all_data.append(data['4. close'].rename(ticker))
                except: failed_tickers.append(ticker)
            if all_data: st.line_chart(pd.concat(all_data, axis=1).iloc[::-1])
            if failed_tickers: st.warning(f"无法获取以下股票的数据: {', '.join(failed_tickers)}")
        else: st.info("没有持仓股票可供显示图表。")

    with tab3:
        st.subheader("⚙️ 管理资产")

        st.subheader("编辑现金账户")
        edited_cash_accounts = st.data_editor(
            cash_accounts, 
            num_rows="dynamic", 
            key="cash_editor", 
            column_config={
                "name": "账户名称", 
                "currency": st.column_config.SelectboxColumn("货币", options=SUPPORTED_CURRENCIES, required=True),
                "balance": st.column_config.NumberColumn("余额", format="%.2f", required=True)
            }
        )
        if st.button("💾 保存对现金账户的修改"):
            valid_accounts = [acc for acc in edited_cash_accounts if acc.get("name") and acc.get("currency")]
            user_data["users"][current_user_email]["portfolio"]["cash_accounts"] = valid_accounts
            if save_user_data_to_onedrive(user_data):
                st.success("现金账户已更新！"); time.sleep(1); st.rerun()

        with st.expander("➕ 添加新的现金账户"):
            with st.form("new_cash_account_form", clear_on_submit=True):
                new_acc_name = st.text_input("账户名称 (例如: 微信零钱)")
                new_acc_currency = st.selectbox("货币", options=SUPPORTED_CURRENCIES)
                new_acc_balance = st.number_input("初始余额", value=0.0, format="%.2f")
                if st.form_submit_button("添加账户"):
                    if new_acc_name and new_acc_currency:
                        user_data["users"][current_user_email]["portfolio"]["cash_accounts"].append({"name": new_acc_name, "currency": new_acc_currency, "balance": new_acc_balance})
                        if save_user_data_to_onedrive(user_data):
                            st.success(f"账户 '{new_acc_name}' 已添加！"); time.sleep(1); st.rerun()
                    else: st.warning("账户名称和货币不能为空。")

        st.write("---")
        st.subheader("编辑股票持仓")
        edited_stocks = st.data_editor(stock_holdings, num_rows="dynamic", key="stock_editor", column_config={"ticker": "股票代码", "quantity": st.column_config.NumberColumn("数量", format="%.2f")})
        if st.button("💾 保存对股票持仓的修改"):
            valid_stocks = [s for s in edited_stocks if s.get("ticker")]
            user_data["users"][current_user_email]["portfolio"]["stocks"] = valid_stocks
            if save_user_data_to_onedrive(user_data):
                st.success("股票持仓已更新！"); time.sleep(1); st.rerun()
        
        with st.expander("➕ 添加新的股票持仓"):
            with st.form("new_stock_form", clear_on_submit=True):
                new_stock_ticker = st.text_input("股票代码 (例如: AAPL)").upper()
                new_stock_quantity = st.number_input("持有数量", value=0.0, format="%.2f")
                if st.form_submit_button("添加持仓"):
                    if new_stock_ticker:
                        user_data["users"][current_user_email]["portfolio"]["stocks"].append({"ticker": new_stock_ticker, "quantity": new_stock_quantity})
                        if save_user_data_to_onedrive(user_data):
                            st.success(f"持仓 '{new_stock_ticker}' 已添加！"); time.sleep(1); st.rerun()
                    else: st.warning("股票代码不能为空。")

        st.write("---")
        st.subheader("记录一笔新流水")
        with st.form("transaction_form"):
            trans_type = st.selectbox("类型", ["收入", "支出", "买入股票", "卖出股票"])
            description = st.text_input("描述")
            amount = st.number_input("金额", min_value=0.0, format="%.2f")
            account_names = [acc.get("name", "") for acc in cash_accounts]
            affected_account_name = st.selectbox("选择现金账户", options=account_names) if account_names else None
            
            if trans_type in ["买入股票", "卖出股票"]:
                ticker = st.text_input("股票代码").upper()
                quantity = st.number_input("数量", min_value=0.0)

            if st.form_submit_button("记录流水"):
                if affected_account_name is None:
                    st.error("操作失败：请先至少创建一个现金账户。"); st.stop()
                
                new_transaction = {"date": datetime.now().strftime("%Y-%m-%d %H:%M"), "type": trans_type, "description": description, "amount": amount, "account": affected_account_name}
                
                for acc in user_data["users"][current_user_email]["portfolio"]["cash_accounts"]:
                    if acc.get("name") == affected_account_name:
                        if trans_type == "收入": acc["balance"] += amount
                        elif trans_type == "支出": acc["balance"] -= amount; new_transaction["amount"] = -amount
                        elif trans_type == "买入股票": acc["balance"] -= amount
                        elif trans_type == "卖出股票": acc["balance"] += amount
                        new_transaction["currency"] = acc.get("currency") # 记录流水货币
                        break
                
                if trans_type in ["买入股票", "卖出股票"]:
                    new_transaction.update({"ticker": ticker, "quantity": quantity})
                    current_holdings = {s['ticker']: s['quantity'] for s in user_data["users"][current_user_email]["portfolio"]["stocks"]}
                    if trans_type == "买入股票":
                        current_holdings[ticker] = current_holdings.get(ticker, 0) + quantity
                    elif trans_type == "卖出股票":
                        if current_holdings.get(ticker, 0) < quantity:
                            st.error("卖出数量超过持有数量！"); st.stop()
                        current_holdings[ticker] -= quantity
                    user_data["users"][current_user_email]["portfolio"]["stocks"] = [{"ticker": t, "quantity": q} for t, q in current_holdings.items() if q > 0]

                user_transactions.append(new_transaction)
                if save_user_data_to_onedrive(user_data):
                    st.success("流水记录成功！"); time.sleep(1); st.rerun()

# --- 主程序渲染 ---
if st.session_state.logged_in:
    with st.sidebar:
        st.success(f"欢迎, {st.session_state.user_email}")
        if st.button("退出登录"):
            st.session_state.logged_in = False; st.session_state.user_email = ""; st.session_state.login_step = "enter_email"
            st.rerun()
    display_dashboard()
    if st.session_state.user_email == ADMIN_EMAIL:
        display_admin_panel()
else:
    display_login_form()

