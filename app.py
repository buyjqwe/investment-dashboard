import streamlit as st
import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import requests
import re
import random
import time
import json
from datetime import datetime, timedelta
import secrets

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


# --- 初始化 Session State ---
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'user_email' not in st.session_state:
    st.session_state.user_email = ""
if 'login_step' not in st.session_state:
    st.session_state.login_step = "enter_email"
if 'display_currency' not in st.session_state:
    st.session_state.display_currency = "USD"


# --- Session Management ---
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
    elif token in st.query_params:
        st.query_params.clear()

check_session_from_query_params()


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

@st.cache_data(ttl=60)
def get_user_data_from_onedrive():
    try:
        token = get_ms_graph_token()
        headers = {"Authorization": f"Bearer {token}"}
        content_url = f"{ONEDRIVE_API_URL}:/content"
        resp = requests.get(content_url, headers=headers)
        if resp.status_code == 404:
            initial_data = {"users": {ADMIN_EMAIL: {"role": "admin", "portfolio": {"stocks": [{"ticker": "TSLA", "quantity": 10}], "cash_accounts": [{"name": "默认现金", "balance": 50000, "currency": "USD"}]}, "transactions": [], "asset_history": []}}, "codes": {}, "sessions": {}}
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
    # ... (code unchanged)
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
    # ... (code unchanged)
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
    # ... (code unchanged)
    user_data = get_user_data_from_onedrive()
    if user_data is None: return
    code_info = user_data.get("codes", {}).get(email)
    if not code_info or time.time() > code_info["expires_at"]:
        st.sidebar.error("验证码已过期或不存在。")
        return
    if code_info["code"] == code:
        if email not in user_data["users"]:
            user_data["users"][email] = {"role": "user", "portfolio": {"stocks": [{"ticker": "AAPL", "quantity": 10}, {"ticker": "GOOG", "quantity": 5}], "cash_accounts": [{"name": "美元银行卡", "balance": 10000, "currency": "USD"}, {"name": "人民币支付宝", "balance": 2000, "currency": "CNY"}]}, "transactions": [], "asset_history": []}
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
    else:
        st.sidebar.error("验证码错误。")

# --- UI 界面函数 ---
def display_login_form():
    # ... (code unchanged)
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
    # ... (code unchanged)
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
    # ... (code unchanged)
    prices = {}
    ts = TimeSeries(key=st.secrets["alpha_vantage"]["api_key"], output_format='pandas')
    for ticker in tickers:
        try:
            # For daily prices, use get_daily
            data, _ = ts.get_daily(symbol=ticker, outputsize='compact')
            prices[ticker] = data['4. close'].iloc[0]
        except Exception as e:
            st.warning(f"获取 {ticker} 股价失败: {e}")
            prices[ticker] = 0
    return prices

@st.cache_data(ttl=3600)
def get_historical_stock_price(ticker, date_str):
    try:
        ts = TimeSeries(key=st.secrets["alpha_vantage"]["api_key"], output_format='pandas')
        # Using get_daily with full outputsize to find the closest date
        data, _ = ts.get_daily(symbol=ticker, outputsize='full')
        # The index is datetime, so we can try to get the exact date
        if date_str in data.index:
            return data.loc[date_str]['4. close']
        else:
            # If exact date not found (e.g., weekend), find the closest previous date
            for i in range(1, 4):
                prev_date = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=i)).strftime("%Y-%m-%d")
                if prev_date in data.index:
                    return data.loc[prev_date]['4. close']
            return 0 # Fallback
    except:
        return 0

@st.cache_data(ttl=3600)
def get_exchange_rates(base_currency='USD', date_str=None):
    try:
        api_date = date_str if date_str else "latest"
        url = f"https://open.er-api.com/v6/{api_date}/{base_currency}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if data.get("result") == "success":
            return data.get("rates")
        else:
            st.error(f"获取 {api_date} 汇率API返回错误。")
            return None
    except Exception as e:
        st.error(f"获取 {api_date} 汇率失败: {e}")
        return None

def update_asset_snapshot(user_data, email, total_assets_usd, current_rates):
    today_str = datetime.now().strftime("%Y-%m-%d")
    user_profile = user_data["users"][email]
    asset_history = user_profile.setdefault("asset_history", [])
    
    # Check if the last snapshot was taken today
    if not asset_history or asset_history[-1]["date"] != today_str:
        snapshot = {
            "date": today_str,
            "total_assets_usd": total_assets_usd,
            "exchange_rates": current_rates,
            "stock_holdings": user_profile["portfolio"]["stocks"],
            "cash_accounts": user_profile["portfolio"]["cash_accounts"]
        }
        asset_history.append(snapshot)
        if save_user_data_to_onedrive(user_data):
            st.toast("今日资产快照已生成！")
        return True
    return False

# --- 新增：资产分析标签页 ---
def display_analysis_tab(user_data, email, display_curr, display_symbol, display_rate):
    st.subheader("📈 历史资产总览")
    
    asset_history = user_data["users"][email].get("asset_history", [])
    
    if len(asset_history) < 2:
        st.info("历史数据不足（少于2天），暂无法进行分析。请明天再来看看！")
        return

    history_df = pd.DataFrame(asset_history)
    history_df["date"] = pd.to_datetime(history_df["date"])
    history_df = history_df.set_index("date")
    history_df[f"total_assets_{display_curr}"] = history_df["total_assets_usd"] * display_rate
    
    st.line_chart(history_df[f"total_assets_{display_curr}"])
    
    st.subheader("🔍 资产变动归因分析")
    
    options = [7, 15, 30, 60]
    period_days = st.selectbox("选择分析周期（天）", options, index=0)
    
    end_snapshot = asset_history[-1]
    start_date = (datetime.strptime(end_snapshot["date"], "%Y-%m-%d") - timedelta(days=period_days)).strftime("%Y-%m-%d")
    
    # Find the closest snapshot to the start_date
    start_snapshot = None
    for snapshot in reversed(asset_history):
        if snapshot["date"] <= start_date:
            start_snapshot = snapshot
            break
            
    if not start_snapshot:
        st.warning(f"未找到 {period_days} 天前的资产快照，无法进行精确比较。")
        return

    # 1. Calculate Total Change
    total_change_usd = end_snapshot["total_assets_usd"] - start_snapshot["total_assets_usd"]
    
    # 2. Decompose Change: Market Fluctuation
    market_change_usd = 0
    end_stock_prices = st.session_state.get('stock_prices', {})
    
    # Combine stocks from both snapshots to handle cases where a stock was bought or sold
    all_tickers = set([s['ticker'] for s in start_snapshot.get("stock_holdings", [])] + [s['ticker'] for s in end_snapshot.get("stock_holdings", [])])

    for ticker in all_tickers:
        start_holding = next((s for s in start_snapshot["stock_holdings"] if s["ticker"] == ticker), {"quantity": 0})
        end_holding = next((s for s in end_snapshot["stock_holdings"] if s["ticker"] == ticker), {"quantity": 0})
        
        # We only attribute market change to the shares that were held throughout the period
        common_quantity = min(start_holding["quantity"], end_holding["quantity"])
        if common_quantity > 0:
            start_price = get_historical_stock_price(ticker, start_snapshot["date"])
            end_price = end_stock_prices.get(ticker, 0)
            market_change_usd += common_quantity * (end_price - start_price)

    # 3. Decompose Change: User Cash Flow
    cash_flow_usd = 0
    transactions = user_data["users"][email].get("transactions", [])
    for trans in transactions:
        if start_snapshot["date"] < trans["date"] <= end_snapshot["date"]:
            amount = trans.get("amount", 0)
            if trans["type"] in ["收入", "卖出股票"]:
                cash_flow_usd += abs(amount)
            elif trans["type"] in ["支出", "买入股票"]:
                cash_flow_usd -= abs(amount)

    # 4. Decompose Change: FX Fluctuation
    fx_change_usd = 0
    start_rates = start_snapshot.get("exchange_rates", {})
    end_rates = end_snapshot.get("exchange_rates", {})
    
    for account in start_snapshot.get("cash_accounts", []):
        currency = account.get("currency")
        if currency != 'USD' and currency in start_rates and currency in end_rates:
            balance = account.get("balance", 0)
            start_rate_inv = 1 / start_rates[currency]
            end_rate_inv = 1 / end_rates[currency]
            fx_change_usd += balance * (end_rate_inv - start_rate_inv)
            
    st.metric(
        f"期间总资产变化 ({display_curr})",
        f"{display_symbol}{total_change_usd * display_rate:,.2f}",
        f"{display_symbol}{(total_change_usd - market_change_usd - cash_flow_usd - fx_change_usd) * display_rate:,.2f} (其他)"
    )
    
    col1, col2, col3 = st.columns(3)
    col1.metric("📈 市场波动盈亏", f"{display_symbol}{market_change_usd * display_rate:,.2f}")
    col2.metric("💸 主动资金流动", f"{display_symbol}{cash_flow_usd * display_rate:,.2f}")
    col3.metric("💱 汇率波动影响", f"{display_symbol}{fx_change_usd * display_rate:,.2f}")


def display_dashboard():
    st.title(f"💰 {st.session_state.user_email} 的资产仪表盘")
    user_data = get_user_data_from_onedrive()
    if user_data is None: st.stop()

    current_user_email = st.session_state.user_email
    user_portfolio = user_data["users"][current_user_email].setdefault("portfolio", {"stocks": [], "cash_accounts": [], "transactions": []})
    user_data["users"][current_user_email].setdefault("asset_history", [])
    
    # --- 数据结构迁移 ---
    data_migrated = False
    if "cash" in user_portfolio:
        cash_value = user_portfolio.pop("cash")
        user_portfolio["cash_accounts"] = [{"name": "默认现金", "balance": cash_value, "currency": "USD"}]
        data_migrated = True
    for account in user_portfolio.get("cash_accounts", []):
        if "currency" not in account:
            account["currency"] = "USD"
            data_migrated = True
    if data_migrated and save_user_data_to_onedrive(user_data):
        st.toast("数据结构已自动更新以支持多货币！"); st.rerun()
    
    # --- 获取数据 ---
    user_transactions = user_data["users"][current_user_email].setdefault("transactions", [])
    cash_accounts = user_portfolio.get("cash_accounts", [])
    stock_holdings = user_portfolio.get("stocks", [])
    
    tickers_to_fetch = [s['ticker'] for s in stock_holdings if s.get('ticker')]
    if 'stock_prices' not in st.session_state or st.button('🔄 刷新市场数据'):
        with st.spinner("正在获取最新市场数据..."):
            st.session_state.stock_prices = get_stock_prices(tickers_to_fetch)
            st.session_state.exchange_rates = get_exchange_rates()
    
    stock_prices = st.session_state.get('stock_prices', {})
    exchange_rates = st.session_state.get('exchange_rates', {})

    if not exchange_rates:
        st.error("无法加载汇率，资产总值可能不准确。"); st.stop()

    # --- 资产计算 ---
    total_stock_value_usd = sum(s['quantity'] * stock_prices.get(s['ticker'], 0) for s in stock_holdings)
    total_cash_balance_usd = sum(acc.get('balance', 0) / exchange_rates.get(acc.get('currency', 'USD').upper(), 1) for acc in cash_accounts)
    total_assets_usd = total_stock_value_usd + total_cash_balance_usd

    # --- 创建今日快照 ---
    update_asset_snapshot(user_data, current_user_email, total_assets_usd, exchange_rates)

    # --- 顶部UI ---
    st.sidebar.selectbox("选择显示货币", options=SUPPORTED_CURRENCIES, key="display_currency")
    display_curr = st.session_state.display_currency
    display_rate = exchange_rates.get(display_curr, 1)
    display_symbol = CURRENCY_SYMBOLS.get(display_curr, "")

    col1, col2, col3 = st.columns(3)
    col1.metric("💰 资产总值", f"{display_symbol}{total_assets_usd * display_rate:,.2f} {display_curr}")
    col2.metric("📈 股票市值", f"{display_symbol}{total_stock_value_usd * display_rate:,.2f} {display_curr}")
    col3.metric("💵 现金总额", f"{display_symbol}{total_cash_balance_usd * display_rate:,.2f} {display_curr}")

    # --- 标签页 ---
    tab1, tab2, tab3, tab4 = st.tabs(["📊 持仓与流水", "📈 资产分析", "💹 股价图表", "⚙️ 管理资产"])

    with tab1:
        # ... (code unchanged from previous version)
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
        display_analysis_tab(user_data, current_user_email, display_curr, display_symbol, display_rate)

    with tab3:
        # ... (code unchanged, moved from old tab2)
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
        
    with tab4:
        # ... (code unchanged, moved from old tab3)
        st.subheader("⚙️ 管理资产")
        st.subheader("编辑现金账户")
        edited_cash_accounts = st.data_editor(cash_accounts, num_rows="dynamic", key="cash_editor", column_config={"name": "账户名称", "currency": st.column_config.SelectboxColumn("货币", options=SUPPORTED_CURRENCIES, required=True), "balance": st.column_config.NumberColumn("余额", format="%.2f", required=True)})
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
                        new_transaction["currency"] = acc.get("currency")
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
            token_to_remove = st.query_params.get("session_token")
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            
            if token_to_remove:
                user_data = get_user_data_from_onedrive()
                if user_data:
                    sessions = user_data.get("sessions", {})
                    if token_to_remove in sessions:
                        del sessions[token_to_remove]
                        save_user_data_to_onedrive(user_data)
                st.query_params.clear()
            else:
                st.rerun()

    display_dashboard()
    if st.session_state.user_email == ADMIN_EMAIL:
        display_admin_panel()
else:
    display_login_form()

