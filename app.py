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
import yfinance as yf

# --- 页面基础设置 ---
st.set_page_config(page_title="专业投资分析仪表盘", page_icon="🚀", layout="wide")

# --- 全局常量 ---
SUPPORTED_CURRENCIES = ["USD", "CNY", "EUR", "HKD", "JPY", "GBP"]
CURRENCY_SYMBOLS = {"USD": "$", "CNY": "¥", "EUR": "€", "HKD": "HK$", "JPY": "¥", "GBP": "£"}
SESSION_EXPIRATION_DAYS = 7
DATA_REFRESH_INTERVAL_SECONDS = 3600 # 1 hour
BASE_ONEDRIVE_PATH = "root:/Apps/StreamlitDashboard"
OUNCES_TO_GRAMS = 31.1035

# --- 初始化 Session State ---
if 'logged_in' not in st.session_state: st.session_state.logged_in = False
if 'user_email' not in st.session_state: st.session_state.user_email = ""
if 'login_step' not in st.session_state: st.session_state.login_step = "enter_email"
if 'display_currency' not in st.session_state: st.session_state.display_currency = "USD"
if 'last_market_data_fetch' not in st.session_state: st.session_state.last_market_data_fetch = 0
if 'migration_done' not in st.session_state: st.session_state.migration_done = False

# --- API 配置 ---
MS_GRAPH_CONFIG = st.secrets["microsoft_graph"]
ADMIN_EMAIL = MS_GRAPH_CONFIG["admin_email"]
ONEDRIVE_SENDER_EMAIL = MS_GRAPH_CONFIG['sender_email']
CF_CONFIG = st.secrets["cloudflare"]

# --- 核心功能函数定义 ---
def get_email_hash(email): return hashlib.sha256(email.encode('utf-8')).hexdigest()

@st.cache_data(ttl=3500)
def get_ms_graph_token():
    url = f"https://login.microsoftonline.com/{MS_GRAPH_CONFIG['tenant_id']}/oauth2/v2.0/token"
    data = {"grant_type": "client_credentials", "client_id": MS_GRAPH_CONFIG['client_id'], "client_secret": MS_GRAPH_CONFIG['client_secret'], "scope": "https://graph.microsoft.com/.default"}
    resp = requests.post(url, data=data); resp.raise_for_status(); return resp.json()["access_token"]

def onedrive_api_request(method, path, headers, data=None):
    base_url = f"https://graph.microsoft.com/v1.0/users/{ONEDRIVE_SENDER_EMAIL}/drive"
    url = f"{base_url}/{path}"
    if method.lower() == 'get': return requests.get(url, headers=headers)
    if method.lower() == 'put': return requests.put(url, headers=headers, data=data)
    return None

def get_onedrive_data(path, is_json=True):
    try:
        token = get_ms_graph_token(); headers = {"Authorization": f"Bearer {token}"}
        resp = onedrive_api_request('get', f"{path}:/content", headers)
        if resp.status_code == 404: return None
        resp.raise_for_status(); return resp.json() if is_json else resp.text
    except Exception as e:
        if "404" not in str(e): st.error(f"从 OneDrive 加载数据失败 ({path}): {e}")
        return None

def save_onedrive_data(path, data):
    try:
        token = get_ms_graph_token(); headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        json_data = json.dumps(data, indent=2, ensure_ascii=False)
        onedrive_api_request('put', f"{path}:/content", headers, data=json_data.encode('utf-8'))
        return True
    except Exception as e: st.error(f"保存数据到 OneDrive 失败 ({path}): {e}"); return False

def get_user_profile(email): return get_onedrive_data(f"{BASE_ONEDRIVE_PATH}/users/{get_email_hash(email)}.json")
def save_user_profile(email, data): return save_onedrive_data(f"{BASE_ONEDRIVE_PATH}/users/{get_email_hash(email)}.json", data)
def get_global_data(file_name): data = get_onedrive_data(f"{BASE_ONEDRIVE_PATH}/{file_name}.json"); return data if data else {}
def save_global_data(file_name, data): return save_onedrive_data(f"{BASE_ONEDRIVE_PATH}/{file_name}.json", data)

def send_verification_code(email, code):
    try:
        token = get_ms_graph_token(); url = f"https://graph.microsoft.com/v1.0/users/{ONEDRIVE_SENDER_EMAIL}/sendMail"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {"message": {"subject": f"[{code}] 您的登录/注册验证码", "body": {"contentType": "Text", "content": f"您的验证码是：{code}，5分钟内有效。"}, "toRecipients": [{"emailAddress": {"address": email}}]}, "saveToSentItems": "true"}
        requests.post(url, headers=headers, json=payload, timeout=10).raise_for_status(); return True
    except Exception as e: st.error(f"邮件发送失败: {e}"); return False

def handle_send_code(email):
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email): st.sidebar.error("请输入有效的邮箱地址。"); return
    codes = get_global_data("codes"); code = str(random.randint(100000, 999999))
    codes[email] = {"code": code, "expires_at": time.time() + 300}
    if not save_global_data("codes", codes) or not send_verification_code(email, code): return
    st.sidebar.success("验证码已发送，请查收。"); st.session_state.login_step = "enter_code"; st.session_state.temp_email = email; st.rerun()

def handle_verify_code(email, code):
    codes = get_global_data("codes"); code_info = codes.get(email)
    if not code_info or time.time() > code_info["expires_at"]: st.sidebar.error("验证码已过期或不存在。"); return
    if code_info["code"] == code:
        if not get_user_profile(email):
            new_profile = {"role": "user", "portfolio": {"stocks": [], "cash_accounts": [], "crypto": [], "liabilities": [], "transactions": [], "gold": []}}
            save_user_profile(email, new_profile); st.toast("🎉 欢迎新用户！已为您创建账户。")
        sessions, token = get_global_data("sessions"), secrets.token_hex(16)
        sessions[token] = {"email": email, "expires_at": time.time() + (SESSION_EXPIRATION_DAYS * 24 * 60 * 60)}
        save_global_data("sessions", sessions); del codes[email]; save_global_data("codes", codes)
        st.session_state.logged_in, st.session_state.user_email, st.session_state.login_step, st.query_params["session_token"] = True, email, "logged_in", token
        st.rerun()
    else: st.sidebar.error("验证码错误。")

def check_session_from_query_params():
    if st.session_state.get('logged_in'): return
    token = st.query_params.get("session_token")
    if not token: return
    sessions = get_global_data("sessions"); session_info = sessions.get(token)
    if session_info and time.time() < session_info.get("expires_at", 0):
        st.session_state.logged_in, st.session_state.user_email, st.session_state.login_step = True, session_info["email"], "logged_in"
    elif "session_token" in st.query_params: st.query_params.clear()

@st.cache_data(ttl=600)
def get_market_data_yf(tickers_to_fetch, for_date=None):
    market_data = {}
    if not tickers_to_fetch: return market_data
    try:
        if for_date:
            # --- 历史数据模式 (逻辑不变) ---
            start_date, end_date = for_date, for_date + timedelta(days=1)
            data = yf.download(tickers=tickers_to_fetch, start=start_date, end=end_date, progress=False, timeout=10)
            if data.empty: return {}
            prices = data['Close'].iloc[0] if 'Close' in data.columns else data.iloc[0]
            for ticker in tickers_to_fetch:
                price = prices.get(ticker) if isinstance(prices, pd.Series) else prices[0] if not prices.empty else 0
                market_data[ticker] = {"latest_price": price if pd.notna(price) else 0}
        else:
            # --- 实时数据模式 (核心修改部分) ---
            # 使用 yf.download 批量获取数据，更高效稳定
            data = yf.download(tickers=tickers_to_fetch, period="5d", progress=False, timeout=10)
            if data.empty:
                st.warning(f"无法通过 yf.download 获取任何价格数据。")
                return market_data

            for ticker in tickers_to_fetch:
                try:
                    # 优先从批量下载的数据中获取最新收盘价
                    # yfinance对于单/多ticker返回的数据结构不同，需要分别处理
                    if isinstance(data.columns, pd.MultiIndex):
                        # 多ticker情况
                        ticker_close_series = data[('Close', ticker)].dropna()
                    else:
                        # 单ticker情况
                        ticker_close_series = data['Close'].dropna()
                    
                    if not ticker_close_series.empty:
                        price = ticker_close_series.iloc[-1]
                        market_data[ticker] = {"latest_price": price}
                    else:
                        # 如果批量下载的数据中没有此ticker，则回退到单独查询
                        t_info = yf.Ticker(ticker).info
                        price = t_info.get('regularMarketPrice') or t_info.get('currentPrice') or t_info.get('previousClose') or 0
                        market_data[ticker] = {"latest_price": price}
                except Exception:
                    market_data[ticker] = {"latest_price": 0}

    except Exception as e: st.warning(f"使用yfinance获取市场价格时出错: {e}")
    return market_data


def get_prices_from_market_data(market_data, tickers):
    prices = {}
    for t in tickers:
        original_ticker = t.replace('-USD', '')
        prices[original_ticker] = market_data.get(t, {}).get("latest_price", 0)
    return prices

@st.cache_data(ttl=86400)
def get_stock_profile_yf(symbol):
    try:
        ticker = yf.Ticker(symbol); info = ticker.info
        if info and info.get('shortName'): return info
    except Exception: return None
    return None

@st.cache_data(ttl=3600)
def get_historical_data_yf(symbol, days=365):
    try:
        ticker = yf.Ticker(symbol); hist = ticker.history(period=f"{days}d")
        if not hist.empty: return hist # Return the full dataframe
    except Exception: return pd.DataFrame() # Return empty dataframe on error
    return pd.DataFrame()

def get_exchange_rates():
    try:
        resp = requests.get(f"https://open.er-api.com/v6/latest/USD")
        resp.raise_for_status(); data = resp.json()
        return data.get("rates") if data.get("result") == "success" else None
    except Exception as e: st.error(f"获取汇率失败: {e}"); return None

def get_asset_history(email):
    history = []
    try:
        token = get_ms_graph_token(); headers = {"Authorization": f"Bearer {token}"}
        email_hash = get_email_hash(email)
        path = f"{BASE_ONEDRIVE_PATH}/history/{email_hash}:/children"
        resp = onedrive_api_request('get', path, headers)
        if resp.status_code == 404: return []
        resp.raise_for_status(); files = resp.json().get('value', [])
        for file in files:
            file_path = f"{BASE_ONEDRIVE_PATH}/history/{email_hash}/{file['name']}"
            snapshot = get_onedrive_data(file_path)
            if snapshot: history.append(snapshot)
    except Exception: return []
    return sorted(history, key=lambda x: x['date'])

def get_closest_snapshot(target_date, asset_history):
    if not asset_history: return None
    relevant_snapshots = [s for s in asset_history if s['date'] <= target_date.strftime('%Y-%m-%d')]
    if not relevant_snapshots: return None
    return max(relevant_snapshots, key=lambda x: x['date'])

def update_asset_snapshot(email, user_profile, total_assets_usd, total_liabilities_usd, total_stock_value_usd, total_cash_balance_usd, total_crypto_value_usd, total_gold_value_usd, current_rates):
    today_str = datetime.now().strftime("%Y-%m-%d")
    if not get_onedrive_data(f"{BASE_ONEDRIVE_PATH}/history/{get_email_hash(email)}/{today_str}.json"):
        st.toast("今日资产快照已生成！")
        snapshot = {"date": today_str, "total_assets_usd": total_assets_usd, "total_liabilities_usd": total_liabilities_usd, "net_worth_usd": total_assets_usd - total_liabilities_usd, "total_stock_value_usd": total_stock_value_usd, "total_cash_balance_usd": total_cash_balance_usd, "total_crypto_value_usd": total_crypto_value_usd, "total_gold_value_usd": total_gold_value_usd, "exchange_rates": current_rates, "portfolio": user_profile["portfolio"]}
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
    except Exception as e: return f"无法连接到 AI 服务进行分析: {e}"

def display_login_form():
    with st.sidebar:
        st.header("🔐 邮箱登录/注册")
        if st.session_state.login_step == "enter_email":
            email = st.text_input("邮箱地址", key="email_input");
            if st.button("发送验证码"): handle_send_code(email)
        elif st.session_state.login_step == "enter_code":
            email_display = st.session_state.get("temp_email", "")
            st.info(f"验证码已发送至: {email_display}")
            code = st.text_input("验证码", key="code_input")
            if st.button("登录或注册"): handle_verify_code(email_display, code)
            if st.button("返回"): st.session_state.login_step = "enter_email"; st.rerun()

def display_admin_panel(): st.sidebar.header("👑 管理员面板"); st.info("管理员功能待适配新数据结构。")
def display_asset_allocation_chart(stock_usd, cash_usd, crypto_usd, gold_usd, display_curr, display_rate, display_symbol):
    labels, values_usd = ['股票', '现金', '加密货币', '黄金'], [stock_usd, cash_usd, crypto_usd, gold_usd]
    non_zero_labels, non_zero_values = [l for l, v in zip(labels, values_usd) if v > 0.01], [v for v in values_usd if v > 0.01]
    if not non_zero_values: st.info("暂无资产可供分析。"); return
    fig = go.Figure(data=[go.Pie(labels=non_zero_labels, values=[v * display_rate for v in non_zero_values], hole=.4, textinfo='percent+label', hovertemplate=f"<b>%{{label}}</b><br>价值: {display_symbol}%{{value:,.2f}} {display_curr}<br>占比: %{{percent}}<extra></extra>")])
    fig.update_layout(title_text='资产配置', showlegend=False, height=300, margin=dict(l=10, r=10, t=40, b=10))
    st.plotly_chart(fig, use_container_width=True)

def display_dashboard():
    st.sidebar.header("分析模式")
    analysis_mode = st.sidebar.radio("选择视图", ["实时数据", "历史快照"], key="analysis_mode")
    asset_history = get_asset_history(st.session_state.user_email)
    
    start_date, end_date, start_snapshot, end_snapshot, user_profile = None, None, None, None, None
    
    if analysis_mode == "历史快照":
        if len(asset_history) < 1: st.warning("无任何历史数据，无法使用快照分析。"); st.stop()
        max_date, min_date = datetime.strptime(asset_history[-1]['date'], '%Y-%m-%d').date(), datetime.strptime(asset_history[0]['date'], '%Y-%m-%d').date()
        end_date = st.sidebar.date_input("结束日期", value=max_date, min_value=min_date, max_value=max_date)
        default_start_date = end_date - timedelta(days=7)
        if default_start_date < min_date: default_start_date = min_date
        start_date = st.sidebar.date_input("开始日期", value=default_start_date, min_value=min_date, max_value=end_date)
        start_snapshot, end_snapshot = get_closest_snapshot(start_date, asset_history), get_closest_snapshot(end_date, asset_history)
        if not end_snapshot: st.error("未能找到所选日期范围内的有效数据快照。"); st.stop()
        st.title(f"🚀 资产分析 (快照: {end_snapshot['date']})")
        user_portfolio, exchange_rates = end_snapshot['portfolio'], end_snapshot['exchange_rates']
        stock_tickers, crypto_symbols = [s['ticker'] for s in user_portfolio.get("stocks", [])], [c['symbol'] for c in user_portfolio.get("crypto", [])]
        y_crypto_tickers = [f"{s.upper()}-USD" for s in crypto_symbols]
        all_yf_tickers = stock_tickers + y_crypto_tickers + ["GC=F"]
        with st.spinner(f"正在获取 {end_snapshot['date']} 的历史价格..."):
            market_data = get_market_data_yf(all_yf_tickers, for_date=datetime.strptime(end_snapshot['date'], '%Y-%m-%d'))
        prices = get_prices_from_market_data(market_data, stock_tickers + crypto_symbols + ["GC=F"])

    else: # 实时数据模式
        st.title(f"🚀 {st.session_state.user_email} 的专业仪表盘")
        user_profile = get_user_profile(st.session_state.user_email)
        if user_profile is None: st.error("无法加载用户数据。"); st.stop()
        user_portfolio = user_profile.setdefault("portfolio", {})
        for key in ["stocks", "cash_accounts", "crypto", "liabilities", "transactions", "gold"]: user_portfolio.setdefault(key, [])
        stock_tickers, crypto_symbols = [s['ticker'] for s in user_portfolio.get("stocks", [])], [c['symbol'] for c in user_portfolio.get("crypto", [])]
        last_fetched_tickers, current_tickers = st.session_state.get('last_fetched_tickers', set()), set(stock_tickers + crypto_symbols)
        tickers_changed = current_tickers != last_fetched_tickers
        if st.sidebar.button('🔄 刷新市场数据'): st.session_state.last_market_data_fetch = 0 
        now = time.time()
        if tickers_changed or (now - st.session_state.last_market_data_fetch > DATA_REFRESH_INTERVAL_SECONDS):
            with st.spinner("正在获取最新市场数据 (yfinance)..."):
                y_crypto_tickers = [f"{s.upper()}-USD" for s in crypto_symbols]
                all_yf_tickers = list(set(stock_tickers + y_crypto_tickers + ["GC=F"])) # Use set to avoid duplicates
                st.session_state.market_data = get_market_data_yf(all_yf_tickers)
                st.session_state.exchange_rates = get_exchange_rates()
                st.session_state.last_market_data_fetch, st.session_state.last_fetched_tickers = now, current_tickers
                st.rerun()
        market_data, prices, exchange_rates = st.session_state.get('market_data', {}), get_prices_from_market_data(st.session_state.get('market_data', {}), stock_tickers + crypto_symbols + ["GC=F"]), st.session_state.get('exchange_rates', {})
        if not exchange_rates: st.error("无法加载汇率，资产总值不准确。"); st.stop()

    all_holdings = user_portfolio.get("stocks", []) + user_portfolio.get("crypto", []) + user_portfolio.get("gold", [])
    failed_tickers = []
    for h in all_holdings:
        ticker = h.get('ticker') or h.get('symbol') or "黄金"
        price_key = h.get('ticker') or h.get('symbol')
        if ticker == "黄金": price_key = "GC=F"
        if prices.get(price_key, 0) == 0:
            failed_tickers.append(ticker)
    if failed_tickers:
        st.warning(f"警告：未能获取以下资产的价格，其市值可能显示为0: {', '.join(failed_tickers)}")
    
    gold_price_per_ounce = prices.get("GC=F", 0)
    gold_price_per_gram = gold_price_per_ounce / OUNCES_TO_GRAMS if gold_price_per_ounce > 0 else 0

    stock_holdings, cash_accounts, crypto_holdings, liabilities, gold_holdings = user_portfolio.get("stocks", []), user_portfolio.get("cash_accounts", []), user_portfolio.get("crypto", []), user_portfolio.get("liabilities", []), user_portfolio.get("gold", [])
    total_stock_value_usd = sum(s.get('quantity',0) * prices.get(s['ticker'], 0) / exchange_rates.get(s.get('currency', 'USD'), 1) for s in stock_holdings)
    total_cash_balance_usd = sum(acc.get('balance',0) / exchange_rates.get(acc.get('currency', 'USD'), 1) for acc in cash_accounts)
    total_crypto_value_usd = sum(c.get('quantity',0) * prices.get(c['symbol'], 0) for c in crypto_holdings)
    total_gold_value_usd = sum(g.get('grams', 0) * gold_price_per_gram for g in gold_holdings)
    total_assets_usd = total_stock_value_usd + total_cash_balance_usd + total_crypto_value_usd + total_gold_value_usd
    total_liabilities_usd = sum(liab.get('balance',0) / exchange_rates.get(liab.get('currency', 'USD'), 1) for liab in liabilities)
    net_worth_usd = total_assets_usd - total_liabilities_usd
    
    if analysis_mode == "实时数据" and user_profile is not None:
        update_asset_snapshot(st.session_state.user_email, user_profile, total_assets_usd, total_liabilities_usd, total_stock_value_usd, total_cash_balance_usd, total_crypto_value_usd, total_gold_value_usd, exchange_rates)

    display_curr = st.sidebar.selectbox("选择显示货币", options=SUPPORTED_CURRENCIES, key="display_currency")
    display_rate, display_symbol = exchange_rates.get(display_curr, 1), CURRENCY_SYMBOLS.get(display_curr, "")

    st.header("财务状况核心指标")
    delta_value, delta_str = None, ""
    if analysis_mode == "历史快照" and start_snapshot:
        start_net_worth_usd = start_snapshot.get('net_worth_usd', 0); delta_value = net_worth_usd - start_net_worth_usd
        delta_str = f"({start_snapshot['date']} 至今)"
    col1, col2, col3 = st.columns(3)
    col1.metric("🏦 净资产", f"{display_symbol}{net_worth_usd * display_rate:,.2f} {display_curr}", delta=f"{display_symbol}{delta_value * display_rate:,.2f} {delta_str}" if delta_value is not None else None)
    col2.metric("💰 总资产", f"{display_symbol}{total_assets_usd * display_rate:,.2f} {display_curr}")
    col3.metric("💳 总负债", f"{display_symbol}{total_liabilities_usd * display_rate:,.2f} {display_curr}")

    stock_df_data = [{"代码": s['ticker'], "数量": s['quantity'], "货币": s['currency'], "成本价": f"{CURRENCY_SYMBOLS.get(s.get('currency', 'USD'), '')}{s.get('average_cost', 0):,.2f}", "现价": f"{CURRENCY_SYMBOLS.get(s.get('currency', 'USD'), '')}{prices.get(s['ticker'], 0):,.2f}", "市值": f"{CURRENCY_SYMBOLS.get(s.get('currency', 'USD'), '')}{s.get('quantity', 0) * prices.get(s['ticker'], 0):,.2f}", "未实现盈亏": f"{CURRENCY_SYMBOLS.get(s.get('currency', 'USD'), '')}{(s.get('quantity', 0) * prices.get(s['ticker'], 0)) - (s.get('quantity', 0) * s.get('average_cost', 0)):,.2f}", "回报率(%)": f"{(((s.get('quantity', 0) * prices.get(s['ticker'], 0)) - (s.get('quantity', 0) * s.get('average_cost', 0))) / (s.get('quantity', 0) * s.get('average_cost', 0)) * 100) if (s.get('quantity', 0) * s.get('average_cost', 0)) > 0 else 0:.2f}%"} for s in stock_holdings]
    crypto_df_data = [{"代码": c['symbol'], "数量": f"{c.get('quantity',0):.6f}", "成本价": f"${c.get('average_cost', 0):,.2f}", "现价": f"${prices.get(c['symbol'], 0):,.2f}", "市值": f"${c.get('quantity', 0) * prices.get(c['symbol'], 0):,.2f}", "未实现盈亏": f"${(c.get('quantity', 0) * prices.get(c['symbol'], 0)) - (c.get('quantity', 0) * c.get('average_cost', 0)):,.2f}", "回报率(%)": f"{(((c.get('quantity', 0) * prices.get(c['symbol'], 0)) - (c.get('quantity', 0) * c.get('average_cost', 0))) / (c.get('quantity', 0) * c.get('average_cost', 0)) * 100) if (c.get('quantity', 0) * c.get('average_cost', 0)) > 0 else 0:.2f}%"} for c in crypto_holdings]
    gold_df_data = [{"资产": "黄金", "克数 (g)": g.get('grams', 0), "成本价 ($/g)": f"${g.get('average_cost_per_gram', 0):,.2f}", "现价 ($/g)": f"${gold_price_per_gram:,.2f}", "市值": f"${g.get('grams', 0) * gold_price_per_gram:,.2f}", "未实现盈亏": f"${(g.get('grams', 0) * gold_price_per_gram) - (g.get('grams', 0) * g.get('average_cost_per_gram', 0)):,.2f}", "回报率(%)": f"{(((g.get('grams', 0) * gold_price_per_gram) - (g.get('grams', 0) * g.get('average_cost_per_gram', 0))) / (g.get('grams', 0) * g.get('average_cost_per_gram', 0)) * 100) if (g.get('grams', 0) * g.get('average_cost_per_gram', 0)) > 0 else 0:.2f}%"} for g in gold_holdings]

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📊 资产总览", "✍️ 交易管理", "⚙️ 编辑资产", "📈 历史趋势", "🔬 行业透视", "🤖 AI深度分析"])

    with tab1:
        st.subheader("资产配置概览"); display_asset_allocation_chart(total_stock_value_usd, total_cash_balance_usd, total_crypto_value_usd, total_gold_value_usd, display_curr, display_rate, display_symbol)
        st.subheader("资产与盈亏明细")
        st.write("📈 **股票持仓**"); st.table(pd.DataFrame(stock_df_data))
        st.write("🥇 **黄金持仓**"); st.table(pd.DataFrame(gold_df_data))
        c1, c2, c3 = st.columns(3)
        with c1: st.write("💵 **现金账户**"); st.table(pd.DataFrame([{"账户名称": acc['name'],"货币": acc['currency'], "余额": f"{CURRENCY_SYMBOLS.get(acc['currency'], '')}{acc['balance']:,.2f}"} for acc in cash_accounts]))
        with c2: st.write("🪙 **加密货币持仓**"); st.table(pd.DataFrame(crypto_df_data))
        with c3: st.write("💳 **负债账户**"); st.table(pd.DataFrame([{"名称": liab['name'],"货币": liab['currency'], "金额": f"{CURRENCY_SYMBOLS.get(liab['currency'], '')}{liab['balance']:,.2f}"} for liab in liabilities]))

    with tab2:
        if analysis_mode == "历史快照":
            st.info("在历史快照模式下，交易管理功能被禁用。")
        else:
            st.subheader("✍️ 记录一笔新流水")
            with st.form("transaction_form", clear_on_submit=True):
                trans_type = st.selectbox("类型", ["收入", "支出", "买入股票", "卖出股票", "买入加密货币", "卖出加密货币", "转账"]); col1, col2 = st.columns(2)
                with col1:
                    description = st.text_input("描述"); amount = st.number_input("总金额", min_value=0.01, format="%.2f")
                    from_account_name = st.selectbox("选择现金账户", [acc.get("name", "") for acc in cash_accounts], key="from_acc")
                with col2:
                    symbol, quantity, to_account_name = "", 0.0, None
                    if "股票" in trans_type or "加密货币" in trans_type:
                        symbol = st.text_input("资产代码").upper()
                        if "股票" in trans_type: quantity = st.number_input("数量", min_value=1e-4, format="%.4f")
                        else: quantity = st.number_input("数量", min_value=1e-8, format="%.8f")
                    elif trans_type == "转账": to_account_name = st.selectbox("转入账户", [n for n in [acc.get("name", "") for acc in cash_accounts] if n != from_account_name], key="to_acc")
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
                    elif trans_type == "买入股票":
                        if from_account["balance"] < amount: st.error("现金账户余额不足！"); st.stop()
                        if quantity <= 0: st.error("数量必须大于0"); st.stop()
                        profile = get_stock_profile_yf(symbol)
                        if not profile or not profile.get("currency"): st.error(f"无法获取股票 {symbol} 的信息，请检查代码是否有效。"); st.stop()
                        stock_currency, cash_currency = profile["currency"].upper(), from_account["currency"]
                        amount_in_usd, cost_in_stock_currency = amount / exchange_rates.get(cash_currency, 1), (amount / exchange_rates.get(cash_currency, 1)) * exchange_rates.get(stock_currency, 1)
                        price_per_unit = cost_in_stock_currency / quantity
                        from_account["balance"] -= amount
                        holding = next((h for h in stock_holdings if h.get("ticker") == symbol), None)
                        if holding:
                            old_cost_basis, new_quantity = (holding.get('average_cost', 0) * holding.get('quantity', 0)), (holding.get('quantity', 0) + quantity)
                            holding['quantity'], holding['average_cost'] = new_quantity, (old_cost_basis + cost_in_stock_currency) / new_quantity
                        else: stock_holdings.append({"ticker": symbol, "quantity": quantity, "average_cost": price_per_unit, "currency": stock_currency})
                    elif trans_type == "买入加密货币":
                        if from_account["balance"] < amount: st.error("现金账户余额不足！"); st.stop()
                        if quantity <= 0: st.error("数量必须大于0"); st.stop()
                        from_account["balance"] -= amount; price_per_unit = amount / quantity
                        holding = next((h for h in crypto_holdings if h.get("symbol") == symbol), None)
                        if holding:
                            new_total_cost = (holding.get('average_cost', 0) * holding.get('quantity', 0)) + amount
                            holding['quantity'] += quantity; holding['average_cost'] = new_total_cost / holding['quantity']
                        else: crypto_holdings.append({"symbol": symbol, "quantity": quantity, "average_cost": price_per_unit})
                    elif "卖出" in trans_type:
                        if quantity <= 0: st.error("数量必须大于0"); st.stop()
                        asset_list, symbol_key = (stock_holdings, "ticker") if "股票" in trans_type else (crypto_holdings, "symbol")
                        holding = next((h for h in asset_list if h.get(symbol_key) == symbol), None)
                        if not holding or holding.get('quantity', 0) < quantity: st.error(f"卖出失败：{symbol} 数量不足。"); st.stop()
                        from_account["balance"] += amount; price_per_unit = amount / quantity
                        realized_pl = (price_per_unit - holding.get('average_cost', 0)) * quantity
                        holding_currency = holding.get('currency', 'USD') if "股票" in trans_type else "USD"
                        st.toast(f"实现盈亏: {CURRENCY_SYMBOLS.get(holding_currency, '$')}{realized_pl:,.2f}")
                        holding['quantity'] -= quantity
                        if holding['quantity'] < 1e-9: asset_list.remove(holding)
                    
                    user_profile.setdefault("transactions", []).insert(0, new_transaction)
                    if save_user_profile(st.session_state.user_email, user_profile): st.success("流水记录成功！"); time.sleep(1); st.rerun()

            st.subheader("📑 交易流水")
            transactions = user_profile.get("transactions", [])
            if transactions:
                transactions_df = pd.DataFrame(transactions).sort_values(by="date", ascending=False)
                st.table(transactions_df)
            else:
                st.write("暂无交易记录。")
    
    with tab3:
        st.subheader("⚙️ 编辑现有资产与负债")
        st.warning("危险操作：直接修改资产可能导致数据不一致。推荐使用“交易管理”页的流水功能进行记录。")
        if analysis_mode == "历史快照":
            st.info("在历史快照模式下，资产编辑功能被禁用。")
        else:
            edit_tabs = st.tabs(["💵 现金", "💳 负债", "📈 股票", "🪙 加密货币", "🥇 黄金"])
            def to_df_with_schema(data, schema):
                df = pd.DataFrame(data);
                for col, col_type in schema.items():
                    if col not in df.columns: df[col] = pd.Series(dtype=col_type)
                return df
            with edit_tabs[0]:
                schema = {'name': 'object', 'currency': 'object', 'balance': 'float64'}
                df = to_df_with_schema(user_portfolio.get("cash_accounts",[]), schema)
                edited_df = st.data_editor(df, num_rows="dynamic", key="cash_editor_adv", column_config={"name": "账户名称", "currency": st.column_config.SelectboxColumn("货币", options=SUPPORTED_CURRENCIES, required=True), "balance": st.column_config.NumberColumn("余额", format="%.2f", required=True)})
                if st.button("💾 保存现金账户修改", key="save_cash"):
                    edited_list, original_map = edited_df.dropna(subset=['name']).to_dict('records'), {acc['name']: acc for acc in deepcopy(user_portfolio["cash_accounts"])}
                    for edited_acc in edited_list:
                        original_acc = original_map.get(edited_acc.get('name'))
                        if original_acc and abs(original_acc['balance'] - edited_acc['balance']) > 0.01:
                            delta = edited_acc['balance'] - original_acc['balance']
                            user_profile.setdefault("transactions", []).insert(0, {"date": datetime.now().strftime("%Y-%m-%d %H:%M"), "type": "收入" if delta > 0 else "支出", "description": "手动调整现金账户余额", "amount": abs(delta), "currency": edited_acc["currency"], "account": edited_acc["name"]})
                    user_portfolio["cash_accounts"] = edited_list
                    if save_user_profile(st.session_state.user_email, user_profile): st.success("现金账户已更新！"); time.sleep(1); st.rerun()
            with edit_tabs[1]:
                schema = {'name': 'object', 'currency': 'object', 'balance': 'float64'}
                df = to_df_with_schema(user_portfolio.get("liabilities",[]), schema)
                edited_df = st.data_editor(df, num_rows="dynamic", key="liabilities_editor_adv", column_config={"name": "名称", "currency": st.column_config.SelectboxColumn("货币", options=SUPPORTED_CURRENCIES, required=True), "balance": st.column_config.NumberColumn("金额", format="%.2f", required=True)})
                if st.button("💾 保存负债账户修改", key="save_liabilities"):
                    edited_list, original_map = edited_df.dropna(subset=['name']).to_dict('records'), {liab['name']: liab for liab in deepcopy(user_portfolio["liabilities"])}
                    for edited_liab in edited_list:
                        original_liab = original_map.get(edited_liab.get('name'))
                        if original_liab and abs(original_liab['balance'] - edited_liab['balance']) > 0.01:
                            delta = edited_liab['balance'] - original_liab['balance']
                            user_profile.setdefault("transactions", []).insert(0, {"date": datetime.now().strftime("%Y-%m-%d %H:%M"), "type": "负债增加" if delta > 0 else "负债减少", "description": "手动调整负债余额", "amount": abs(delta), "currency": edited_liab["currency"], "account": edited_liab["name"]})
                    user_portfolio["liabilities"] = edited_list
                    if save_user_profile(st.session_state.user_email, user_profile): st.success("负债账户已更新！"); time.sleep(1); st.rerun()
            with edit_tabs[2]:
                schema = {'ticker': 'object', 'quantity': 'float64', 'average_cost': 'float64', 'currency': 'object'}
                df = to_df_with_schema(user_portfolio.get("stocks",[]), schema)
                edited_df = st.data_editor(df, num_rows="dynamic", key="stock_editor_adv", column_config={"ticker": st.column_config.TextColumn("代码", help="请输入Yahoo Finance格式的代码", required=True), "quantity": st.column_config.NumberColumn("数量", format="%.4f", required=True), "average_cost": st.column_config.NumberColumn("平均成本", help="请以该股票的交易货币计价", format="%.2f", required=True), "currency": st.column_config.TextColumn("货币", disabled=True)})
                if st.button("💾 保存股票持仓修改", key="save_stocks"):
                    edited_list, original_tickers, invalid_new_tickers = edited_df.dropna(subset=['ticker', 'quantity', 'average_cost']).to_dict('records'), {s['ticker'] for s in deepcopy(user_portfolio.get("stocks", []))}, []
                    for holding in edited_list:
                        holding['ticker'] = holding['ticker'].upper()
                        if (holding['ticker'] not in original_tickers) or (not holding.get('currency') or pd.isna(holding.get('currency'))):
                            with st.spinner(f"正在验证 {holding['ticker']}..."): profile = get_stock_profile_yf(holding['ticker'])
                            if profile and profile.get('currency'):
                                holding['currency'] = profile['currency'].upper()
                            elif '.' not in holding['ticker']:
                                with st.spinner(f"信息不完整, 尝试获取 {holding['ticker']} 价格..."):
                                    price_check = get_market_data_yf([holding['ticker']])
                                if price_check and price_check.get(holding['ticker'], {}).get('latest_price', 0) > 0:
                                    st.warning(f"未能获取 {holding['ticker']} 的完整货币信息, 已默认设为 USD。")
                                    holding['currency'] = 'USD'
                                else: invalid_new_tickers.append(holding['ticker'])
                            else: invalid_new_tickers.append(holding['ticker'])
                    if invalid_new_tickers: st.error(f"以下新增的代码无效或无法获取信息: {', '.join(invalid_new_tickers)}"); st.stop()
                    user_portfolio["stocks"] = edited_list
                    if save_user_profile(st.session_state.user_email, user_profile): st.success("股票持仓已更新！"); time.sleep(1); st.rerun()
            with edit_tabs[3]:
                schema = {'symbol': 'object', 'quantity': 'float64', 'average_cost': 'float64'}
                df = to_df_with_schema(user_portfolio.get("crypto",[]), schema)
                edited_df = st.data_editor(df, num_rows="dynamic", key="crypto_editor_adv", column_config={"symbol": st.column_config.TextColumn("代码", required=True), "quantity": st.column_config.NumberColumn("数量", format="%.8f", required=True), "average_cost": st.column_config.NumberColumn("平均成本 (USD)", format="%.2f", required=True)})
                if st.button("💾 保存加密货币修改", key="save_crypto"):
                    edited_list = edited_df.dropna(subset=['symbol', 'quantity', 'average_cost']).to_dict('records')
                    for holding in edited_list: holding['symbol'] = holding['symbol'].upper()
                    user_portfolio["crypto"] = edited_list
                    if save_user_profile(st.session_state.user_email, user_profile): st.success("加密货币持仓已更新！"); time.sleep(1); st.rerun()
            with edit_tabs[4]:
                st.info("记录您持有的实物或纸黄金。成本价请以美元/克计价。")
                schema = {'grams': 'float64', 'average_cost_per_gram': 'float64'}
                df = to_df_with_schema(user_portfolio.get("gold",[]), schema)
                edited_df = st.data_editor(df, num_rows="dynamic", key="gold_editor_adv", column_config={"grams": st.column_config.NumberColumn("克数 (g)", format="%.3f", required=True), "average_cost_per_gram": st.column_config.NumberColumn("平均成本 ($/g)", format="%.2f", required=True)})
                if st.button("💾 保存黄金持仓修改", key="save_gold"):
                    user_portfolio["gold"] = edited_df.dropna(subset=['grams', 'average_cost_per_gram']).to_dict('records')
                    if save_user_profile(st.session_state.user_email, user_profile): st.success("黄金持仓已更新！"); time.sleep(1); st.rerun()

    with tab4:
        st.subheader("📈 历史趋势与基准")
        if len(asset_history) < 2:
            st.info("历史数据不足（少于2天），无法生成图表。请在明天再次使用本应用以开始追踪历史趋势。")
        else:
            with st.spinner("正在生成详细历史市值图表，这可能需要一些时间..."):
                # ---  Detailed history generation logic ---
                start_hist_date = datetime.strptime(asset_history[0]['date'], '%Y-%m-%d').date()
                end_hist_date = datetime.strptime(asset_history[-1]['date'], '%Y-%m-%d').date()
                
                # 1. 收集整个历史中出现过的所有tickers
                all_historical_tickers = set()
                for snapshot in asset_history:
                    portfolio = snapshot.get('portfolio', {})
                    for s in portfolio.get("stocks", []): all_historical_tickers.add(s['ticker'])
                    for c in portfolio.get("crypto", []): all_historical_tickers.add(f"{c['symbol'].upper()}-USD")
                all_historical_tickers.add("GC=F")
                
                # 2. 一次性批量下载所有需要的历史价格数据 (Open and Close)
                hist_prices_df = yf.download(list(all_historical_tickers), start=start_hist_date, end=end_hist_date + timedelta(days=1), progress=False)
                
                daily_net_worth_data = []
                all_dates = pd.date_range(start=start_hist_date, end=end_hist_date, freq='D')

                for date in all_dates:
                    # 3. 为每一天找到对应的持仓快照
                    snapshot = get_closest_snapshot(date.date(), asset_history)
                    if not snapshot: continue

                    portfolio = snapshot.get('portfolio', {})
                    exchange_rates = snapshot.get('exchange_rates', {})

                    # 4. 从已下载的数据中获取当天的价格 (处理非交易日)
                    open_prices_series = None
                    close_prices_series = None
                    try:
                        # 尝试直接定位日期
                        day_prices = hist_prices_df.loc[date.strftime('%Y-%m-%d')]
                        open_prices_series = day_prices['Open']
                        close_prices_series = day_prices['Close']
                    except KeyError:
                        # 如果当天不是交易日, 使用之前的最后一个交易日数据 (前向填充)
                        temp_df = hist_prices_df[hist_prices_df.index < date]
                        if not temp_df.empty:
                            last_day_prices = temp_df.iloc[-1]
                            # 对于非交易日，开盘和收盘都视为与前一收盘价相同
                            open_prices_series = last_day_prices['Close']
                            close_prices_series = last_day_prices['Close']
                        else:
                            continue # 在此日期之前没有任何价格数据

                    # 辅助函数，用于根据给定的价格序列计算净资产
                    def calculate_net_worth(prices_series):
                        if prices_series is None: return 0
                        
                        stock_holdings = portfolio.get("stocks", [])
                        crypto_holdings = portfolio.get("crypto", [])
                        gold_holdings = portfolio.get("gold", [])
                        cash_accounts = portfolio.get("cash_accounts", [])
                        liabilities = portfolio.get("liabilities", [])

                        gold_price_per_ounce = prices_series.get("GC=F", 0)
                        gold_price_per_gram = (gold_price_per_ounce / OUNCES_TO_GRAMS) if pd.notna(gold_price_per_ounce) and gold_price_per_ounce > 0 else 0

                        total_stock_value_usd = sum(s.get('quantity',0) * prices_series.get(s['ticker'], 0) / exchange_rates.get(s.get('currency', 'USD'), 1) for s in stock_holdings if pd.notna(prices_series.get(s['ticker'])))
                        total_crypto_value_usd = sum(c.get('quantity',0) * prices_series.get(f"{c['symbol'].upper()}-USD", 0) for c in crypto_holdings if pd.notna(prices_series.get(f"{c['symbol'].upper()}-USD")))
                        total_gold_value_usd = sum(g.get('grams', 0) * gold_price_per_gram for g in gold_holdings)
                        total_cash_balance_usd = sum(acc.get('balance',0) / exchange_rates.get(acc.get('currency', 'USD'), 1) for acc in cash_accounts)
                        total_liabilities_usd = sum(liab.get('balance',0) / exchange_rates.get(liab.get('currency', 'USD'), 1) for liab in liabilities)

                        total_assets_usd = total_stock_value_usd + total_cash_balance_usd + total_crypto_value_usd + total_gold_value_usd
                        return total_assets_usd - total_liabilities_usd

                    # 5. 计算开盘和收盘的净资产
                    net_worth_open = calculate_net_worth(open_prices_series)
                    net_worth_close = calculate_net_worth(close_prices_series)
                    
                    daily_net_worth_data.append({
                        'date': date,
                        'net_worth_open': net_worth_open,
                        'net_worth_close': net_worth_close
                    })
            
            benchmark_ticker = st.text_input("添加市场基准对比 (例如 SPY)", "", key="benchmark_ticker_hist")
            history_df = pd.DataFrame(daily_net_worth_data)
            history_df['date'] = pd.to_datetime(history_df['date'])
            history_df = history_df.set_index('date').sort_index()
            
            fig = go.Figure()

            # 添加收盘市值轨迹
            fig.add_trace(go.Scatter(
                x=history_df.index, 
                y=history_df['net_worth_close'] * display_rate, 
                mode='lines', 
                name='收盘市值',
                line=dict(color='royalblue'),
                hovertemplate=f"日期: %{{x|%Y-%m-%d}}<br>收盘市值: {display_symbol}%{{y:,.2f}} {display_curr}<extra></extra>"
            ))
            
            # 添加开盘市值轨迹并填充区域
            fig.add_trace(go.Scatter(
                x=history_df.index, 
                y=history_df['net_worth_open'] * display_rate, 
                mode='lines', 
                name='开盘市值',
                line=dict(width=0), # 隐藏开盘价的线
                fill='tonexty', # 填充到上一条轨迹的区域
                fillcolor='rgba(65,105,225,0.2)',
                hovertemplate=f"日期: %{{x|%Y-%m-%d}}<br>开盘市值: {display_symbol}%{{y:,.2f}} {display_curr}<extra></extra>"
            ))
            
            if benchmark_ticker:
                # 基准对比逻辑保持不变，但现在会基于更精确的起始点
                benchmark_df = get_historical_data_yf(benchmark_ticker, days=(end_hist_date - start_hist_date).days + 1)
                if not benchmark_df.empty:
                    benchmark_data = benchmark_df['Close'] # 选择'Close'列
                    # --- FIX: Ensure benchmark index is timezone-naive ---
                    if benchmark_data.index.tz is not None:
                        benchmark_data.index = benchmark_data.index.tz_localize(None)
                    
                    # 确保benchmark数据和我们的历史数据对齐
                    benchmark_data_reindexed = benchmark_data.reindex(history_df.index, method='ffill').dropna()
                    if not benchmark_data_reindexed.empty and not history_df.empty:
                        # 归一化处理，使起点一致
                        initial_portfolio_value = history_df['net_worth_close'].iloc[0] * display_rate
                        benchmark_data_normalized = (benchmark_data_reindexed / benchmark_data_reindexed.iloc[0]) * initial_portfolio_value
                        fig.add_trace(go.Scatter(x=benchmark_data_normalized.index, y=benchmark_data_normalized, mode='lines', name=benchmark_ticker))

            if analysis_mode == "历史快照":
                # --- FIX: Convert datetime.date to datetime.datetime for Plotly ---
                if start_date: fig.add_vline(x=datetime.combine(start_date, datetime.min.time()), line_width=2, line_dash="dash", line_color="green", annotation_text="开始日期")
                if end_date: fig.add_vline(x=datetime.combine(end_date, datetime.min.time()), line_width=2, line_dash="dash", line_color="red", annotation_text="结束日期")
            
            fig.update_layout(
                title_text=f"净资产历史趋势 ({display_curr})", 
                yaxis_title=f"净资产 ({display_symbol})",
                hovermode="x unified"
            )
            st.plotly_chart(fig, use_container_width=True)

    with tab5:
        st.subheader("🔬 行业板块分布")
        sector_values = {}
        with st.spinner("正在获取持仓股票的行业信息..."):
            for s in stock_holdings:
                profile = get_stock_profile_yf(s['ticker'])
                sector = profile.get('sector', 'N/A') if profile else 'N/A'
                value_usd = s.get('quantity',0) * prices.get(s['ticker'], 0) / exchange_rates.get(s.get('currency', 'USD'), 1)
                sector_values[sector] = sector_values.get(sector, 0) + value_usd
        if not sector_values or all(s == 'N/A' for s in sector_values.keys()):
            st.info("未能获取到股票的行业分类信息，或您尚未持有任何股票。")
        else:
            sector_df = pd.DataFrame(list(sector_values.items()), columns=['sector', 'value_usd']).sort_values(by='value_usd', ascending=False)
            fig = go.Figure(data=[go.Pie(labels=sector_df['sector'], values=sector_df['value_usd'] * display_rate, hole=.4, textinfo='percent+label', hovertemplate=f"<b>%{{label}}</b><br>市值: {display_symbol}%{{value:,.2f}}<br>占比: %{{percent}}<extra></extra>")])
            fig.update_layout(title_text='股票持仓行业分布', showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    with tab6:
        st.subheader("🤖 AI 深度分析")
        st.info("此功能会将您匿名的持仓明细发送给AI进行全面分析，以提供更具洞察力的建议。")
        prompt, show_button = "", True
        if analysis_mode == "历史快照":
            if start_snapshot and end_snapshot:
                st.write(f"#### 分析周期: {start_snapshot['date']}  ➡️  {end_snapshot['date']}")
                prompt = f"""(历史对比模式的详细Prompt在此处构建)""" # Placeholder
            else:
                st.warning("历史快照数据不足，无法进行对比分析。"); show_button = False
        else:
            st.write("#### 分析当前实时持仓")
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
{pd.DataFrame(stock_df_data).to_markdown(index=False)}

### 黄金持仓
{pd.DataFrame(gold_df_data).to_markdown(index=False)}

### 加密货币持仓
{pd.DataFrame(crypto_df_data).to_markdown(index=False)}

### 现金账户
{pd.DataFrame([{"账户名称": acc['name'], "货币": acc['currency'], "余额": f"{acc['balance']:,.2f}"} for acc in cash_accounts]).to_markdown(index=False)}

### 负债情况
{pd.DataFrame([{"名称": liab['name'], "货币": liab['currency'], "金额": f"{liab['balance']:,.2f}"} for liab in liabilities]).to_markdown(index=False)}
---
请开始您的中文分析报告。
"""
        if show_button and st.button("开始深度分析", key="run_detailed_analysis"):
            with st.spinner("AI 正在进行深度分析，请稍候..."):
                ai_summary = get_detailed_ai_analysis(prompt)
                st.write(ai_summary)

def run_migration(): st.session_state.migration_done = True
if not st.session_state.migration_done: run_migration()
check_session_from_query_params()
if st.session_state.logged_in:
    with st.sidebar:
        st.success(f"欢迎, {st.session_state.user_email}")
        if st.button("退出登录"):
            token_to_remove = st.query_params.get("session_token")
            if token_to_remove:
                sessions = get_global_data("sessions")
                if token_to_remove in sessions: del sessions[token_to_remove]; save_global_data("sessions", sessions)
            for key in list(st.session_state.keys()): del st.session_state[key]
            st.query_params.clear(); st.rerun()
    display_dashboard()
    if st.session_state.user_email == ADMIN_EMAIL: display_admin_panel()
else: display_login_form()


