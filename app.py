import streamlit as st
import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import requests
import re
import random
import time
import json

# --- 页面基础设置 ---
st.set_page_config(
    page_title="个人投资仪表盘",
    page_icon="🔐",
    layout="wide"
)

# --- 初始化 Session State ---
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'user_email' not in st.session_state:
    st.session_state.user_email = ""
if 'login_step' not in st.session_state:
    st.session_state.login_step = "enter_email"

# --- 微软 Graph API 配置 (已修复) ---
MS_GRAPH_CONFIG = st.secrets["microsoft_graph"]
ADMIN_EMAIL = MS_GRAPH_CONFIG["admin_email"]
# 修复了 URL 构造逻辑，移除了错误的 .replace() 调用
ONEDRIVE_FILE_PATH = MS_GRAPH_CONFIG["onedrive_user_file_path"]
ONEDRIVE_API_URL = f"https://graph.microsoft.com/v1.0/users/{MS_GRAPH_CONFIG['sender_email']}/drive/{ONEDRIVE_FILE_PATH}"


# --- Graph API 核心函数 ---

@st.cache_data(ttl=3500) # 缓存 token 近一个小时
def get_ms_graph_token():
    """获取访问令牌"""
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

def get_user_data_from_onedrive():
    """从 OneDrive 下载用户数据文件"""
    try:
        token = get_ms_graph_token()
        headers = {"Authorization": f"Bearer {token}"}
        content_url = f"{ONEDRIVE_API_URL}/content"
        resp = requests.get(content_url, headers=headers)
        
        if resp.status_code == 404: # 文件不存在，创建并返回初始管理员结构
            initial_data = {
                "users": {
                    ADMIN_EMAIL: {
                        "role": "admin",
                        "assets": {"tickers": "IBM,TSLA,MSFT"}
                    }
                },
                "codes": {}
            }
            save_user_data_to_onedrive(initial_data) # 首次创建文件
            return initial_data
        
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        st.error(f"从 OneDrive 加载用户数据失败: {e}")
        return None

def save_user_data_to_onedrive(data):
    """将用户数据保存回 OneDrive"""
    try:
        token = get_ms_graph_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        content_url = f"{ONEDRIVE_API_URL}/content"
        resp = requests.put(content_url, headers=headers, data=json.dumps(data, indent=2))
        resp.raise_for_status()
        return True
    except Exception as e:
        st.error(f"保存用户数据到 OneDrive 失败: {e}")
        return False

def send_verification_code(email, code):
    """发送邮件验证码"""
    try:
        token = get_ms_graph_token()
        url = f"https://graph.microsoft.com/v1.0/users/{MS_GRAPH_CONFIG['sender_email']}/sendMail"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {
            "message": {
                "subject": f"[{code}] 您的登录/注册验证码",
                "body": {"contentType": "Text", "content": f"您的验证码是：{code}，5分钟内有效。"},
                "toRecipients": [{"emailAddress": {"address": email}}]
            }, "saveToSentItems": "true"
        }
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        return True
    except Exception as e:
        st.error(f"邮件发送失败: {e}")
        return False

# --- 登录和用户管理逻辑 (已更新为自动注册) ---

def handle_send_code(email):
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        st.sidebar.error("请输入有效的邮箱地址。")
        return

    user_data = get_user_data_from_onedrive()
    if user_data is None: return
    
    code = str(random.randint(100000, 999999))
    expires_at = time.time() + 300
    user_data["codes"][email] = {"code": code, "expires_at": expires_at}
    
    if not save_user_data_to_onedrive(user_data): return 
    if not send_verification_code(email, code): return

    st.sidebar.success("验证码已发送，请查收。")
    st.session_state.login_step = "enter_code"
    st.session_state.temp_email = email
    st.rerun()

def handle_verify_code(email, code):
    if not code or not code.isdigit() or len(code) != 6:
        st.sidebar.error("请输入6位数字验证码。")
        return

    user_data = get_user_data_from_onedrive()
    if user_data is None: return
    
    code_info = user_data.get("codes", {}).get(email)

    if not code_info or time.time() > code_info["expires_at"]:
        st.sidebar.error("验证码已过期或不存在。")
        return
    
    if code_info["code"] == code:
        # 核心注册逻辑：如果用户不存在，则创建
        if email not in user_data["users"]:
            user_data["users"][email] = {
                "role": "user",
                "assets": {"tickers": "AAPL,GOOG,MSFT"} # 新用户的默认持仓
            }
            st.toast("🎉 注册成功！已为您创建新账户。")

        # 登录用户
        st.session_state.logged_in = True
        st.session_state.user_email = email
        st.session_state.login_step = "logged_in"
        del user_data["codes"][email] # 验证成功后删除验证码
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
            if st.button("发送验证码"):
                handle_send_code(email)
        
        elif st.session_state.login_step == "enter_code":
            email_display = st.session_state.get("temp_email", "")
            st.info(f"验证码已发送至: {email_display}")
            code = st.text_input("验证码", key="code_input")
            if st.button("登录或注册"):
                handle_verify_code(email_display, code)
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

def display_dashboard():
    st.title(f"📈 {st.session_state.user_email} 的投资仪表盘")
    
    user_data = get_user_data_from_onedrive()
    if user_data is None: st.stop()

    current_user_email = st.session_state.user_email
    user_assets = user_data["users"][current_user_email].get("assets", {"tickers": ""})

    # 使用表单来保存用户的持仓
    with st.sidebar.form(key="assets_form"):
        st.header("⚙️ 我的持仓")
        ticker_string_from_db = user_assets.get("tickers", "IBM,TSLA,MSFT")
        new_ticker_string = st.text_area("股票代码 (用英文逗号隔开)", value=ticker_string_from_db)
        submitted = st.form_submit_button("保存持仓")

        if submitted:
            user_data["users"][current_user_email]["assets"]["tickers"] = new_ticker_string
            if save_user_data_to_onedrive(user_data):
                st.sidebar.success("持仓已保存!")
                time.sleep(1)
                st.rerun()
            else:
                st.sidebar.error("保存失败!")
    
    ticker_list = [s.strip().upper() for s in new_ticker_string.split(',') if s.strip()]
    
    if ticker_list:
        # (这部分股票数据显示逻辑和之前完全一样)
        st.header("股价走势")
        ts = TimeSeries(key=st.secrets["alpha_vantage"]["api_key"], output_format='pandas')
        all_data, failed_tickers = [], []
        progress_bar = st.progress(0, text="正在下载数据...")
        for i, ticker in enumerate(ticker_list):
            try:
                data, _ = ts.get_daily(symbol=ticker, outputsize='compact')
                close_data = data['4. close']; close_data.name = ticker
                all_data.append(close_data)
            except: failed_tickers.append(ticker)
            progress_bar.progress((i + 1) / len(ticker_list), text=f"正在下载 {ticker}...")
        progress_bar.empty()
        if all_data:
            st.line_chart(pd.concat(all_data, axis=1).iloc[::-1])
        else: st.error("无法下载任何股票数据。")
        if failed_tickers: st.warning(f"无法获取以下股票的数据: {', '.join(failed_tickers)}")
    else:
        st.info("您尚未输入任何持仓，请在左侧“我的持仓”中输入股票代码并保存。")

# --- 主程序渲染 ---
if st.session_state.logged_in:
    with st.sidebar:
        st.success(f"欢迎, {st.session_state.user_email}")
        if st.button("退出登录"):
            st.session_state.logged_in = False
            st.session_state.user_email = ""
            st.session_state.login_step = "enter_email"
            st.rerun()
    
    display_dashboard()

    if st.session_state.user_email == ADMIN_EMAIL:
        display_admin_panel()
else:
    display_login_form()

