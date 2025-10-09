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

# --- 微软 Graph API 配置 ---
MS_GRAPH_CONFIG = st.secrets["microsoft_graph"]
ADMIN_EMAIL = MS_GRAPH_CONFIG["admin_email"]
# 构造 OneDrive 文件 API 的 URL
# 注意：文件路径中的 ':' 后面需要再加一个 ':/'
ONEDRIVE_FILE_PATH = MS_GRAPH_CONFIG["onedrive_user_file_path"].replace(":", ":/", 1)
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
        # 请求文件内容
        content_url = f"{ONEDRIVE_API_URL}/content"
        resp = requests.get(content_url, headers=headers)
        
        if resp.status_code == 404: # 文件不存在，返回初始结构
            return {"users": [ADMIN_EMAIL], "codes": {}}
        
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
        # 上传文件内容
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
                "subject": f"[{code}] 您的登录验证码",
                "body": {"contentType": "Text", "content": f"您的登录验证码是：{code}，5分钟内有效。"},
                "toRecipients": [{"emailAddress": {"address": email}}]
            },
            "saveToSentItems": "true"
        }
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

    if email not in user_data.get("users", []):
        st.sidebar.error("该用户不存在。")
        return
    
    code = str(random.randint(100000, 999999))
    expires_at = time.time() + 300
    user_data["codes"][email] = {"code": code, "expires_at": expires_at}
    
    if save_user_data_to_onedrive(user_data):
        if send_verification_code(email, code):
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

    if not code_info:
        st.sidebar.error("验证码已过期或不存在。")
        return

    if time.time() > code_info["expires_at"]:
        st.sidebar.error("验证码已过期。")
        del user_data["codes"][email]
        save_user_data_to_onedrive(user_data)
        return
    
    if code_info["code"] == code:
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
        st.header("🔐 邮箱验证码登录")
        if st.session_state.login_step == "enter_email":
            email = st.text_input("邮箱地址", key="email_input")
            if st.button("发送验证码"):
                handle_send_code(email)
        
        elif st.session_state.login_step == "enter_code":
            email_display = st.session_state.get("temp_email", "")
            st.info(f"验证码已发送至: {email_display}")
            code = st.text_input("验证码", key="code_input")
            if st.button("登录"):
                handle_verify_code(email_display, code)
            if st.button("返回"):
                st.session_state.login_step = "enter_email"
                st.rerun()

def display_admin_panel():
    with st.sidebar:
        st.header("👑 管理员面板")
        
        user_data = get_user_data_from_onedrive()
        if user_data is None: return

        with st.expander("添加新用户"):
            new_user_email = st.text_input("新用户邮箱", key="new_user_email")
            if st.button("添加"):
                if new_user_email and re.match(r"[^@]+@[^@]+\.[^@]+", new_user_email):
                    if new_user_email not in user_data["users"]:
                        user_data["users"].append(new_user_email)
                        if save_user_data_to_onedrive(user_data):
                            st.success(f"用户 {new_user_email} 添加成功！")
                            st.rerun()
                    else:
                        st.warning("用户已存在。")
                else:
                    st.error("请输入有效的邮箱。")

        with st.expander("管理现有用户"):
            users_copy = user_data.get("users", []).copy()
            for user_email in users_copy:
                if user_email != ADMIN_EMAIL:
                    col1, col2 = st.columns([3, 1])
                    col1.write(user_email)
                    if col2.button("删除", key=f"del_{user_email}"):
                        user_data["users"].remove(user_email)
                        if save_user_data_to_onedrive(user_data):
                            st.rerun()

def display_dashboard():
    st.title("📈 个人投资仪表盘")
    av_api_key = st.secrets["alpha_vantage"]["api_key"]
    st.sidebar.header("输入你的持仓")
    ticker_string = st.sidebar.text_input("股票代码 (用英文逗号隔开)", "IBM,TSLA,MSFT")
    ticker_list = [s.strip().upper() for s in ticker_string.split(',') if s.strip()]
    
    if ticker_list:
        st.header("股价走势")
        ts = TimeSeries(key=av_api_key, output_format='pandas')
        all_data, failed_tickers = [], []
        
        progress_bar = st.progress(0, text="正在下载数据...")
        for i, ticker in enumerate(ticker_list):
            try:
                data, _ = ts.get_daily(symbol=ticker, outputsize='compact')
                close_data = data['4. close']
                close_data.name = ticker
                all_data.append(close_data)
            except Exception:
                failed_tickers.append(ticker)
            
            progress_bar.progress((i + 1) / len(ticker_list), text=f"正在下载 {ticker}...")
        
        progress_bar.empty()
        
        if all_data:
            combined_data = pd.concat(all_data, axis=1).iloc[::-1]
            st.line_chart(combined_data)
        else:
            st.error("无法下载任何股票数据。")
            
        if failed_tickers:
            st.warning(f"无法获取以下股票的数据: {', '.join(failed_tickers)}")
    else:
        st.info("请在左侧边栏输入至少一个股票代码。")

# --- 主程序渲染 ---
if st.session_state.logged_in:
    st.sidebar.success(f"欢迎, {st.session_state.user_email}")
    if st.sidebar.button("退出登录"):
        st.session_state.logged_in = False
        st.session_state.user_email = ""
        st.session_state.login_step = "enter_email"
        st.rerun()
    
    display_dashboard()

    if st.session_state.user_email == ADMIN_EMAIL:
        display_admin_panel()
else:
    display_login_form()

