import streamlit as st
import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import io

# --- 页面基础设置 ---
st.set_page_config(
    page_title="个人投资仪表盘",
    page_icon="🔐",
    layout="wide"
)

# --- 登录验证函数 ---
def check_password():
    """Returns `True` if the user had the correct password."""

    # 1. 从 secrets 中读取正确的用户名和密码
    correct_username = st.secrets["credentials"]["username"]
    correct_password = st.secrets["credentials"]["password"]

    # 2. 在侧边栏创建一个表单用于登录
    with st.sidebar:
        st.header("🔐 请先登录")
        username = st.text_input("用户名", key="username_input")
        password = st.text_input("密码", type="password", key="password_input")
        login_button = st.button("登录")

    # 3. 检查输入是否匹配
    if login_button:
        if username == correct_username and password == correct_password:
            st.session_state["password_correct"] = True
            # 清除输入框内容，避免重复提交
            st.rerun() 
        else:
            st.sidebar.error("用户名或密码不正确")
    
    return st.session_state.get("password_correct", False)


# --- 主程序 ---

# 调用登录验证函数
if not check_password():
    st.stop()  # 如果密码不正确，停止执行下面的代码

# --- 登录成功后，显示仪表盘 ---

# 标题
st.title("📈 个人投资仪表盘 (Alpha Vantage版)")

# 从 Streamlit Secrets 获取 API 密钥
try:
    av_api_key = st.secrets["alpha_vantage"]["api_key"]
except KeyError:
    st.error("错误：请在应用的Secrets中设置您的Alpha Vantage API密钥。")
    st.stop()

# 侧边栏
st.sidebar.header("输入你的持仓")
ticker_string = st.sidebar.text_input("股票代码 (用英文逗号隔开)", "IBM,TSLA,MSFT")
ticker_list = [s.strip().upper() for s in ticker_string.split(',') if s.strip()]

# 主页面
if ticker_list:
    st.header("股价走势")

    # Alpha Vantage 初始化
    ts = TimeSeries(key=av_api_key, output_format='pandas')
    
    all_data = []
    failed_tickers = []
    
    progress_bar = st.progress(0, text="正在下载数据...")
    for i, ticker in enumerate(ticker_list):
        try:
            data, meta_data = ts.get_daily(symbol=ticker, outputsize='compact')
            close_data = data['4. close']
            close_data.name = ticker
            all_data.append(close_data)
        except Exception as e:
            failed_tickers.append(ticker)
        
        progress_bar.progress((i + 1) / len(ticker_list), text=f"正在下载 {ticker}...")
    
    progress_bar.empty()

    if all_data:
        combined_data = pd.concat(all_data, axis=1)
        combined_data = combined_data.iloc[::-1]
        st.line_chart(combined_data)
    else:
        st.error("无法下载任何股票数据。请检查代码或API密钥。")

    if failed_tickers:
        st.warning(f"无法获取以下股票的数据: {', '.join(failed_tickers)}")
else:
    st.info("请在左侧边栏输入至少一个股票代码。")
