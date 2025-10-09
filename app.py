import streamlit as st
import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import io

# --- 页面基础设置 ---
st.set_page_config(
    page_title="个人投资仪表盘",
    page_icon="📈",
    layout="wide"
)

# --- 标题 ---
st.title("📈 个人投资仪表盘 (Alpha Vantage版)")

# --- 从 Streamlit Secrets 获取 API 密钥 ---
try:
    av_api_key = st.secrets["alpha_vantage"]["api_key"]
except KeyError:
    st.error("错误：请在应用的Secrets中设置您的Alpha Vantage API密钥。")
    st.stop()

# --- 侧边栏 ---
st.sidebar.header("输入你的持仓")
ticker_string = st.sidebar.text_input("股票代码 (用英文逗号隔开)", "IBM,TSLA,MSFT")
ticker_list = [s.strip().upper() for s in ticker_string.split(',') if s.strip()]

# --- 主页面 ---
if ticker_list:
    st.header("股价走势")

    # Alpha Vantage 初始化
    ts = TimeSeries(key=av_api_key, output_format='pandas')
    
    all_data = []
    failed_tickers = []
    
    progress_bar = st.progress(0, text="正在下载数据...")
    for i, ticker in enumerate(ticker_list):
        try:
            # 获取日线数据，'compact'表示最近100天的数据
            data, meta_data = ts.get_daily(symbol=ticker, outputsize='compact')
            # 我们只需要收盘价
            close_data = data['4. close']
            close_data.name = ticker
            all_data.append(close_data)
        except Exception as e:
            failed_tickers.append(ticker)
        
        progress_bar.progress((i + 1) / len(ticker_list), text=f"正在下载 {ticker}...")
    
    progress_bar.empty()

    if all_data:
        # 合并所有成功获取的数据
        combined_data = pd.concat(all_data, axis=1)
        # Alpha Vantage 返回的数据是倒序的，需要反转一下
        combined_data = combined_data.iloc[::-1]
        st.line_chart(combined_data)
    else:
        st.error("无法下载任何股票数据。请检查代码或API密钥。")

    if failed_tickers:
        st.warning(f"无法获取以下股票的数据: {', '.join(failed_tickers)}")
else:
    st.info("请在左侧边栏输入至少一个股票代码。")
