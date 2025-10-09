import streamlit as st
import yfinance as yf
import pandas as pd

# --- Page Config ---
st.set_page_config(
    page_title="Personal Investment Dashboard",
    page_icon="📈",
    layout="wide"
)

# --- Title ---
st.title("📈 Personal Investment Dashboard")

# --- Sidebar ---
st.sidebar.header("Enter Your Holdings")

# Ticker input
ticker_string = st.sidebar.text_input("Stock Tickers (comma-separated)", "AAPL,GOOG,NVDA,00700.HK")
ticker_list = [s.strip().upper() for s in ticker_string.split(',') if s.strip()]

# --- Main Page ---
if ticker_list:
    st.header("Stock Price Chart")
    
    # Period selection
    period = st.selectbox(
        'Select Time Period',
        ('1mo', '3mo', '6mo', '1y', '2y', '5y', 'max')
    )

    all_data = []
    failed_tickers = []

    # --- NEW: Loop to download one by one ---
    progress_bar = st.progress(0)
    for i, ticker in enumerate(ticker_list):
        try:
            # Download data for a single ticker
            data = yf.download(ticker, period=period, progress=False)['Adj Close']
            if not data.empty:
                # Rename the series to the ticker name for the legend
                data.name = ticker
                all_data.append(data)
            else:
                failed_tickers.append(ticker)
        except Exception as e:
            failed_tickers.append(ticker)
        
        # Update progress bar
        progress_bar.progress((i + 1) / len(ticker_list))

    progress_bar.empty() # Remove the progress bar after completion

    # --- Combine and display the data ---
    if all_data:
        # Combine all successful downloads into one DataFrame
        combined_data = pd.concat(all_data, axis=1)
        st.line_chart(combined_data)
    else:
        st.error("Could not download any stock data. Please check your network connection or ticker symbols.")

    # --- Show failed tickers ---
    if failed_tickers:
        st.warning(f"Could not retrieve data for: {', '.join(failed_tickers)}")
else:
    st.info("Please enter at least one stock ticker in the sidebar to begin.")