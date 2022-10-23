import pandas as pd
import yfinance as yf

def get_fx_rate(symbol: str = 'USDVND', start_date: str = '2017-01-01') -> pd.DataFrame:
    # Set the ticker as 'USDVND=X'
    fx_df = yf.download(f'{symbol}=X', start=start_date)

    # Set the index to a datetime object
    fx_df.index = pd.to_datetime(fx_df.index)

    # Reset index into column
    fx_df = fx_df.reset_index()

    fx_df = fx_df[['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close']]

    return fx_df