import requests
import pandas as pd
from datetime import date as dt

def get_url(token: str, feed_type: str, file_format: str = 'CSV') -> str:
    feed_type_dict = {  
        'daily_export': '',
        'historical': 'historical',
        'full_historical': 'historical2',
        'specs_export': 'specs'
    }
    url = f'https://www.agricensus.com/feed/?token={token}&format={file_format}&{feed_type_dict[feed_type]}'
    return url

def get_agricensus_data(token: str, feed_type: str = 'daily_export', file_format: str = 'CSV') -> pd.DataFrame:

    url = get_url(token, feed_type, file_format)

    if file_format == 'JSON':
        # GET API Request
        response = requests.get(
            url = url,
            headers = {
                'Accept': f'application/json',
                'Content-Type': f'application/json'
            }
        )
        df = pd.json_normalize(response.json())

    elif file_format == 'CSV':
        df = pd.read_csv(url)
    
    return df