import requests
import pandas as pd
from datetime import date as dt

def get_url(token: str, feed_type: str, file_format: str = 'JSON') -> str:
    feed_type_dict = {  
        'daily_export': '',
        'historical': 'historical',
        'full_historical': 'historical2',
        'specs_export': 'specs'
    }
    url = f'https://www.agricensus.com/feed/?token={token}&format={file_format}&{feed_type_dict[feed_type]}'
    return url

def get_agricensus_api(token: str, feed_type: str = 'daily_export', file_format: str = 'JSON'):
    url = get_url(token, feed_type, file_format)
    response = requests.get(
        url = url,
        headers = {
            'Accept': 'application/csv',
            'Content-Type': 'text/csv'
        }
    )
    return response

def get_agricensus_data(token: str, feed_type: str = 'daily_export', file_format: str = 'JSON') -> pd.DataFrame:
    file_name_dict = {  
        'daily_export': 'daily_price',
        'historical': 'historical_price',
        'full_historical': 'full_historical_price',
        'specs_export': 'agricensus_specs'
    }

    # GET API Request
    response = get_agricensus_api(token, feed_type, file_format)

    df = pd.json_normalize(response.json())

    return df