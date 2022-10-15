from operator import methodcaller
import requests
import csv

def get_url(token: str, feed_type: str, file_format: str) -> str:
    feed_type_dict = {  
        'daily_export': '',
        'historical': 'historical',
        'full_historical': 'historical2',
        'specs_export': 'specs'
    }
    url = f'https://www.agricensus.com/feed/?token={token}&format={file_format}&{feed_type_dict[feed_type]}'
    return url

def write_csv(file_path, file_name, data):
    with open(f'{file_path}/{file_name}', 'w', newline='') as f:
        writer = csv.writer(f)
        for line in data.iter_lines():
            writer.writerow(line.decode('utf-8').split(','))

def get_agricensus_api(token: str, feed_type: str = 'daily_export', file_format: str = 'CSV'):
    url = get_url(token, feed_type, file_format)
    response = requests.get(
        url = url,
        headers = {
            'Accept': 'application/csv',
            'Content-Type': 'text/csv'
        }
    )
    return response

def get_agricensus_data(token: str, feed_type: str = 'daily_export', file_format: str = 'CSV', file_path: str = 'Data', is_test: bool = True):
    file_name_dict = {  
        'daily_export': 'daily_price',
        'historical': 'historical_price',
        'full_historical': 'full_historical_price',
        'specs_export': 'agricensus_specs'
    }

    # GET API Request
    response = get_agricensus_api(token, feed_type, file_format)

    # If test, write to csv file
    if is_test == True:
        write_csv(file_path, file_name=f'{file_name_dict[feed_type]}.csv', data=response)
