import agricensus_api
import config
import pandas as pd
import numpy as np
import re
from datetime import date as dt
from pathlib import Path  

def write_to_csv(df: pd.DataFrame, feed_type: str):
    
    # Based on feed_type to get file_name
    file_name_dict = {  
        'daily_export': 'DailyExport',
        'historical_export': 'HistoricalExport',
        'specs_export': 'SpecsExport'
    }
    
    file_path = Path(f'Data/{file_name_dict[feed_type]}_{dt.today().year*10000+dt.today().month*100+dt.today().day}.csv')  
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(file_path, index=False)

def data_transformation(is_export_to_csv: bool = True, is_upload_to_gdrive: bool = True) -> pd.DataFrame:
    
    # Daily Export Price
    # tx_df = agricensus_api.get_agricensus_data(config.token, 'daily_export', 'CSV')
    
    # Historical Export Price
    tx_df = agricensus_api.get_agricensus_data(config.token, 'full_historical', 'CSV')

    specs_df = agricensus_api.get_agricensus_data(config.token, 'specs_export', 'CSV')

    # SPM and ARG/BRZ Only
    specs_df = specs_df[
        specs_df['Market'].isin(['Corn', 'Soybean', 'Soymeal'])
        & specs_df['Name'].str.contains('Argentina|Brazil') # Check China (Brazil)
    ]

    full_df = pd.merge(tx_df, specs_df, how='inner', left_on='code', right_on='Price Code', validate='m:1')

    # Final DF to be returned
    final_df = pd.DataFrame()

    # MAIN TRANSFORMATION
    final_df['DATE OFFER'] = pd.to_datetime(full_df.date)
    final_df['COMMODITY'] = np.where(full_df['Market'] == 'Corn', 'CORN',
                            np.where(full_df['Market'].isin(['Soybean', 'Soymeal']), 'SBM', 'Others'))
    
    # 20221017 - Change Source Data
    # final_df['comm'] = full_df['Name'].apply(lambda name: re.sub('M[1-9] ', '', name))
    final_df['COMM'] = full_df['Market'] + ' ' + full_df['Name'].apply(lambda name: re.sub(' M[1-9]', '', name)) + ' ' + full_df['Currency'] + '/' + \
                        np.where(full_df['Units'] == 'bushel (corn)', 'buc',
                        np.where(full_df['Units'] == 'bushel (wheat/soy)', 'buw',
                        np.where(full_df['Units'] == 'Metric tonnes', 'mt',
                        np.where(full_df['Units'] == 'Short tons', 'st', full_df['Units']))))
    
    final_df['CURRENCY'] = full_df['Currency']
    final_df['M_MONTH'] = full_df['shipment_delivery_month'] + ' ' + np.where(full_df['Structure'] == 'SP', 'M0', full_df['Structure'])
    final_df['ORIGIN'] = np.where(full_df['Name'].str.contains('Argentina'), 'ARG', 'BRZ')
    final_df['SELLER'] = np.where(full_df['Name'].str.contains('Argentina'), 'ARG', 'BRZ') # Need to check how to identify seller
    final_df['REGION'] = 'SA' # Need to check - South America
    final_df['PRICE_TYPE'] = 'CNF'
    final_df['NOTE'] = ''
    final_df['PRICE'] = full_df['value']
    
    # 20221017 - Change Source Data
    # final_df['unit_price'] = np.where(full_df['comm'].str.contains('/mt|/st'), 'flat',
    #                         np.where(full_df['comm'].str.contains('/bu'), 'basic', 'Unknown'))

    final_df['UNIT PRICE'] = np.where(full_df['Units'] == 'bushel (corn)', 'basic',
                            np.where(full_df['Units'] == 'bushel (wheat/soy)', 'basic',
                            np.where(full_df['Units'] == 'Metric tonnes', 'flat',
                            np.where(full_df['Units'] == 'Short tons', 'flat', 'Unknown'))))

    final_df['SHIPMENT'] = final_df.apply(lambda row: row['DATE OFFER'] + pd.DateOffset(months=1) if row['M_MONTH'] == 'M0' or row['M_MONTH'] == 'M1' else (
                                                        row['DATE OFFER'] + pd.DateOffset(months=2) if row['M_MONTH'] == 'M2' else (
                                                        row['DATE OFFER'] + pd.DateOffset(months=3) if row['M_MONTH'] == 'M3' else (
                                                        row['DATE OFFER'] + pd.DateOffset(months=4) if row['M_MONTH'] == 'M4' else (
                                                        row['DATE OFFER'] + pd.DateOffset(months=5) if row['M_MONTH'] == 'M5' else (
                                                        row['DATE OFFER'] + pd.DateOffset(months=6) if row['M_MONTH'] == 'M6' else (
                                                        row['DATE OFFER'] + pd.DateOffset(months=7) if row['M_MONTH'] == 'M7' else (
                                                        row['DATE OFFER'] + pd.DateOffset(months=8) if row['M_MONTH'] == 'M8' else row['DATE OFFER'] + pd.DateOffset(months=9)))))))),
                                            axis=1)

    final_df['DELIVERY'] = final_df['SHIPMENT'] + pd.DateOffset(months=2)

    if is_export_to_csv == True:
        write_to_csv(final_df, 'daily_export')
        write_to_csv(final_df, 'historical_export')
        # write_to_csv(specs_df, 'specs_export')

    return final_df

if __name__ == '__main__':
    data_transformation(is_export_to_csv = True)