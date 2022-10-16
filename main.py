import agricensus_api
import config
import pandas as pd
import numpy as np
import re
from datetime import date as dt
from pathlib import Path  

def data_transformation(is_export_to_csv: bool = True, is_upload_to_gdrive: bool = True) -> pd.DataFrame:
    tx_df = agricensus_api.get_agricensus_data(config.token, 'daily_export', 'JSON')
    specs_df = agricensus_api.get_agricensus_data(config.token, 'specs_export', 'JSON')

    # SPM and ARG/BRZ Only
    specs_df = specs_df[
        specs_df['Market'].isin(['Corn', 'Soybean', 'Soymeal'])
        & specs_df['Name'].str.contains('Argentina|Brazil') # Check China (Brazil)
    ]

    full_df = pd.merge(tx_df, specs_df, how='inner', left_on='code', right_on='Price Code', validate='m:1')

    # Final DF to be returned
    final_df = pd.DataFrame()

    # MAIN TRANSFORMATION
    final_df['offer_date'] = pd.to_datetime(full_df.date)
    final_df['commodity'] = np.where(full_df['Market'] == 'Corn', 'CORN',
                            np.where(full_df['Market'].isin(['Soybean', 'Soymeal']), 'SBM', 'Others'))
    final_df['comm'] = full_df['name'].apply(lambda name: re.sub('M[1-9]', '', name))
    final_df['currency'] = full_df['Currency']
    final_df['m_month'] = np.where(full_df['Structure'] == 'SP', 'M0', full_df['Structure'])
    final_df['origin'] = np.where(full_df['Name'].str.contains('Argentina'), 'ARG', 'BRZ')
    final_df['seller'] = 'Agricensus API' # Need to check
    final_df['region'] = 'SA' # Need to check - South America
    final_df['price_type'] = 'CNF'
    final_df['note'] = ''
    final_df['price'] = full_df['value']
    final_df['unit_price'] = np.where(full_df['name'].str.contains('/mt|/st'), 'flat',
                            np.where(full_df['name'].str.contains('/bu'), 'basic', 'Unknown'))

    final_df['shipment_date'] = final_df.apply(lambda row: row['offer_date'] + pd.DateOffset(months=1) if row['m_month'] == 'M0' or row['m_month'] == 'M1' else (
                                                        row['offer_date'] + pd.DateOffset(months=2) if row['m_month'] == 'M2' else (
                                                        row['offer_date'] + pd.DateOffset(months=3) if row['m_month'] == 'M3' else (
                                                        row['offer_date'] + pd.DateOffset(months=4) if row['m_month'] == 'M4' else (
                                                        row['offer_date'] + pd.DateOffset(months=5) if row['m_month'] == 'M5' else (
                                                        row['offer_date'] + pd.DateOffset(months=6) if row['m_month'] == 'M6' else (
                                                        row['offer_date'] + pd.DateOffset(months=7) if row['m_month'] == 'M7' else (
                                                        row['offer_date'] + pd.DateOffset(months=8) if row['m_month'] == 'M8' else row['offer_date'] + pd.DateOffset(months=9)))))))),
                                            axis=1)

    final_df['delivery_date'] = final_df['shipment_date'] + pd.DateOffset(months=2)

    if is_export_to_csv == True:
        file_path = Path(f'Data/DailyExport{dt.today().year*10000+dt.today().month*100+dt.today().day}.csv')  
        file_path.parent.mkdir(parents=True, exist_ok=True)
        final_df.to_csv(file_path, index=False)

    return final_df

if __name__ == '__main__':
    data_transformation(is_export_to_csv = True)