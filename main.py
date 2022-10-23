import agricensus_api
import yahoo_finance_api
from upload_to_gdrive import upload_file_to_gdrive
import config
import pandas as pd
import numpy as np
import re
from datetime import date as dt
from pathlib import Path  

def get_file_name(feed_type: str) -> str:
    file_name_dict = {  
        'fw': 'FWHistoricalExport',
        'freight': 'freightHistoricalExport',
        'specs': 'SpecsExport',
        'fxrate': 'FXRateExport',
        'test': 'TestExport'
    }

    file_path = Path(f'Data/{file_name_dict[feed_type]}_{dt.today().year*10000+dt.today().month*100+dt.today().day}.csv')
    file_path.parent.mkdir(parents=True, exist_ok=True)

    return file_path

def write_to_csv(df: pd.DataFrame, file_path: str):
    df.to_csv(file_path, index=False)

def data_transformation(is_export_to_csv: bool = True, is_upload_to_gdrive: bool = True) -> pd.DataFrame:
    
    # Daily Export Price
    # tx_df = agricensus_api.get_agricensus_data(config.token, 'daily_export', 'CSV')
    
    # Historical Export Price
    tx_df = agricensus_api.get_agricensus_data(config.token, 'full_historical', 'CSV')

    specs_df = agricensus_api.get_agricensus_data(config.token, 'specs_export', 'CSV')

    # SPM and ARG/BRZ Only
    # specs_df = specs_df[
    #     specs_df['Market'].isin(['Corn', 'Soybean', 'Soymeal'])
    #     & specs_df['Name'].str.contains('Argentina|Brazil') # Check China (Brazil)
    # ]

    # 2022-10-23 REMOVE SOYBEAN, ADDED WHEAT, CRUSH MARGIN, FREIGHT
    is_sbm = (specs_df['Market'] == 'Soymeal') & (specs_df['Name'].str.contains('Argentina|Brazil')) & (~specs_df['Name'].str.contains('China'))
    is_corn = (specs_df['Market'] == 'Corn') & (specs_df['Name'].str.contains('Argentina|Brazil')) & (~specs_df['Name'].str.contains('China'))
    is_wheat = (specs_df['Market'] == 'Wheat') & (specs_df['Name'].str.contains('AUS ASW|Ukraine FW'))
    is_crush_margin = (specs_df['Market'] == 'Crush Margin') & (specs_df['Name'].str.contains('Brazil|Argentina|US')) & (~specs_df['Name'].str.contains('China'))
    is_freight = (specs_df['Market'] == 'Freight') & (specs_df['Name'].str.contains('Brazil - Vietnam|ARG - NE Asia'))

    specs_df = specs_df[is_sbm | is_corn | is_wheat | is_crush_margin | is_freight]

    full_df = pd.merge(tx_df, specs_df, how='inner', left_on='code', right_on='Price Code', validate='m:1')

    # Final DF to be returned
    final_df = pd.DataFrame()

    # MAIN TRANSFORMATION
    final_df['DATE OFFER'] = pd.to_datetime(full_df.date)
    
    # 2022-10-23 UPDATE AS SBM = Soymeal
    # final_df['COMMODITY'] = np.where(full_df['Market'] == 'Corn', 'CORN',
    #                         np.where(full_df['Market'].isin(['Soybean', 'Soymeal']), 'SBM', 'Others'))
    
    final_df['COMMODITY'] = np.where(full_df['Market'] == 'Soymeal', 'SBM', full_df['Market'].str.upper())

    # 20221017 - Change Source Data
    # final_df['comm'] = full_df['Name'].apply(lambda name: re.sub('M[1-9] ', '', name))
    final_df['COMM'] = full_df['Market'] + ' ' + full_df['Name'].apply(lambda name: re.sub(' M[1-9]', '', name)) + ' ' + full_df['Currency'] + '/' + \
                        np.where(full_df['Units'] == 'bushel (corn)', 'buc',
                        np.where(full_df['Units'] == 'bushel (wheat/soy)', 'buw',
                        np.where(full_df['Units'] == 'Metric tonnes', 'mt',
                        np.where(full_df['Units'] == 'Short tons', 'st', full_df['Units']))))
    
    final_df['CURRENCY'] = full_df['Currency']
    
    final_df['M_MONTH'] = full_df['shipment_delivery_month'] + ' ' + np.where(full_df['Structure'] == 'SP', 'M0', full_df['Structure'])
    
    final_df['ORIGIN'] = np.where(full_df['Name'].str.contains('Argentina'), 'ARG',
                        np.where(full_df['Name'].str.contains('Brazil'), 'BRA',
                        np.where(full_df['Name'].str.contains('Ukraine'), 'UKR',
                        np.where(full_df['Name'].str.contains('AUS|Australia'), 'AUS',
                        np.where(full_df['Name'].str.contains('US'), 'USA', '')))))

    final_df['REGION'] = np.where(full_df['Name'].str.contains('Argentina'), 'SA',
                        np.where(full_df['Name'].str.contains('Brazil'), 'SA',
                        np.where(full_df['Name'].str.contains('Ukraine'), 'EU',
                        np.where(full_df['Name'].str.contains('AUS|Australia'), 'OC',
                        np.where(full_df['Name'].str.contains('US'), 'NA', '')))))
    
    # 2022-10-23 UPDATE AS "AGRICENSUS"
    # final_df['SELLER'] = np.where(full_df['Name'].str.contains('Argentina'), 'ARG', 'BRZ') # Need to check how to identify seller
    
    final_df['SELLER'] = 'Agricensus'
    final_df['PRICE_TYPE'] = 'CNF'
    final_df['NOTE'] = ''
    final_df['PRICE'] = full_df['value']
    final_df['LETTER'] = full_df['letter']
    
    # 20221017 - Change Source Data
    # final_df['unit_price'] = np.where(full_df['comm'].str.contains('/mt|/st'), 'flat',
    #                         np.where(full_df['comm'].str.contains('/bu'), 'basic', 'Unknown'))

    final_df['UNIT PRICE'] = np.where(full_df['Units'] == 'bushel (corn)', 'basic',
                            np.where(full_df['Units'] == 'bushel (wheat/soy)', 'basic',
                            np.where(full_df['Units'] == 'Metric tonnes', 'flat',
                            np.where(full_df['Units'] == 'Short tons', 'flat', ''))))

    final_df['SHIPMENT'] = pd.to_datetime('01-' + full_df['shipment_delivery_month'] + full_df['date'].str.slice(0,4))
    final_df.loc[final_df['SHIPMENT'] < final_df['DATE OFFER'], 'SHIPMENT'] = final_df['SHIPMENT'] + pd.DateOffset(years=1)

    # REPLACE WITH SHIPMENT_DELIVERY_MONTH DIRECTLY
    # final_df['SHIPMENT'] = final_df.apply(lambda row: row['DATE OFFER'] + pd.DateOffset(months=1) if 'M0' in row['M_MONTH'] or 'M1' in row['M_MONTH'] else (
    #                                                     row['DATE OFFER'] + pd.DateOffset(months=2) if 'M2' in row['M_MONTH'] else (
    #                                                     row['DATE OFFER'] + pd.DateOffset(months=3) if 'M3' in row['M_MONTH'] else (
    #                                                     row['DATE OFFER'] + pd.DateOffset(months=4) if 'M4' in row['M_MONTH'] else (
    #                                                     row['DATE OFFER'] + pd.DateOffset(months=5) if 'M5' in row['M_MONTH'] else (
    #                                                     row['DATE OFFER'] + pd.DateOffset(months=6) if 'M6' in row['M_MONTH'] else (
    #                                                     row['DATE OFFER'] + pd.DateOffset(months=7) if 'M7' in row['M_MONTH'] else (
    #                                                     row['DATE OFFER'] + pd.DateOffset(months=8) if 'M8' in row['M_MONTH'] else row['DATE OFFER'] + pd.DateOffset(months=9)))))))),
    #                                         axis=1)

    final_df['DELIVERY'] = final_df['SHIPMENT'] + pd.DateOffset(months=2)

    # 2022-10-23 ADDED FULL MONTH, INCOTERM
    final_df['FULL_M_MONTH'] = final_df['SHIPMENT'].dt.strftime('%b-%Y')
    final_df['INCOTERM'] = np.where(full_df['Name'].str.contains('FOB'), 'FOB',
                            np.where(full_df['Name'].str.contains('CIF'), 'CIF',
                            np.where(full_df['Name'].str.contains('CFR'), 'CFR', '')))

    # REORDER COLUMN IN FINAL DATAFRAME
    final_df = final_df[['DATE OFFER', 'SELLER', 'COMMODITY', 'ORIGIN', 'SHIPMENT', 'DELIVERY',
                        'REGION', 'M_MONTH', 'PRICE_TYPE', 'NOTE', 'UNIT PRICE', 'PRICE', 'COMM', 'CURRENCY', 'LETTER', 'FULL_M_MONTH', 'INCOTERM']]

    fw_file_path = get_file_name('fw')
    freigth_file_path = get_file_name('freight')
    specs_file_path = get_file_name('specs')
    fx_rate_file_path = get_file_name('fxrate')

    if is_export_to_csv == True:
        write_to_csv(final_df[final_df['COMMODITY'] != 'FREIGHT'], fw_file_path)
        write_to_csv(final_df[final_df['COMMODITY'] == 'FREIGHT'], freigth_file_path)
        write_to_csv(specs_df, specs_file_path)
        write_to_csv(yahoo_finance_api.get_fx_rate(), fx_rate_file_path)
        # write_to_csv(full_df, 'test')

    if is_upload_to_gdrive == True:
        upload_file_to_gdrive(fw_file_path)
    
    return final_df

if __name__ == '__main__':
    data_transformation(is_export_to_csv = True, is_upload_to_gdrive = True)