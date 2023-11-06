import pandas as pd
import numpy as np
import os
from app import config 
from ds_utils.db_manager import DBManager
from ds_utils.unified_logging import setup_logging

ETL_DB_MANAGER_PS = DBManager('snowflake', config.DB_SETTINGS, config.SHARED_LOGGING_PARAMETERS)
ETL_LOGGER = setup_logging("etl", config.SHARED_LOGGING_PARAMETERS)
SCHEMA_NAME = config.DB_SETTINGS["SF_DB_SCHEMA"]
DB_NAME = config.DB_SETTINGS["SF_DB_DATABASE"]
DBM = DBManager( 'snowflake', config.DB_SETTINGS, config.SHARED_LOGGING_PARAMETERS)


def fetch_soh():
    query = """SELECT 
                    PRODUCT_SOURCE_IDENTIFIER,
                    DS.PRODUCT_GENERATED_IDENTIFIER,
                    DS.LOCATION_GENERATED_IDENTIFIER,
                    DAY_DATE,
                    DAILY_STOCK_ON_HAND_QUANTITY,
                    (DAILY_STOCK_ON_HAND_QUANTITY * LOCATION_AWC_AMOUNT) AS SOH_DOLLAR_VALUE
                FROM 
                        (SELECT * 
                        FROM KSF_DATAANALYTICS_DEV.DS_SHRINK.DP_LATEST_NSOH_TIME_SPAN 
                        WHERE DATE_DIFFERENCE > 30) DS
                INNER JOIN (SELECT PRODUCT_GENERATED_IDENTIFIER  
                                    , LOCATION_GENERATED_IDENTIFIER  
                                    , EFFECTIVE_DATE
                                    , LOCATION_AWC_AMOUNT
                            FROM KSFPA.MR2.SS_AVERAGE_WEIGHTED_COST WC
                            WHERE EXPIRY_DATE = '9999-12-31') DV
                ON DV.LOCATION_GENERATED_IDENTIFIER  = DS.LOCATION_GENERATED_IDENTIFIER AND DV.PRODUCT_GENERATED_IDENTIFIER = DS.PRODUCT_GENERATED_IDENTIFIER
                INNER JOIN KSFPA.MR2C."LOCATION" LOC
                ON DS.LOCATION_GENERATED_IDENTIFIER = LOC.LOCATION_GENERATED_IDENTIFIER 
                INNER JOIN KSFPA.MR2C.KEYCODE KC
                ON DS.PRODUCT_GENERATED_IDENTIFIER = KC.PRODUCT_GENERATED_IDENTIFIER """
    df = ETL_DB_MANAGER_PS.pull_into_dataframe(query)

    sorter_category = pd.read_csv('C:\\Users\\Ksaini\\OneDrive - Kmart Australia Limited\\Documents\\Projects\\LossPrevention\\SHR-137_category.csv')
    sorter_category = sorter_category[['Keycode','Sortation Area']].drop_duplicates()

    sorter_category = sorter_category.dropna()
    sorter_category['Keycode'] = sorter_category['Keycode'].astype(int).astype(str)
    df_w_cat = df.merge(sorter_category, left_on='product_source_identifier', right_on='Keycode', how = 'left').reset_index()
    
    grp_df = df_w_cat.groupby(['Sortation Area']).agg({
        'index':'count',
        'daily_stock_on_hand_quantity':'sum',
        'soh_dollar_value' : 'sum',
        'Keycode':'nunique'})

    grp_df.to_csv('SHR-137_analysis.csv')
    return grp_df