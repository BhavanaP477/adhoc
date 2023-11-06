#######################################################################################################################################################
# Delta Neg. SOH Analysis
# Models TY (2022) vs. LY (2021) neg. SOH (in terms of difference and % change) pre-August stocktake using the following features:
# - TY vs. LY Avg. SMS Integrity (average across all weeks from post-Feb. stocktake to pre-Aug. stocktake)
# - TY vs. LY known shrink adjustments (all code 20 and 62 adjustments occuring from post-Feb. stocktake to pre-Aug. stocktake)
# - TY vs. LY number of distinct days in which a gapscan adjustment was performed (from post-Feb. stocktake to pre-Aug. stocktake)
# - TY vs. LY stocktake adjustments during Feb.
# - TY vs. LY product units which have been dispatched but not receipted (i.e., unreceipted manifests) at pre-Aug. stocktake
#######################################################################################################################################################

import imp
import logging
from app import config
from app.dataengine.db_manager import DBManager
from app.helpers import utils
from app.helpers.utils import *
from sklearn.inspection import permutation_importance
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression, LogisticRegression, PoissonRegressor
import statsmodels.formula.api as sm
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.inspection import permutation_importance
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
import openpyxl
from sqlalchemy import false

# Gets the version
SCHEMA_NAME = config.DB_SETTINGS["SF_DB_SCHEMA"]
DB_NAME = config.DB_SETTINGS["SF_DB_DATABASE"]
ETL_LOGGER = setup_logging("etl", config.SHARED_LOGGING_PARAMETERS)
ETL_DB_MANAGER_PS = DBManager('snowflake', config.DB_SETTINGS)

# Import and clean data:
def fetch_and_clean_neg_soh_data():
    query1 = f"""
        SELECT
            soh.location_source_identifier,
            soh.region_description,
            soh.dc_group_description,
            soh.fy23_tot_store_keycodes_pre_aug_stktake,
            soh.fy22_tot_store_keycodes_pre_aug_stktake,
            soh.fy23_neg_soh_keycodes_pre_aug_stktake,
            soh.fy22_neg_soh_keycodes_pre_aug_stktake,
            soh.fy23_neg_soh_keycodes_pre_aug_stktake / soh.fy23_tot_store_keycodes_pre_aug_stktake AS fy23_prop_keycodes_with_neg_soh_pre_aug_stktake,
            soh.fy22_neg_soh_keycodes_pre_aug_stktake / soh.fy22_tot_store_keycodes_pre_aug_stktake AS fy22_prop_keycodes_with_neg_soh_pre_aug_stktake,
            soh.fy23_neg_soh_units_pre_aug_stktake,
            soh.fy22_neg_soh_units_pre_aug_stktake,
            soh.fy23_neg_soh_units_pre_aug_stktake / soh.fy23_tot_store_keycodes_pre_aug_stktake AS fy23_neg_soh_units_per_keycode_pre_aug_stktake,
            soh.fy22_neg_soh_units_pre_aug_stktake / soh.fy22_tot_store_keycodes_pre_aug_stktake AS fy22_neg_soh_units_per_keycode_pre_aug_stktake,
            manifest.fy23_manifested_since_feb_stktake,
            manifest.fy22_manifested_since_feb_stktake,
            sms_integrity.fy23_avg_sms_integrity_since_feb_stktake,
            sms_integrity.fy22_avg_sms_integrity_since_feb_stktake,
            fy23_gapscan.fy23_days_with_gapscan_adj_since_feb_stktake,
            fy22_gapscan.fy22_days_with_gapscan_adj_since_feb_stktake,
            gapscan_adj.fy23_abs_gapscan_adj_qty_since_feb_stktake,
            gapscan_adj.fy22_abs_gapscan_adj_qty_since_feb_stktake,
            unreceipted_manifest.fy23_outstanding_manifest_keycodes_pre_aug_stktake,
            unreceipted_manifest.fy23_outstanding_manifest_units_pre_aug_stktake,
            unreceipted_manifest.fy22_outstanding_manifest_keycodes_pre_aug_stktake,
            unreceipted_manifest.fy22_outstanding_manifest_units_pre_aug_stktake,
            feb_stktake_adj.fy23_feb_stktake_neg_adj_qty,
            feb_stktake_adj.fy22_feb_stktake_neg_adj_qty,
            feb_stktake_adj.fy23_feb_stktake_neg_adj_keycodes,
            feb_stktake_adj.fy22_feb_stktake_neg_adj_keycodes,
            feb_stktake_adj.fy23_feb_stktake_abs_adj_qty,
            feb_stktake_adj.fy22_feb_stktake_abs_adj_qty,
            feb_stktake_adj.fy23_feb_stktake_adj_keycodes,
            feb_stktake_adj.fy22_feb_stktake_adj_keycodes,
            shrink_adj.fy23_known_shrink_adj_qty_since_feb_stktake,
            shrink_adj.fy22_known_shrink_adj_qty_since_feb_stktake,
            shrink_adj.fy23_known_shrink_adj_keycodes_since_feb_stktake,
            shrink_adj.fy22_known_shrink_adj_keycodes_since_feb_stktake
        FROM (
        SELECT
            stores.location_source_identifier,
            stores.region_description,
            stores.dc_group_description,
            SUM(
                CASE
                    WHEN soh.day_date = '2022-07-31' THEN 1
                    ELSE 0
                END) AS fy23_tot_store_keycodes_pre_aug_stktake,
            SUM(
                CASE
                    WHEN soh.day_date = '2021-07-31' THEN 1
                    ELSE 0
                END) AS fy22_tot_store_keycodes_pre_aug_stktake,
            SUM(
                CASE
                    WHEN soh.daily_stock_on_hand_quantity < 0 AND soh.day_date = '2022-07-31' THEN 1
                    ELSE 0
                END) AS fy23_neg_soh_keycodes_pre_aug_stktake,
            SUM(
                CASE
                    WHEN soh.daily_stock_on_hand_quantity < 0 AND soh.day_date = '2021-07-31' THEN 1
                    ELSE 0
                END) AS fy22_neg_soh_keycodes_pre_aug_stktake,
            SUM(
                CASE
                    WHEN soh.daily_stock_on_hand_quantity < 0 AND soh.day_date = '2022-07-31' THEN ABS(soh.daily_stock_on_hand_quantity)
                    ELSE 0
                END) AS fy23_neg_soh_units_pre_aug_stktake,
            SUM(
                CASE
                    WHEN soh.daily_stock_on_hand_quantity < 0 AND soh.day_date = '2021-07-31' THEN ABS(soh.daily_stock_on_hand_quantity)
                    ELSE 0
                END) AS fy22_neg_soh_units_pre_aug_stktake
        FROM "KSFPA"."MR2C"."DAILY_SOH" soh
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON soh.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON soh.product_generated_identifier = products.product_generated_identifier
        WHERE
            LEFT(stores.location_source_identifier, 1) IN ('1', '3', '8') AND
            products.department_source_identifier NOT IN ('053', '097', '099') AND
            soh.day_date IN ('2022-07-31', '2021-07-31')
        GROUP BY stores.location_source_identifier, stores.region_description, stores.dc_group_description
        HAVING
            fy23_tot_store_keycodes_pre_aug_stktake > 10000 AND
            fy22_tot_store_keycodes_pre_aug_stktake > 10000 AND
            ABS(fy23_tot_store_keycodes_pre_aug_stktake - fy22_tot_store_keycodes_pre_aug_stktake) / fy22_tot_store_keycodes_pre_aug_stktake < 0.3
        ) soh
        INNER JOIN (
            SELECT
                stores.location_source_identifier,
                SUM(
                    CASE
                        WHEN ips.day_date BETWEEN '2022-03-01' AND '2022-07-31' THEN ips.stock_movement_quantity
                        ELSE 0
                    END) AS fy23_manifested_since_feb_stktake,
                SUM(
                    CASE
                        WHEN ips.day_date BETWEEN '2021-03-01' AND '2021-07-31' THEN ips.stock_movement_quantity
                        ELSE 0
                    END) AS fy22_manifested_since_feb_stktake
            FROM "KSFPA"."MR2"."IPS_STOCK_AUDIT" ips
            INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON ips.location_generated_identifier = stores.location_generated_identifier
            INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON ips.product_generated_identifier = products.product_generated_identifier
            WHERE
                products.department_source_identifier NOT IN ('053', '097', '099') AND
                ips.day_date BETWEEN '2021-03-01' AND '2022-07-31' AND
                ips.stock_movement_source = '1Manifest'
            GROUP BY stores.location_source_identifier
        ) manifest ON soh.location_source_identifier = manifest.location_source_identifier
        INNER JOIN (
            SELECT
                ews.location_code,
                SUM(
                    CASE
                        WHEN ews.calendar_week_end_date BETWEEN '2022-03-01' AND '2022-07-31' THEN sms_integrity
                        ELSE 0
                    END) / 
                SUM(
                    CASE
                        WHEN ews.calendar_week_end_date BETWEEN '2022-03-01' AND '2022-07-31' THEN 1
                        ELSE 0
                    END) AS fy23_avg_sms_integrity_since_feb_stktake,
                SUM(
                    CASE
                        WHEN ews.calendar_week_end_date BETWEEN '2021-03-01' AND '2021-07-31' THEN sms_integrity
                        ELSE 0
                    END) / 
                SUM(
                    CASE
                        WHEN ews.calendar_week_end_date BETWEEN '2021-03-01' AND '2021-07-31' THEN 1
                        ELSE 0
                    END) AS fy22_avg_sms_integrity_since_feb_stktake
            FROM "KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD"."EWS"."EWS_TREND_SCORES" ews
            WHERE
                ews.sms_integrity > 0 AND
                ews.sms_integrity < 1 AND
                ews.calendar_week_end_date BETWEEN '2021-03-01' AND '2022-07-31'
            GROUP BY ews.location_code
            HAVING
                SUM(
                    CASE
                        WHEN ews.calendar_week_end_date BETWEEN '2022-03-01' AND '2022-07-31' THEN 1
                        ELSE 0
                    END) > 10 AND
                SUM(
                    CASE
                        WHEN ews.calendar_week_end_date BETWEEN '2021-03-01' AND '2021-07-31' THEN 1
                        ELSE 0
                    END) > 10
        ) sms_integrity ON soh.location_source_identifier = sms_integrity.location_code
        INNER JOIN (
            SELECT
                stores.location_source_identifier,
                COUNT(DISTINCT ips.day_date) AS fy23_days_with_gapscan_adj_since_feb_stktake
            FROM "KSFPA"."MR2"."IPS_STOCK_AUDIT" ips
            INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON ips.location_generated_identifier = stores.location_generated_identifier
            INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON ips.product_generated_identifier = products.product_generated_identifier
            WHERE
                products.department_source_identifier NOT IN ('053', '097', '099') AND
                ips.day_date BETWEEN '2022-03-01' AND '2022-07-31' AND
                ips.stock_movement_code IN ('20', '62')
            GROUP BY stores.location_source_identifier
        ) fy23_gapscan ON soh.location_source_identifier = fy23_gapscan.location_source_identifier
        INNER JOIN (
            SELECT
                stores.location_source_identifier,
                SUM(
                    CASE
                        WHEN ips.day_date BETWEEN '2022-03-01' AND '2022-07-31' THEN ABS(ips.stock_movement_quantity)
                        ELSE 0
                    END) AS fy23_abs_gapscan_adj_qty_since_feb_stktake,
                SUM(
                    CASE
                        WHEN ips.day_date BETWEEN '2021-03-01' AND '2021-07-31' THEN ABS(ips.stock_movement_quantity)
                        ELSE 0
                    END) AS fy22_abs_gapscan_adj_qty_since_feb_stktake
            FROM "KSFPA"."MR2"."IPS_STOCK_AUDIT" ips
            INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON ips.location_generated_identifier = stores.location_generated_identifier
            INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON ips.product_generated_identifier = products.product_generated_identifier
            WHERE
                ips.day_date BETWEEN '2021-03-01' AND '2022-07-31' AND
                products.department_source_identifier NOT IN ('053', '097', '099') AND
                ips.stock_movement_code IN ('20', '62')
            GROUP BY stores.location_source_identifier
        ) gapscan_adj ON soh.location_source_identifier = gapscan_adj.location_source_identifier
        INNER JOIN (
            SELECT
                stores.location_source_identifier,
                COUNT(DISTINCT ips.day_date) AS fy22_days_with_gapscan_adj_since_feb_stktake
            FROM "KSFPA"."MR2"."IPS_STOCK_AUDIT" ips
            INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON ips.location_generated_identifier = stores.location_generated_identifier
            INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON ips.product_generated_identifier = products.product_generated_identifier
            WHERE
                products.department_source_identifier NOT IN ('053', '097', '099') AND
                ips.day_date BETWEEN '2021-03-01' AND '2021-07-31' AND
                ips.stock_movement_code IN ('20', '62')
            GROUP BY stores.location_source_identifier
        ) fy22_gapscan ON soh.location_source_identifier = fy22_gapscan.location_source_identifier
        INNER JOIN (
            SELECT
                stores1.location_source_identifier,
                SUM(
                    CASE
                        WHEN
                            transfer_despatch.despatch_date <= '2022-07-31' AND
                            (transfer_receipt.receipt_date > '2022-07-31' OR transfer_receipt.receipt_date IS NULL) THEN 1
                        ELSE 0
                    END) AS fy23_outstanding_manifest_keycodes_pre_aug_stktake,
                SUM(
                    CASE
                        WHEN
                            transfer_despatch.despatch_date <= '2022-07-31' AND
                            (transfer_receipt.receipt_date > '2022-07-31' OR transfer_receipt.receipt_date IS NULL) THEN ABS(transfer_product.despatch_quantity)
                        ELSE 0
                    END) AS fy23_outstanding_manifest_units_pre_aug_stktake,
                SUM(
                    CASE
                        WHEN
                            transfer_despatch.despatch_date <= '2021-07-31' AND
                            (transfer_receipt.receipt_date > '2021-07-31' OR transfer_receipt.receipt_date IS NULL) THEN 1
                        ELSE 0
                    END) AS fy22_outstanding_manifest_keycodes_pre_aug_stktake,
                SUM(
                    CASE
                        WHEN
                            transfer_despatch.despatch_date <= '2021-07-31' AND
                            (transfer_receipt.receipt_date > '2021-07-31' OR transfer_receipt.receipt_date IS NULL) THEN ABS(transfer_product.despatch_quantity)
                        ELSE 0
                    END) AS fy22_outstanding_manifest_units_pre_aug_stktake
            FROM "KSFPA"."MR2"."TRANSFER_PRODUCT" transfer_product
            INNER JOIN "KSFPA"."MR2"."TRANSFER_DESPATCH" transfer_despatch ON (
                transfer_product.transfer_generated_identifier = transfer_despatch.transfer_generated_identifier AND
                transfer_product.expiry_date = transfer_despatch.expiry_date)
            LEFT JOIN "KSFPA"."MR2"."TRANSFER_RECEIPT" transfer_receipt ON (
                transfer_product.transfer_generated_identifier = transfer_receipt.transfer_generated_identifier AND
                transfer_product.expiry_date = transfer_receipt.expiry_date)
            INNER JOIN "KSFPA"."MR2C"."LOCATION" stores1 ON transfer_receipt.location_generated_identifier = stores1.location_generated_identifier
            INNER JOIN "KSFPA"."MR2C"."LOCATION" stores2 ON transfer_despatch.location_generated_identifier = stores2.location_generated_identifier
            INNER JOIN "KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD"."COMMON_DIMENSIONS"."DIM_DATE" dates ON transfer_despatch.despatch_date = dates.date
            WHERE
                transfer_despatch.expiry_date = '9999-12-31' AND
                transfer_despatch.transfer_type_code IN ('DD', 'XD')
            GROUP BY stores1.location_source_identifier
        ) unreceipted_manifest ON soh.location_source_identifier = unreceipted_manifest.location_source_identifier
        INNER JOIN (
            SELECT
                stores.location_source_identifier,
                SUM(
                    CASE
                        WHEN dates.calendar_year = 2022 AND ips.stock_movement_quantity < 0 THEN ABS(ips.stock_movement_quantity)
                        ELSE 0
                    END) AS fy23_feb_stktake_neg_adj_qty,
                SUM(
                    CASE
                        WHEN dates.calendar_year = 2021 AND ips.stock_movement_quantity < 0 THEN ABS(ips.stock_movement_quantity)
                        ELSE 0
                    END) AS fy22_feb_stktake_neg_adj_qty,
                SUM(
                    CASE
                        WHEN dates.calendar_year = 2022 AND ips.stock_movement_quantity < 0 THEN 1
                        ELSE 0
                    END) AS fy23_feb_stktake_neg_adj_keycodes,
                SUM(
                    CASE
                        WHEN dates.calendar_year = 2021 AND ips.stock_movement_quantity < 0 THEN 1
                        ELSE 0
                    END) AS fy22_feb_stktake_neg_adj_keycodes,
                SUM(
                    CASE
                        WHEN dates.calendar_year = 2022 THEN ABS(ips.stock_movement_quantity)
                        ELSE 0
                    END) AS fy23_feb_stktake_abs_adj_qty,
                SUM(
                    CASE
                        WHEN dates.calendar_year = 2021 THEN ABS(ips.stock_movement_quantity)
                        ELSE 0
                    END) AS fy22_feb_stktake_abs_adj_qty,
                SUM(
                    CASE
                        WHEN dates.calendar_year = 2022 THEN 1
                        ELSE 0
                    END) AS fy23_feb_stktake_adj_keycodes,
                SUM(
                    CASE
                        WHEN dates.calendar_year = 2021 THEN 1
                        ELSE 0
                    END) AS fy22_feb_stktake_adj_keycodes
            FROM "KSFPA"."MR2"."IPS_STOCK_AUDIT" ips
            INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON ips.location_generated_identifier = stores.location_generated_identifier
            INNER JOIN "KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD"."COMMON_DIMENSIONS"."DIM_DATE" dates ON ips.day_date = dates.date
            INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON ips.product_generated_identifier = products.product_generated_identifier
            WHERE
                ips.stock_movement_quantity < 0 AND
                ips.stock_movement_code IN ('04', '17') AND
                dates.calendar_year IN (2021, 2022) AND
                dates.calendar_month_name = 'February' AND
                products.department_source_identifier NOT IN ('053', '097', '099')
            GROUP BY stores.location_source_identifier    
        ) feb_stktake_adj ON soh.location_source_identifier = feb_stktake_adj.location_source_identifier
        INNER JOIN (
            SELECT
                stores.location_source_identifier,
                SUM(
                    CASE
                        WHEN ips.day_date BETWEEN '2021-03-01' AND '2021-07-31' THEN ABS(ips.stock_movement_quantity)
                        ELSE 0
                    END) AS fy22_known_shrink_adj_qty_since_feb_stktake,
                SUM(
                    CASE
                        WHEN ips.day_date BETWEEN '2022-03-01' AND '2022-07-31' THEN ABS(ips.stock_movement_quantity)
                        ELSE 0
                    END) AS fy23_known_shrink_adj_qty_since_feb_stktake,
                SUM(
                    CASE
                        WHEN ips.day_date BETWEEN '2021-03-01' AND '2021-07-31' THEN 1
                        ELSE 0
                    END) AS fy22_known_shrink_adj_keycodes_since_feb_stktake,
                SUM(
                    CASE
                        WHEN ips.day_date BETWEEN '2022-03-01' AND '2022-07-31' THEN 1
                        ELSE 0
                    END) AS fy23_known_shrink_adj_keycodes_since_feb_stktake
            FROM "KSFPA"."MR2"."IPS_STOCK_AUDIT" ips
            INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON ips.location_generated_identifier = stores.location_generated_identifier
            INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON ips.product_generated_identifier = products.product_generated_identifier
            WHERE
                ips.stock_movement_source = 'Shrinkage' AND
                ips.stock_movement_quantity < 0 AND
                ips.day_date BETWEEN '2021-03-01' AND '2022-07-31' AND
                products.department_source_identifier NOT IN ('053', '097', '099')
            GROUP BY stores.location_source_identifier
        ) shrink_adj ON soh.location_source_identifier = shrink_adj.location_source_identifier
    """
    neg_soh_raw = ETL_DB_MANAGER_PS.pull_into_dataframe(query1)
    neg_soh_clean = neg_soh_raw.copy()
    for col_name in neg_soh_clean.columns:
        neg_soh_clean = neg_soh_clean.rename(columns = {col_name: col_name.upper()})
    neg_soh_clean["FY23_PROP_KEYCODES_WITH_NEG_SOH_PRE_AUG_STKTAKE"] = neg_soh_clean["FY23_PROP_KEYCODES_WITH_NEG_SOH_PRE_AUG_STKTAKE"].astype(float)
    neg_soh_clean["FY22_PROP_KEYCODES_WITH_NEG_SOH_PRE_AUG_STKTAKE"] = neg_soh_clean["FY22_PROP_KEYCODES_WITH_NEG_SOH_PRE_AUG_STKTAKE"].astype(float)
    neg_soh_clean["FY23_NEG_SOH_UNITS_PER_KEYCODE_PRE_AUG_STKTAKE"] = neg_soh_clean["FY23_NEG_SOH_UNITS_PER_KEYCODE_PRE_AUG_STKTAKE"].astype(float)
    neg_soh_clean["FY22_NEG_SOH_UNITS_PER_KEYCODE_PRE_AUG_STKTAKE"] = neg_soh_clean["FY22_NEG_SOH_UNITS_PER_KEYCODE_PRE_AUG_STKTAKE"].astype(float)
    # neg_soh_clean = neg_soh_clean.loc[(neg_soh_clean!=0).all(axis=1)]
    return(neg_soh_clean)

# Outlier removal
def outlier_removal(degree, dat, dat_cols):
    for x in dat_cols:
        q75, q25 = np.percentile(dat.loc[:, x], [75, 25])
        intr_qr = q75 - q25
        max = q75 + (degree * intr_qr)
        min = q25 - (degree * intr_qr) 
        dat.loc[dat[x] < min, x] = np.nan
        dat.loc[dat[x] > max, x] = np.nan
    dat = dat.dropna(axis = 0).reset_index(drop = True)
    return(dat)

# The model variables will be the differences between FY23 and FY22 in all quantites
def diff_dat_units():
    neg_soh_clean = fetch_and_clean_neg_soh_data()
    new_dat_dict = {
        "LOCATION_SOURCE_IDENTIFIER": neg_soh_clean["LOCATION_SOURCE_IDENTIFIER"],
        "FY23_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE": neg_soh_clean["FY23_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE"],
        "FY22_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE": neg_soh_clean["FY22_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE"],
        "DIFF_NEG_SOH_UNITS_PRE_AUG_STKTAKE": neg_soh_clean["FY23_NEG_SOH_UNITS_PRE_AUG_STKTAKE"] - neg_soh_clean["FY22_NEG_SOH_UNITS_PRE_AUG_STKTAKE"],
        "DIFF_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE": neg_soh_clean["FY23_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE"] - neg_soh_clean["FY22_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE"],
        "DIFF_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE": neg_soh_clean["FY23_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE"] - neg_soh_clean["FY22_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE"],
        "DIFF_OUTSTANDING_MANIFEST_UNITS_PRE_AUG_STKTAKE": neg_soh_clean["FY23_OUTSTANDING_MANIFEST_UNITS_PRE_AUG_STKTAKE"] - neg_soh_clean["FY22_OUTSTANDING_MANIFEST_UNITS_PRE_AUG_STKTAKE"],
        "DIFF_FEB_STKTAKE_NEG_ADJ_UNITS": neg_soh_clean["FY23_FEB_STKTAKE_NEG_ADJ_QTY"] - neg_soh_clean["FY22_FEB_STKTAKE_NEG_ADJ_QTY"],
        "DIFF_KNOWN_SHRINK_ADJ_UNITS": neg_soh_clean["FY23_KNOWN_SHRINK_ADJ_QTY_SINCE_FEB_STKTAKE"] - neg_soh_clean["FY22_KNOWN_SHRINK_ADJ_QTY_SINCE_FEB_STKTAKE"],
    }
    neg_soh_final = pd.DataFrame(new_dat_dict)
    neg_soh_final = outlier_removal(
        2.5,
        neg_soh_final,
        [
            "FY23_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE", 
            "FY22_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE",
            "DIFF_NEG_SOH_UNITS_PRE_AUG_STKTAKE",
            "DIFF_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE", 
            "DIFF_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE", 
            "DIFF_OUTSTANDING_MANIFEST_UNITS_PRE_AUG_STKTAKE",
            "DIFF_FEB_STKTAKE_NEG_ADJ_UNITS",
            "DIFF_KNOWN_SHRINK_ADJ_UNITS"
        ]
    )
    return(neg_soh_final)

def diff_dat_cases():
    neg_soh_clean = fetch_and_clean_neg_soh_data()
    new_dat_dict = {
        "LOCATION_SOURCE_IDENTIFIER": neg_soh_clean["LOCATION_SOURCE_IDENTIFIER"],
        "FY23_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE": neg_soh_clean["FY23_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE"],
        "FY22_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE": neg_soh_clean["FY22_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE"],
        "DIFF_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE": neg_soh_clean["FY23_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"] - neg_soh_clean["FY22_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"],
        "DIFF_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE": neg_soh_clean["FY23_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE"] - neg_soh_clean["FY22_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE"],
        "DIFF_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE": neg_soh_clean["FY23_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE"] - neg_soh_clean["FY22_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE"],
        "DIFF_OUTSTANDING_MANIFEST_KEYCODES_PRE_AUG_STKTAKE": neg_soh_clean["FY23_OUTSTANDING_MANIFEST_KEYCODES_PRE_AUG_STKTAKE"] - neg_soh_clean["FY22_OUTSTANDING_MANIFEST_KEYCODES_PRE_AUG_STKTAKE"],
        "DIFF_FEB_STKTAKE_NEG_ADJ_KEYCODES": neg_soh_clean["FY23_FEB_STKTAKE_NEG_ADJ_KEYCODES"] - neg_soh_clean["FY22_FEB_STKTAKE_NEG_ADJ_KEYCODES"],
        "DIFF_KNOWN_SHRINK_ADJ_KEYCODES": neg_soh_clean["FY23_KNOWN_SHRINK_ADJ_KEYCODES_SINCE_FEB_STKTAKE"] - neg_soh_clean["FY22_KNOWN_SHRINK_ADJ_KEYCODES_SINCE_FEB_STKTAKE"],
    }
    neg_soh_final = pd.DataFrame(new_dat_dict)
    neg_soh_final = outlier_removal(
        2.5,
        neg_soh_final,
        [
            "FY23_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE", 
            "FY22_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE",
            "DIFF_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE",
            "DIFF_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE", 
            "DIFF_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE", 
            "DIFF_OUTSTANDING_MANIFEST_KEYCODES_PRE_AUG_STKTAKE",
            "DIFF_FEB_STKTAKE_NEG_ADJ_KEYCODES",
            "DIFF_KNOWN_SHRINK_ADJ_KEYCODES"
        ]
    )
    return(neg_soh_final)

# Alternative form of variables: % YoY Change (as opposed to YoY differences)
def prop_change_dat_units():
    neg_soh_clean = fetch_and_clean_neg_soh_data()
    alt_dat_dict = {
        "LOCATION_SOURCE_IDENTIFIER": neg_soh_clean["LOCATION_SOURCE_IDENTIFIER"],
        "FY23_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE": neg_soh_clean["FY23_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE"],
        "FY22_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE": neg_soh_clean["FY22_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE"],
        "PROP_CHANGE_NEG_SOH_UNITS_PRE_AUG_STKTAKE": (neg_soh_clean["FY23_NEG_SOH_UNITS_PRE_AUG_STKTAKE"] - neg_soh_clean["FY22_NEG_SOH_UNITS_PRE_AUG_STKTAKE"]) / neg_soh_clean["FY22_NEG_SOH_UNITS_PRE_AUG_STKTAKE"],
        "PROP_CHANGE_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE": (neg_soh_clean["FY23_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE"] - neg_soh_clean["FY22_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE"]) / neg_soh_clean["FY22_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE"],
        "PROP_CHANGE_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE": (neg_soh_clean["FY23_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE"] - neg_soh_clean["FY22_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE"]) / neg_soh_clean["FY22_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE"],
        "PROP_CHANGE_OUTSTANDING_MANIFEST_UNITS_PRE_AUG_STKTAKE": (neg_soh_clean["FY23_OUTSTANDING_MANIFEST_UNITS_PRE_AUG_STKTAKE"] - neg_soh_clean["FY22_OUTSTANDING_MANIFEST_UNITS_PRE_AUG_STKTAKE"]) / neg_soh_clean["FY22_OUTSTANDING_MANIFEST_UNITS_PRE_AUG_STKTAKE"],
        "PROP_CHANGE_FEB_STKTAKE_NEG_ADJ_UNITS": (neg_soh_clean["FY23_FEB_STKTAKE_NEG_ADJ_QTY"] - neg_soh_clean["FY22_FEB_STKTAKE_NEG_ADJ_QTY"]) / neg_soh_clean["FY22_FEB_STKTAKE_NEG_ADJ_QTY"],
        "PROP_CHANGE_KNOWN_SHRINK_ADJ_UNITS": (neg_soh_clean["FY23_KNOWN_SHRINK_ADJ_QTY_SINCE_FEB_STKTAKE"] - neg_soh_clean["FY22_KNOWN_SHRINK_ADJ_QTY_SINCE_FEB_STKTAKE"]) / neg_soh_clean["FY22_KNOWN_SHRINK_ADJ_QTY_SINCE_FEB_STKTAKE"]
    }
    neg_soh_alt = pd.DataFrame(alt_dat_dict)
    neg_soh_alt = outlier_removal(
        2.5,
        neg_soh_alt,
        [
            "FY23_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE", 
            "FY22_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE",
            "PROP_CHANGE_NEG_SOH_UNITS_PRE_AUG_STKTAKE",
            "PROP_CHANGE_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE", 
            "PROP_CHANGE_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE", 
            "PROP_CHANGE_OUTSTANDING_MANIFEST_UNITS_PRE_AUG_STKTAKE",
            "PROP_CHANGE_FEB_STKTAKE_NEG_ADJ_UNITS",
            "PROP_CHANGE_KNOWN_SHRINK_ADJ_UNITS"
        ]
    )
    return(neg_soh_alt)

def prop_change_dat_cases():
    neg_soh_clean = fetch_and_clean_neg_soh_data()
    alt_dat_dict = {
        "LOCATION_SOURCE_IDENTIFIER": neg_soh_clean["LOCATION_SOURCE_IDENTIFIER"],
        "FY23_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE": neg_soh_clean["FY23_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE"],
        "FY22_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE": neg_soh_clean["FY22_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE"],
        "PROP_CHANGE_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE": (neg_soh_clean["FY23_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"] - neg_soh_clean["FY22_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"]) / neg_soh_clean["FY22_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"],
        "PROP_CHANGE_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE": (neg_soh_clean["FY23_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE"] - neg_soh_clean["FY22_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE"]) / neg_soh_clean["FY22_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE"],
        "PROP_CHANGE_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE": (neg_soh_clean["FY23_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE"] - neg_soh_clean["FY22_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE"]) / neg_soh_clean["FY22_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE"],
        "PROP_CHANGE_OUTSTANDING_MANIFEST_KEYCODES_PRE_AUG_STKTAKE": (neg_soh_clean["FY23_OUTSTANDING_MANIFEST_KEYCODES_PRE_AUG_STKTAKE"] - neg_soh_clean["FY22_OUTSTANDING_MANIFEST_KEYCODES_PRE_AUG_STKTAKE"]) / neg_soh_clean["FY22_OUTSTANDING_MANIFEST_KEYCODES_PRE_AUG_STKTAKE"],
        "PROP_CHANGE_FEB_STKTAKE_NEG_ADJ_KEYCODES": (neg_soh_clean["FY23_FEB_STKTAKE_NEG_ADJ_KEYCODES"] - neg_soh_clean["FY22_FEB_STKTAKE_NEG_ADJ_KEYCODES"]) / neg_soh_clean["FY22_FEB_STKTAKE_NEG_ADJ_KEYCODES"],
        "PROP_CHANGE_KNOWN_SHRINK_ADJ_KEYCODES": (neg_soh_clean["FY23_KNOWN_SHRINK_ADJ_KEYCODES_SINCE_FEB_STKTAKE"] - neg_soh_clean["FY22_KNOWN_SHRINK_ADJ_KEYCODES_SINCE_FEB_STKTAKE"]) / neg_soh_clean["FY22_KNOWN_SHRINK_ADJ_KEYCODES_SINCE_FEB_STKTAKE"]
    }
    neg_soh_alt = pd.DataFrame(alt_dat_dict)
    neg_soh_alt = outlier_removal(
        2.5,
        neg_soh_alt,
        [
            "FY23_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE", 
            "FY22_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE",
            "PROP_CHANGE_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE",
            "PROP_CHANGE_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE", 
            "PROP_CHANGE_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE", 
            "PROP_CHANGE_OUTSTANDING_MANIFEST_KEYCODES_PRE_AUG_STKTAKE",
            "PROP_CHANGE_FEB_STKTAKE_NEG_ADJ_KEYCODES",
            "PROP_CHANGE_KNOWN_SHRINK_ADJ_KEYCODES"
        ]
    )
    return(neg_soh_alt)

# K-means clustering of stores based on volume (i.e., daily SOH figures)
def clusters(dat, dat_cols):
    scaled_x = StandardScaler().fit_transform(dat[dat_cols])
    scores = []
    for i in range(2, 10):
        k_means = KMeans(
            init = "random",
            n_init = 10,
            max_iter = 100,
            random_state = 0,
            n_clusters = i
        )
        k_means.fit(scaled_x)
        score = silhouette_score(scaled_x, k_means.labels_)
        scores.append(score)
    optimal_clusters = np.argmax(scores) + 2
    k_means = KMeans(
        init = "random",
            n_init = 10,
            max_iter = 100,
            random_state = 0,
            n_clusters = optimal_clusters
    )
    k_means.fit(scaled_x)
    dat["CLUSTER"] = k_means.labels_
    return(dat)

# EDA
def EDA(dat, x_col, y_col, hue_col = None):
    dat = clusters(dat = dat, dat_cols = ["FY23_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE"])
    p1 = sns.lmplot(
        x = x_col,
        y = y_col,
        hue = hue_col,
        data = dat,
        ci = None
    )
    type = []
    if dat.iloc[:, :-1].equals(diff_dat_units()):
        type = "Difference Units"
    elif dat.iloc[:, :-1].equals(diff_dat_cases()):
        type = "Difference Cases"
    elif dat.iloc[:, :-1].equals(prop_change_dat_units()):
        type = "% Change Units"
    elif dat.iloc[:, :-1].equals(prop_change_dat_cases()):
        type = "% Change Cases"
    else:
        raise ValueError("Data input is invalid")
    if x_col in ["DIFF_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE", "PROP_CHANGE_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE"]:
        plt.xlabel("TY-LY {} in Avg. SMS Integrity (From Post-Feb Stocktake)".format(type))
    elif x_col in ["DIFF_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE", "PROP_CHANGE_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE"]:
        plt.xlabel("TY-LY {} in Total Days with Gapscan Adj. (From Post-Feb Stocktake)".format(type))
    elif x_col in [
        "DIFF_OUTSTANDING_MANIFEST_UNITS_PRE_AUG_STKTAKE",
        "DIFF_OUTSTANDING_MANIFEST_KEYCODES_PRE_AUG_STKTAKE", 
        "PROP_CHANGE_OUTSTANDING_MANIFEST_UNITS_PRE_AUG_STKTAKE",
        "PROP_CHANGE_OUTSTANDING_MANIFEST_KEYCODES_PRE_AUG_STKTAKE"
    ]:
        plt.xlabel("TY-LY {} in Outstanding Manifest (From Post-Feb Stocktake)".format(type))
    elif x_col in [
        "DIFF_FEB_STKTAKE_NEG_ADJ_UNITS", 
        "DIFF_FEB_STKTAKE_NEG_ADJ_KEYCODES", 
        "PROP_CHANGE_FEB_STKTAKE_NEG_ADJ_UNITS",
        "PROP_CHANGE_FEB_STKTAKE_NEG_ADJ_KEYCODES"
    ]:
        plt.xlabel("TY-LY {} in Feb. Stocktake Neg. Adjustments".format(type))
    elif x_col in [
        "DIFF_KNOWN_SHRINK_ADJ_UNITS",
        "DIFF_KNOWN_SHRINK_ADJ_KEYCODES", 
        "PROP_CHANGE_KNOWN_SHRINK_ADJ_UNITS",
        "PROP_CHANGE_KNOWN_SHRINK_ADJ_KEYCODES"
    ]:
        plt.xlabel("TY-LY {} in Known Shrink Adj. (From Post-Feb. Stocktake)".format(type))
    else:
        raise ValueError("Column names invalid")
    plt.ylabel("TY-LY {} in Neg. SOH (Pre-Stocktake)".format(type))
    plt.show()

# Linear regression
def linear_reg(dat, x_cols, y_col):
    equation = y_col + " ~ " + "".join([x_cols[i] + " + " for i in range(len(x_cols))])
    equation = equation[:-3]
    lin_mod = sm.ols(
        formula = equation, 
        data = dat
    ).fit()
    return(print(lin_mod.summary()))

# Random Forest Regressor - Assess the relative importance of each of the above considered factors
def rf_importance(dat, x_cols, y_col):
    X = dat[x_cols]
    y = dat[y_col]
    rf_mod = RandomForestRegressor(random_state = 0, max_depth = 3, n_estimators = 1000)
    rf_mod.fit(X, y)
    result = permutation_importance(
        estimator = rf_mod,
        X = X,
        y = y
    )
    importances = pd.Series(result.importances_mean, index = X.columns)
    feature_names_cleaned = ["SMS Integrity", "Days with Gapscan Adj.", "Outstanding Manifests", "Feb. Stocktake Neg. Adj.", "Known Shrink Adj."]
    importance_df = pd.DataFrame({
        "Feature": feature_names_cleaned,
        "Mean Accuracy Decrease": importances
    }).sort_values(by = "Mean Accuracy Decrease", axis = 0, ascending = False).reset_index(drop = True)
    p_rf = sns.barplot(x = "Mean Accuracy Decrease", y = "Feature", data = importance_df, color = "blue")
    plt.xlabel("Accuracy Decrease")
    if dat.equals(diff_dat_units()):
        plt.title("TY-LY Difference Neg. SOH Units: Relative Importance Using RF")
    elif dat.equals(diff_dat_cases()):
        plt.title("TY-LY Difference Neg. SOH Store-Keycodes: Relative Importance Using RF")
    elif dat.equals(prop_change_dat_units()):
        plt.title("TY-LY % Change Neg. SOH Units: Relative Importance Using RF")
    elif dat.equals(prop_change_dat_cases()):
        plt.title("TY-LY % Change Neg. SOH Store-Keycodes: Relative Importance Using RF")
    else:
        raise ValueError("Data input is invalid")
    plt.show()

# Analytical pipeline
def analysis_pipeline(metric = "Difference Units"):
    if metric == "Difference Units":
        df = diff_dat_units()
        EDA(
            df, 
            "DIFF_OUTSTANDING_MANIFEST_UNITS_PRE_AUG_STKTAKE", 
            "DIFF_NEG_SOH_UNITS_PRE_AUG_STKTAKE"
        )
        linear_reg(
            df,
            x_cols = [
                "DIFF_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE", 
                "DIFF_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE", 
                "DIFF_OUTSTANDING_MANIFEST_UNITS_PRE_AUG_STKTAKE",
                "DIFF_FEB_STKTAKE_NEG_ADJ_UNITS",
                "DIFF_KNOWN_SHRINK_ADJ_UNITS"
            ],
            y_col = "DIFF_NEG_SOH_UNITS_PRE_AUG_STKTAKE"
        )
        rf_importance(
            diff_dat_units(), 
            x_cols = [
                "DIFF_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE", 
                "DIFF_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE", 
                "DIFF_OUTSTANDING_MANIFEST_UNITS_PRE_AUG_STKTAKE",
                "DIFF_FEB_STKTAKE_NEG_ADJ_UNITS",
                "DIFF_KNOWN_SHRINK_ADJ_UNITS"
            ], 
            y_col = "DIFF_NEG_SOH_UNITS_PRE_AUG_STKTAKE"
        )
    if metric == "Difference Cases":
        df = diff_dat_cases()
        EDA(
            df, 
            "DIFF_OUTSTANDING_MANIFEST_KEYCODES_PRE_AUG_STKTAKE", 
            "DIFF_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"
        )
        linear_reg(
            df,
            x_cols = [
                "DIFF_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE", 
                "DIFF_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE", 
                "DIFF_OUTSTANDING_MANIFEST_KEYCODES_PRE_AUG_STKTAKE",
                "DIFF_FEB_STKTAKE_NEG_ADJ_KEYCODES",
                "DIFF_KNOWN_SHRINK_ADJ_KEYCODES"
            ],
            y_col = "DIFF_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"
        )
        rf_importance(
            diff_dat_cases(), 
            x_cols = [
                "DIFF_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE", 
                "DIFF_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE", 
                "DIFF_OUTSTANDING_MANIFEST_KEYCODES_PRE_AUG_STKTAKE",
                "DIFF_FEB_STKTAKE_NEG_ADJ_KEYCODES",
                "DIFF_KNOWN_SHRINK_ADJ_KEYCODES"
            ], 
            y_col = "DIFF_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"
        )
    elif metric == "% Change Units":
        df = prop_change_dat_units()
        EDA(
            df, 
            "PROP_CHANGE_OUTSTANDING_MANIFEST_UNITS_PRE_AUG_STKTAKE", 
            "PROP_CHANGE_NEG_SOH_UNITS_PRE_AUG_STKTAKE"
        )
        linear_reg(
            df,
            x_cols = [
                "PROP_CHANGE_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE", 
                "PROP_CHANGE_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE", 
                "PROP_CHANGE_OUTSTANDING_MANIFEST_UNITS_PRE_AUG_STKTAKE",
                "PROP_CHANGE_FEB_STKTAKE_NEG_ADJ_UNITS",
                "PROP_CHANGE_KNOWN_SHRINK_ADJ_UNITS"
            ], 
            y_col = "PROP_CHANGE_NEG_SOH_UNITS_PRE_AUG_STKTAKE"
        )
        rf_importance(
            prop_change_dat_units(), 
            x_cols = [
                "PROP_CHANGE_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE", 
                "PROP_CHANGE_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE", 
                "PROP_CHANGE_OUTSTANDING_MANIFEST_UNITS_PRE_AUG_STKTAKE",
                "PROP_CHANGE_FEB_STKTAKE_NEG_ADJ_UNITS",
                "PROP_CHANGE_KNOWN_SHRINK_ADJ_UNITS"
            ], 
            y_col = "PROP_CHANGE_NEG_SOH_UNITS_PRE_AUG_STKTAKE"
        )
    elif metric == "% Change Cases":
        df = prop_change_dat_cases()
        EDA(
            df, 
            "PROP_CHANGE_OUTSTANDING_MANIFEST_KEYCODES_PRE_AUG_STKTAKE", 
            "PROP_CHANGE_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"
        )
        linear_reg(
            df,
            x_cols = [
                "PROP_CHANGE_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE", 
                "PROP_CHANGE_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE", 
                "PROP_CHANGE_OUTSTANDING_MANIFEST_KEYCODES_PRE_AUG_STKTAKE",
                "PROP_CHANGE_FEB_STKTAKE_NEG_ADJ_KEYCODES",
                "PROP_CHANGE_KNOWN_SHRINK_ADJ_KEYCODES"
            ], 
            y_col = "PROP_CHANGE_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"
        )
        rf_importance(
            prop_change_dat_cases(), 
            x_cols = [
                "PROP_CHANGE_AVG_SMS_INTEGRITY_SINCE_FEB_STKTAKE", 
                "PROP_CHANGE_DAYS_WITH_GAPSCAN_ADJ_SINCE_FEB_STKTAKE", 
                "PROP_CHANGE_OUTSTANDING_MANIFEST_KEYCODES_PRE_AUG_STKTAKE",
                "PROP_CHANGE_FEB_STKTAKE_NEG_ADJ_KEYCODES",
                "PROP_CHANGE_KNOWN_SHRINK_ADJ_KEYCODES"
            ], 
            y_col = "PROP_CHANGE_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"
        )
    else:
        return ValueError("Only metrics 'Difference Units', 'Difference Cases', '% Change Units' and '% Change Cases' allowed")

analysis_pipeline(metric = "Difference Units")
