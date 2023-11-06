import snowflake.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression, LogisticRegression, PoissonRegressor
import statsmodels.formula.api as sm
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
import openpyxl
from sqlalchemy import false
# Gets the version
ctx = snowflake.connector.connect(
account='kmartau.ap-southeast-2',
authenticator='externalbrowser',
role='KSF_FINANCE',
warehouse='KSF_INVENTORY_WH',
database='KSFPA',
schema='RFID',
user = 'Cal.Roff@kmart.com.au'
)
# Import and clean data:
def fetch_and_clean_house_manifest_binary_data_subset():
    query1 = f"""
    SELECT
        soh.location_source_identifier,
        soh.product_source_identifier,
        soh.is_neg_soh,
        CASE
            WHEN house_manifest.house_manifest_freq > 0 THEN 1
            ELSE 0
        END AS had_at_least_one_house_manifest
    FROM (
        SELECT
            stores.location_source_identifier,
            products.product_source_identifier,
            CASE
                WHEN soh.daily_stock_on_hand_quantity < 0 THEN 1
                ELSE 0
            END AS is_neg_soh,
            COUNT(DISTINCT products.product_source_identifier) OVER (PARTITION BY stores.location_source_identifier) AS distinct_keycodes_in_store
        FROM "KSFPA"."MR2C"."DAILY_SOH" soh
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON soh.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON soh.product_generated_identifier = products.product_generated_identifier
        WHERE
            LEFT(stores.location_source_identifier, 1) IN ('1', '3', '8') AND
            products.department_source_identifier NOT IN ('053', '097', '099') AND
            soh.day_date = '2022-07-31' AND
            stores.dc_group_description NOT LIKE '%UNKNOWN%'
        QUALIFY
            distinct_keycodes_in_store >= 10000
    ) soh
    LEFT JOIN (
        SELECT
            stores.location_source_identifier,
            products.product_source_identifier,
            COUNT(*) AS house_manifest_freq
        FROM "KSFPA"."MR2"."WMS_MANIFEST" wms_manifest
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON wms_manifest.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON wms_manifest.product_generated_identifier = products.product_generated_identifier
        WHERE
            products.department_source_identifier NOT IN ('053', '097', '099') AND
            wms_manifest.day_date BETWEEN '2022-03-01' AND '2022-07-31' AND
            wms_manifest.wms_manifest_type_code = 'H' AND
            wms_manifest.despatch_quantity > 0
        GROUP BY stores.location_source_identifier, products.product_source_identifier
    ) house_manifest ON soh.location_source_identifier = house_manifest.location_source_identifier AND soh.product_source_identifier = house_manifest.product_source_identifier
    INNER JOIN (
        SELECT DISTINCT
            stores.location_source_identifier
        FROM "KSFPA"."MR2"."IPS_STOCK_AUDIT" ips
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON ips.location_generated_identifier = stores.location_generated_identifier
        WHERE
            ips.day_date BETWEEN '2022-02-01' AND '2022-02-28' AND
            ips.stock_movement_code IN ('04', '17')
    ) feb_stocktake_stores ON soh.location_source_identifier = feb_stocktake_stores.location_source_identifier
    INNER JOIN (
        SELECT
            stores.location_source_identifier,
            SUM(despatch_quantity) AS despatch_qty,
            RANK() OVER (ORDER BY despatch_qty DESC)
        FROM "KSFPA"."MR2"."WMS_MANIFEST" wms_manifest
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON wms_manifest.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON wms_manifest.product_generated_identifier = products.product_generated_identifier
        WHERE
            products.department_source_identifier NOT IN ('053', '097', '099') AND
            wms_manifest.day_date BETWEEN '2022-03-01' AND '2022-07-31' AND
            wms_manifest.wms_manifest_type_code = 'H' AND
            wms_manifest.despatch_quantity > 0
        GROUP BY stores.location_source_identifier
        QUALIFY
            RANK() OVER (ORDER BY despatch_qty DESC) <= 10
    ) top_house_manifest_stores ON soh.location_source_identifier = top_house_manifest_stores.location_source_identifier
    """
    cur1 = ctx.cursor().execute(query1)
    dat_raw = pd.DataFrame.from_records(iter(cur1), columns=[x[0] for x in cur1.description])
    return(dat_raw)
def fetch_prop_neg_soh_by_house_manifest_status():
    query1 = f"""
    SELECT
        soh.location_source_identifier,
        CASE
            WHEN house_manifest.house_manifest_freq > 0 THEN 1
            ELSE 0
        END AS had_at_least_one_house_manifest,
        SUM(soh.is_neg_soh) AS neg_soh_keycodes,
        COUNT(*) AS tot_keycodes,
        neg_soh_keycodes / tot_keycodes AS prop_neg_soh
    FROM (
        SELECT
            stores.location_source_identifier,
            products.product_source_identifier,
            CASE
                WHEN soh.daily_stock_on_hand_quantity < 0 THEN 1
                ELSE 0
            END AS is_neg_soh,
            COUNT(DISTINCT products.product_source_identifier) OVER (PARTITION BY stores.location_source_identifier) AS distinct_keycodes_in_store
        FROM "KSFPA"."MR2C"."DAILY_SOH" soh
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON soh.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON soh.product_generated_identifier = products.product_generated_identifier
        WHERE
            LEFT(stores.location_source_identifier, 1) IN ('1', '3', '8') AND
            products.department_source_identifier NOT IN ('053', '097', '099') AND
            soh.day_date = '2022-07-31' AND
            stores.dc_group_description NOT LIKE '%UNKNOWN%'
        QUALIFY
            distinct_keycodes_in_store >= 10000
    ) soh
    LEFT JOIN (
        SELECT
            stores.location_source_identifier,
            products.product_source_identifier,
            COUNT(*) AS house_manifest_freq
        FROM "KSFPA"."MR2"."WMS_MANIFEST" wms_manifest
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON wms_manifest.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON wms_manifest.product_generated_identifier = products.product_generated_identifier
        WHERE
            products.department_source_identifier NOT IN ('053', '097', '099') AND
            wms_manifest.day_date BETWEEN '2022-03-01' AND '2022-07-31' AND
            wms_manifest.wms_manifest_type_code = 'H' AND
            wms_manifest.despatch_quantity > 0
        GROUP BY stores.location_source_identifier, products.product_source_identifier
    ) house_manifest ON soh.location_source_identifier = house_manifest.location_source_identifier AND soh.product_source_identifier = house_manifest.product_source_identifier
    INNER JOIN (
        SELECT DISTINCT
            stores.location_source_identifier
        FROM "KSFPA"."MR2"."IPS_STOCK_AUDIT" ips
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON ips.location_generated_identifier = stores.location_generated_identifier
        WHERE
            ips.day_date BETWEEN '2022-02-01' AND '2022-02-28' AND
            ips.stock_movement_code IN ('04', '17')
    ) feb_stocktake_stores ON soh.location_source_identifier = feb_stocktake_stores.location_source_identifier
    GROUP BY soh.location_source_identifier, had_at_least_one_house_manifest
    """
    cur1 = ctx.cursor().execute(query1)
    dat_raw = pd.DataFrame.from_records(iter(cur1), columns=[x[0] for x in cur1.description])
    dat_clean = dat_raw.copy()
    dat_clean["PROP_NEG_SOH"] = dat_clean["PROP_NEG_SOH"].astype(float)
    return(dat_clean)
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
def EDA():
    dat = outlier_removal(
        1.5, 
        fetch_prop_neg_soh_by_house_manifest_status(), 
        ["PROP_NEG_SOH"]
    )
    p1 = sns.boxplot(
        x = "HAD_AT_LEAST_ONE_HOUSE_MANIFEST",
        y = "PROP_NEG_SOH", 
        data = dat
    )
    plt.xlabel("Did the Keycode Have at Least One House Manifest?")
    plt.ylabel("Avg. probability of Having Neg. SOH")
    plt.show()
def logistic_reg():
    dat = fetch_and_clean_house_manifest_binary_data_subset()
    log_reg = sm.logit(
        "IS_NEG_SOH ~ HAD_AT_LEAST_ONE_HOUSE_MANIFEST", 
        data = dat).fit()
    effect_size = np.round(np.exp(log_reg.params["HAD_AT_LEAST_ONE_HOUSE_MANIFEST"]) - 1, 4) * 100
    effect_direction = []
    if effect_size >= 0:
        effect_direction = "greater"
    else:
        effect_direction = "less"
    return(
        print(
            log_reg.summary(),
            "On average, the odds of a keycode having neg. SOH (at pre-Aug. stocktake) is {}% {} if it was house manifested at least once (from post-Feb stocktake).".format(np.abs(effect_size), effect_direction)
        )
    )
def analysis_pipeline():
    EDA()
    logistic_reg()
analysis_pipeline()