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
warehouse='KSF_KHUB_WH',
database='KSFPA',
schema='RFID',
user = 'Cal.Roff@kmart.com.au'
)
# Import and clean data:
def fetch_and_clean_house_manifest_data():
    query1 = f"""
    SELECT
        soh.location_source_identifier,
        soh.region_description,
        soh.dc_group_description,
        soh.ty_tot_store_keycodes_pre_aug_stktake,
        soh.ly_tot_Store_keycodes_pre_aug_stktake,
        soh.ty_neg_soh_keycodes_pre_aug_stktake,
        soh.ly_neg_soh_keycodes_pre_aug_stktake,
        soh.ty_neg_soh_units_pre_aug_stktake,
        soh.ly_neg_soh_units_pre_aug_stktake,
        house_manifest.ty_house_manifested_keycodes,
        house_manifest.ly_house_manifested_keycodes,
        house_manifest.ty_house_manifested_units,
        house_manifest.ly_house_manifested_units
    FROM (
        SELECT
            stores.location_source_identifier,
            stores.region_description,
            stores.dc_group_description,
            SUM(
                CASE
                    WHEN soh.day_date = '2022-07-31' THEN 1
                    ELSE 0
                END) AS ty_tot_store_keycodes_pre_aug_stktake,
            SUM(
                CASE
                    WHEN soh.day_date = '2021-07-31' THEN 1
                    ELSE 0
                END) AS ly_tot_store_keycodes_pre_aug_stktake,
            SUM(
                CASE
                    WHEN soh.daily_stock_on_hand_quantity < 0 AND soh.day_date = '2022-07-31' THEN 1
                    ELSE 0
                END) AS ty_neg_soh_keycodes_pre_aug_stktake,
            SUM(
                CASE
                    WHEN soh.daily_stock_on_hand_quantity < 0 AND soh.day_date = '2021-07-31' THEN 1
                    ELSE 0
                END) AS ly_neg_soh_keycodes_pre_aug_stktake,
            SUM(
                CASE
                    WHEN soh.daily_stock_on_hand_quantity < 0 AND soh.day_date = '2022-07-31' THEN ABS(soh.daily_stock_on_hand_quantity)
                    ELSE 0
                END) AS ty_neg_soh_units_pre_aug_stktake,
            SUM(
                CASE
                    WHEN soh.daily_stock_on_hand_quantity < 0 AND soh.day_date = '2021-07-31' THEN ABS(soh.daily_stock_on_hand_quantity)
                    ELSE 0
                END) AS ly_neg_soh_units_pre_aug_stktake
        FROM "KSFPA"."MR2C"."DAILY_SOH" soh
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON soh.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON soh.product_generated_identifier = products.product_generated_identifier
        WHERE
            LEFT(stores.location_source_identifier, 1) IN ('1', '3', '8') AND
            products.department_source_identifier NOT IN ('053', '097', '099') AND
            soh.day_date IN ('2022-07-31', '2021-07-31') AND
            stores.dc_group_description NOT LIKE '%UNKNOWN%'
        GROUP BY stores.location_source_identifier, stores.region_description, stores.dc_group_description
        HAVING
            ty_tot_store_keycodes_pre_aug_stktake > 10000 AND
            ly_tot_store_keycodes_pre_aug_stktake > 10000 AND
            ABS(ty_tot_store_keycodes_pre_aug_stktake - ly_tot_store_keycodes_pre_aug_stktake) / ly_tot_store_keycodes_pre_aug_stktake < 0.3
    ) soh
    INNER JOIN (
        SELECT
            stores.location_source_identifier,
            SUM(
                CASE
                    WHEN wms_manifest.day_date BETWEEN '2021-03-01' AND '2021-07-31' THEN wms_manifest.despatch_quantity
                    ELSE 0
                END) AS ly_house_manifested_units,
            SUM(
                CASE
                    WHEN wms_manifest.day_date BETWEEN '2022-03-01' AND '2022-07-31' THEN wms_manifest.despatch_quantity
                    ELSE 0
                END) AS ty_house_manifested_units,
            SUM(
                CASE
                    WHEN wms_manifest.day_date BETWEEN '2021-03-01' AND '2021-07-31' THEN 1
                    ELSE 0
                END) AS ly_house_manifested_keycodes,
            SUM(
                CASE
                    WHEN wms_manifest.day_date BETWEEN '2022-03-01' AND '2022-07-31' THEN 1
                    ELSE 0
                END) AS ty_house_manifested_keycodes
        FROM "KSFPA"."MR2"."WMS_MANIFEST" wms_manifest
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON wms_manifest.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON wms_manifest.product_generated_identifier = products.product_generated_identifier
        WHERE
            products.department_source_identifier NOT IN ('053', '097', '099') AND
            wms_manifest.day_date BETWEEN '2021-03-01' AND '2022-07-31' AND
            wms_manifest.wms_manifest_type_code = 'H'
        GROUP BY stores.location_source_identifier
    ) house_manifest ON soh.location_source_identifier = house_manifest.location_source_identifier  
    """
    cur1 = ctx.cursor().execute(query1)
    dat_raw = pd.DataFrame.from_records(iter(cur1), columns=[x[0] for x in cur1.description])
    dat_clean = dat_raw.copy()
    dat_clean = dat_clean[~(dat_clean == 0).any(axis = 1)]
    return(dat_clean)
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
def diff_dat():
    dat = fetch_and_clean_house_manifest_data()
    new_dat_dict = {
        "LOCATION_SOURCE_IDENTIFIER": dat["LOCATION_SOURCE_IDENTIFIER"],
        "REGION_DESCRIPTION": dat["REGION_DESCRIPTION"],
        "DC_GROUP_DESCRIPTION": dat["DC_GROUP_DESCRIPTION"],
        "TY_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE": dat["TY_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE"],
        "LY_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE": dat["LY_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE"],
        "DIFF_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE": dat["TY_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"] - dat["LY_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"],
        "DIFF_HOUSE_MANIFESTED_KEYCODES_SINCE_FEB_STKTAKE": dat["TY_HOUSE_MANIFESTED_KEYCODES"] - dat["LY_HOUSE_MANIFESTED_KEYCODES"]
    }
    dat_final = pd.DataFrame(new_dat_dict)
    dat_final = outlier_removal(
        2.5,
        dat_final,
        [
            "TY_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE", 
            "LY_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE",
            "DIFF_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE",
            "DIFF_HOUSE_MANIFESTED_KEYCODES_SINCE_FEB_STKTAKE"
        ]
    )
    return(dat_final)
# Alternative form of variables: % YoY Change (as opposed to YoY differences)
def prop_change_dat():
    dat = fetch_and_clean_house_manifest_data()
    new_dat_dict = {
        "LOCATION_SOURCE_IDENTIFIER": dat["LOCATION_SOURCE_IDENTIFIER"],
        "REGION_DESCRIPTION": dat["REGION_DESCRIPTION"],
        "DC_GROUP_DESCRIPTION": dat["DC_GROUP_DESCRIPTION"],
        "TY_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE": dat["TY_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE"],
        "LY_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE": dat["LY_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE"],
        "PROP_CHANGE_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE": (dat["TY_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"] - dat["LY_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"]) / dat["LY_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"],
        "PROP_CHANGE_HOUSE_MANIFESTED_KEYCODES_SINCE_FEB_STKTAKE": (dat["TY_HOUSE_MANIFESTED_KEYCODES"] - dat["LY_HOUSE_MANIFESTED_KEYCODES"]) / dat["LY_HOUSE_MANIFESTED_KEYCODES"]
    }
    dat_final = pd.DataFrame(new_dat_dict)
    dat_final = outlier_removal(
        2.5,
        dat_final,
        [
            "TY_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE", 
            "LY_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE",
            "PROP_CHANGE_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE",
            "PROP_CHANGE_HOUSE_MANIFESTED_KEYCODES_SINCE_FEB_STKTAKE"
        ]
    )
    return(dat_final)
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
    dat = clusters(dat = dat, dat_cols = ["TY_TOT_STORE_KEYCODES_PRE_AUG_STKTAKE"])
    p1 = sns.lmplot(
        x = x_col,
        y = y_col,
        hue = hue_col,
        data = dat,
        ci = None
    )
    type = []
    if dat.iloc[:, :-1].equals(diff_dat()):
        type = "Difference"
    elif dat.iloc[:, :-1].equals(prop_change_dat()):
        type = "% Change"
    else:
        raise ValueError("Data input is invalid")
    plt.xlabel("TY-LY {} in House Manifested Keycodes (From Post-Feb. Stocktake)".format(type))
    plt.ylabel("TY-LY {} in Neg. SOH Keycodes (Pre-Aug. Stocktake)".format(type))
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
# Analytical pipeline
def analysis_pipeline(metric = "Difference"):
    if metric == "Difference":
        EDA(
            diff_dat(), 
            "DIFF_HOUSE_MANIFESTED_KEYCODES_SINCE_FEB_STKTAKE", 
            "DIFF_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"
        )
        linear_reg(
            diff_dat(),
            x_cols = [
                "DIFF_HOUSE_MANIFESTED_KEYCODES_SINCE_FEB_STKTAKE"
            ],
            y_col = "DIFF_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"
        )
    elif metric == "% Change":
        EDA(
            prop_change_dat(), 
            "PROP_CHANGE_HOUSE_MANIFESTED_KEYCODES_SINCE_FEB_STKTAKE", 
            "PROP_CHANGE_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"
        )
        linear_reg(
            prop_change_dat(),
            x_cols = [
                "PROP_CHANGE_HOUSE_MANIFESTED_KEYCODES_SINCE_FEB_STKTAKE"
            ], 
            y_col = "PROP_CHANGE_NEG_SOH_KEYCODES_PRE_AUG_STKTAKE"
        )
    else:
        return ValueError("Only metrics 'Difference' and '% Change' allowed")
analysis_pipeline(metric = "Difference")