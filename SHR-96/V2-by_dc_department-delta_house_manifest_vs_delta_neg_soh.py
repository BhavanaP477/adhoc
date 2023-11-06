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
warehouse='KSF_DATA_SCIENTIST_WH',
database='KSFPA',
schema='RFID',
user = 'Cal.Roff@kmart.com.au'
)
# Import and clean data:
def fetch_and_clean_house_manifest_dataV2():
    query1 = f"""
    SELECT
        soh.dc_group_description,
        soh.department_description,
        SUM(soh.ty_neg_soh_keycodes_pre_aug_stktake) AS ty_neg_soh_keycodes,
        SUM(soh.ly_neg_soh_keycodes_pre_aug_stktake) AS ly_neg_soh_keycodes,
        SUM(soh.ty_neg_soh_units_pre_aug_stktake) AS ty_neg_soh_units,
        SUM(soh.ly_neg_soh_units_pre_aug_stktake) AS ly_neg_soh_units,
        SUM(house_manifest.ty_house_manifested_keycodes) AS ty_house_manifested_keycodes,
        SUM(house_manifest.ly_house_manifested_keycodes) AS ly_house_manifested_keycodes,
        SUM(house_manifest.ty_house_manifested_units) AS ty_house_manifested_units,
        SUM(house_manifest.ly_house_manifested_units) AS ly_house_manifested_units
    FROM (
        SELECT
            stores.dc_group_description,
            stores.location_source_identifier,
            products.department_description,
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
            stores.location_source_identifier IN (
                SELECT DISTINCT
                    location_source_identifier
                FROM (
                    SELECT
                        stores.location_source_identifier,
                        SUM(
                            CASE
                                WHEN soh.day_date = '2022-07-31' THEN 1
                                ELSE 0
                            END) AS ty_store_keycodes,
                        SUM(
                            CASE
                                WHEN soh.day_date = '2021-07-31' THEN 1
                                ELSE 0
                            END) AS ly_store_keycodes
                    FROM "KSFPA"."MR2C"."DAILY_SOH" soh
                    INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON soh.location_generated_identifier = stores.location_generated_identifier
                    INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON soh.product_generated_identifier = products.product_generated_identifier
                    WHERE
                        LEFT(stores.location_source_identifier, 1) IN ('1', '3', '8') AND
                        soh.day_date IN ('2022-07-31', '2021-07-31') AND
                        products.department_source_identifier NOT IN ('053', '097', '099')
                    GROUP BY stores.location_source_identifier
                    HAVING
                        ty_store_keycodes >= 10000 AND
                        ly_store_keycodes >= 10000 AND
                        ABS(ty_store_keycodes - ly_store_keycodes) / ly_store_keycodes < 0.3
                )
            ) AND
            products.department_source_identifier NOT IN ('053', '097', '099') AND
            soh.day_date IN ('2022-07-31', '2021-07-31') AND
            stores.dc_group_description NOT LIKE '%UNKNOWN%'
        GROUP BY stores.location_source_identifier, stores.dc_group_description, products.department_description
        HAVING
            ty_tot_store_keycodes_pre_aug_stktake > 0 AND
            ly_tot_store_keycodes_pre_aug_stktake > 0 AND
            ABS(ty_tot_store_keycodes_pre_aug_stktake - ly_tot_store_keycodes_pre_aug_stktake) / ly_tot_store_keycodes_pre_aug_stktake < 0.3
    ) soh
    INNER JOIN (
        SELECT
            stores.dc_group_description,
            stores.location_source_identifier,
            products.department_description,
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
        GROUP BY stores.dc_group_description, stores.location_source_identifier, products.department_description
    ) house_manifest ON soh.dc_group_description = house_manifest.dc_group_description AND soh.location_source_identifier = house_manifest.location_source_identifier AND soh.department_description = house_manifest.department_description
    GROUP BY soh.dc_group_description, soh.department_description
    HAVING
        ty_neg_soh_keycodes >= 100 AND
        ly_neg_soh_keycodes >= 100
    """
    cur1 = ctx.cursor().execute(query1)
    dat_raw = pd.DataFrame.from_records(iter(cur1), columns=[x[0] for x in cur1.description])
    dat_clean = dat_raw.copy()
    dat_clean = dat_clean.dropna(axis = 0)
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

def diff_dat(outlier_removal_degree = None, remove_nz = False):
    if remove_nz == True:
        dat = fetch_and_clean_house_manifest_dataV2()
        dat["DC_SHORT"] = [item[0:6] for item in dat["DC_GROUP_DESCRIPTION"]]
        dat = dat[dat["DC_SHORT"] != "DC NZL"]
    else:
        dat = fetch_and_clean_house_manifest_dataV2()
    new_dat_dict = {
        "DC_GROUP_DESCRIPTION": dat["DC_GROUP_DESCRIPTION"],
        "DEPARTMENT_DESCRIPTION": dat["DEPARTMENT_DESCRIPTION"],
        "DIFF_NEG_SOH_KEYCODES": dat["TY_NEG_SOH_KEYCODES"] - dat["LY_NEG_SOH_KEYCODES"],
        "DIFF_NEG_SOH_UNITS": dat["TY_NEG_SOH_UNITS"] - dat["LY_NEG_SOH_UNITS"],
        "DIFF_HOUSE_MANIFESTED_KEYCODES": dat["TY_HOUSE_MANIFESTED_KEYCODES"] - dat["LY_HOUSE_MANIFESTED_KEYCODES"],
        "DIFF_HOUSE_MANIFESTED_UNITS": dat["TY_HOUSE_MANIFESTED_UNITS"] - dat["LY_HOUSE_MANIFESTED_UNITS"]
    }
    dat_final = pd.DataFrame(new_dat_dict)
    if outlier_removal_degree != None:
        dat_final_no_outlier = outlier_removal(
            outlier_removal_degree,
            dat_final,
            [
                "DIFF_NEG_SOH_KEYCODES",
                "DIFF_HOUSE_MANIFESTED_KEYCODES"
            ]
        )
        return(dat_final_no_outlier)
    else:
        return(dat_final)

def prop_change_dat(outlier_removal_degree = None, remove_nz = False):
    if remove_nz == True:
        dat = fetch_and_clean_house_manifest_dataV2()
        dat["DC_SHORT"] = [item[0:6] for item in dat["DC_GROUP_DESCRIPTION"]]
        dat = dat[dat["DC_SHORT"] != "DC NZL"]
    else:
        dat = fetch_and_clean_house_manifest_dataV2()
    new_dat_dict = {
        "DC_GROUP_DESCRIPTION": dat["DC_GROUP_DESCRIPTION"],
        "DEPARTMENT_DESCRIPTION": dat["DEPARTMENT_DESCRIPTION"],
        "PROP_CHANGE_NEG_SOH_KEYCODES": (dat["TY_NEG_SOH_KEYCODES"] - dat["LY_NEG_SOH_KEYCODES"]) / dat["LY_NEG_SOH_KEYCODES"],
        "PROP_CHANGE_NEG_SOH_UNITS": (dat["TY_NEG_SOH_UNITS"] - dat["LY_NEG_SOH_UNITS"]) / dat["LY_NEG_SOH_UNITS"],
        "PROP_CHANGE_HOUSE_MANIFESTED_KEYCODES": (dat["TY_HOUSE_MANIFESTED_KEYCODES"] - dat["LY_HOUSE_MANIFESTED_KEYCODES"]) / dat["LY_HOUSE_MANIFESTED_KEYCODES"],
        "PROP_CHANGE_HOUSE_MANIFESTED_UNITS": (dat["TY_HOUSE_MANIFESTED_UNITS"] - dat["LY_HOUSE_MANIFESTED_UNITS"]) / dat["LY_HOUSE_MANIFESTED_UNITS"]
    }
    dat_final = pd.DataFrame(new_dat_dict)
    if outlier_removal_degree != None:
        dat_final_no_outlier = outlier_removal(
            outlier_removal_degree,
            dat_final,
            [
                "PROP_CHANGE_NEG_SOH_KEYCODES",
                "PROP_CHANGE_HOUSE_MANIFESTED_KEYCODES"
            ]
        )
        return(dat_final_no_outlier)
    else:
        return(dat_final)
        
def EDA(dat_format, x_col, y_col, outlier_removal_deg = None, hue_col = None, remove_nz = False):
    if dat_format == "Difference":
        dat = diff_dat(outlier_removal_degree = outlier_removal_deg, remove_nz = remove_nz)
    elif dat_format == "% Change":
        dat = prop_change_dat(outlier_removal_degree = outlier_removal_deg, remove_nz = remove_nz)
    else:
        raise ValueError("Only 'Difference' and '%Change' values are allowed for dat_format input")
    p1 = sns.lmplot(
        x = x_col,
        y = y_col,
        hue = hue_col,
        data = dat,
        ci = None
    )
    plt.xlabel("TY-LY {} in House Manifested Products".format(dat_format))
    plt.ylabel("TY-LY {} in Neg. SOH Cases".format(dat_format))
    plt.show()

def linear_reg(dat_format, x_cols, y_col, outlier_removal_deg = None, remove_nz = False):
    equation = y_col + " ~ " + "".join([x_cols[i] + " * " for i in range(len(x_cols))])
    equation = equation[:-3]
    if dat_format == "Difference":
        lin_mod = sm.ols(
            formula = equation, 
            data = diff_dat(outlier_removal_degree = outlier_removal_deg, remove_nz = remove_nz)
        ).fit()
        return(print(lin_mod.summary()))
    elif dat_format == "% Change":
        lin_mod = sm.ols(
            formula = equation, 
            data = diff_dat(outlier_removal_degree = outlier_removal_deg, remove_nz = remove_nz)
        ).fit()
        return(print(lin_mod.summary()))
    else:
        raise ValueError("Only 'Difference' and '%Change' values are allowed for dat_format input")

def analysis_pipeline(dat_format, outlier_removal_deg = None, remove_nz = False):
    if dat_format == "Difference":
        EDA(
            dat_format = "Difference", 
            x_col = "DIFF_HOUSE_MANIFESTED_KEYCODES", 
            y_col = "DIFF_NEG_SOH_KEYCODES",
            outlier_removal_deg = outlier_removal_deg,
            hue_col = "DC_GROUP_DESCRIPTION",
            remove_nz = remove_nz
        )
        linear_reg(
            dat_format = "Difference",
            x_cols = ["DC_GROUP_DESCRIPTION", "DIFF_HOUSE_MANIFESTED_KEYCODES"], 
            y_col = "DIFF_NEG_SOH_KEYCODES",
            outlier_removal_deg = outlier_removal_deg,
            remove_nz = remove_nz
        )
    elif dat_format == "% Change":
        EDA(
            dat_format = "% Change", 
            x_col = "PROP_CHANGE_HOUSE_MANIFESTED_KEYCODES", 
            y_col = "PROP_CHANGE_NEG_SOH_KEYCODES",
            outlier_removal_deg = outlier_removal_deg,
            hue_col = "DC_GROUP_DESCRIPTION",
            remove_nz = remove_nz
        )
        linear_reg(
            dat_format = "% Change",
            x_cols = ["DC_GROUP_DESCRIPTION", "PROP_CHANGE_HOUSE_MANIFESTED_KEYCODES"], 
            y_col = "PROP_CHANGE_NEG_SOH_KEYCODES",
            outlier_removal_deg = outlier_removal_deg,
            remove_nz = remove_nz
        )
    else:
        raise ValueError("Only 'Difference' and '%Change' values are allowed for dat_format input")

analysis_pipeline("Difference", outlier_removal_deg = 3, remove_nz = False)