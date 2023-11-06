/* Does an increase in house manifests drive an increase in neg. SOH? By DC-department */
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