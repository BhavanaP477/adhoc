/* Here, we will get the store-keycode combinations of house manifested (at least once) vs. non-house manifested keycodes, and model against the pre-Aug. stocktake neg. SOH status */
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
        wms_manifest.despatch_quantity >= 0
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