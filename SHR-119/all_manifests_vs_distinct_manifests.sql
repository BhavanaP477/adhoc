SELECT
    ips.location_source_identifier,
    manifest.date_receipted,
    ips.product_source_identifier,
    ips.ips_1manifest_qty,
    manifest.wms_manifest_qty,
    distinct_manifest.distinct_wms_manifest_qty
FROM (
    SELECT
        receipt.str_id,
        CAST(receipt.receipt_date AS DATE) AS date_receipted,
        products.product_keycode,
        SUM(dispatch.total_qty) AS wms_manifest_qty
    FROM "KSFPA"."WMS"."STORE_MANIFEST_RECEIPT_RAW" receipt
    INNER JOIN "KSFPA"."WMS"."STORE_MANIFESTS_RAW" dispatch ON receipt.str_id = dispatch.str_id AND receipt.manifest_no = dispatch.manifest_no
    INNER JOIN "KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD"."COMMON_DIMENSIONS"."DIM_PRODUCT" products ON CAST(dispatch.key_code AS VARCHAR(100)) = products.product_keycode
    WHERE
        date_receipted BETWEEN '2022-03-01' AND '2022-07-31' AND
        products.department_code NOT IN ('053', '97', '099')
    GROUP BY receipt.str_id, date_receipted, products.product_keycode
) manifest
INNER JOIN (
    SELECT
        str_id,
        date_receipted,
        product_keycode,
        SUM(total_qty) AS distinct_wms_manifest_qty
    FROM (
        SELECT DISTINCT
            receipt.str_id,
            dispatch.manifest_no,
            CAST(receipt.receipt_date AS DATE) AS date_receipted,
            products.product_keycode,
            total_qty
        FROM "KSFPA"."WMS"."STORE_MANIFEST_RECEIPT_RAW" receipt
        INNER JOIN "KSFPA"."WMS"."STORE_MANIFESTS_RAW" dispatch ON receipt.str_id = dispatch.str_id AND receipt.manifest_no = dispatch.manifest_no
        INNER JOIN "KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD"."COMMON_DIMENSIONS"."DIM_PRODUCT" products ON CAST(dispatch.key_code AS VARCHAR(100)) = products.product_keycode
        WHERE
            date_receipted BETWEEN '2022-03-01' AND '2022-07-31' AND
            products.department_code NOT IN ('053', '97', '099')
    )
    GROUP BY str_id, date_receipted, product_keycode
) distinct_manifest ON manifest.str_id = distinct_manifest.str_id AND manifest.date_receipted = distinct_manifest.date_receipted AND manifest.product_keycode = distinct_manifest.product_keycode
INNER JOIN (
    SELECT
        stores.location_source_identifier,
        ips.day_date,
        products.product_source_identifier,
        SUM(ips.stock_movement_quantity) AS ips_1manifest_qty
    FROM "KSFPA"."MR2"."IPS_STOCK_AUDIT" ips
    INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON ips.location_generated_identifier = stores.location_generated_identifier
    INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON ips.product_generated_identifier = products.product_generated_identifier
    WHERE
        ips.day_date BETWEEN '2022-03-01' AND '2022-07-31' AND
        products.department_source_identifier NOT IN ('053', '097', '099') AND
        ips.stock_movement_source = '1Manifest'
    GROUP BY stores.location_source_identifier, ips.day_date, products.product_source_identifier
) ips ON CAST(manifest.str_id AS VARCHAR(100)) = ips.location_source_identifier AND manifest.date_receipted = ips.day_date AND manifest.product_keycode = ips.product_source_identifier
WHERE
    ips.location_source_identifier = '1053' AND
    ips.product_source_identifier = '71184937'
ORDER BY ips.location_source_identifier, manifest.date_receipted, ips.product_source_identifier