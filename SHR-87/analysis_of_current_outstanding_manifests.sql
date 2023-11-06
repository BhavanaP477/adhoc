SELECT
    outstanding_manifest.location_source_identifier,
    soh.date,
    outstanding_manifest.outstanding_manifest_units_current,
    outstanding_manifest.outstanding_manifest_units_10_days,
    outstanding_manifest.outstanding_manifest_units_30_days,
    soh.neg_soh_qty,
    soh.neg_soh_cost
FROM (
    SELECT
        stores1.location_source_identifier,
        SUM(
            CASE
                WHEN
                    transfer_despatch.despatch_date <= CURRENT_DATE() - 1 AND
                    (transfer_receipt.receipt_date > CURRENT_DATE() - 1OR transfer_receipt.receipt_date IS NULL) THEN ABS(transfer_product.despatch_quantity)
                ELSE 0
            END) AS outstanding_manifest_units_current,
        SUM(
            CASE
                WHEN
                    DATEDIFF(DAY, transfer_despatch.despatch_date, CURRENT_DATE() - 1) > 10 AND
                    (transfer_receipt.receipt_date > CURRENT_DATE() - 1 OR transfer_receipt.receipt_date IS NULL) THEN ABS(transfer_product.despatch_quantity)
                ELSE 0
            END) AS outstanding_manifest_units_10_days,
        SUM(
            CASE
                WHEN
                    DATEDIFF(DAY, transfer_despatch.despatch_date, CURRENT_DATE() - 1) > 30 AND
                    (transfer_receipt.receipt_date >= CURRENT_DATE() - 1 OR transfer_receipt.receipt_date IS NULL) THEN ABS(transfer_product.despatch_quantity)
                ELSE 0
            END) AS outstanding_manifest_units_30_days
    FROM "KSFPA"."MR2"."TRANSFER_PRODUCT" transfer_product
    INNER JOIN "KSFPA"."MR2"."TRANSFER_DESPATCH" transfer_despatch ON (
        transfer_product.transfer_generated_identifier = transfer_despatch.transfer_generated_identifier AND
        transfer_product.expiry_date = transfer_despatch.expiry_date)
    LEFT JOIN "KSFPA"."MR2"."TRANSFER_RECEIPT" transfer_receipt ON (
        transfer_product.transfer_generated_identifier = transfer_receipt.transfer_generated_identifier AND
        transfer_product.expiry_date = transfer_receipt.expiry_date)
    INNER JOIN "KSFPA"."MR2C"."LOCATION" stores1 ON transfer_receipt.location_generated_identifier = stores1.location_generated_identifier
    INNER JOIN "KSFPA"."MR2C"."LOCATION" stores2 ON transfer_despatch.location_generated_identifier = stores2.location_generated_identifier
    INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON transfer_product.product_generated_identifier = products.product_generated_identifier
    INNER JOIN "KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD"."COMMON_DIMENSIONS"."DIM_DATE" dates ON transfer_despatch.despatch_date = dates.date
    WHERE
        transfer_despatch.expiry_date = '9999-12-31' AND
        transfer_despatch.transfer_type_code IN ('DD', 'XD') AND
        products.department_source_identifier NOT IN ('053', '097', '099')
    GROUP BY stores1.location_source_identifier
) outstanding_manifest
INNER JOIN (
    SELECT
        stores.location_source_identifier,
        soh.day_date AS date,
        SUM(soh.daily_stock_on_hand_quantity) AS neg_soh_qty,
        SUM(soh.daily_stock_on_hand_quantity * awc.awc) AS neg_soh_cost
    FROM "KSFPA"."MR2C"."DAILY_SOH" soh
    INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON soh.location_generated_identifier = stores.location_generated_identifier
    INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON soh.product_generated_identifier = products.product_generated_identifier
    INNER JOIN (
        SELECT
            location_generated_identifier,
            product_generated_identifier,
            location_awc_amount AS awc
        FROM "KSFPA"."MR2"."SS_AVERAGE_WEIGHTED_COST"
        WHERE
            CURRENT_DATE() < expiry_date AND
            CURRENT_DATE >= effective_date
    ) awc ON soh.location_generated_identifier = awc.location_generated_identifier AND soh.product_generated_identifier = awc.product_generated_identifier
    WHERE
        LEFT(stores.location_source_identifier, 1) = 1 AND
        stores.location_source_identifier NOT IN (
            '1095',
            '1194',
            '1294',
            '1295',
            '1296',
            '1297'
        ) AND
        soh.day_date = CURRENT_DATE() - 1 AND
        soh.daily_stock_on_hand_quantity < 0 AND
        products.department_source_identifier NOT IN ('053', '097', '099')
    GROUP BY stores.location_source_identifier, soh.day_date
) soh ON outstanding_manifest.location_source_identifier = soh.location_source_identifier;
