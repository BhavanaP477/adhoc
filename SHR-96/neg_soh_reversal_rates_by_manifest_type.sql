WITH agg AS (
    WITH neg_soh_manifest AS (
        SELECT
            soh_before.location_source_identifier,
            soh_before.product_source_identifier,
            manifest.manifest_type,
            manifest.manifest_no,
            manifest.receipted_date AS manifest_receipt_date,
            soh_before.daily_stock_on_hand_quantity AS soh_day_before_manifest,
            manifest.total_qty AS manifested_qty,
            soh_after.daily_stock_on_hand_quantity AS soh_day_after_manifest,
            CASE
                WHEN soh_day_after_manifest >= 0 THEN 'NO'
                ELSE 'YES'
            END AS is_soh_still_neg,
            CASE
                WHEN soh_day_after_manifest >= 0 AND soh_day_before_manifest + manifested_qty >= 0 THEN 'YES'
                ELSE 'NO'
            END AS did_manifest_alone_reverse_neg_soh
        FROM (
            SELECT
                manifest.str_id,
                manifest.dc_number,
                CAST(receipt.receipt_date AS DATE) AS receipted_date,
                receipt_date AS receipted_datetime,
                manifest.key_code,
                manifest.manifest_type,
                manifest.manifest_no,
                manifest.total_qty
            FROM "KSFPA"."WMS"."STORE_MANIFESTS_RAW" manifest
            INNER JOIN "KSFPA"."WMS"."STORE_MANIFEST_RECEIPT_RAW" receipt ON manifest.str_id = receipt.str_id AND manifest.manifest_no = receipt.manifest_no
            WHERE
                receipted_date BETWEEN '2022-03-01' AND '2022-07-31'
        ) manifest
        INNER JOIN (
            SELECT
                stores.location_source_identifier,
                soh.day_date,
                products.product_source_identifier,
                soh.daily_stock_on_hand_quantity
            FROM "KSFPA"."MR2C"."DAILY_SOH" soh
            INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON soh.location_generated_identifier = stores.location_generated_identifier
            INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON soh.product_generated_identifier = products.product_generated_identifier
            WHERE
                soh.day_date BETWEEN '2022-03-01' AND '2022-07-31' AND
                soh.daily_stock_on_hand_quantity < 0
        ) soh_before ON CAST(manifest.str_id AS VARCHAR(100)) = soh_before.location_source_identifier AND manifest.receipted_date = DATEADD(DAY, 1, soh_before.day_date) AND manifest.key_code = soh_before.product_source_identifier
        INNER JOIN (
            SELECT
                stores.location_source_identifier,
                soh.day_date,
                products.product_source_identifier,
                soh.daily_stock_on_hand_quantity
            FROM "KSFPA"."MR2C"."DAILY_SOH" soh
            INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON soh.location_generated_identifier = stores.location_generated_identifier
            INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON soh.product_generated_identifier = products.product_generated_identifier
            WHERE
                soh.day_date BETWEEN '2022-03-01' AND '2022-07-31'
        ) soh_after ON CAST(manifest.str_id AS VARCHAR(100)) = soh_after.location_source_identifier AND manifest.receipted_date = DATEADD(DAY, -1, soh_after.day_date) AND manifest.key_code = soh_after.product_source_identifier
    )
    SELECT
        neg_soh_manifest.location_source_identifier,
        neg_soh_manifest.product_source_identifier,
        neg_soh_manifest.manifest_receipt_date,
        neg_soh_manifest.manifest_type,
        AVG(neg_soh_manifest.soh_day_before_manifest) AS soh_day_before_manifest,
        SUM(neg_soh_manifest.manifested_qty) AS manifested_qty,
        CASE
            WHEN SUM(neg_soh_manifest.manifested_qty) + AVG(neg_soh_manifest.soh_day_before_manifest) >= 0 THEN 'YES'
            ELSE 'NO'
        END AS is_neg_soh_reversed
    FROM neg_soh_manifest
    GROUP BY neg_soh_manifest.location_source_identifier, neg_soh_manifest.manifest_receipt_date, neg_soh_manifest.product_source_identifier, neg_soh_manifest.manifest_type
)
SELECT
    agg.manifest_type,
    COUNT(*) AS total_cases_of_manifest_to_neg_soh_products,
    SUM(
        CASE
            WHEN is_neg_soh_reversed = 'YES' THEN 1
            ELSE 0
        END) AS total_cases_of_neg_soh_reversal,
    total_cases_of_neg_soh_reversal / total_cases_of_manifest_to_neg_soh_products AS prop_of_neg_soh_instances_reversed,
    SUM(agg.manifested_qty * IFNULL(awc.location_awc_amount, 0)) AS total_cost_of_manifest_products,
    SUM(
        CASE
            WHEN is_neg_soh_reversed = 'YES' THEN ABS(agg.soh_day_before_manifest) * IFNULL(awc.location_awc_amount, 0)
            ELSE ABS(agg.soh_day_before_manifest + agg.manifested_qty) * IFNULL(awc.location_awc_amount, 0)
        END) AS total_neg_soh_cost_cleared_due_to_manifest
FROM agg
LEFT JOIN (
    SELECT 
        stores.location_source_identifier,
        products.product_source_identifier,
        awc.expiry_date,
        awc.location_awc_amount
    FROM "KSFPA"."MR2"."SS_AVERAGE_WEIGHTED_COST" awc
    INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON awc.location_generated_identifier = stores.location_generated_identifier
    INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON awc.product_generated_identifier = products.product_generated_identifier
    WHERE
        awc.expiry_date >= CURRENT_DATE() AND
        awc.effective_date <= CURRENT_DATE()
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY stores.location_source_identifier, products.product_source_identifier ORDER BY awc.effective_date DESC) = 1
) awc ON agg.location_source_identifier = awc.location_source_identifier AND agg.product_source_identifier = awc.product_source_identifier
GROUP BY agg.manifest_type