/* Focus attention on the problematic areas in phase 1 of the analysis at keycode level, for soh today's updated SOH */
WITH X AS (
    SELECT
        stores.region_description,
        soh.str_id AS location_code,
        products.rbu_description,
        products.department_description,
        soh.m_kd AS keycode,
        soh.soh AS current_soh
    FROM "KSFPA"."SOH"."SOH_REALTIME" soh
    INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON CAST(soh.str_id AS VARCHAR(100)) = stores.location_source_identifier
    INNER JOIN "KSFPA"."MR2C"."DAY" dates ON CAST(soh.update_date AS DATE) = dates.day_date
    INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON CAST(soh.m_kd AS VARCHAR(100)) = products.product_source_identifier
    WHERE
        LEFT(CAST(soh.str_id AS VARCHAR(100)), 1) = '1' AND
        dates.day_date = CURRENT_DATE() AND
        products.rbu_description NOT IN ('NON KMART MERCH', 'undefined') AND
        products.department_description NOT LIKE '%PHOTO%'
    ORDER BY location_code, products.rbu_description, products.department_description
)
SELECT
    X.region_description,
    X.rbu_description,
    SUM(CASE WHEN X.current_soh < 0 THEN 1 ELSE 0 END) AS neg_soh_keycodes,
    COUNT(*) AS tot_keycodes,
    SUM(CASE WHEN X.current_soh < 0 THEN 1 ELSE 0 END) / COUNT(*) AS prop_neg_soh
FROM X
GROUP BY X.region_description, X.rbu_description
ORDER BY prop_neg_soh DESC
LIMIT 5



/* Take the top 5 region-rbu combinations from above and display each keycode, and whether they have negative soh */
/* Then join with thenumber of manifests and code 20 adjustments since the last February stocktake */
SELECT
    stores.location_source_identifier,
    stores.region_description,
    products.rbu_description,
    soh.m_kd AS keycode,
    soh.soh AS current_soh,
    CASE
        WHEN current_soh < 0 THEN 1
        ELSE 0
    END AS is_neg_soh,
    COALESCE(ips_move.manifested_units, 0) AS manifested_units,
    COALESCE(ips_move.manifest_freq, 0) AS manifest_freq,
    COALESCE(ips_move.shrinkage_units, 0) AS shrink_units,
    CASE 
        WHEN shrink_units != 0 THEN 1
        ELSE 0
    END AS had_shrink,
    COALESCE(code_20.code_20_net_adj_qty, 0) AS code_20_net_adj_qty,
    COALESCE(code_20.code_20_neg_adj_freq, 0) AS code_20_neg_adj_freq,
    COALESCE(code_20.code_20_pos_adj_freq, 0) AS code_20_pos_adj_freq,
    CASE
        WHEN COALESCE(code_20.code_20_net_adj_qty, 0) < 0 THEN 1
        ELSE 0
    END AS net_neg_code_20_adj
FROM "KSFPA"."SOH"."SOH_REALTIME" soh
INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON CAST(soh.str_id AS VARCHAR(100)) = stores.location_source_identifier
INNER JOIN "KSFPA"."MR2C"."DAY" dates ON CAST(soh.update_date AS DATE) = dates.day_date
INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON CAST(soh.m_kd AS VARCHAR(100)) = products.product_source_identifier
LEFT JOIN (
    SELECT
        stores.location_source_identifier,
        products.product_source_identifier,
        SUM(CASE WHEN ips.stock_movement_source = '1Manifest' THEN ips.stock_movement_quantity ELSE 0 END) AS manifested_units,
        SUM(CASE WHEN ips.stock_movement_source = '1Manifest' THEN 1 ELSE 0 END) AS manifest_freq,
        SUM(CASE WHEN ips.stock_movement_source = 'Shrinkage' THEN -ips.stock_movement_quantity ELSE 0 END) AS shrinkage_units
    FROM "KSFPA"."MR2"."IPS_STOCK_AUDIT" ips
    INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON ips.location_generated_identifier = stores.location_generated_identifier
    INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON ips.product_generated_identifier = products.product_generated_identifier
    WHERE
        stores.region_description IN ('NEW SOUTH WALES', 'VICTORIA TAS', 'WESTERN AUSTRALIA/NT/SA') AND
        ips.day_date > '2022-02-13' AND
        products.rbu_description NOT IN ('NON KMART MERCH', 'undefined') AND
        ((products.rbu_description IN ('KIDS-CLOTHING', 'CLOTHING-WOMENS') AND stores.region_description LIKE '%NEW%') OR
        (products.rbu_description = 'LIVING-FAMILY ENT' AND stores.region_description IN ('VICTORIA TAS', 'WESTER AUSTRALIA/NT/SA', 'QUEENSLAND'))) AND
        products.department_description NOT LIKE '%PHOTO%'
    GROUP BY stores.location_source_identifier, products.product_source_identifier
) ips_move ON stores.location_source_identifier = ips_move.location_source_identifier AND products.product_source_identifier = ips_move.product_source_identifier
LEFT JOIN (
    SELECT
        stores.location_source_identifier,
        products.product_source_identifier,
        SUM(adj.stock_adjustment_quantity) AS code_20_net_adj_qty,
        SUM(CASE WHEN adj.stock_adjustment_quantity < 0 THEN 1 ELSE 0 END) AS code_20_neg_adj_freq,
        SUM(CASE WHEN adj.stock_adjustment_quantity > 0 THEN 1 ELSE 0 END) AS code_20_pos_adj_freq
    FROM "KSFPA"."MR2"."SS_STOCK_ADJUSTMENT" adj
    INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON adj.location_generated_identifier = stores.location_generated_identifier
    INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON adj.product_generated_identifier = products.product_generated_identifier
    WHERE
        stores.region_description IN ('NEW SOUTH WALES', 'VICTORIA TAS', 'WESTERN AUSTRALIA/NT/SA') AND
        adj.process_date > '2022-02-13' AND
        products.rbu_description NOT IN ('NON KMART MERCH', 'undefined') AND
        ((products.rbu_description IN ('KIDS-CLOTHING', 'CLOTHING-WOMENS') AND stores.region_description LIKE '%NEW%') OR
         (products.rbu_description = 'LIVING-FAMILY ENT' AND stores.region_description IN ('VICTORIA TAS', 'WESTER AUSTRALIA/NT/SA', 'QUEENSLAND'))) AND
        products.department_description NOT LIKE '%PHOTO%'
    GROUP BY stores.location_source_identifier, products.product_source_identifier  
) code_20 ON stores.location_source_identifier = code_20.location_source_identifier AND products.product_source_identifier = code_20.product_source_identifier    
WHERE
    stores.region_description IN ('NEW SOUTH WALES', 'VICTORIA TAS', 'WESTERN AUSTRALIA/NT/SA') AND
    dates.day_date = CURRENT_DATE() AND
    products.rbu_description NOT IN ('NON KMART MERCH', 'undefined') AND
    ((products.rbu_description IN ('KIDS-CLOTHING', 'CLOTHING-WOMENS') AND stores.region_description LIKE '%NEW%') OR
     (products.rbu_description = 'LIVING-FAMILY ENT' AND stores.region_description IN ('VICTORIA TAS', 'WESTER AUSTRALIA/NT/SA', 'QUEENSLAND'))) AND
    products.department_description NOT LIKE '%PHOTO%'
ORDER BY stores.region_description, products.rbu_description;