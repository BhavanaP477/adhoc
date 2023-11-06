/* Optimized version of above query (Restricted time period, post-stocktake to current date) */
SELECT
    T.location_source_identifier,
    T.location_name,
    T.keycode,
    T.soh AS daily_soh,
    post_stktake_soh.soh AS post_stocktake_soh,
    post_stktake_soh.soh + COALESCE(adj1.manifest_qty, 0) + COALESCE(adj1.sales_qty, 0) + COALESCE(adj1.other_ips_qty, 0) AS ips_soh,
    post_stktake_soh.soh + COALESCE(received.received_qty, 0) - COALESCE(desp.despatched_qty, 0) + COALESCE(adj1.sales_qty, 0) + COALESCE(adj1.other_ips_qty, 0) AS theoretical_soh,
    COALESCE(adj1.manifest_qty, 0) AS manifest_qty,
    COALESCE(received.received_qty, 0) AS received_qty,
    COALESCE(desp.despatched_qty, 0) AS despatched_qty,
    COALESCE(adj1.sales_qty, 0) AS net_sales_qty,
    COALESCE(adj1.other_ips_qty, 0) AS net_other_ips_qty,
    CASE
        WHEN theoretical_soh >= 0 AND ips_soh >= 0 THEN 'A'
        WHEN theoretical_soh < 0 AND net_sales_qty + COALESCE(received.received_qty, 0) - COALESCE(desp.despatched_qty, 0) < 0 THEN 'B'
        WHEN theoretical_soh < 0 AND ips_soh - net_other_ips_qty >= 0 AND net_other_ips_qty < 0 THEN 'C'
        WHEN ips_soh < 0 AND theoretical_soh >= 0 AND COALESCE(adj1.manifest_qty, 0) < COALESCE(received.received_qty, 0) - COALESCE(desp.despatched_qty, 0) THEN 'D'
        ELSE 'B'
    END AS neg_soh_scenario,
    CASE
        WHEN neg_soh_scenario = 'A' THEN 'IPS SOH >= 0; daily SOH incorrect'
        WHEN neg_soh_scenario = 'B' THEN 'Missing transfer?'
        WHEN neg_soh_scenario = 'C' THEN 'Investigate other IPS adjustment'
        WHEN neg_soh_scenario = 'D' THEN 'Unreceipted/miscounted manifest at store'
        ELSE 'Yet to classify'
    END AS detailed_neg_soh_scenario
FROM (
    SELECT
        stores.location_source_identifier,
        stores.location_name,
        products.product_source_identifier AS keycode,
        soh.daily_stock_on_hand_quantity AS soh
    FROM "KSFPA"."MR2C"."DAILY_SOH" soh
    INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON soh.location_generated_identifier = stores.location_generated_identifier
    INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON soh.product_generated_identifier = products.product_generated_identifier
    WHERE
        LEFT(stores.location_source_identifier, 1) = '1' AND
        products.rbu_description NOT IN ('NON KMART MERCH', 'undefined') AND
        products.department_description NOT IN ('REUSABLE BAGS', 'PHOTO CENTRE') AND
        soh.day_date = '2022-08-07'
) T
LEFT JOIN (
    SELECT
        stores.location_source_identifier,
        products.product_source_identifier,
        SUM(CASE WHEN ips.stock_movement_source = '1Manifest' THEN ips.stock_movement_quantity ELSE 0 END) AS manifest_qty,
        SUM(CASE WHEN ips.stock_movement_source = '1Manifest' THEN 1 ELSE 0 END) AS manifest_freq,
        SUM(CASE WHEN ips.stock_movement_source IN ('WEB_ADJ', 'Web_ADJ', 'Web_Adj') THEN ips.stock_movement_quantity ELSE 0 END) AS web_adj_qty,
        SUM(CASE WHEN ips.stock_movement_source IN ('WEB_ADJ', 'Web_ADJ', 'Web_Adj') THEN 1 ELSE 0 END) AS web_adj_freq,
        SUM(CASE WHEN ips.stock_movement_source = 'SALES' THEN ips.stock_movement_quantity ELSE 0 END) AS sales_qty,
        SUM(CASE WHEN ips.stock_movement_source NOT IN ('1Manifest', 'SALES') THEN ips.stock_movement_quantity ELSE 0 END) AS other_ips_qty
    FROM "KSFPA"."MR2"."IPS_STOCK_AUDIT" ips
    INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON ips.location_generated_identifier = stores.location_generated_identifier
    INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON ips.product_generated_identifier = products.product_generated_identifier
    WHERE
        LEFT(stores.location_source_identifier, 1) = '1' AND
        products.rbu_description NOT IN ('NON KMART MERCH', 'undefined') AND
        products.department_description NOT IN ('REUSABLE BAGS', 'PHOTO CENTRE') AND
        ips.day_date BETWEEN '2022-03-01' AND '2022-08-07'
GROUP BY stores.location_source_identifier, products.product_source_identifier
) adj1 ON T.location_source_identifier = adj1.location_source_identifier AND T.keycode = adj1.product_source_identifier
LEFT JOIN (
    SELECT
        stores.location_source_identifier,
        products.product_source_identifier,
        SUM(transfer.despatch_quantity) AS received_qty
    FROM "KSFPA"."MR2"."TRANSFER_PRODUCT" transfer
    INNER JOIN "KSFPA"."MR2"."TRANSFER_RECEIPT" receipt ON transfer.transfer_generated_identifier = receipt.transfer_generated_identifier AND transfer.effective_date = receipt.effective_date
    INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON receipt.location_generated_identifier = stores.location_generated_identifier
    INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON transfer.product_generated_identifier = products.product_generated_identifier
    WHERE
        LEFT(stores.location_source_identifier, 1) = '1' AND
        products.rbu_description NOT IN ('NON KMART MERCH', 'undefined') AND
        products.department_description NOT IN ('REUSABLE BAGS', 'PHOTO CENTRE') AND
        transfer.effective_date BETWEEN '2022-03-01' AND '2022-08-07'
    GROUP BY stores.location_source_identifier, products.product_source_identifier
) received ON T.location_source_identifier = received.location_source_identifier AND T.keycode = received.product_source_identifier
LEFT JOIN (
    SELECT
        stores.location_source_identifier,
        products.product_source_identifier,
        SUM(transfer.despatch_quantity) AS despatched_qty
    FROM "KSFPA"."MR2"."TRANSFER_PRODUCT" transfer
    INNER JOIN "KSFPA"."MR2"."TRANSFER_DESPATCH" despatch ON transfer.transfer_generated_identifier = despatch.transfer_generated_identifier AND transfer.effective_date = despatch.effective_date
    INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON despatch.location_generated_identifier = stores.location_generated_identifier
    INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON transfer.product_generated_identifier = products.product_generated_identifier 
    WHERE
        LEFT(stores.location_source_identifier, 1) = '1' AND
        products.rbu_description NOT IN ('NON KMART MERCH', 'undefined') AND
        products.department_description NOT IN ('REUSABLE BAGS', 'PHOTO CENTRE') AND
        transfer.effective_date BETWEEN '2022-03-01' AND '2022-08-07'
    GROUP BY stores.location_source_identifier, products.product_source_identifier
) desp ON T.location_source_identifier = desp.location_source_identifier AND T.keycode = desp.product_source_identifier
LEFT JOIN (
    SELECT
        stores.location_source_identifier,
        products.product_source_identifier,
        SUM(CASE WHEN adj.stock_adjustment_reason_code IN ('04', '17') THEN adj.stock_adjustment_quantity ELSE 0 END) AS net_stktake_adj,
        SUM(CASE WHEN adj.stock_adjustment_reason_code = '20' THEN adj.stock_adjustment_quantity ELSE 0 END) AS net_gapscan_adj,
        SUM(adj.stock_adjustment_quantity) AS net_tot_adj
    FROM "KSFPA"."MR2"."SS_STOCK_ADJUSTMENT" adj
    INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON adj.location_generated_identifier = stores.location_generated_identifier
    INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON adj.product_generated_identifier = products.product_generated_identifier
    WHERE
        LEFT(stores.location_source_identifier, 1) = '1' AND
        products.rbu_description NOT IN ('NON KMART MERCH', 'undefined') AND
        products.department_description NOT IN ('REUSABLE BAGS', 'PHOTO CENTRE')
    GROUP BY stores.location_source_identifier, products.product_source_identifier
) adj2 ON T.location_source_identifier = adj2.location_source_identifier AND T.keycode = adj2.product_source_identifier
INNER JOIN (
    SELECT
        stores.location_source_identifier,
        products.product_source_identifier,
        soh.daily_stock_on_hand_quantity AS soh
    FROM "KSFPA"."MR2C"."DAILY_SOH" soh
    INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON soh.location_generated_identifier = stores.location_generated_identifier
    INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON soh.product_generated_identifier = products.product_generated_identifier
    WHERE
        LEFT(stores.location_source_identifier, 1) = '1' AND
        products.rbu_description NOT IN ('NON KMART MERCH', 'undefined') AND
        products.department_description NOT IN ('REUSABLE BAGS', 'PHOTO CENTRE') AND
        soh.day_date = '2022-02-28'
) post_stktake_soh ON T.location_source_identifier = post_stktake_soh.location_source_identifier AND T.keycode = post_stktake_soh.product_source_identifier
WHERE
    T.soh < 0 AND
    post_stktake_soh.soh >= 0 AND
    COALESCE(adj2.net_tot_adj, 0) = 0 AND
    COALESCE(adj2.net_stktake_adj, 0) = 0 AND
    COALESCE(adj2.net_gapscan_adj, 0) = 0
ORDER BY T.location_source_identifier;