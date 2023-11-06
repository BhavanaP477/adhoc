 /* Find the day and APPAREL keycode for which the lowest SOH was recorded for each store */
WITH raw AS (
    SELECT
        stores.location_source_identifier AS location_code,
        stores.location_name,
        soh.day_date,
        products.product_source_identifier AS keycode,
        soh.daily_stock_on_hand_quantity,
        ROW_NUMBER() OVER (PARTITION BY location_name ORDER BY daily_stock_on_hand_quantity ASC) AS row_num
    FROM "KSFPA"."MR2C"."DAILY_SOH" soh
    INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON soh.location_generated_identifier = stores.location_generated_identifier
    INNER JOIN "KSFPA"."MR2C"."DAY" dates ON soh.day_date = dates.day_date
    INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON soh.product_generated_identifier = products.product_generated_identifier
    WHERE
        products.rbu_description IN ('CLOTHING-MENS/FTWR', 'CLOTHING-WOMENS', 'ACTIVE', 'KIDS-CLOTHING', 'BEAUTY/ACC/FTWR') AND
        products.department_source_identifier NOT IN ('048', '076', '077', '078', '020', '085', '014', '094') AND
        CAST(location_code AS VARCHAR(100)) IN (
            SELECT DISTINCT dosa_store_id
            FROM "KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD"."INVENTORY"."DOSA_CONTROL_STORES_RAW") AND
        dates.accounting_period_id >= 202201 AND
        soh.daily_stock_on_hand_quantity < 0
    ORDER BY daily_stock_on_hand_quantity ASC
)
SELECT
    raw.location_code,
    raw.location_name,
    raw.day_date AS date_of_min_soh,
    raw.keycode,
    raw.daily_stock_on_hand_quantity AS min_soh
FROM raw
WHERE row_num = 1
ORDER BY location_code



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



/* Looks like keycode 70791259 is particularly problematic for WEST LAKES. Let's see the SOH history, then join it to agrregated stock moveemnt tables to inspect potential root causes... */
WITH X AS (
    WITH soh AS (
        SELECT
            stores.location_source_identifier AS location_code,
            stores.location_name,
            soh.day_date,
            products.product_source_identifier AS keycode,
            soh.daily_stock_on_hand_quantity AS soh
        FROM "KSFPA"."MR2C"."DAILY_SOH" soh
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON soh.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."DAY" dates ON soh.day_date = dates.day_date
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON soh.product_generated_identifier = products.product_generated_identifier
        WHERE
            CAST(location_code AS VARCHAR(100)) = '1021' AND
            dates.accounting_period_id >= 202201 AND
            products.product_source_identifier = '70791259'
        ORDER BY day_date ASC
    )
    SELECT 
        soh.location_code,
        soh.location_name,
        soh.day_date,
        soh.keycode,
        soh.soh,
        COALESCE(stocktake.stocktake_adj, 0) AS stocktake_adjustments,
        COALESCE(adjustment2.manifest_adj, 0) AS manifested_adjustments,
        COALESCE(adjustment2.sales_adj, 0) AS sales_adjustments,
        COALESCE(adjustment2.shrinkage_adj, 0) AS shrinkage_adjustments,
        COALESCE(adjustment2.refund_adj, 0) AS refund_adjustments,
        COALESCE(adjustment2.other_adj, 0) AS other_adjustments,
        COALESCE(adjustment2.tot_IPS_recorded_adj, 0) AS tot_IPS_recorded_adjustments,
        ROW_NUMBER() OVER (PARTITION BY soh.location_code, soh.location_name, soh.keycode ORDER BY soh.day_date) AS row_num,
        ROW_NUMBER() OVER (PARTITION BY soh.location_code, soh.location_name, soh.keycode ORDER BY soh.day_date DESC) AS row_num_reverse
    FROM soh
    LEFT JOIN (
        SELECT
            stores.location_source_identifier AS location_code,
            adj.process_date,
            products.product_source_identifier AS keycode,
            SUM(adj.stock_adjustment_quantity) AS stocktake_adj
        FROM "KSFPA"."MR2"."SS_STOCK_ADJUSTMENT" adj
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON adj.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."DAY" dates ON adj.process_date = dates.day_date
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON adj.product_generated_identifier = products.product_generated_identifier
        WHERE
            CAST(location_code AS VARCHAR(100)) = '1021' AND
            dates.accounting_period_id >= 202201 AND
            products.product_source_identifier = '70791259' AND
            stock_adjustment_reason_code IN ('04', '17')
        GROUP BY location_code, adj.process_date, keycode) stocktake ON (soh.location_code = stocktake.location_code AND soh.day_date = stocktake.process_date AND soh.keycode = stocktake.keycode)
    LEFT JOIN (
        SELECT
            stores.location_source_identifier AS location_code,
            adj.day_date,
            products.product_source_identifier AS keycode,
            SUM(CASE WHEN adj.stock_movement_source = '1Manifest' THEN stock_movement_quantity ELSE 0 END) AS manifest_adj,
            SUM(CASE WHEN adj.stock_movement_source = 'SALES' THEN stock_movement_quantity ELSE 0 END) AS sales_adj,
            SUM(CASE WHEN adj.stock_movement_source = 'Shrinkage' THEN stock_movement_quantity ELSE 0 END) AS shrinkage_adj,
            SUM(CASE WHEN adj.stock_movement_source = 'REFUND' THEN stock_movement_quantity ELSE 0 END) AS refund_adj,
            SUM(CASE WHEN adj.stock_movement_source NOT IN ('1Manifest', 'SALES', 'Shrinkage', 'REFUND') THEN stock_movement_quantity ELSE 0 END) AS other_adj,
            SUM(stock_movement_quantity) AS tot_IPS_recorded_adj
        FROM "KSFPA"."MR2"."IPS_STOCK_AUDIT" adj
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON adj.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."DAY" dates ON adj.day_date = dates.day_date
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON adj.product_generated_identifier = products.product_generated_identifier
        WHERE
            CAST(location_code AS VARCHAR(100)) = '1021' AND
            dates.accounting_period_id >= 202201 AND
            products.product_source_identifier = '70791259'
        GROUP BY location_code, adj.day_date, keycode) adjustment2 ON soh.location_code = adjustment2.location_code AND soh.day_date = adjustment2.day_date AND soh.keycode = adjustment2.keycode
    ORDER BY soh.day_date ASC
)
SELECT
    X.location_code,
    X.location_name,
    X.keycode,
    SUM(CASE WHEN row_num != 1 THEN X.tot_IPS_recorded_adjustments ELSE 0 END) AS IPS_movement_after_first_record,
    SUM(CASE WHEN X.row_num = 1 THEN soh ELSE 0 END) AS first_soh,
    SUM(CASE WHEN X.row_num_reverse = 1 THEN soh ELSE 0 END) AS last_soh,
    first_soh + IPS_movement_after_first_record AS IPS_accounted_for_soh
FROM X
GROUP BY X.location_code, X.location_name, X.keycode



/* Looks like keycode 68550899 is particularly problematic for TOOWONG. Let's see the SOH history, then join it to agrregated stock moveemnt tables to inspect potential root causes... */
WITH X AS (
    WITH soh AS (
        SELECT
            stores.location_source_identifier AS location_code,
            stores.location_name,
            soh.day_date,
            products.product_source_identifier AS keycode,
            soh.daily_stock_on_hand_quantity AS soh
        FROM "KSFPA"."MR2C"."DAILY_SOH" soh
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON soh.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."DAY" dates ON soh.day_date = dates.day_date
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON soh.product_generated_identifier = products.product_generated_identifier
        WHERE
            CAST(location_code AS VARCHAR(100)) = '1124' AND
            dates.accounting_period_id >= 202201 AND
            products.product_source_identifier = '68550899'
        ORDER BY day_date ASC
    )
    SELECT 
        soh.location_code,
        soh.location_name,
        soh.day_date,
        soh.keycode,
        soh.soh,
        COALESCE(stocktake.stocktake_adj, 0) AS stocktake_adjustments,
        COALESCE(adjustment2.manifest_adj, 0) AS manifested_adjustments,
        COALESCE(adjustment2.sales_adj, 0) AS sales_adjustments,
        COALESCE(adjustment2.shrinkage_adj, 0) AS shrinkage_adjustments,
        COALESCE(adjustment2.refund_adj, 0) AS refund_adjustments,
        COALESCE(adjustment2.other_adj, 0) AS other_adjustments,
        COALESCE(adjustment2.tot_IPS_recorded_adj, 0) AS tot_IPS_recorded_adjustments,
        ROW_NUMBER() OVER (PARTITION BY soh.location_code, soh.location_name, soh.keycode ORDER BY soh.day_date) AS row_num,
        ROW_NUMBER() OVER (PARTITION BY soh.location_code, soh.location_name, soh.keycode ORDER BY soh.day_date DESC) AS row_num_reverse
    FROM soh
    LEFT JOIN (
        SELECT
            stores.location_source_identifier AS location_code,
            adj.process_date,
            products.product_source_identifier AS keycode,
            SUM(adj.stock_adjustment_quantity) AS stocktake_adj
        FROM "KSFPA"."MR2"."SS_STOCK_ADJUSTMENT" adj
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON adj.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."DAY" dates ON adj.process_date = dates.day_date
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON adj.product_generated_identifier = products.product_generated_identifier
        WHERE
            CAST(location_code AS VARCHAR(100)) = '1124' AND
            dates.accounting_period_id >= 202201 AND
            products.product_source_identifier = '68550899' AND
            stock_adjustment_reason_code IN ('04', '17')
        GROUP BY location_code, adj.process_date, keycode) stocktake ON (soh.location_code = stocktake.location_code AND soh.day_date = stocktake.process_date AND soh.keycode = stocktake.keycode)
    LEFT JOIN (
        SELECT
            stores.location_source_identifier AS location_code,
            adj.day_date,
            products.product_source_identifier AS keycode,
            SUM(CASE WHEN adj.stock_movement_source = '1Manifest' THEN stock_movement_quantity ELSE 0 END) AS manifest_adj,
            SUM(CASE WHEN adj.stock_movement_source = 'SALES' THEN stock_movement_quantity ELSE 0 END) AS sales_adj,
            SUM(CASE WHEN adj.stock_movement_source = 'Shrinkage' THEN stock_movement_quantity ELSE 0 END) AS shrinkage_adj,
            SUM(CASE WHEN adj.stock_movement_source = 'REFUND' THEN stock_movement_quantity ELSE 0 END) AS refund_adj,
            SUM(CASE WHEN adj.stock_movement_source NOT IN ('1Manifest', 'SALES', 'Shrinkage', 'REFUND') THEN stock_movement_quantity ELSE 0 END) AS other_adj,
            SUM(stock_movement_quantity) AS tot_IPS_recorded_adj
        FROM "KSFPA"."MR2"."IPS_STOCK_AUDIT" adj
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON adj.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."DAY" dates ON adj.day_date = dates.day_date
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON adj.product_generated_identifier = products.product_generated_identifier
        WHERE
            CAST(location_code AS VARCHAR(100)) = '1124' AND
            dates.accounting_period_id >= 202201 AND
            products.product_source_identifier = '68550899'
        GROUP BY location_code, adj.day_date, keycode) adjustment2 ON soh.location_code = adjustment2.location_code AND soh.day_date = adjustment2.day_date AND soh.keycode = adjustment2.keycode
    ORDER BY soh.day_date ASC
)
SELECT
    X.location_code,
    X.location_name,
    X.keycode,
    SUM(CASE WHEN row_num != 1 THEN X.tot_IPS_recorded_adjustments ELSE 0 END) AS IPS_movement_after_first_record,
    SUM(CASE WHEN X.row_num = 1 THEN soh ELSE 0 END) AS first_soh,
    SUM(CASE WHEN X.row_num_reverse = 1 THEN soh ELSE 0 END) AS last_soh,
    first_soh + IPS_movement_after_first_record AS IPS_accounted_for_soh
FROM X
GROUP BY X.location_code, X.location_name, X.keycode



/* Looks like keycode 68971977 is particularly problematic for INNALOO. Let's see the SOH history, then join it to agrregated stock moveemnt tables to inspect potential root causes... */
WITH X AS (
    WITH soh AS (
        SELECT
            stores.location_source_identifier AS location_code,
            stores.location_name,
            soh.day_date,
            products.product_source_identifier AS keycode,
            soh.daily_stock_on_hand_quantity AS soh
        FROM "KSFPA"."MR2C"."DAILY_SOH" soh
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON soh.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."DAY" dates ON soh.day_date = dates.day_date
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON soh.product_generated_identifier = products.product_generated_identifier
        WHERE
            CAST(location_code AS VARCHAR(100)) = '1139' AND
            dates.accounting_period_id >= 202201 AND
            products.product_source_identifier = '68971977'
        ORDER BY day_date ASC
    )
    SELECT 
        soh.location_code,
        soh.location_name,
        soh.day_date,
        soh.keycode,
        soh.soh,
        COALESCE(stocktake.stocktake_adj, 0) AS stocktake_adjustments,
        COALESCE(adjustment2.manifest_adj, 0) AS manifested_adjustments,
        COALESCE(adjustment2.sales_adj, 0) AS sales_adjustments,
        COALESCE(adjustment2.shrinkage_adj, 0) AS shrinkage_adjustments,
        COALESCE(adjustment2.refund_adj, 0) AS refund_adjustments,
        COALESCE(adjustment2.other_adj, 0) AS other_adjustments,
        COALESCE(adjustment2.tot_IPS_recorded_adj, 0) AS tot_IPS_recorded_adjustments,
        ROW_NUMBER() OVER (PARTITION BY soh.location_code, soh.location_name, soh.keycode ORDER BY soh.day_date) AS row_num,
        ROW_NUMBER() OVER (PARTITION BY soh.location_code, soh.location_name, soh.keycode ORDER BY soh.day_date DESC) AS row_num_reverse
    FROM soh
    LEFT JOIN (
        SELECT
            stores.location_source_identifier AS location_code,
            adj.process_date,
            products.product_source_identifier AS keycode,
            SUM(adj.stock_adjustment_quantity) AS stocktake_adj
        FROM "KSFPA"."MR2"."SS_STOCK_ADJUSTMENT" adj
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON adj.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."DAY" dates ON adj.process_date = dates.day_date
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON adj.product_generated_identifier = products.product_generated_identifier
        WHERE
            CAST(location_code AS VARCHAR(100)) = '1139' AND
            dates.accounting_period_id >= 202201 AND
            products.product_source_identifier = '68971977' AND
            stock_adjustment_reason_code IN ('04', '17')
        GROUP BY location_code, adj.process_date, keycode) stocktake ON (soh.location_code = stocktake.location_code AND soh.day_date = stocktake.process_date AND soh.keycode = stocktake.keycode)
    LEFT JOIN (
        SELECT
            stores.location_source_identifier AS location_code,
            adj.day_date,
            products.product_source_identifier AS keycode,
            SUM(CASE WHEN adj.stock_movement_source = '1Manifest' THEN stock_movement_quantity ELSE 0 END) AS manifest_adj,
            SUM(CASE WHEN adj.stock_movement_source = 'SALES' THEN stock_movement_quantity ELSE 0 END) AS sales_adj,
            SUM(CASE WHEN adj.stock_movement_source = 'Shrinkage' THEN stock_movement_quantity ELSE 0 END) AS shrinkage_adj,
            SUM(CASE WHEN adj.stock_movement_source = 'REFUND' THEN stock_movement_quantity ELSE 0 END) AS refund_adj,
            SUM(CASE WHEN adj.stock_movement_source NOT IN ('1Manifest', 'SALES', 'Shrinkage', 'REFUND') THEN stock_movement_quantity ELSE 0 END) AS other_adj,
            SUM(stock_movement_quantity) AS tot_IPS_recorded_adj
        FROM "KSFPA"."MR2"."IPS_STOCK_AUDIT" adj
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON adj.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."DAY" dates ON adj.day_date = dates.day_date
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON adj.product_generated_identifier = products.product_generated_identifier
        WHERE
            CAST(location_code AS VARCHAR(100)) = '1139' AND
            dates.accounting_period_id >= 202201 AND
            products.product_source_identifier = '68971977'
        GROUP BY location_code, adj.day_date, keycode) adjustment2 ON soh.location_code = adjustment2.location_code AND soh.day_date = adjustment2.day_date AND soh.keycode = adjustment2.keycode
    ORDER BY soh.day_date ASC
)
SELECT
    X.location_code,
    X.location_name,
    X.keycode,
    SUM(CASE WHEN row_num != 1 THEN X.tot_IPS_recorded_adjustments ELSE 0 END) AS IPS_movement_after_first_record,
    SUM(CASE WHEN X.row_num = 1 THEN soh ELSE 0 END) AS first_soh,
    SUM(CASE WHEN X.row_num_reverse = 1 THEN soh ELSE 0 END) AS last_soh,
    first_soh + IPS_movement_after_first_record AS IPS_accounted_for_soh
FROM X
GROUP BY X.location_code, X.location_name, X.keycode



/* Looks like keycode 70026689 is particularly problematic for MIRRABOOKA. Let's see the SOH history, then join it to agrregated stock moveemnt tables to inspect potential root causes... */
WITH X AS (
    WITH soh AS (
        SELECT
            stores.location_source_identifier AS location_code,
            stores.location_name,
            soh.day_date,
            products.product_source_identifier AS keycode,
            soh.daily_stock_on_hand_quantity AS soh
        FROM "KSFPA"."MR2C"."DAILY_SOH" soh
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON soh.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."DAY" dates ON soh.day_date = dates.day_date
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON soh.product_generated_identifier = products.product_generated_identifier
        WHERE
            CAST(location_code AS VARCHAR(100)) = '1142' AND
            dates.accounting_period_id >= 202201 AND
            products.product_source_identifier = '70026689'
        ORDER BY day_date ASC
    )
    SELECT 
        soh.location_code,
        soh.location_name,
        soh.day_date,
        soh.keycode,
        soh.soh,
        COALESCE(stocktake.stocktake_adj, 0) AS stocktake_adjustments,
        COALESCE(adjustment2.manifest_adj, 0) AS manifested_adjustments,
        COALESCE(adjustment2.sales_adj, 0) AS sales_adjustments,
        COALESCE(adjustment2.shrinkage_adj, 0) AS shrinkage_adjustments,
        COALESCE(adjustment2.refund_adj, 0) AS refund_adjustments,
        COALESCE(adjustment2.other_adj, 0) AS other_adjustments,
        COALESCE(adjustment2.tot_IPS_recorded_adj, 0) AS tot_IPS_recorded_adjustments,
        ROW_NUMBER() OVER (PARTITION BY soh.location_code, soh.location_name, soh.keycode ORDER BY soh.day_date) AS row_num,
        ROW_NUMBER() OVER (PARTITION BY soh.location_code, soh.location_name, soh.keycode ORDER BY soh.day_date DESC) AS row_num_reverse
    FROM soh
    LEFT JOIN (
        SELECT
            stores.location_source_identifier AS location_code,
            adj.process_date,
            products.product_source_identifier AS keycode,
            SUM(adj.stock_adjustment_quantity) AS stocktake_adj
        FROM "KSFPA"."MR2"."SS_STOCK_ADJUSTMENT" adj
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON adj.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."DAY" dates ON adj.process_date = dates.day_date
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON adj.product_generated_identifier = products.product_generated_identifier
        WHERE
            CAST(location_code AS VARCHAR(100)) = '1142' AND
            dates.accounting_period_id >= 202201 AND
            products.product_source_identifier = '70026689' AND
            stock_adjustment_reason_code IN ('04', '17')
        GROUP BY location_code, adj.process_date, keycode) stocktake ON (soh.location_code = stocktake.location_code AND soh.day_date = stocktake.process_date AND soh.keycode = stocktake.keycode)
    LEFT JOIN (
        SELECT
            stores.location_source_identifier AS location_code,
            adj.day_date,
            products.product_source_identifier AS keycode,
            SUM(CASE WHEN adj.stock_movement_source = '1Manifest' THEN stock_movement_quantity ELSE 0 END) AS manifest_adj,
            SUM(CASE WHEN adj.stock_movement_source = 'SALES' THEN stock_movement_quantity ELSE 0 END) AS sales_adj,
            SUM(CASE WHEN adj.stock_movement_source = 'Shrinkage' THEN stock_movement_quantity ELSE 0 END) AS shrinkage_adj,
            SUM(CASE WHEN adj.stock_movement_source = 'REFUND' THEN stock_movement_quantity ELSE 0 END) AS refund_adj,
            SUM(CASE WHEN adj.stock_movement_source NOT IN ('1Manifest', 'SALES', 'Shrinkage', 'REFUND') THEN stock_movement_quantity ELSE 0 END) AS other_adj,
            SUM(stock_movement_quantity) AS tot_IPS_recorded_adj
        FROM "KSFPA"."MR2"."IPS_STOCK_AUDIT" adj
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON adj.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."DAY" dates ON adj.day_date = dates.day_date
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON adj.product_generated_identifier = products.product_generated_identifier
        WHERE
            CAST(location_code AS VARCHAR(100)) = '1142' AND
            dates.accounting_period_id >= 202201 AND
            products.product_source_identifier = '70026689'
        GROUP BY location_code, adj.day_date, keycode) adjustment2 ON soh.location_code = adjustment2.location_code AND soh.day_date = adjustment2.day_date AND soh.keycode = adjustment2.keycode
    ORDER BY soh.day_date ASC
)
SELECT
    X.location_code,
    X.location_name,
    X.keycode,
    SUM(CASE WHEN row_num != 1 THEN X.tot_IPS_recorded_adjustments ELSE 0 END) AS IPS_movement_after_first_record,
    SUM(CASE WHEN X.row_num = 1 THEN soh ELSE 0 END) AS first_soh,
    SUM(CASE WHEN X.row_num_reverse = 1 THEN soh ELSE 0 END) AS last_soh,
    first_soh + IPS_movement_after_first_record AS IPS_accounted_for_soh
FROM X
GROUP BY X.location_code, X.location_name, X.keycode



/* Looks like keycode 71303864 is particularly problematic for INDOOROOPILLY. Let's see the SOH history, then join it to agrregated stock moveemnt tables to inspect potential root causes... */
WITH X AS (
    WITH soh AS (
        SELECT
            stores.location_source_identifier AS location_code,
            stores.location_name,
            soh.day_date,
            products.product_source_identifier AS keycode,
            soh.daily_stock_on_hand_quantity AS soh
        FROM "KSFPA"."MR2C"."DAILY_SOH" soh
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON soh.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."DAY" dates ON soh.day_date = dates.day_date
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON soh.product_generated_identifier = products.product_generated_identifier
        WHERE
            CAST(location_code AS VARCHAR(100)) = '1217' AND
            dates.accounting_period_id >= 202201 AND
            products.product_source_identifier = '71303864'
        ORDER BY day_date ASC
    )
    SELECT 
        soh.location_code,
        soh.location_name,
        soh.day_date,
        soh.keycode,
        soh.soh,
        COALESCE(stocktake.stocktake_adj, 0) AS stocktake_adjustments,
        COALESCE(adjustment2.manifest_adj, 0) AS manifested_adjustments,
        COALESCE(adjustment2.sales_adj, 0) AS sales_adjustments,
        COALESCE(adjustment2.shrinkage_adj, 0) AS shrinkage_adjustments,
        COALESCE(adjustment2.refund_adj, 0) AS refund_adjustments,
        COALESCE(adjustment2.other_adj, 0) AS other_adjustments,
        COALESCE(adjustment2.tot_IPS_recorded_adj, 0) AS tot_IPS_recorded_adjustments,
        ROW_NUMBER() OVER (PARTITION BY soh.location_code, soh.location_name, soh.keycode ORDER BY soh.day_date) AS row_num,
        ROW_NUMBER() OVER (PARTITION BY soh.location_code, soh.location_name, soh.keycode ORDER BY soh.day_date DESC) AS row_num_reverse
    FROM soh
    LEFT JOIN (
        SELECT
            stores.location_source_identifier AS location_code,
            adj.process_date,
            products.product_source_identifier AS keycode,
            SUM(adj.stock_adjustment_quantity) AS stocktake_adj
        FROM "KSFPA"."MR2"."SS_STOCK_ADJUSTMENT" adj
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON adj.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."DAY" dates ON adj.process_date = dates.day_date
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON adj.product_generated_identifier = products.product_generated_identifier
        WHERE
            CAST(location_code AS VARCHAR(100)) = '1217' AND
            dates.accounting_period_id >= 202201 AND
            products.product_source_identifier = '71303864' AND
            stock_adjustment_reason_code IN ('04', '17')
        GROUP BY location_code, adj.process_date, keycode) stocktake ON (soh.location_code = stocktake.location_code AND soh.day_date = stocktake.process_date AND soh.keycode = stocktake.keycode)
    LEFT JOIN (
        SELECT
            stores.location_source_identifier AS location_code,
            adj.day_date,
            products.product_source_identifier AS keycode,
            SUM(CASE WHEN adj.stock_movement_source = '1Manifest' THEN stock_movement_quantity ELSE 0 END) AS manifest_adj,
            SUM(CASE WHEN adj.stock_movement_source = 'SALES' THEN stock_movement_quantity ELSE 0 END) AS sales_adj,
            SUM(CASE WHEN adj.stock_movement_source = 'Shrinkage' THEN stock_movement_quantity ELSE 0 END) AS shrinkage_adj,
            SUM(CASE WHEN adj.stock_movement_source = 'REFUND' THEN stock_movement_quantity ELSE 0 END) AS refund_adj,
            SUM(CASE WHEN adj.stock_movement_source NOT IN ('1Manifest', 'SALES', 'Shrinkage', 'REFUND') THEN stock_movement_quantity ELSE 0 END) AS other_adj,
            SUM(stock_movement_quantity) AS tot_IPS_recorded_adj
        FROM "KSFPA"."MR2"."IPS_STOCK_AUDIT" adj
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON adj.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."DAY" dates ON adj.day_date = dates.day_date
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON adj.product_generated_identifier = products.product_generated_identifier
        WHERE
            CAST(location_code AS VARCHAR(100)) = '1217' AND
            dates.accounting_period_id >= 202201 AND
            products.product_source_identifier = '71303864'
        GROUP BY location_code, adj.day_date, keycode) adjustment2 ON soh.location_code = adjustment2.location_code AND soh.day_date = adjustment2.day_date AND soh.keycode = adjustment2.keycode
    ORDER BY soh.day_date ASC
)
SELECT
    X.location_code,
    X.location_name,
    X.keycode,
    SUM(CASE WHEN row_num != 1 THEN X.tot_IPS_recorded_adjustments ELSE 0 END) AS IPS_movement_after_first_record,
    SUM(CASE WHEN X.row_num = 1 THEN soh ELSE 0 END) AS first_soh,
    SUM(CASE WHEN X.row_num_reverse = 1 THEN soh ELSE 0 END) AS last_soh,
    first_soh + IPS_movement_after_first_record AS IPS_accounted_for_soh
FROM X
GROUP BY X.location_code, X.location_name, X.keycode