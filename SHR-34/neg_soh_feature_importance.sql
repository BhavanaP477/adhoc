/* Data for shrink analysis in R */
/* Any variables related to stocktake, or SOH error will be omitted; We want to look for possible causes of negative SOH, not effects */
/* New variables to consider: Apparel/GM, DC, Class/Supplier */
WITH agg AS (
    WITH soh AS (
        SELECT
            stores.location_source_identifier AS location_code,
            stores.region_description,
            CASE
                WHEN stores.location_source_identifier IN ('1021', '1124', '1139', '1142', '1217', '1223', '1231', '1285') THEN 1
                ELSE 0
            END AS is_dosa_store,
            CASE
                WHEN
                    products.rbu_description IN ('CLOTHING-WOMENS', 'ACTIVE', 'CLOTHING-MENS/FTWR', 'KIDS-CLOTHING') AND
                    products.department_source_identifier NOT IN ('048', '076', '077', '078', '020', '085', '014', '094') THEN 'APPAREL'
                ELSE 'GENERAL MERCHANDISE'
            END AS type,
            products.rbu_description,
            products.department_description,
            products.class_description,
            soh.day_date,
            SUM(CASE WHEN soh.daily_stock_on_hand_quantity < 0 THEN 1 ELSE 0 END) AS negative_soh_keycodes,
            COUNT(*) AS tot_keycodes,
            negative_soh_keycodes / tot_keycodes AS prop_keycodes_with_negative_soh
        FROM "KSFPA"."MR2C"."DAILY_SOH" soh
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON soh.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."DAY" dates ON soh.day_date = dates.day_date
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON soh.product_generated_identifier = products.product_generated_identifier
        WHERE
            products.rbu_description NOT IN ('NON KMART MERCH', 'undefined') AND
            LEFT(stores.location_source_identifier, 1) = 1 AND
            dates.accounting_period_id >= 202201
        GROUP BY location_code, stores.region_description, type, products.rbu_description, products.department_description, products.class_description, soh.day_date
        ORDER BY location_code, stores.region_description, products.rbu_description, products.department_description, soh.day_date
    )
    SELECT DISTINCT
        soh.location_code,
        soh.is_dosa_store,
        soh.region_description,
        soh.type,
        soh.rbu_description,
        soh.department_description,
        soh.class_description,
        soh.day_date,
        soh.negative_soh_keycodes,
        soh.tot_keycodes,
        COALESCE(ips.manifested_qty, 0) AS manifest_quantity,
        COALESCE(ips.manifested_keycodes, 0) AS manifest_count
    FROM soh
    LEFT JOIN (
        SELECT
            stores.location_source_identifier AS location_code,
            CASE
                WHEN
                    products.rbu_description IN ('CLOTHING-WOMENS', 'ACTIVE', 'CLOTHING-MENS/FTWR', 'KIDS-CLOTHING') AND
                    products.department_source_identifier NOT IN ('048', '076', '077', '078', '020', '085', '014', '094') THEN 'APPAREL'
                ELSE 'GENERAL MERCHANDISE'
            END AS type,
            products.rbu_description,
            products.department_description,
            products.class_description,
            ips.day_date,
            SUM(ips.stock_movement_quantity) AS manifested_qty,
            COUNT(*) AS manifested_keycodes
        FROM "KSFPA"."MR2"."IPS_STOCK_AUDIT" ips
        INNER JOIN "KSFPA"."MR2C"."LOCATION" stores ON ips.location_generated_identifier = stores.location_generated_identifier
        INNER JOIN "KSFPA"."MR2C"."DAY" dates ON ips.day_date = dates.day_date
        INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON ips.product_generated_identifier = products.product_generated_identifier
        WHERE
            products.rbu_description NOT IN ('NON KMART MERCH', 'undefined') AND
            LEFT(stores.location_source_identifier, 1) = 1 AND
            dates.accounting_period_id >= 202201 AND
            ips.stock_movement_source = '1Manifest'
        GROUP BY stores.location_source_identifier, type, products.rbu_description, products.department_description, products.class_description, ips.day_date
        ORDER BY stores.location_source_identifier, products.rbu_description, products.department_description, ips.day_date) ips
    ON soh.location_code = ips.location_code AND soh.type = ips.type AND soh.rbu_description = ips.rbu_description AND soh.department_description = ips.department_description AND soh.class_description = ips.class_description AND soh.day_date = ips.day_date
)
SELECT
    agg.location_code,
    agg.is_dosa_store,
    agg.region_description,
    agg.type,
    agg.rbu_description,
    agg.department_description,
    agg.class_description,
    AVG(agg.negative_soh_keycodes) AS avg_negative_soh_keycodes,
    AVG(agg.tot_keycodes) AS avg_tot_keycodes,
    SUM(agg.manifest_quantity) AS tot_manifested_units,
    SUM(agg.manifest_count) AS tot_manifests,
    CASE
        WHEN tot_manifests > 0 THEN tot_manifested_units / tot_manifests
        ELSE 0
    END AS avg_units_per_manifest
FROM agg
GROUP BY agg.location_code, agg.is_dosa_store, agg.region_description, agg.type, agg.rbu_description, agg.department_description, agg.class_description
ORDER BY agg.location_code, agg.is_dosa_store, agg.region_description, agg.type, agg.rbu_description, agg.department_description, agg.class_description
