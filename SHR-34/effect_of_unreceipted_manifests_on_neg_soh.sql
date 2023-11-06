SELECT
    str_id AS location_code,
    products.rbu_description,
    products.department_description,
    products.product_source_identifier,
    SUM(soh.soh) AS current_soh,
    CASE
        WHEN current_soh < 0 THEN 1
        ELSE 0
    END AS is_neg_soh,
    COALESCE(not_received.not_received_qty, 0) AS unreceipted_manifest_qty,
    COALESCE(not_received.not_received_occasions, 0) AS unreceipted_manifest_occasions
FROM "KSFPA"."SOH"."SOH" soh
INNER JOIN "KSFPA"."MR2C"."KEYCODE" products ON CAST(soh.m_kd AS VARCHAR(100)) = CAST(products.product_source_identifier AS VARCHAR(100))
FULL OUTER JOIN (
    WITH X AS (
        SELECT 
            td.transfer_generated_identifier, 
            td.location_generated_identifier from_locn,
            td.despatch_date,
            td.transfer_source_identifier manifest_no,
            loc.location_source_identifier to_locn,
            k.rbu_description,
            k.department_description,
            k.product_source_identifier,
            SUM(tp.despatch_quantity) quantity,
            SUM(CASE WHEN despatch_quantity > 0 THEN 1 ELSE 0 END) AS occurences,
            tr.receipt_date
        FROM 
            mr2.transfer_receipt tr,
            mr2.transfer_product tp,
            mr2c.location loc,
            mr2c.keycode k,
            mr2.transfer_despatch td
        WHERE --tp.product_generated_identifier=4296464300004
            tr.transfer_generated_identifier=tp.transfer_generated_identifier AND
            td.transfer_generated_identifier=tp.transfer_generated_identifier AND
            tr.location_generated_identifier = loc.location_generated_identifier AND
            --and tr.location_generated_identifier=1040123100001
            tr.expiry_date='9999-12-31' AND
            tp.expiry_date='9999-12-31' AND
            td.expiry_date='9999-12-31' AND 
            k.product_generated_identifier=tp.product_generated_identifier AND
            despatch_date > '2022-03-01' AND
            --and k.department_source_identifier='017'
            tr.receipt_date IS null
            --and transfer_source_identifier='820699778'
            --order by despatch_date desc
            --and stock_movement_source='1Manifest'
            --and stock_movement_reference_id=6106870
        GROUP BY 
            td.transfer_generated_identifier, 
            td.location_generated_identifier,
            td.despatch_date,
            td.transfer_source_identifier,
            k.rbu_description,
            k.department_description,
            k.product_source_identifier,
            loc.location_source_identifier,
            tr.receipt_date
    )
    SELECT
        X.to_locn,
        X.rbu_description,
        X.department_description,
        X.product_source_identifier,
        SUM(X.quantity) AS not_received_qty,
        SUM(X.occurences) AS not_received_occasions
    FROM X
    GROUP BY X.to_locn, X.rbu_description, X.department_description, X.product_source_identifier
) not_received ON CAST(soh.str_id AS VARCHAR(100)) = CAST(not_received.to_locn AS VARCHAR(100)) AND products.product_source_identifier = not_received.product_source_identifier    
WHERE
    LEFT(CAST(soh.str_id AS VARCHAR(100)), 1) = '1' AND
    CAST(insert_date AS DATE) = CURRENT_DATE() AND
    products.rbu_description NOT IN ('NON KMART MERCH', 'undefined') AND
    products.department_description NOT LIKE '%PHOTO%'
GROUP BY location_code, products.rbu_description, products.department_description, products.product_source_identifier, not_received.not_received_qty, not_received.not_received_occasions
ORDER BY current_soh ASC