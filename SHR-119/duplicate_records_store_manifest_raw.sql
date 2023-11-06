/* Test for duplicate manifests - Take into account SCM */  
SELECT
    man1.str_id,
    man1.manifest_no,
    man1.seal_date,
    man1.purchase_order_number,
    man1.dc_number,
    man1.cscm,
    man1.scm,
    man1.key_code,
    man1.total_qty
FROM "KSFPA"."WMS"."STORE_MANIFESTS_RAW" man1
INNER JOIN (
    SELECT
        str_id,
        manifest_no,
        seal_date,
        purchase_order_number,
        dc_number,
        cscm,
        scm,
        key_code,
        total_qty,
        COUNT(*)
    FROM "KSFPA"."WMS"."STORE_MANIFESTS_RAW"
    WHERE
        CAST(seal_date AS DATE) >= '2022-07-01'
    GROUP BY
        str_id,
        manifest_no,
        seal_date,
        purchase_order_number,
        dc_number,
        cscm,
        scm,
        key_code,
        total_qty
    HAVING
        COUNT(*) > 1
) man2 ON
    man1.str_id = man2.str_id AND
    man1.manifest_no = man2.manifest_no AND
    man1.seal_date = man2.seal_date AND
    man1.purchase_order_number = man2.purchase_order_number AND
    man1.dc_number = man2.dc_number AND
    man1.cscm = man2.cscm AND
    man1.scm = man2.scm AND
    man1.key_code = man2.key_code AND
    man1.total_qty = man2.total_qty
WHERE
    CAST(man1.seal_date AS DATE) >= '2022-07-01'