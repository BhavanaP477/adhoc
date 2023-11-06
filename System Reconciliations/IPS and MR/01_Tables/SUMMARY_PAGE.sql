CREATE TABLE KSFDA.SHRINK_TACTICAL.SUMMARY_PAGE_ADJUSTMENT AS
(
SELECT a.*, b.ADJUSTMENT FROM 
(SELECT * FROM KSFDA.SHRINK_TACTICAL.SUMMARY_PAGE6)a
LEFT JOIN
(SELECT PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER, SUM(STOCK_ADJUSTMENT_QUANTITY) AS ADJUSTMENT
FROM KSFPA.MR2.SS_STOCK_ADJUSTMENT
WHERE STOCK_ADJUSTMENT_REASON_CODE = '06'
AND PERIOD_END_DATE >= '2022-02-20' AND PERIOD_END_DATE <= '2023-02-03'
GROUP BY PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER)b
ON a.PRODUCT_GENERATED_IDENTIFIER = b.PRODUCT_GENERATED_IDENTIFIER
AND a.LOCATION_GENERATED_IDENTIFIER = b.LOCATION_GENERATED_IDENTIFIER
--WHERE a.PRODUCT_GENERATED_IDENTIFIER = '4245814200004' AND a.LOCATION_GENERATED_IDENTIFIER = '1040122500001'
)

SELECT d.*,s.adjustment FROM KSFDA.SHRINK_TACTICAL.SUMMARY_PAGE_ADJUSTMENT s
JOIN KSFDA.SHRINK_TACTICAL.PAPER_SHRINK_DETAILED6 d
ON s.PRODUCT_GENERATED_IDENTIFIER = d.PRODUCT_GENERATED_IDENTIFIER
AND s.LOCATION_GENERATED_IDENTIFIER = d.LOCATION_GENERATED_IDENTIFIER
WHERE s.PRODUCT_GENERATED_IDENTIFIER = '4245814200004' AND s.LOCATION_GENERATED_IDENTIFIER = '1040122500001'

SELECT DIFF_DERIVED_SOH, DIFF_MANIFEST, DIFF_SALES, ADJUSTMENT 
FROM KSFDA.SHRINK_TACTICAL.SUMMARY_PAGE_ADJUSTMENT
WHERE PRODUCT_GENERATED_IDENTIFIER = '4245814200004' 
AND LOCATION_GENERATED_IDENTIFIER = '1040122500001'


CREATE OR REPLACE TABLE KSFDA.SHRINK_TACTICAL.PAPER_SHRINK_COST_ANALYSIS_Step_3_6 AS
(
SELECT a.*, b.ADJUSTMENT,
IFF((abs(a.DIFF_DERIVED_SOH) - abs(a.DIFF_MANIFEST) = abs(ADJUSTMENT))or(ADJUSTMENT is NULL), 0, MANIFEST_COST) AS NEW_MANIFEST_COST,
IFF((abs(a.DIFF_DERIVED_SOH) - abs(a.DIFF_SALES) = abs(ADJUSTMENT))or(ADJUSTMENT is NULL), 0, SALES_COST) AS NEW_SALES_COST,
IFF((abs(a.DIFF_DERIVED_SOH) - abs(a.DIFF_SHRINK) = abs(ADJUSTMENT))or(ADJUSTMENT is NULL), 0, SHRINK_COST) AS NEW_SHRINK_COST,
IFF((abs(a.DIFF_DERIVED_SOH) - abs(a.DIFF_ITC) = abs(ADJUSTMENT))or(ADJUSTMENT is NULL), 0, ITC_COST) AS NEW_ITC_COST,
IFF((abs(a.DIFF_DERIVED_SOH) - abs(a.DIFF_RLO) = abs(ADJUSTMENT))or(ADJUSTMENT is NULL), 0, RLO_COST) AS NEW_RLO_COST,
IFF((abs(a.DIFF_DERIVED_SOH) - abs(a.DIFF_PO) = abs(ADJUSTMENT))or(ADJUSTMENT is NULL), 0, PO_COST) AS NEW_PO_COST
FROM KSFDA.SHRINK_TACTICAL.PAPER_SHRINK_COST_ANALYSIS_Step_2_6 a
JOIN KSFDA.SHRINK_TACTICAL.SUMMARY_PAGE_ADJUSTMENT b
ON a.PRODUCT_GENERATED_IDENTIFIER = b.PRODUCT_GENERATED_IDENTIFIER
AND a.LOCATION_GENERATED_IDENTIFIER = b.LOCATION_GENERATED_IDENTIFIER
--WHERE a.PRODUCT_GENERATED_IDENTIFIER = '4245814200004' 
--AND a.LOCATION_GENERATED_IDENTIFIER = '1040122500001'
)
