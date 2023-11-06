CREATE OR REPLACE TABLE KSFDA.SHRINK_TACTICAL.PAPER_SHRINK_COST_ANALYSIS_Step_2_ADJUSTMENT AS
(
SELECT RES.*, (MANIFEST_COST + SALES_COST + SHRINK_COST + ITC_COST + RLO_COST + PO_COST) AS SOH_COST FROM
(
SELECT pg.*,
--IFF(abs(ADJUSTMENT) = abs(DIFF_MANIFEST), DIFF_MANIFEST, 0) AS NEW_DIFF_MANIFEST,
--IFF(abs(ADJUSTMENT) = abs(DIFF_SALES), DIFF_SALES, 0) AS NEW_DIFF_SALES,
--IFF(abs(ADJUSTMENT) = abs(DIFF_SHRINK), DIFF_SHRINK, 0) AS NEW_DIFF_SHRINK,
--IFF(abs(ADJUSTMENT) = abs(DIFF_ITC), DIFF_ITC, 0) AS NEW_DIFF_ITC,
--IFF(abs(ADJUSTMENT) = abs(DIFF_RLO), DIFF_RLO, 0) AS NEW_DIFF_RLO,
--IFF(abs(ADJUSTMENT) = abs(DIFF_PO), DIFF_PO, 0) AS NEW_DIFF_PO,
NEW_DIFF_MANIFEST,
NEW_DIFF_SALES,
NEW_DIFF_SHRINK,
NEW_DIFF_ITC,
NEW_DIFF_RLO,
NEW_DIFF_PO,
MANIFEST_COST,
SALES_COST,
SHRINK_COST,
ITC_COST,
RLO_COST,
PO_COST

--IFF(DIFF_MANIFEST != 0, IFF(MANIFEST_COST), 0) AS MANIFEST_COST,
--IFF(DIFF_SALES != 0, IFF(SALES_COST > 0, SALES_COST, SALES_COST), 0) AS SALES_COST,
--IFF(DIFF_SHRINK != 0, IFF(SHRINK_COST > 0, SHRINK_COST, SHRINK_COST), 0) AS SHRINK_COST,
--IFF(DIFF_ITC != 0, IFF(ITC_COST > 0, ITC_COST, ITC_COST), 0) AS ITC_COST,
--IFF(DIFF_RLO != 0,IFF(RLO_COST > 0, RLO_COST, RLO_COST), 0) AS RLO_COST,
--IFF(DIFF_PO != 0, PO_COST, 0 ) AS PO_COST
FROM
(SELECT PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER,
SUM(NEW_MANIFEST_COST) AS MANIFEST_COST,
SUM(NEW_SALES_COST) AS SALES_COST,
SUM(NEW_SHRINK_COST) AS SHRINK_COST,
SUM(NEW_ITC_COST) AS ITC_COST,
SUM(NEW_RLO_COST) AS RLO_COST,
SUM(NEW_PO_COST) AS PO_COST,
SUM(NEW_DIFF_MANIFEST) AS NEW_DIFF_MANIFEST,
SUM(NEW_DIFF_SALES) AS NEW_DIFF_SALES,
SUM(NEW_DIFF_SHRINK) AS NEW_DIFF_SHRINK,
SUM(NEW_DIFF_ITC) AS NEW_DIFF_ITC,
SUM(NEW_DIFF_RLO) AS NEW_DIFF_RLO,
SUM(NEW_DIFF_PO) AS NEW_DIFF_PO
FROM KSFDA.SHRINK_TACTICAL.PAPER_SHRINK_COST_ADJUSTMENT
GROUP BY PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER) a 
JOIN "KSFDA"."SHRINK_TACTICAL"."SUMMARY_PAGE_ADJUSTMENT" pg
ON pg.PRODUCT_GENERATED_IDENTIFIER = a.PRODUCT_GENERATED_IDENTIFIER
AND pg.LOCATION_GENERATED_IDENTIFIER = a.LOCATION_GENERATED_IDENTIFIER
  )RES
--  WHERE PRODUCT_GENERATED_IDENTIFIER = '4317562800004' 
--AND LOCATION_GENERATED_IDENTIFIER = '1040136000001'
)
