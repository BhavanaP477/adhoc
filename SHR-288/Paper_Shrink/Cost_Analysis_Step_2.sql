CREATE OR REPLACE TABLE KSFDA.SHRINK_TACTICAL.PAPER_SHRINK_COST_ANALYSIS_Step_2 AS
(
SELECT RES.*, (MANIFEST_COST + SALES_COST + SHRINK_COST + ITC_COST + RLO_COST + PO_COST) AS SOH_COST FROM
(
SELECT pg.*,
IFF(DIFF_MANIFEST != 0, MANIFEST_COST, 0) AS MANIFEST_COST,
IFF(DIFF_SALES != 0, IFF(SALES_COST > 0, SALES_COST, SALES_COST), 0) AS SALES_COST,
IFF(DIFF_SHRINK != 0, IFF(SHRINK_COST > 0, SHRINK_COST, SHRINK_COST), 0) AS SHRINK_COST,
IFF(DIFF_ITC != 0, IFF(ITC_COST > 0, ITC_COST, ITC_COST), 0) AS ITC_COST,
IFF(DIFF_RLO != 0,IFF(RLO_COST > 0, RLO_COST, RLO_COST), 0) AS RLO_COST,
IFF(DIFF_PO != 0, PO_COST, 0 ) AS PO_COST
FROM
(SELECT PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER,
SUM(MANIFEST_COST) AS MANIFEST_COST,
SUM(SALES_COST) AS SALES_COST,
SUM(SHRINK_COST) AS SHRINK_COST,
SUM(ITC_COST) AS ITC_COST,
SUM(RLO_COST) AS RLO_COST,
SUM(PO_COST) AS PO_COST
FROM KSFDA.SHRINK_TACTICAL.PAPER_SHRINK_COST_ANALYSIS_Step_1
GROUP BY PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER) a 
JOIN "KSFDA"."SHRINK_TACTICAL"."SUMMARY_PAGE" pg
ON pg.PRODUCT_GENERATED_IDENTIFIER = a.PRODUCT_GENERATED_IDENTIFIER
AND pg.LOCATION_GENERATED_IDENTIFIER = a.LOCATION_GENERATED_IDENTIFIER
  )RES
)