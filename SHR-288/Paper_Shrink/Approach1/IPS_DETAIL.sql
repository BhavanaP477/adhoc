CREATE OR REPLACE TABLE KSFDA.SHRINK_TACTICAL.IPS_Detail6 AS
(
WITH 
W_MANIFEST AS (
  SELECT 
  PRODUCT_GENERATED_IDENTIFIER,
  LOCATION_GENERATED_IDENTIFIER, 
  DATEADD (DAY,
  7 - DAYOFWEEK (DAY_DATE),
  DAY_DATE
  ) AS DATE_,
  'MANIFEST' AS LINE_ITEM,
  SUM(STOCK_MOVEMENT_QUANTITY) AS LINE_VALUE
  FROM "KSFPA"."IPS_STOCK_AUDIT"."IPS_STOCK_AUDIT"
  WHERE
  STOCK_MOVEMENT_SOURCE = '1Manifest' 
  AND DAY_DATE > '2022-02-20' 
  and DAY_DATE <= '2023-02-26'
  GROUP BY
  PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER, DATE_
  ),

W_SALES AS (
  SELECT 
  PRODUCT_GENERATED_IDENTIFIER,
  LOCATION_GENERATED_IDENTIFIER, 
  DATEADD (DAY,
  7 - DAYOFWEEK (DAY_DATE),
  DAY_DATE
  ) AS DATE_,
  'SALES' AS LINE_ITEM,
  SUM(STOCK_MOVEMENT_QUANTITY) AS LINE_VALUE
  FROM "KSFPA"."IPS_STOCK_AUDIT"."IPS_STOCK_AUDIT"
  WHERE 
  (DAY_DATE > '2022-02-20' and 
  DAY_DATE <= '2023-02-26')
  AND (((STOCK_MOVEMENT_SOURCE = 'HOST') AND (STOCK_MOVEMENT_CODE IS NULL OR STOCK_MOVEMENT_CODE NOT IN ('00','0','02','2','03','3','04','4','5','05','7','07','08','8','11','17','20','21','29','34','35','36','37','50','51','53','54','55','56','57','58','60','61','62','63','64','65','66','67','68','69','70','NS','NP')))
  OR ((STOCK_MOVEMENT_SOURCE = 'OMS') AND (STOCK_MOVEMENT_CODE IS NULL OR STOCK_MOVEMENT_CODE NOT IN ('00','0','02','2','03','3','04','4','5','05','7','07','08','8','11','17','20','21','29','34','35','36','37','50','51','53','54','55','56','57','58','60','61','62','63','64','65','66','67','68','69','70','NS','NP')))
  OR ((STOCK_MOVEMENT_SOURCE = 'IPS') AND (STOCK_MOVEMENT_CODE IS NULL OR STOCK_MOVEMENT_CODE NOT IN ('00','0','02','2','03','3','04','4','5','05','7','07','08','8','11','17','20','21','29','34','35','36','37','50','51','53','54','55','56','57','58','60','61','62','63','64','65','66','67','68','69','70','NS','NP')))
  OR ((STOCK_MOVEMENT_SOURCE = 'WEB_ADJ') AND (STOCK_MOVEMENT_CODE IS NULL OR STOCK_MOVEMENT_CODE NOT IN ('00','0','02','2','03','3','04','4','5','05','7','07','08','8','11','17','20','21','29','34','35','36','37','50','51','53','54','55','56','57','58','60','61','62','63','64','65','66','67','68','69','70','NS','NP')))
  OR STOCK_MOVEMENT_SOURCE = 'SALES' OR STOCK_MOVEMENT_SOURCE = 'REFUND') 
  GROUP BY
  PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER, DATE_
  ),
  
W_SHRINK AS (
  SELECT 
  PRODUCT_GENERATED_IDENTIFIER,
  LOCATION_GENERATED_IDENTIFIER, 
  DATEADD (DAY,
  7 - DAYOFWEEK (DAY_DATE),
  DAY_DATE
  ) AS DATE_,
  'SHRINK' AS LINE_ITEM,
  SUM(STOCK_MOVEMENT_QUANTITY) AS LINE_VALUE
  FROM "KSFPA"."IPS_STOCK_AUDIT"."IPS_STOCK_AUDIT"
  WHERE
  ((STOCK_MOVEMENT_SOURCE = 'Shrinkage' AND STOCK_MOVEMENT_CODE IS NULL) or STOCK_MOVEMENT_CODE IN ('00','0','02','2','03','3','04','4','5','05','7','07','08','8','11','17','20','21','29','34','35','36','37','50','51','53','54','55','56','57','58','60','61','62','63','64','65','66','67','68','69','70','NS','NP'))
  AND DAY_DATE > '2022-02-20' 
  and DAY_DATE <= '2023-02-26'
  GROUP BY
  PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER, DATE_
  ),
  
W_ITC AS (
  SELECT 
  PRODUCT_GENERATED_IDENTIFIER,
  LOCATION_GENERATED_IDENTIFIER, 
  DATEADD (DAY,
  7 - DAYOFWEEK (DAY_DATE),
  DAY_DATE
  ) AS DATE_,
  'ITC' AS LINE_ITEM,
  SUM(STOCK_MOVEMENT_QUANTITY) AS LINE_VALUE
  FROM "KSFPA"."IPS_STOCK_AUDIT"."IPS_STOCK_AUDIT"
  WHERE 
  (STOCK_MOVEMENT_SOURCE = 'ERD_ITC_S' OR STOCK_MOVEMENT_SOURCE = 'ERD_ITC_R')
  AND DAY_DATE > '2022-02-20' 
  and DAY_DATE <= '2023-02-26'
  GROUP BY
  PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER, DATE_
),

W_RLO AS (
  SELECT 
  PRODUCT_GENERATED_IDENTIFIER,
  LOCATION_GENERATED_IDENTIFIER, 
  DATEADD (DAY,
  7 - DAYOFWEEK (DAY_DATE),
  DAY_DATE
  ) AS DATE_,
  'RLO' AS LINE_ITEM,
  SUM(STOCK_MOVEMENT_QUANTITY) AS LINE_VALUE
  FROM "KSFPA"."IPS_STOCK_AUDIT"."IPS_STOCK_AUDIT"
  WHERE 
  STOCK_MOVEMENT_SOURCE = 'RLO'
  AND DAY_DATE > '2022-02-20' 
  and DAY_DATE <= '2023-02-26'
  GROUP BY
  PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER, DATE_
),

W_PO AS (
  SELECT 
  PRODUCT_GENERATED_IDENTIFIER,
  LOCATION_GENERATED_IDENTIFIER, 
  DATEADD (DAY,
  7 - DAYOFWEEK (DAY_DATE),
  DAY_DATE
  ) AS DATE_,
  'PO' AS LINE_ITEM,
  SUM(STOCK_MOVEMENT_QUANTITY) AS LINE_VALUE
  FROM "KSFPA"."IPS_STOCK_AUDIT"."IPS_STOCK_AUDIT"
  WHERE 
  STOCK_MOVEMENT_SOURCE = 'PO'
  AND DAY_DATE > '2022-02-20' 
  and DAY_DATE <= '2023-02-26'
  GROUP BY
  PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER, DATE_
)

SELECT IPS.* exclude(PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER),
sm.PRODUCT_GENERATED_IDENTIFIER,sm.LOCATION_GENERATED_IDENTIFIER,sm.PRODUCT_SOURCE_IDENTIFIER,sm.LOCATION_SOURCE_IDENTIFIER,sm.LOCATION_NAME,sm.STATE,sm.PRODUCT_DESCRIPTION,sm.DEPARTMENT_DESCRIPTION,
DENSE_RANK() OVER (ORDER BY IPS.PRODUCT_GENERATED_IDENTIFIER, IPS.LOCATION_GENERATED_IDENTIFIER) AS rnk FROM
(
  SELECT PRODUCT_GENERATED_IDENTIFIER, 
  LOCATION_GENERATED_IDENTIFIER, 
  DATE_,
  LINE_ITEM,
  LINE_VALUE
  FROM W_MANIFEST
  
  UNION
 
  SELECT PRODUCT_GENERATED_IDENTIFIER, 
  LOCATION_GENERATED_IDENTIFIER, 
  DATE_,
  LINE_ITEM,
  LINE_VALUE
  FROM W_SALES 

  UNION
 
  SELECT PRODUCT_GENERATED_IDENTIFIER, 
  LOCATION_GENERATED_IDENTIFIER, 
  DATE_,
  LINE_ITEM,
  LINE_VALUE
  FROM W_SHRINK
   
  UNION
 
  SELECT PRODUCT_GENERATED_IDENTIFIER, 
  LOCATION_GENERATED_IDENTIFIER, 
  DATE_,
  LINE_ITEM,
  LINE_VALUE
  FROM W_ITC
  
  UNION
 
  SELECT PRODUCT_GENERATED_IDENTIFIER, 
  LOCATION_GENERATED_IDENTIFIER, 
  DATE_,
  LINE_ITEM,
  LINE_VALUE
  FROM W_RLO 
   
  UNION
 
  SELECT PRODUCT_GENERATED_IDENTIFIER, 
  LOCATION_GENERATED_IDENTIFIER, 
  DATE_,
  LINE_ITEM,
  LINE_VALUE
  FROM W_PO 
)IPS
  
JOIN KSFDA.SHRINK_TACTICAL.SUMMARY_PAGE_FY24 sm 
ON TO_NUMBER(CONCAT(IPS.PRODUCT_GENERATED_IDENTIFIER, '00004')) = sm.PRODUCT_GENERATED_IDENTIFIER
AND TO_NUMBER(CONCAT('1040',IPS.LOCATION_GENERATED_IDENTIFIER, '00001')) = sm.LOCATION_GENERATED_IDENTIFIER
  
WHERE sm.FLAG_REASON IS NOT NULL
)
