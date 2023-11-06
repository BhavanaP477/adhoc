WITH 
W_MANIFEST AS (
  SELECT 
  PRODUCT_GENERATED_IDENTIFIER,
  LOCATION_GENERATED_IDENTIFIER, 
  SUM(STOCK_MOVEMENT_QUANTITY) AS MANIFEST
  FROM "KSFPA"."IPS_STOCK_AUDIT"."IPS_STOCK_AUDIT"
  WHERE
  STOCK_MOVEMENT_SOURCE = '1Manifest' 
  AND DAY_DATE >= '2022-02-01' and DAY_DATE <= '2023-05-29'
  GROUP BY
  PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER
  ),

W_SALES AS (
  SELECT 
  PRODUCT_GENERATED_IDENTIFIER,
  LOCATION_GENERATED_IDENTIFIER, 
  SUM(STOCK_MOVEMENT_QUANTITY) AS SALES
  FROM "KSFPA"."IPS_STOCK_AUDIT"."IPS_STOCK_AUDIT"
  WHERE 
  (DAY_DATE >= '2022-02-01' and DAY_DATE <= '2023-05-29')
  AND (((STOCK_MOVEMENT_SOURCE = 'HOST') AND (STOCK_MOVEMENT_CODE IS NULL OR STOCK_MOVEMENT_CODE NOT IN ('00','0','02','2','03','3','04','4','5','05','7','07','08','8','11','17','20','21','29','34','35','36','37','50','51','53','54','55','56','57','58','60','61','62','63','64','65','66','67','68','69','70','NS','NP')))
  OR ((STOCK_MOVEMENT_SOURCE = 'OMS') AND (STOCK_MOVEMENT_CODE IS NULL OR STOCK_MOVEMENT_CODE NOT IN ('00','0','02','2','03','3','04','4','5','05','7','07','08','8','11','17','20','21','29','34','35','36','37','50','51','53','54','55','56','57','58','60','61','62','63','64','65','66','67','68','69','70','NS','NP')))
  OR ((STOCK_MOVEMENT_SOURCE = 'IPS') AND (STOCK_MOVEMENT_CODE IS NULL OR STOCK_MOVEMENT_CODE NOT IN ('00','0','02','2','03','3','04','4','5','05','7','07','08','8','11','17','20','21','29','34','35','36','37','50','51','53','54','55','56','57','58','60','61','62','63','64','65','66','67','68','69','70','NS','NP')))
  OR ((STOCK_MOVEMENT_SOURCE = 'WEB_ADJ') AND (STOCK_MOVEMENT_CODE IS NULL OR STOCK_MOVEMENT_CODE NOT IN ('00','0','02','2','03','3','04','4','5','05','7','07','08','8','11','17','20','21','29','34','35','36','37','50','51','53','54','55','56','57','58','60','61','62','63','64','65','66','67','68','69','70','NS','NP')))
  OR STOCK_MOVEMENT_SOURCE = 'SALES' OR STOCK_MOVEMENT_SOURCE = 'REFUND') 
  GROUP BY
  PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER
  ),
  
W_SHRINK AS (
  SELECT 
  PRODUCT_GENERATED_IDENTIFIER,
  LOCATION_GENERATED_IDENTIFIER, 
  SUM(STOCK_MOVEMENT_QUANTITY) AS SHRINK
  FROM "KSFPA"."IPS_STOCK_AUDIT"."IPS_STOCK_AUDIT"
  WHERE
  ((STOCK_MOVEMENT_SOURCE = 'Shrinkage' AND  STOCK_MOVEMENT_CODE IS NULL) OR STOCK_MOVEMENT_CODE IN ('00','0','02','2','03','3','04','4','5','05','7','07','08','8','11','17','20','21','29','34','35','36','37','50','51','53','54','55','56','57','58','60','61','62','63','64','65','66','67','68','69','70','NS','NP'))
  AND DAY_DATE >= '2022-02-01' and DAY_DATE <= '2023-05-29'
  GROUP BY
  PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER
  ),
  
W_ITC AS (
  SELECT 
  PRODUCT_GENERATED_IDENTIFIER,
  LOCATION_GENERATED_IDENTIFIER, 
  SUM(STOCK_MOVEMENT_QUANTITY) AS ITC
  FROM "KSFPA"."IPS_STOCK_AUDIT"."IPS_STOCK_AUDIT"
  WHERE 
  (STOCK_MOVEMENT_SOURCE = 'ERD_ITC_S' OR STOCK_MOVEMENT_SOURCE = 'ERD_ITC_R')
  AND DAY_DATE >= '2022-02-01' and DAY_DATE <= '2023-05-29'
  GROUP BY
  PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER
),

W_RLO AS (
  SELECT 
  PRODUCT_GENERATED_IDENTIFIER,
  LOCATION_GENERATED_IDENTIFIER, 
  SUM(STOCK_MOVEMENT_QUANTITY) AS RLO
  FROM "KSFPA"."IPS_STOCK_AUDIT"."IPS_STOCK_AUDIT"
  WHERE 
  STOCK_MOVEMENT_SOURCE = 'RLO'
  AND DAY_DATE >= '2022-02-01' and DAY_DATE <= '2023-05-29'
  GROUP BY
  PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER
),

W_PO AS (
  SELECT 
  PRODUCT_GENERATED_IDENTIFIER,
  LOCATION_GENERATED_IDENTIFIER, 
  SUM(STOCK_MOVEMENT_QUANTITY) AS PO
  FROM "KSFPA"."IPS_STOCK_AUDIT"."IPS_STOCK_AUDIT"
  WHERE 
  STOCK_MOVEMENT_SOURCE = 'PO'
  AND DAY_DATE >= '2022-02-01' and DAY_DATE <= '2023-05-29'
  GROUP BY
  PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER
),

W_KEY_CODE AS (
  SELECT 
  PRODUCT_GENERATED_IDENTIFIER, 
  DEPARTMENT_DESCRIPTION, 
  SEASON_CODE
  FROM KSFPA.MR2C.KEYCODE
  WHERE DEPARTMENT_DESCRIPTION not in ('GARDEN GREENS','MAGAZINES','REUSABLE BAGS','CARDS & WRAP','PHOTO CENTRE','GIFT CARDS & RCHRG')
)
 
SELECT DISTINCT PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER 
FROM
(
SELECT IPS.*, (IPS_MANIFEST + IPS_SALES + IPS_SHRINK + IPS_ITC + IPS_RLO + IPS_PO) AS IPS_DERIVED_SOH FROM
(
  SELECT MAN.PRODUCT_GENERATED_IDENTIFIER, 
  MAN.LOCATION_GENERATED_IDENTIFIER, 
  ZEROIFNULL(MAN.MANIFEST) AS IPS_MANIFEST, 
  ZEROIFNULL(SAL.SALES) AS IPS_SALES, 
  ZEROIFNULL(SHR.SHRINK) AS IPS_SHRINK, 
  ZEROIFNULL(ITC.ITC) AS IPS_ITC, 
  ZEROIFNULL(RLO.RLO) AS IPS_RLO, 
  ZEROIFNULL(PO.PO) AS IPS_PO
  FROM W_MANIFEST MAN 
 
  FULL OUTER JOIN W_SALES SAL 
  ON MAN.PRODUCT_GENERATED_IDENTIFIER = SAL.PRODUCT_GENERATED_IDENTIFIER
  AND MAN.LOCATION_GENERATED_IDENTIFIER = SAL.LOCATION_GENERATED_IDENTIFIER
   
  FULL OUTER JOIN W_SHRINK SHR
  ON MAN.PRODUCT_GENERATED_IDENTIFIER = SHR.PRODUCT_GENERATED_IDENTIFIER
  AND MAN.LOCATION_GENERATED_IDENTIFIER = SHR.LOCATION_GENERATED_IDENTIFIER
   
  FULL OUTER JOIN W_ITC ITC
  ON MAN.PRODUCT_GENERATED_IDENTIFIER = ITC.PRODUCT_GENERATED_IDENTIFIER
  AND MAN.LOCATION_GENERATED_IDENTIFIER = ITC.LOCATION_GENERATED_IDENTIFIER
   
  FULL OUTER JOIN W_RLO RLO
  ON MAN.PRODUCT_GENERATED_IDENTIFIER = RLO.PRODUCT_GENERATED_IDENTIFIER
  AND MAN.LOCATION_GENERATED_IDENTIFIER = RLO.LOCATION_GENERATED_IDENTIFIER
   
  FULL OUTER JOIN W_PO PO
  ON MAN.PRODUCT_GENERATED_IDENTIFIER = PO.PRODUCT_GENERATED_IDENTIFIER
  AND MAN.LOCATION_GENERATED_IDENTIFIER = PO.LOCATION_GENERATED_IDENTIFIER
) IPS

JOIN W_KEY_CODE KEY_CODE
ON TO_NUMBER(CONCAT(IPS.PRODUCT_GENERATED_IDENTIFIER, '00004')) = KEY_CODE.PRODUCT_GENERATED_IDENTIFIER
  )