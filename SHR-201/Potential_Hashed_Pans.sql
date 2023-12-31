CREATE OR REPLACE TABLE KSF_SOPHIA_DATA_INTELLIGENCE_HUB_DEV.SHRINK_TACTICAL.POTENTIAL_HASHED_PANS AS
(
Select distinct HASHED_PAN from
(
  SELECT coalesce (a.HASHED_PAN, b.HASHED_PAN) AS HASHED_PAN, IFNULL(REFUND_AMT, 0), IFNULL(PURCHASE_AMT,0) FROM
  (
    SELECT HASHED_PAN, SUM(AMOUNT) AS REFUND_AMT FROM
    (
      SELECT HASHED_PAN, AMOUNT FROM "KSFPA"."SWITCH"."CARD" 
      WHERE AMOUNT < 0 
      AND ON2_RESP_CODE in ('0000')
      and (ON2_Trans_Code is NULL or ON2_Trans_Code like '02%' or  ON2_Trans_Code like '04%' or ON2_Trans_Code like '05%')
      AND DATE(TERMINAL_DATETIME) > '2020-08-01'
    )
    GROUP BY HASHED_PAN)a
  
  FULL OUTER JOIN

  (
    SELECT HASHED_PAN, SUM(AMOUNT) AS PURCHASE_AMT FROM
    (
      SELECT HASHED_PAN, AMOUNT FROM "KSFPA"."SWITCH"."CARD" 
      WHERE AMOUNT > 0
      AND ON2_RESP_CODE in ('0000')
      and (ON2_Trans_Code is NULL or ON2_Trans_Code like '02%' or  ON2_Trans_Code like '04%' or ON2_Trans_Code like '05%')
      AND DATE(TERMINAL_DATETIME) > '2020-06-01'
    )
    GROUP BY HASHED_PAN)b
  on a.HASHED_PAN = b.HASHED_PAN 
  WHERE ABS(IFNULL(REFUND_AMT, 0)) > IFNULL(PURCHASE_AMT,0) + 10
)
)