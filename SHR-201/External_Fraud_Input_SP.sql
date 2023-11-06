CREATE OR REPLACE PROCEDURE KSF_SOPHIA_DATA_INTELLIGENCE_HUB_DEV.SHRINK_TACTICAL.LOAD_TEST_EXT_FRUAD_INPUT_SP()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
var table_query = `WITH 
POI_WITH_POTENTIAL AS(
(SELECT distinct * from (
  SELECT "Account Number" FROM ksfda.shrink_tactical.poi_data 
  UNION 
  SELECT HASHED_PAN AS "Account Number" FROM KSF_SOPHIA_DATA_INTELLIGENCE_HUB_DEV.SHRINK_TACTICAL.POTENTIAL_HASHED_PANS)
)
),

W_POI AS (
SELECT crd.m_store, crd.terminal_datetime AS DATED, crd.hashed_pan, crd.Amount
FROM ksfpa.switch.card crd 
JOIN POI_WITH_POTENTIAL poi
ON poi."Account Number" = crd.hashed_pan
),

CRD AS
(SELECT distinct M_STORE, m_trn_id, hashed_pan, c.terminal_datetime, c.Amount
      FROM ksfpa.switch.card c
      JOIN POI_WITH_POTENTIAL poi
      ON poi."Account Number" = c.hashed_pan
),

RES1 AS 
(SELECT distinct T1.t_card_no, CRD.hashed_pan, CRD.terminal_datetime AS DATED, CRD.Amount
FROM ksfpa.ips_sales.trn_tender T1
JOIN CRD
ON T1.m_trn_id = CRD.m_trn_id AND T1.M_STORE = CRD.M_STORE
WHERE a_tend_type = 16
),

W_VOUCHER AS
(SELECT T2.m_store, RES1.dated, RES1.hashed_pan, RES1.Amount
FROM RES1
JOIN ksfpa.ips_sales.trn_tender T2
ON T2.t_card_no = RES1.t_card_no)

SELECT m_store, TO_DATE(dated) as DATED, Count(hashed_pan) AS FREQ, SUM(AMOUNT) AS AMT
FROM ((SELECT distinct * FROM W_POI) UNION (SELECT distinct * FROM W_VOUCHER)) p
GROUP BY m_store, TO_DATE(dated)`

var merge_query = `MERGE INTO KSF_SOPHIA_DATA_INTELLIGENCE_HUB_DEV.SHRINK_TACTICAL.TEST_EXT_FRUAD_INPUT AS TGT USING (`+table_query+`) SRC
ON TGT.m_store = SRC.m_store AND TGT.dated = SRC.dated
WHEN MATCHED 
THEN UPDATE SET
TGT.FREQ = SRC.FREQ,
TGT.AMT = SRC.AMT

WHEN NOT MATCHED THEN INSERT(
m_store,
dated,
FREQ,
AMT
)
VALUES(
SRC.m_store,
SRC.dated,
SRC.FREQ,
SRC.AMT)`

var create_tbl_statement = snowflake.createStatement({sqlText:merge_query});
create_tbl_statement.execute();
var commit_exe = snowflake.createStatement({sqlText:''commit''});
commit_exe.execute();
return ''Done'';
';