CREATE TABLE KSFDA.SHRINK_TACTICAL.PAPER_SHRINK_ALL AS
(
SELECT A.*,
sum(IPS_MANIFEST + IPS_SALES + IPS_SHRINK + IPS_ITC + IPS_RLO + IPS_PO) over (partition by PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER order by DATE_ asc rows between unbounded preceding and current row) AS IPS_DERIVED_SOH,
sum(MR_MANIFEST + MR_SALES + MR_SHRINK + MR_ITC + MR_RLO + MR_PO) over (partition by PRODUCT_GENERATED_IDENTIFIER, LOCATION_GENERATED_IDENTIFIER order by DATE_ asc rows between unbounded preceding and current row) AS MR_DERIVED_SOH 
FROM

(
SELECT IPS2.PRODUCT_GENERATED_IDENTIFIER,
IPS2.LOCATION_GENERATED_IDENTIFIER,
IPS2.PRODUCT_SOURCE_IDENTIFIER,
IPS2.LOCATION_SOURCE_IDENTIFIER,
IPS2.LOCATION_NAME,
IPS2.STATE,
IPS2.PRODUCT_DESCRIPTION,
IPS2.DEPARTMENT_DESCRIPTION,
IPS2.DATE_,
ZEROIFNULL(IPS2."'MANIFEST'") AS IPS_MANIFEST,
ZEROIFNULL(MR2."'MANIFEST'") AS MR_MANIFEST,
ZEROIFNULL(IPS2."'SALES'") AS IPS_SALES,
ZEROIFNULL(MR2."'SALES'") AS MR_SALES,
ZEROIFNULL(IPS2."'SHRINK'") AS IPS_SHRINK,
ZEROIFNULL(MR2."'SHRINK'") AS MR_SHRINK,
ZEROIFNULL(IPS2."'ITC'") AS IPS_ITC,
ZEROIFNULL(MR2."'ITC'") AS MR_ITC,
ZEROIFNULL(IPS2."'RLO'") AS IPS_RLO,
ZEROIFNULL(MR2."'RLO'") AS MR_RLO,
ZEROIFNULL(IPS2."'PO'") AS IPS_PO,
ZEROIFNULL(MR2."'PO'") AS MR_PO,
sum(IPS2."'SALES'") over (partition by IPS2.PRODUCT_GENERATED_IDENTIFIER, IPS2.LOCATION_GENERATED_IDENTIFIER order by IPS2.DATE_ asc rows between unbounded preceding and current row) AS IPS_CUMULATIVE_SALES,
sum(MR2."'SALES'") over (partition by MR2.PRODUCT_GENERATED_IDENTIFIER, MR2.LOCATION_GENERATED_IDENTIFIER order by MR2.DATE_ asc rows between unbounded preceding and current row) AS MR_CUMULATIVE_SALES,
IPS2.RNK
FROM
-------------------------------------------------------IPS--------------------------------------------------
(SELECT IPS1.* , rnk.RNK FROM
(SELECT * exclude(RNK) FROM KSFDA.SHRINK_TACTICAL.IPS_DETAIL_ALL
PIVOT(SUM(LINE_VALUE) FOR LINE_ITEM IN ('MANIFEST', 'SALES', 'SHRINK', 'ITC', 'RLO', 'PO')))IPS1


JOIN

(SELECT distinct ips.PRODUCT_GENERATED_IDENTIFIER, ips.LOCATION_GENERATED_IDENTIFIER, 
DENSE_RANK() OVER(ORDER BY ips.PRODUCT_GENERATED_IDENTIFIER, ips.LOCATION_GENERATED_IDENTIFIER) AS RNK
FROM KSFDA.SHRINK_TACTICAL.IPS_DETAIL_ALL ips 
JOIN KSFDA.SHRINK_TACTICAL.MR_DETAIL_ALL mr
ON ips.PRODUCT_GENERATED_IDENTIFIER = mr.PRODUCT_GENERATED_IDENTIFIER
AND ips.LOCATION_GENERATED_IDENTIFIER = mr.LOCATION_GENERATED_IDENTIFIER
) RNK
ON IPS1.PRODUCT_GENERATED_IDENTIFIER = RNK.PRODUCT_GENERATED_IDENTIFIER
AND IPS1.LOCATION_GENERATED_IDENTIFIER = RNK.LOCATION_GENERATED_IDENTIFIER)IPS2

JOIN

-----------------------------------------------------MR----------------------------------------------------------
(SELECT MR1.* , rnk.RNK FROM
(SELECT * exclude(RNK) FROM KSFDA.SHRINK_TACTICAL.MR_DETAIL_ALL
PIVOT(SUM(LINE_VALUE) FOR LINE_ITEM IN ('MANIFEST', 'SALES', 'SHRINK', 'ITC', 'RLO', 'PO')))MR1

JOIN

(SELECT distinct ips.PRODUCT_GENERATED_IDENTIFIER, ips.LOCATION_GENERATED_IDENTIFIER, 
DENSE_RANK() OVER(ORDER BY ips.PRODUCT_GENERATED_IDENTIFIER, ips.LOCATION_GENERATED_IDENTIFIER) AS RNK
FROM KSFDA.SHRINK_TACTICAL.IPS_DETAIL_ALL ips 
JOIN KSFDA.SHRINK_TACTICAL.MR_DETAIL_ALL mr
ON ips.PRODUCT_GENERATED_IDENTIFIER = mr.PRODUCT_GENERATED_IDENTIFIER
AND ips.LOCATION_GENERATED_IDENTIFIER = mr.LOCATION_GENERATED_IDENTIFIER
) RNK
ON MR1.PRODUCT_GENERATED_IDENTIFIER = RNK.PRODUCT_GENERATED_IDENTIFIER
AND MR1.LOCATION_GENERATED_IDENTIFIER = RNK.LOCATION_GENERATED_IDENTIFIER) MR2

ON MR2.RNK = IPS2.RNK
AND MR2.DATE_ = IPS2.DATE_
ORDER BY RNK, IPS2.DATE_

)A
ORDER BY RNK, DATE_
)