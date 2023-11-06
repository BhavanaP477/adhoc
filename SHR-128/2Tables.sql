--SELECT * FROM KSFPA.MR2C.DC_CHARGE_RECONCILIATION_STATUS ;
-------------------------------------------------------------------------------

--NOTES :
-- left join the status code meaning
-- left join the PO table to get order status
-- 

-------------------------------------------------------------------------------


WITH 
DCVAR AS 
(
SELECT

	-- LEVEL --
	RECON_WEEK_END_DATE AS WEEK_END_DATE ,
	
    ITEM_GENERATED_IDENTIFIER  		AS ITEM_ID,
    ORDER_GENERATED_IDENTIFIER 		AS ORDER_ID ,
	LOCATION_GENERATED_IDENTIFIER 	AS LOCATION_ID ,
	-- LEVEL --
	
	SUPPLIER_GENERATED_IDENTIFIER AS SUPPLIER_ID ,
    DC_CHARGE_RECONCILIATION_STATUS_CODE AS RECON_CODE ,
    
    OS_RECEIPT_COST_AMT 	AS RECEIPT_AMT ,
    OS_CHARGE_COST_AMT  	AS CHARGE_AMT ,
    OS_RECEIPT_VAR_COST_AMT AS VARIANCE_AMT ,

    OS_RECEIPT_QUANTITY 	AS RECEIPT_QTY ,
    OS_CHARGE_QTY 			AS CHARGE_QTY ,
    OS_RECEIPT_VAR_QUANTITY AS VARIANCE_QTY ,
    
    ROW_NUMBER() 
		OVER 
    		( 
			PARTITION BY ITEM_GENERATED_IDENTIFIER , ORDER_GENERATED_IDENTIFIER  
			ORDER BY RECON_WEEK_END_DATE DESC
			) 
		AS LATEST_WEEK_RANK
		
FROM KSFPA.MR2C.DC_CHARGE_RECONCILIATION 
ORDER BY 2,3,4,5
)



-------------------------------------------------------------------------------
	
--SELECT RECON_CODE , COUNT(*)
--FROM DCVAR 
--GROUP BY 1

		-- 1	1557408
		-- 2	529728
		-- 6	32853
		-- 3	31703
		-- 5	10051

-------------------------------------------------------------------------------

-- entries which violate level of the Data 
-- level being WEEK-ORDER-ITEM

-- 3893 Entries of which alll are duplicates

SELECT * 
FROM DCVAR
WHERE WEEK_END_DATE || ITEM_ID || ORDER_ID 
IN 
	(
	SELECT WEEK_END_DATE || ITEM_ID || ORDER_ID --, COUNT(*)
	FROM DCVAR
	GROUP BY 1
	HAVING COUNT(*) > 1
	)
ORDER BY 1,2,3


-------------------------------------------------------------------------------

-- Testing where entries which violate level of the Data 

--of 3893 , only 397 are non 5|6 discrepancies

--SELECT *
--FROM DCVAR A
--INNER JOIN
--	(
--		SELECT WEEK_END_DATE , ITEM_ID , ORDER_ID , COUNT(*)
--		FROM DCVAR
--		GROUP BY 1,2,3
--		HAVING COUNT(*) > 1
--	) B
--ON 
--		A.ORDER_ID = B.ORDER_ID 
--	AND A.ITEM_ID = B.ITEM_ID
--	AND A.WEEK_END_DATE =  B.WEEK_END_DATE 
--	
--AND RECON_CODE NOT IN ( 5,6 )
--ORDER BY 2,3,4



-------------------------------------------------------------------------------
-- Variance calculation discreapncy , 
-- 1241 Data points of discrepancy

-- it is not the case that this discrepancy only exists for NON-  FD and XB orders only , it is for all 

-- it is not the case that the Variance QTY matches but the Varaince amount has a Error , or vice-versa
-- there is a one to one mapping for this discrepancies between the amount , and 

-- alot of SKUs are also coincidng wiht the duplicate entry subset 
-- where the RECON STATUS is suplicated once with status 5 and once with status 6

--SELECT * FROM DCVAR
--WHERE 
--	ABS(RECEIPT_QTY - CHARGE_QTY) <> ABS(VARIANCE_QTY)
--	OR
--	ROUND( ABS(RECEIPT_AMT - CHARGE_AMT) ,0) <> ROUND( ABS(VARIANCE_AMT) ,0)
--ORDER BY ITEM_ID , ORDER_ID , WEEK_END_DATE , RECON_CODE
	

-------------------------------------------------------------------------------
-- Variance calculation discreapncy DEEP DIVE
-- 2241 Instances affected 

--SELECT *
--FROM DCVAR A
--INNER JOIN
--	(
--	SELECT * FROM DCVAR
--	WHERE 
--		ABS(RECEIPT_QTY - CHARGE_QTY) <> ABS(VARIANCE_QTY)
--		AND
--		ROUND( ABS(RECEIPT_AMT - CHARGE_AMT) ,0) <> ROUND( ABS(VARIANCE_AMT) ,0)
--	) B
--ON 
--		A.ORDER_ID = B.ORDER_ID 
--	AND A.ITEM_ID = B.ITEM_ID
--	AND A.WEEK_END_DATE = B.WEEK_END_DATE 
--ORDER BY 2,3,1


-------------------------------------------------------------------------------

--SELECT ORDER_STATUS , COUNT(*) 
--FROM DCVAR
--GROUP BY 1
--;
		-- FD	39486????
		-- XB	18622????
		-- PD	1984????
		-- OP	5????

-------------------------------------------------------------------------------


--SELECT * FROM DCVAR 


-------------------------------------------------------------------------------

--SELECT 
----	ORDER_NO , 
--	ITEM ,
--	SUM(ABS( DC_RECEIPT_VAR_AMT ))	AS SUM_ABS_VAR_AMT ,
--	SUM( DC_RECEIPT_VAR_AMT )	 	AS SUM_NET_VAR_AMT 
--FROM DCVAR 
--GROUP BY 1
--HAVING SUM_ABS_VAR_AMT > 1000 OR SUM_NET_VAR_AMT > 1000
--ORDER BY 2 DESC




-------------------------------------------------------------------------------

--    CASE 
--		WHEN NOT
--			(
--				GREATEST( ABS(DC_CHARGE_AMT) , ABS(DC_RECEIPT_AMT) ) = 0
--				OR
--				GREATEST( ABS(DC_CHARGE_AMT) , ABS(DC_RECEIPT_AMT) ) IS NULL
--			)
--		THEN DC_RECEIPT_VAR_AMT / GREATEST( ABS(DC_CHARGE_AMT) , ABS(DC_RECEIPT_AMT) ) 
--		ELSE 0	
--    END *100 AS VARIANCE_AMT_REL,

-------------------------------------------------------------------------------


-------------------------------------------------------------------------------




-------------------------------------------------------------------------------




-------------------------------------------------------------------------------




-------------------------------------------------------------------------------




-------------------------------------------------------------------------------
