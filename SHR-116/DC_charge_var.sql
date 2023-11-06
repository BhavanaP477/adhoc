WITH 
DCVAR AS 
(
SELECT * FROM 
(
SELECT

		-- LEVEL --
    ITEM ,
    ORDER_NO ,
	RECON_WEEK ,
		-- LEVEL --
    RECONCILIATION_STATUS , RECONCILIATION_MESSAGE ,  -- RECON STATUS AND MSG ARE 1-1
    
    DC_RECEIPT_AMT AS RECEIPT_AMT ,
    DC_CHARGE_AMT AS CHARGE_AMT ,
    DC_RECEIPT_VAR_AMT AS VARIANCE_AMT ,

    DC_RECEIPT_QTY AS RECEIPT_QTY ,
    DC_CHARGE_QTY AS CHARGE_QTY ,
    DC_RECEIPT_VAR_QTY AS VARIANCE_QTY ,

    ORDER_STATUS  ,
    SUPPLIER ,
    VARIANCE_NOTE , -- 174 UNIQUE ,

    PERIOD ,
    ITEM_NAME ,
    SUPPLIER_NAME ,
    COUNTRY ,
    
    ROW_NUMBER() 
		OVER 
    		( 
			PARTITION BY ITEM , ORDER_NO 
			ORDER BY RECON_WEEK  desc
			) 
		AS RECON_WEEK_LATEST
		
FROM KSF_SOPHIA_DATA_INTELLIGENCE_HUB_DEV.SHRINK_TACTICAL.DC_CHARGE_VARIANCE
--WHERE 
--	ORDER_STATUS IN ( 'FD' , 'XB' ) AND
--	RECON_WEEK_LATEST = 1

ORDER BY 1,2,3,4,5
)
--WHERE 
--	RECON_WEEK_LATEST <= 1
)


-------------------------------------------------------------------------------

--SELECT ORDER_STATUS , COUNT(*) 
--FROM DCVAR
--GROUP BY 1
--;
		-- FD	39486
		-- XB	18622
		-- PD	1984
		-- OP	5

-------------------------------------------------------------------------------
	
--SELECT RECONCILIATION_STATUS , RECONCILIATION_MESSAGE , COUNT(*)
--FROM DCVAR 
--GROUP BY 1,2

		--1	PK Matched Value Matched			39648
		--2	PK Matched Value Under Tolerance	18477
		--3	PK Matched Value Over Tolerance		 1167
		--5	In Charge Not In Receipt			  215
		--6	In Receipt Not In Charge			  590

-------------------------------------------------------------------------------

-- entries which violate level of the Data 
-- level being WEEK-ORDER-ITEM

-- 135 Entries of which alll are duplicates

--SELECT * 
--FROM DCVAR
--WHERE RECON_WEEK||ORDER_NO||ITEM 
--IN (
--	SELECT RECON_WEEK||ORDER_NO||ITEM --, COUNT(*)
--	FROM DCVAR
--	GROUP BY 1
--	HAVING COUNT(*) > 1
--)
--ORDER BY 3,2,1


-------------------------------------------------------------------------------

-- Testing where entries which violate level of the Data 

--of 155 , 128 are 5/6 discrepancies

--SELECT *
--FROM DCVAR A
--INNER JOIN
--	(
--		SELECT ORDER_NO,ITEM,RECON_WEEK , COUNT(*)
--		FROM DCVAR
--		GROUP BY 1,2,3
--		HAVING COUNT(*) > 1
--	) B
--ON 
--		A.ORDER_NO = B.ORDER_NO 
--	AND A.ITEM = B.ITEM 
--	AND A.RECON_WEEK = B.RECON_WEEK 
--	
----AND RECONCILIATION_STATUS NOT IN ( 5,6 )
--ORDER BY 1,2,3,4



-------------------------------------------------------------------------------
-- Variance calculation discreapncy , 

-- it is not the case that this discrepancy only exists for NON-  FD and XB orders only , it is for all 

-- it is not the case that the Variance QTY matches but the Varaince amount has a Error , or vice-versa
-- there is a one to one mapping for this discrepancies between the amount , and 

-- alot of SKUs are also coincidng wiht the duplicate entry subset 
-- where the RECON STATUS is suplicated once with status 5 and once with status 6

-- Mght need to take one SKU and do a deep dive 

-- it is possible that the varaince might be calculated at some different aggregated level 

--SELECT * FROM DCVAR
--WHERE 
----	ORDER_STATUS IN ( 'FD' , 'XB' ) AND
--	ABS(RECEIPT_QTY - CHARGE_QTY) <> ABS(VARIANCE_QTY)
--	AND
--	ROUND( ABS(RECEIPT_AMT - CHARGE_AMT) ,0) <> ROUND( ABS(VARIANCE_AMT) ,0)
--ORDER BY ITEM , ORDER_NO , RECON_WEEK , RECONCILIATION_STATUS 
	

-------------------------------------------------------------------------------
-- Variance calculation discreapncy DEEP DIVE


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
--		A.ORDER_NO = B.ORDER_NO 
--	AND A.ITEM = B.ITEM 
--	AND A.RECON_WEEK = B.RECON_WEEK 
--ORDER BY 1,2,3


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