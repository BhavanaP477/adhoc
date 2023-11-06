WITH 
DC_LIST AS 
(
	SELECT DISTINCT 
		LOCATION_GENERATED_IDENTIFIER ,
		CASE 
			WHEN LOCATION_GENERATED_IDENTIFIER = 1040960100001 THEN 'Jandakot_WA'
			WHEN LOCATION_GENERATED_IDENTIFIER = 1040940200001 THEN 'Lytton_QLD'
			WHEN LOCATION_GENERATED_IDENTIFIER = 1040932400001 THEN 'Truganina_VIC'
			WHEN LOCATION_GENERATED_IDENTIFIER = 1040925100001 THEN 'ECreek_NSW'
		END AS DC_NAME
	FROM KSFPA.MR2.ORDER_PRODUCT_DLV_LOCN
	WHERE LOCATION_GENERATED_IDENTIFIER IN 
	(
		  1040960100001 -- Jandakot - WA
		, 1040940200001 -- Lytton - QLD
		, 1040932400001 -- Truganina - VIC
		, 1040925100001 -- Eastern Creek - NSW
	)
)
,

GM_DEPTS AS 
(
	SELECT DEPARTMENT_SOURCE_IDENTIFIER 
	FROM KSFPA.LOSS_PREVENTION_PVT.DEPARTMENT_GM_APPAREL_CATEGORY
	WHERE CATEGORY = 'GM'
)
,

SKU_SUBSET AS 
(
	SELECT PRODUCT_GENERATED_IDENTIFIER 
	FROM KSFPA.MR2C.KEYCODE
	WHERE DEPARTMENT_SOURCE_IDENTIFIER IN ( SELECT * FROM GM_DEPTS )
		AND DEPARTMENT_DESCRIPTION NOT IN ('GARDEN GREENS','MAGAZINES','REUSABLE BAGS','CARDS & WRAP','PHOTO CENTRE','GIFT CARDS & RCHRG')
		AND PRODUCT_GENERATED_IDENTIFIER IN 
			(	
				SELECT DISTINCT PRODUCT_GENERATED_IDENTIFIER
				FROM KSFPA.MR2.SS_STOCK_ADJUSTMENT SS
				WHERE   PERIOD_END_DATE > '2022-02-20'
				    AND STOCK_ADJUSTMENT_REASON_CODE = '36'
				    AND LOCATION_GENERATED_IDENTIFIER = 1040932400001 -- TRUGANINA
					AND STOCK_ADJUSTMENT_QUANTITY > 0
			)
)
,

SALES AS 
(
	SELECT 
		PRODUCT_GENERATED_IDENTIFIER 
		, MIN(WEEK_END_DATE) AS FIRST_SALES_DATE
	FROM KSFPA.MR2C.WEEKLY_ADJUSTED_SALES
	WHERE 
			WEEKLY_NET_SALES_QUANTITY != 0
		AND PRODUCT_GENERATED_IDENTIFIER IN ( SELECT * FROM SKU_SUBSET )
	GROUP BY 1
)
,

PO1 AS 
(
	SELECT
		ORDER_GENERATED_IDENTIFIER 
		, PRODUCT_GENERATED_IDENTIFIER 
		, LOCATION_GENERATED_IDENTIFIER 
		, COALESCE(AGREED_ORDER_QUANTITY,ORDER_QUANTITY) AS ORD_QTY
	FROM KSFPA.MR2.ORDER_PRODUCT_DLV_LOCN
	WHERE 
			EXPIRY_DATE = '9999-12-31'
--		AND EFFECTIVE_DATE > '2022-02-20'
		AND PRODUCT_GENERATED_IDENTIFIER IN ( SELECT * FROM SKU_SUBSET )
)
,

PO2 AS 
(
	SELECT 
		ORDER_GENERATED_IDENTIFIER 
		, ORDER_SOURCE_IDENTIFIER 
--		, ORDER_STATUS_CODE
--		, WEEK_END_DATE 
	FROM KSFPA.MR2C.PURCHASE_ORDER 
	WHERE ORDER_STATUS_CODE IN ('FD','XB')
--		AND WEEK_END_DATE > '2022-02-20'
--		AND WEEK_END_DATE <= '2022-10-23'
)
,

PO AS 
(
	SELECT 
		PO1.ORDER_GENERATED_IDENTIFIER
		, PRODUCT_GENERATED_IDENTIFIER 
		, LOCATION_GENERATED_IDENTIFIER 
		, ORD_QTY
	FROM 
		PO1 AS PO1
	INNER JOIN 
		PO2 AS PO2
	ON PO1.ORDER_GENERATED_IDENTIFIER = PO2.ORDER_GENERATED_IDENTIFIER
	WHERE PRODUCT_GENERATED_IDENTIFIER IN ( SELECT * FROM SKU_SUBSET )
)
,

RO AS 
(
	SELECT 
		ORDER_GENERATED_IDENTIFIER 
		, LOCATION_GENERATED_IDENTIFIER 
		, PRODUCT_GENERATED_IDENTIFIER 
		, RECEIPT_DATE 
		, RECEIPT_QUANTITY 
	FROM KSFPA.MR2C.RECEIPT_DETAIL_WORK 
	WHERE 
		PRODUCT_GENERATED_IDENTIFIER IN ( SELECT * FROM SKU_SUBSET )
--		AND RECEIPT_DATE > '2022-02-20'
--		AND RECEIPT_DATE <= '2022-10-23'
)
,

SHORT_SHIPPED AS 
(
	SELECT 
		PO_.PRODUCT_GENERATED_IDENTIFIER 
		, PO_.ORDER_GENERATED_IDENTIFIER
		, PO_.LOCATION_GENERATED_IDENTIFIER
		, ASKED_QTY 
		, RECIEVED_QTY 
		, ASKED_QTY - RECIEVED_QTY AS SHORT_SHIPPED_QTY
	FROM 
		(
			SELECT 
				  PRODUCT_GENERATED_IDENTIFIER
				, ORDER_GENERATED_IDENTIFIER
				, LOCATION_GENERATED_IDENTIFIER 
				, SUM(ORD_QTY) AS ASKED_QTY 
			FROM PO
			GROUP BY 1,2,3
		)PO_
	LEFT JOIN 
		(
			SELECT 
				  PRODUCT_GENERATED_IDENTIFIER
				, ORDER_GENERATED_IDENTIFIER
				, LOCATION_GENERATED_IDENTIFIER 
				, SUM(RECEIPT_QUANTITY) AS RECIEVED_QTY
			FROM RO
			GROUP BY 1,2,3
		)RO_ 
	ON  RO_.PRODUCT_GENERATED_IDENTIFIER 	= PO_.PRODUCT_GENERATED_IDENTIFIER 
	AND RO_.ORDER_GENERATED_IDENTIFIER 		= PO_.ORDER_GENERATED_IDENTIFIER 
	AND RO_.LOCATION_GENERATED_IDENTIFIER 	= PO_.LOCATION_GENERATED_IDENTIFIER
	
	WHERE PO_.PRODUCT_GENERATED_IDENTIFIER IN ( SELECT * FROM SKU_SUBSET )
)
,

SHRINK AS
(
		SELECT
		    SS.PRODUCT_GENERATED_IDENTIFIER ,
		    SS.LOCATION_GENERATED_IDENTIFIER ,
		    SUM( STOCK_ADJUSTMENT_COST_AMOUNT ) AS NET_SHRINK_VALUE,
		    SUM( STOCK_ADJUSTMENT_QUANTITY ) AS NET_SHRINK_QTY
		FROM
		    KSFPA.MR2.SS_STOCK_ADJUSTMENT SS
		WHERE
		    	PERIOD_END_DATE > '2022-02-20'
		    AND STOCK_ADJUSTMENT_REASON_CODE = '36'
		    AND PRODUCT_GENERATED_IDENTIFIER IN ( SELECT * FROM SKU_SUBSET )
		    AND LOCATION_GENERATED_IDENTIFIER IN ( SELECT LOCATION_GENERATED_IDENTIFIER FROM DC_LIST )
		GROUP BY 1,2
		HAVING SUM( STOCK_ADJUSTMENT_QUANTITY ) > 0
)
,
	
FINAL_VIEW AS
(
	SELECT * 
	FROM 
		(
		SELECT 
--			COALESCE( SS.PRODUCT_GENERATED_IDENTIFIER , SH.PRODUCT_GENERATED_IDENTIFIER ) AS PRODUCT_GENERATED_IDENTIFIER 
			SS.PRODUCT_GENERATED_IDENTIFIER 
			, SS.LOCATION_GENERATED_IDENTIFIER 
			, DC_LIST.DC_NAME
			, KC.PRODUCT_DESCRIPTION AS PRODUCT_DESCRIPTION 
			, NET_SHRINK_QTY AS NET_SHRINK_QTY
			, NET_SHRINK_VALUE AS NET_SHRINK_VALUE
			, SUM(ASKED_QTY) AS ASKED_QTY
			, SUM(RECIEVED_QTY) AS RECIEVED_QTY
			, SUM(SHORT_SHIPPED_QTY) AS SS_QTY
			, CASE  WHEN SUM( IFNULL ( ASKED_QTY, 0 ) ) != 0
					THEN SUM(SHORT_SHIPPED_QTY) / SUM(ASKED_QTY) 
					ELSE 0 END
				AS SS_QTY_REL	
			
	
--			, CASE WHEN SS_QTY > 0 THEN 1 ELSE 0 END AS SS_POS
--			, CASE WHEN SS_QTY < 0 THEN 1 ELSE 0 END AS SS_NEG
--			, CASE WHEN SS_QTY = 0 OR SS_QTY IS NULL THEN 1 ELSE 0 END AS SS_ZERO
--			
--			, CASE WHEN SS_QTY_REL >  0.05 THEN 1 ELSE 0 END AS SS_POS_FOS
--			, CASE WHEN SS_QTY_REL < -0.05 THEN 1 ELSE 0 END AS SS_NEG_FOS
--			
--			, CASE  WHEN ( SS_QTY_REL <= 0.05 AND SS_QTY_REL >= -0.05 ) 
--					OR SS_QTY_REL IS NULL 
--					THEN 1 ELSE 0 
--					END AS SS_ZERO_FOS
			
--			, MODE ( SL.FIRST_SALES_DATE ) AS FIRST_SALE_DATE
	
			
		FROM SHORT_SHIPPED AS SS
			
		RIGHT JOIN SHRINK SH
			ON  SS.PRODUCT_GENERATED_IDENTIFIER  = SH.PRODUCT_GENERATED_IDENTIFIER
			AND SS.LOCATION_GENERATED_IDENTIFIER = SH.LOCATION_GENERATED_IDENTIFIER 
	
		LEFT JOIN SALES AS SL
			ON SS.PRODUCT_GENERATED_IDENTIFIER = SL.PRODUCT_GENERATED_IDENTIFIER
		
		INNER JOIN DC_LIST AS DC_LIST
			ON SS.LOCATION_GENERATED_IDENTIFIER = DC_LIST.LOCATION_GENERATED_IDENTIFIER 

		LEFT JOIN KSFPA.MR2C.KEYCODE KC 
	        ON SS.PRODUCT_GENERATED_IDENTIFIER = KC.PRODUCT_GENERATED_IDENTIFIER

		WHERE 
				SS.PRODUCT_GENERATED_IDENTIFIER IN ( SELECT PRODUCT_GENERATED_IDENTIFIER FROM SHRINK )
			AND SL.FIRST_SALES_DATE >= '2022-02-20'  

	
		GROUP BY 1,2,3,4,5,6
		ORDER BY 1,2 DESC
		)
--	WHERE NOT ( SS_QTY = 0 AND NET_SHRINK_QTY IS NULL )
)







--SELECT * FROM FINAL_VIEW ;

-------------------------------------------------------------------------

 SELECT PRODUCT_DESCRIPTION , PRODUCT_GENERATED_IDENTIFIER , DC_NAME , NET_SHRINK_VALUE , NET_SHRINK_QTY , SS_QTY AS SHORT_SHIPPED 
 FROM FINAL_VIEW
 WHERE ABS( SS_QTY ) = ABS( NET_SHRINK_QTY )
 	AND LOCATION_GENERATED_IDENTIFIER = 1040932400001
 ORDER BY NET_SHRINK_VALUE DESC

-------------------------------------------------------------------------


--SELECT 
--	PRODUCT_GENERATED_IDENTIFIER 
--	
--	, IFNULL("'Jandakot_WA'",0) 	AS Jandakot_WA
--	, IFNULL("'Lytton_QLD'" , 0) 	AS Lytton_QLD
--	, IFNULL("'ECreek_NSW'" , 0) 	AS ECreek_NSW
--	, IFNULL("'Truganina_VIC'" , 0) AS Truganina_VIC
--	
--	, IFNULL("'Jandakot_WA'",0) + IFNULL("'Lytton_QLD'" , 0) + IFNULL("'ECreek_NSW'" , 0) + IFNULL("'Truganina_VIC'" , 0) 	AS Balance
--	
--	, ABS(IFNULL("'Jandakot_WA'",0) ) + ABS(IFNULL("'Lytton_QLD'" , 0) ) + ABS(IFNULL("'ECreek_NSW'" , 0) ) + ABS(IFNULL("'Truganina_VIC'" , 0) ) 	AS Magnitude
--	
--	, CASE WHEN magnitude <> 0 THEN ABS( Balance / Magnitude ) ELSE 2 END AS METRIC
--	
--		
--FROM 
--(
--	SELECT 
--		PRODUCT_GENERATED_IDENTIFIER , 
----		LOCATION_GENERATED_IDENTIFIER ,
--		DC_NAME ,
--		SS_QTY
--	FROM FINAL_VIEW
--)
--
--PIVOT
--(
--	SUM( SS_QTY )
--	FOR DC_NAME IN 
--	(
--		  'Jandakot_WA'
--		, 'Lytton_QLD'
--		, 'Truganina_VIC'
--		, 'ECreek_NSW'
--	)
--)
----WHERE NOT 
----(
----		( "'Jandakot_WA'" 	IS NULL OR "'Jandakot_WA'" 		= 0 )
----	AND ( "'Lytton_QLD'" 	IS NULL OR "'Lytton_QLD'" 		= 0 )
----	AND ( "'ECreek_NSW'" 	IS NULL OR "'ECreek_NSW'" 		= 0 )
----	AND ( "'Truganina_VIC'" IS NULL OR "'Truganina_VIC'" 	= 0 )
----)
--
--ORDER BY Metric ASC ;

-------------------------------------------------------------------------


--SELECT * 
--FROM FINAL_VIEW 
--WHERE DC_NAME IN ( 'ECreek_NSW' , 'Truganina_VIC' )
--ORDER BY PRODUCT_GENERATED_IDENTIFIER , LOCATION_GENERATED_IDENTIFIER DESC ;

--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------

-- NOTES

-- MR2C.FFRECEIPT TABLE
-- I TRADE
-- KDW
-- MR 2 .container 
-- of positive shrink keycodes get subset where first trn 
-- MR2C weekly adjusted sales
 
 
 