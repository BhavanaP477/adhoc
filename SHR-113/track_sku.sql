WITH 

SKU_DETAILS AS 
(
	SELECT 
		PRODUCT_DESCRIPTION , ITEM_DESCRIPTION
		, RBU_DESCRIPTION , DIVISION_DESCRIPTION , DEPARTMENT_DESCRIPTION
		, MINOR_DEPARTMENT_DESCRIPTION , FAMILY_DESCRIPTION 
		, CLASS_DESCRIPTION , SUB_CLASS_DESCRIPTION  , SUB_SUB_CLASS_DESCRIPTION , RANGE_DESCRIPTION 
		, PRODUCT_GENERATED_IDENTIFIER , PRODUCT_SOURCE_IDENTIFIER  
		, ITEM_GENERATED_IDENTIFIER , ITEM_SOURCE_IDENTIFIER 
		
	FROM KSFPA.MR2C.KEYCODE
	WHERE PRODUCT_GENERATED_IDENTIFIER 	IN ( '4318601300004' ) -- MAX SH 1
--	WHERE PRODUCT_GENERATED_IDENTIFIER 	IN ( '4315944400004' ) -- MAX SH 2
--	WHERE PRODUCT_GENERATED_IDENTIFIER 	IN ( '4319986000004' ) -- MAX SH 3
--	WHERE PRODUCT_GENERATED_IDENTIFIER 	IN ( '4316119500004' ) -- MAX SH 4
--	WHERE PRODUCT_GENERATED_IDENTIFIER 	IN ( '4316560500004' ) -- MAX SH 5
	
--	WHERE PRODUCT_GENERATED_IDENTIFIER 	IN ( '4315097700004' ) -- SS == SH 1
--	WHERE PRODUCT_GENERATED_IDENTIFIER 	IN ( '4315618400004' ) -- SS == SH 2
--	WHERE PRODUCT_GENERATED_IDENTIFIER 	IN ( '4309428800004' ) -- SS == SH 3
--	WHERE PRODUCT_GENERATED_IDENTIFIER 	IN ( '4312805100004' ) -- SS == SH 4
--	WHERE PRODUCT_GENERATED_IDENTIFIER 	IN ( '4315003800004' ) -- SS == SH 5
)
,

DC_LIST AS 
(
	SELECT DISTINCT 
		LOCATION_GENERATED_IDENTIFIER ,
		CASE 
--			WHEN LOCATION_GENERATED_IDENTIFIER = 1040960100001 THEN 'Jandakot_WA'
--			WHEN LOCATION_GENERATED_IDENTIFIER = 1040940200001 THEN 'Lytton_QLD'
			WHEN LOCATION_GENERATED_IDENTIFIER = 1040932400001 THEN 'Truganina_VIC'
--			WHEN LOCATION_GENERATED_IDENTIFIER = 1040925100001 THEN 'ECreek_NSW'
		END AS DC_NAME
	FROM KSFPA.MR2.ORDER_PRODUCT_DLV_LOCN
	WHERE LOCATION_GENERATED_IDENTIFIER IN 
	(
		  1040932400001 -- Truganina - VIC
--		, 1040960100001 -- Jandakot - WA
--		, 1040940200001 -- Lytton - QLD
--		, 1040925100001 -- Eastern Creek - NSW
	)
)
,

PO AS 
(
	SELECT
		PO1.PRODUCT_GENERATED_IDENTIFIER
		, PO1.LOCATION_GENERATED_IDENTIFIER 
		, PO1.ORDER_GENERATED_IDENTIFIER 
-- 		, COALESCE(PO1.ORDER_GENERATED_IDENTIFIER,PO2.ORDER_GENERATED_IDENTIFIER) AS ORDER_GENERATED_IDENTIFIER

		, EXPIRY_DATE
		, EFFECTIVE_DATE
--		, WEEK_END_DATE 

		, COALESCE(AGREED_ORDER_QUANTITY,ORDER_QUANTITY) AS ORDERED_QTY
		, ORDER_SOURCE_IDENTIFIER 
		, ORDER_STATUS_CODE
		
	FROM 
		KSFPA.MR2.ORDER_PRODUCT_DLV_LOCN PO1
	FULL OUTER JOIN 
		KSFPA.MR2C.PURCHASE_ORDER PO2
	ON PO1.ORDER_GENERATED_IDENTIFIER = PO2.ORDER_GENERATED_IDENTIFIER

	WHERE 
			PO1.PRODUCT_GENERATED_IDENTIFIER IN ( SELECT PRODUCT_GENERATED_IDENTIFIER FROM SKU_DETAILS )
		AND EXPIRY_DATE = '9999-12-31'
--		AND EFFECTIVE_DATE > '2022-02-20'

		AND ORDER_STATUS_CODE IN ('FD','XB')
--		AND WEEK_END_DATE > '2022-02-20'
--		AND WEEK_END_DATE <= '2022-10-23'

)
,

RO AS 
(
	SELECT 
		  ORDER_GENERATED_IDENTIFIER 
		, LOCATION_GENERATED_IDENTIFIER 
		, RECEIPT_DATE 
		, PROCESS_DATE 
		, RECEIPT_QUANTITY AS RECIEVED_QTY 
		, PRODUCT_GENERATED_IDENTIFIER 
	FROM KSFPA.MR2C.RECEIPT_DETAIL_WORK 
	WHERE 
		PRODUCT_GENERATED_IDENTIFIER IN ( SELECT PRODUCT_GENERATED_IDENTIFIER FROM SKU_DETAILS ) 
--		AND RECEIPT_DATE > '2022-02-20' 
--		AND RECEIPT_DATE <= '2022-10-23' 
)
,

SHORT_SHIPPED AS 
(
	SELECT 
		  PO_.ORDER_GENERATED_IDENTIFIER 
		, PO_.LOCATION_GENERATED_IDENTIFIER 
		, PO_.PRODUCT_GENERATED_IDENTIFIER

--		, WEEK_END_DATE 
		, RECEIPT_DATE 
		, PROCESS_DATE 
		, EFFECTIVE_DATE
		, EXPIRY_DATE
		
		, ORDER_STATUS_CODE
		, ORDERED_QTY
		, RECIEVED_QTY 	
		, SUM(RECIEVED_QTY) OVER ( PARTITION BY ORDER_SOURCE_IDENTIFIER)
			AS NET_RECIEVED_QTY
			
		, SUM(RECIEVED_QTY) OVER
			( 
				PARTITION BY ORDER_SOURCE_IDENTIFIER
				ORDER BY RECEIPT_DATE ASC
				ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
			)
			AS CUM_RECIEVED_QTY
		, PO_.ORDER_SOURCE_IDENTIFIER
		
	FROM 
		PO AS PO_
	FULL OUTER JOIN 
		RO AS RO_ 
	
	ON  RO_.PRODUCT_GENERATED_IDENTIFIER 	= PO_.PRODUCT_GENERATED_IDENTIFIER 
	AND RO_.ORDER_GENERATED_IDENTIFIER 		= PO_.ORDER_GENERATED_IDENTIFIER 
	AND RO_.LOCATION_GENERATED_IDENTIFIER 	= PO_.LOCATION_GENERATED_IDENTIFIER

	WHERE 
			PO_.PRODUCT_GENERATED_IDENTIFIER  IN ( SELECT PRODUCT_GENERATED_IDENTIFIER  FROM SKU_DETAILS )
		AND PO_.LOCATION_GENERATED_IDENTIFIER IN ( SELECT LOCATION_GENERATED_IDENTIFIER FROM DC_LIST )
--CHNAGE THIS 
--	ORDER BY 1,2,3,4,5
)
,

SHRINK AS
(
	SELECT
	    LOCATION_GENERATED_IDENTIFIER ,
	    PERIOD_END_DATE ,
	    STOCK_ADJUSTMENT_QUANTITY AS SHRINK_QTY ,
		SUM(STOCK_ADJUSTMENT_QUANTITY) OVER ( PARTITION BY LOCATION_GENERATED_IDENTIFIER ) 
			AS NET_SHRINK_QTY,
			
	    STOCK_ADJUSTMENT_COST_AMOUNT AS SHRINK_VALUE,
	    PRODUCT_GENERATED_IDENTIFIER
	FROM
	    KSFPA.MR2.SS_STOCK_ADJUSTMENT
	WHERE
	    	PERIOD_END_DATE > '2022-02-20'
	    AND STOCK_ADJUSTMENT_REASON_CODE = '36'
	    AND PRODUCT_GENERATED_IDENTIFIER  IN ( SELECT PRODUCT_GENERATED_IDENTIFIER  FROM SKU_DETAILS )
	    AND	LOCATION_GENERATED_IDENTIFIER IN ( SELECT LOCATION_GENERATED_IDENTIFIER FROM DC_LIST )
		AND STOCK_ADJUSTMENT_QUANTITY > 0
)
,
	
FINAL_VIEW AS
(
	SELECT 
	
		  ORDER_SOURCE_IDENTIFIER AS ORDER_ID
		, ORDER_STATUS_CODE AS ORDER_STATUS
		, COALESCE ( RECEIPT_DATE , PERIOD_END_DATE ) AS INDEX_DATE

		, RECIEVED_QTY 
		, CUM_RECIEVED_QTY
		, NET_RECIEVED_QTY
		, ORDERED_QTY AS NET_ORDERED_QTY
		, ORDERED_QTY - NET_RECIEVED_QTY AS NET_SHORT_SHIPPED
--		, ORDERED_QTY - CUM_RECIEVED_QTY AS RUNNING_SHORT_SHIPPED
		
	    , NET_SHRINK_QTY
	    , SHRINK_QTY
	    , SHRINK_VALUE

		, RECEIPT_DATE 
		, EFFECTIVE_DATE
		, PROCESS_DATE 
--		, EXPIRY_DATE
--		, DC_LIST.DC_NAME 
		
--		, ORDER_GENERATED_IDENTIFIER 
--		, A.LOCATION_GENERATED_IDENTIFIER 
--		, A.PRODUCT_GENERATED_IDENTIFIER
	    
	FROM SHORT_SHIPPED A
	FULL OUTER JOIN SHRINK B
		ON  A.RECEIPT_DATE  = B.PERIOD_END_DATE
--		ON  LAST_DAY( A.RECEIPT_DATE , 'week') = B.PERIOD_END_DATE
		AND A.LOCATION_GENERATED_IDENTIFIER = B.LOCATION_GENERATED_IDENTIFIER
	LEFT JOIN DC_LIST
		ON COALESCE ( A.LOCATION_GENERATED_IDENTIFIER ,  B.LOCATION_GENERATED_IDENTIFIER ) = DC_LIST.LOCATION_GENERATED_IDENTIFIER 
)

	


-------------------------------------------------------------------------
-------------------------------------------------------------------------


--SELECT * FROM SHORT_SHIPPED;
--SELECT * FROM SKU_DETAILS ;

SELECT * FROM FINAL_VIEW ORDER BY INDEX_DATE , ORDER_ID , RECEIPT_DATE , PROCESS_DATE ASC ;

  
--SELECT * FROM KSFPA.MR2C.RECEIPT_DETAIL_WORK 
--WHERE PRODUCT_GENERATED_IDENTIFIER IN ( SELECT PRODUCT_GENERATED_IDENTIFIER FROM SKU_DETAILS ) 


;

















-------------------------------------------------------------------------

WITH 

SKU_SUBSET AS
(
	SELECT PRODUCT_GENERATED_IDENTIFIER 
	FROM KSFPA.MR2C.KEYCODE
	WHERE 
			DEPARTMENT_SOURCE_IDENTIFIER IN 
			(
				SELECT DEPARTMENT_SOURCE_IDENTIFIER 
				FROM KSFPA.LOSS_PREVENTION_PVT.DEPARTMENT_GM_APPAREL_CATEGORY
				WHERE CATEGORY = 'GM' 
			)
		AND DEPARTMENT_DESCRIPTION NOT IN ('GARDEN GREENS','MAGAZINES','REUSABLE BAGS','CARDS & WRAP','PHOTO CENTRE','GIFT CARDS & RCHRG')
),

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

SELECT
    SS.PRODUCT_GENERATED_IDENTIFIER ,
    SS.LOCATION_GENERATED_IDENTIFIER ,
    SUM( STOCK_ADJUSTMENT_COST_AMOUNT ) AS NET_SHRINK_VALUE,
    SUM( STOCK_ADJUSTMENT_QUANTITY ) AS NET_SHRINK_QTY
FROM
    KSFPA.MR2.SS_STOCK_ADJUSTMENT SS
LEFT JOIN SALES AS SL
	ON SS.PRODUCT_GENERATED_IDENTIFIER = SL.PRODUCT_GENERATED_IDENTIFIER
WHERE 
		SL.FIRST_SALES_DATE >= '2022-02-20'
    AND PERIOD_END_DATE > '2022-02-20'
    AND STOCK_ADJUSTMENT_REASON_CODE = '36'
    AND SS.PRODUCT_GENERATED_IDENTIFIER IN ( SELECT * FROM SKU_SUBSET )
    AND LOCATION_GENERATED_IDENTIFIER = 1040932400001
GROUP BY 1,2
HAVING SUM( STOCK_ADJUSTMENT_QUANTITY ) > 0
ORDER BY NET_SHRINK_VALUE DESC

-------------------------------------------------------------------------
;
-- ONLY TURGANINA
-- pick up a product and look at all dates when ordered 
-- all receipt dates should be there 
--		but be consistent with the short shipped quatity
-- 		short shipped should be at order LEVEL , not recepit LEVEL 
-- colmns : PRODUCT < LOCATION , ORDERDATE , ORDER_ID , ORD_QTY , RECIEPT_QTY , RECEIPT DATE , WEEK_ENDD

