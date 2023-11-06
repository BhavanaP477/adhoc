With a as(

    SELECT 
    A.LOCATION_NAME
    , A.PRODUCT_DESCRIPTION
    , A.DEPARTMENT_DESCRIPTION
    , A.DIFF_MANIFEST *LOCATION_AWC_AMOUNT as DIFF_MANIFEST
    , A.DIFF_SALES * LOCATION_AWC_AMOUNT as DIFF_SALES
    , A.DIFF_SHRINK * LOCATION_AWC_AMOUNT as DIFF_SHRINK
    , A.DIFF_ITC * LOCATION_AWC_AMOUNT as DIFF_ITC
    , A.DIFF_RLO * LOCATION_AWC_AMOUNT as DIFF_RLO
    , A.DIFF_PO * LOCATION_AWC_AMOUNT as DIFF_PO
    , AWC.LOCATION_AWC_AMOUNT
    from KSFDA.SHRINK_TACTICAL.SUMMARY_PAGE A
    LEFT JOIN 
              (
                  SELECT DISTINCT PRODUCT_GENERATED_IDENTIFIER  
                      , LOCATION_GENERATED_IDENTIFIER  
                      , LOCATION_AWC_AMOUNT
                  FROM KSFPA.MR2.SS_AVERAGE_WEIGHTED_COST
                  WHERE EXPIRY_DATE = '9999-12-31'
              )AWC
    ON AWC.LOCATION_GENERATED_IDENTIFIER = A.LOCATION_GENERATED_IDENTIFIER
    AND AWC.PRODUCT_GENERATED_IDENTIFIER  = A.PRODUCT_GENERATED_IDENTIFIER
    WHERE flag_reason is not null      

    //Limit 150
)
  
  , b as (
  SELECT *  from a
  UNPIVOT(variance for reason in (DIFF_MANIFEST, DIFF_SALES, DIFF_SHRINK, DIFF_ITC, DIFF_RLO, DIFF_PO)) 
    where variance <> 0  
    )
    
    select reason
        , case 
            when variance < 0 then 'MR greater than IPS'
            WHEN variance > 0 then 'MR less than IPS'
          else 'error' end as variance_direction
         , sum (variance)
    from b 
    group by 1,2

//SeLECT * from b