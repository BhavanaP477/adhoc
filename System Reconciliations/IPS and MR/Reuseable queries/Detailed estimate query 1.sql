with a as (
  SELECT  *
    from  KSFDA.SHRINK_TACTICAL.SUMMARY_PAGE_ADJUSTMENT
    UNPIVOT (diff FOR category IN (DIFF_MANIFEST
    ,DIFF_SALES
    ,DIFF_SHRINK
    ,DIFF_ITC
    ,DIFF_RLO
    ,DIFF_PO))
  )
    
    SELECT sum (diff) 
    from a
    where diff <> 0