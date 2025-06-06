# infinityskillshub


SELECT
    F.value:invoiceId::STRING                         AS INVOICE_ID,
    F.value:invValidPrcsId::NUMBER                    AS INV_VALID_PRCS_INV_ID,
    F.value:invValidPrcsLineId::NUMBER                AS INV_VALID_PRCS_INV_LINE_ID,
    err.value:invValidPrcsErrNm::STRING               AS INV_VALID_PRCS_ERR_NM,
    err.value:invValidPrcsErrCd::STRING               AS INV_VALID_PRCS_ERR_CD,
    TRY_TO_TIMESTAMP(err.value:invValidPrcsErrDttm::STRING) AS INV_VALID_PRCS_ERR_DTTM,
    'INVOICEDETAILSTATUS'                             AS FILE_NM

FROM RCFOPYMENTSDB.APP_CFOPYMTS.APIL_API_INVOICE_STATUS_ACTION AS T,
     LATERAL FLATTEN(input => PARSE_JSON(T.INVOICEDETAILSTATUS)) AS F,
     LATERAL FLATTEN(input => F.value:errors) AS err
WHERE ARRAY_SIZE(F.value:errors) > 0


UNION ALL


SELECT
    H:invoiceId::STRING                                AS INVOICE_ID,
    H:invValidPrcsId::NUMBER                           AS INV_VALID_PRCS_INV_ID,
    NULL                                               AS INV_VALID_PRCS_INV_LINE_ID,
    err.value:invValidPrcsErrNm::STRING                AS INV_VALID_PRCS_ERR_NM,
    err.value:invValidPrcsErrCd::STRING                AS INV_VALID_PRCS_ERR_CD,
    TRY_TO_TIMESTAMP(err.value:invValidPrcsErrDttm::STRING) AS INV_VALID_PRCS_ERR_DTTM,
    'INVOICEHEADERSTATUS'                              AS FILE_NM

FROM RCFOPYMENTSDB.APP_CFOPYMTS.APIL_API_INVOICE_STATUS_ACTION AS T,
     LATERAL (SELECT PARSE_JSON(T.INVOICEHEADERSTATUS)) AS H,
     LATERAL FLATTEN(input => H:errors) AS err
WHERE ARRAY_SIZE(H:errors) > 0
