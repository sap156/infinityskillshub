# infinityskillshub


{{ config(
    pre_hook=['TRUNCATE TABLE IF EXISTS {{ this }}'],
    alias=this.table | string | upper,
    database='STG',
    materialized='table'
) }}

WITH detail_errors AS (
    SELECT
        {{ handle_empty_or_null_value(chk_type='VARCHAR', length=36, first_val='F.value:invoiceId') }} AS INVOICE_ID,
        {{ handle_empty_or_null_value(chk_type='VARCHAR', length=36, first_val='F.value:invValidPrcsId') }} AS INV_VALID_PRCS_INV_ID,
        TRIM(F.value:invValidPrcsLineId::STRING)::VARCHAR(36) AS INV_VALID_PRCS_INV_LINE_ID,  -- nullable, no fallback
        TRIM(err.value:invValidPrcsErrNm::STRING)::VARCHAR(150) AS INV_VALID_PRCS_ERR_NM,     -- nullable
        TRIM(err.value:invValidPrcsErrCd::STRING)::VARCHAR(36) AS INV_VALID_PRCS_ERR_CD,      -- nullable
        {{ handle_empty_or_null_value(chk_type='VARCHAR', length=20, first_val="'INVOICEDETAILSTATUS'") }} AS FILE_NM,
        TRY_TO_TIMESTAMP(err.value:invValidPrcsErrDttm::STRING)::TIMESTAMP_LTZ(6) AS INV_VALID_PRCS_ERR_DTTM

    FROM {{ ref('apil_api_invoice_status_action') }} AS T,
         LATERAL FLATTEN(input => PARSE_JSON(T.INVOICEDETAILSTATUS)) AS F,
         LATERAL FLATTEN(input => F.value:errors) AS err
    WHERE ARRAY_SIZE(F.value:errors) > 0
),

header_errors AS (
    SELECT
        {{ handle_empty_or_null_value(chk_type='VARCHAR', length=36, first_val="PARSE_JSON(T.INVOICEHEADERSTATUS):invoiceId") }} AS INVOICE_ID,
        {{ handle_empty_or_null_value(chk_type='VARCHAR', length=36, first_val="PARSE_JSON(T.INVOICEHEADERSTATUS):invValidPrcsId") }} AS INV_VALID_PRCS_INV_ID,
        NULL AS INV_VALID_PRCS_INV_LINE_ID,
        TRIM(err.value:invValidPrcsErrNm::STRING)::VARCHAR(150) AS INV_VALID_PRCS_ERR_NM,
        TRIM(err.value:invValidPrcsErrCd::STRING)::VARCHAR(36) AS INV_VALID_PRCS_ERR_CD,
        {{ handle_empty_or_null_value(chk_type='VARCHAR', length=20, first_val="'INVOICEHEADERSTATUS'") }} AS FILE_NM,
        TRY_TO_TIMESTAMP(err.value:invValidPrcsErrDttm::STRING)::TIMESTAMP_LTZ(6) AS INV_VALID_PRCS_ERR_DTTM

    FROM {{ ref('apil_api_invoice_status_action') }} AS T,
         LATERAL FLATTEN(input => PARSE_JSON(T.INVOICEHEADERSTATUS):errors) AS err
    WHERE ARRAY_SIZE(PARSE_JSON(T.INVOICEHEADERSTATUS):errors) > 0
)

SELECT * FROM detail_errors
UNION ALL
SELECT * FROM header_errors
