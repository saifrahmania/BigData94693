/* ============================================================================
   PART 1 — DATA INGESTION & FINAL TABLE BUILD
   Context: ASSIGNMENT_1.PUBLIC
   Expected: final row count ~ 2,667,041
============================================================================ */

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ASSIGNMENT_1;
USE SCHEMA PUBLIC;

-- --------------------------------------------------------------------------
-- 1) File formats
-- --------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT FF_CSV
  TYPE = CSV
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('','NA','NULL')
  EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE FILE FORMAT FF_JSON
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE;

-- --------------------------------------------------------------------------
-- 2) Stage (your Azure SAS token / container)
-- --------------------------------------------------------------------------
CREATE OR REPLACE STAGE STAGE_ASSIGNMENT
  URL='azure://youtubeassignment.blob.core.windows.net/youtube'
  CREDENTIALS=(AZURE_SAS_TOKEN='?sp=rl&st=2025-08-17T00:19:31Z&se=2025-08-30T08:34:31Z&spr=https&sv=2024-11-04&sr=c&sig=llkITPzbQnmtxOOmDn3%2Fl2Pbbc8z%2B4oPys%2FQucCdCcM%3D')
  DIRECTORY=(ENABLE=TRUE);

-- --------------------------------------------------------------------------
-- 3) EXTERNAL TABLES
-- --------------------------------------------------------------------------
-- trending CSVs
CREATE OR REPLACE EXTERNAL TABLE EX_TABLE_YOUTUBE_TRENDING (
  COUNTRY STRING AS (
    UPPER(REGEXP_SUBSTR(SPLIT_PART(METADATA$FILENAME,'/',-1),
                        '^([A-Z]{2})_', 1, 1, 'e', 1))
  ),
  VIDEO_ID        STRING AS (VALUE[0]::STRING),
  TITLE           STRING AS (VALUE[1]::STRING),
  PUBLISHED_AT    TIMESTAMP_NTZ AS (TRY_TO_TIMESTAMP_NTZ(VALUE[2]::STRING)),
  CHANNEL_ID      STRING AS (VALUE[3]::STRING),
  CHANNEL_TITLE   STRING AS (VALUE[4]::STRING),
  CATEGORY_ID     STRING AS (VALUE[5]::STRING),
  TRENDING_DATE   TIMESTAMP_NTZ AS (TRY_TO_TIMESTAMP_NTZ(VALUE[6]::STRING)),
  VIEW_COUNT      NUMBER AS (TRY_TO_NUMBER(VALUE[7]::STRING)),
  LIKES           NUMBER AS (TRY_TO_NUMBER(VALUE[8]::STRING)),
  DISLIKES        NUMBER AS (TRY_TO_NUMBER(VALUE[9]::STRING)),
  COMMENT_COUNT   NUMBER AS (TRY_TO_NUMBER(VALUE[10]::STRING))
)
WITH LOCATION=@STAGE_ASSIGNMENT/trending/
FILE_FORMAT=(FORMAT_NAME=FF_CSV)
PATTERN='(?i).*\.csv';

ALTER EXTERNAL TABLE EX_TABLE_YOUTUBE_TRENDING REFRESH;

-- category JSONs
CREATE OR REPLACE EXTERNAL TABLE EX_TABLE_YOUTUBE_CATEGORY_JSON (
  RAW VARIANT AS (VALUE),
  COUNTRY STRING AS (
    UPPER(REGEXP_SUBSTR(SPLIT_PART(METADATA$FILENAME,'/',-1),
                        '^([A-Z]{2})_', 1, 1, 'e', 1))
  )
)
WITH LOCATION=@STAGE_ASSIGNMENT/categorical/
FILE_FORMAT=(FORMAT_NAME=FF_JSON)
PATTERN='(?i).*\.json';

ALTER EXTERNAL TABLE EX_TABLE_YOUTUBE_CATEGORY_JSON REFRESH;

-- --------------------------------------------------------------------------
-- 4) INTERNAL TABLES
-- --------------------------------------------------------------------------
CREATE OR REPLACE TABLE TABLE_YOUTUBE_TRENDING (
  VIDEO_ID STRING,
  TITLE STRING,
  PUBLISHED_AT TIMESTAMP_NTZ,
  CHANNEL_ID STRING,
  CHANNEL_TITLE STRING,
  CATEGORY_ID STRING,
  TRENDING_DATE TIMESTAMP_NTZ,
  VIEW_COUNT NUMBER,
  LIKES NUMBER,
  DISLIKES NUMBER,
  COMMENT_COUNT NUMBER,
  COUNTRY STRING
);

CREATE OR REPLACE TABLE TABLE_YOUTUBE_CATEGORY (
  CATEGORY_ID STRING,
  CATEGORY_TITLE STRING,
  COUNTRY STRING
);

-- --------------------------------------------------------------------------
-- 5) LOAD DATA
-- --------------------------------------------------------------------------
-- Trending CSVs → internal
COPY INTO TABLE_YOUTUBE_TRENDING
FROM (
  SELECT
    $1::STRING AS VIDEO_ID,
    $2::STRING AS TITLE,
    TRY_TO_TIMESTAMP_NTZ($3) AS PUBLISHED_AT,
    $4::STRING AS CHANNEL_ID,
    $5::STRING AS CHANNEL_TITLE,
    $6::STRING AS CATEGORY_ID,
    TRY_TO_TIMESTAMP_NTZ($7) AS TRENDING_DATE,
    TRY_TO_NUMBER($8) AS VIEW_COUNT,
    TRY_TO_NUMBER($9) AS LIKES,
    TRY_TO_NUMBER($10) AS DISLIKES,
    TRY_TO_NUMBER($11) AS COMMENT_COUNT,
    REGEXP_SUBSTR(SPLIT_PART(METADATA$FILENAME,'/',-1), '^([A-Z]{2})_', 1, 1, 'e', 1) AS COUNTRY  -- no UPPER
  FROM @STAGE_ASSIGNMENT/trending/
)
FILE_FORMAT=(FORMAT_NAME=FF_CSV)
ON_ERROR='CONTINUE';


LIST @STAGE_ASSIGNMENT/trending;

-- Category JSONs → internal
INSERT OVERWRITE INTO TABLE_YOUTUBE_CATEGORY
SELECT
  f.value:id::STRING            AS CATEGORY_ID,
  f.value:snippet.title::STRING AS CATEGORY_TITLE,
  c.COUNTRY
FROM EX_TABLE_YOUTUBE_CATEGORY_JSON c,
     LATERAL FLATTEN(INPUT => c.RAW:items) f;

LIST @STAGE_ASSIGNMENT/categorical;
     

-- --------------------------------------------------------------------------
-- 6) NORMALIZATION
-- --------------------------------------------------------------------------
CREATE OR REPLACE TABLE TABLE_YOUTUBE_TRENDING_NORM AS
SELECT
  VIDEO_ID,
  TITLE,
  PUBLISHED_AT,
  CHANNEL_ID,
  CHANNEL_TITLE,
  TRIM(TO_VARCHAR(CATEGORY_ID)) AS CATEGORY_ID,
  TRENDING_DATE,
  VIEW_COUNT,
  LIKES,
  DISLIKES,
  COMMENT_COUNT,
  UPPER(TRIM(COUNTRY)) AS COUNTRY
FROM TABLE_YOUTUBE_TRENDING;

CREATE OR REPLACE TABLE TABLE_YOUTUBE_CATEGORY_NORM AS
SELECT
  TRIM(TO_VARCHAR(CATEGORY_ID)) AS CATEGORY_ID,
  TRIM(CATEGORY_TITLE) AS CATEGORY_TITLE,
  UPPER(TRIM(COUNTRY)) AS COUNTRY
FROM TABLE_YOUTUBE_CATEGORY;

-- --------------------------------------------------------------------------
-- 7) FINAL TABLE
-- --------------------------------------------------------------------------
CREATE OR REPLACE TABLE TABLE_YOUTUBE_FINAL AS
SELECT
  t.VIDEO_ID,
  t.TITLE,
  t.PUBLISHED_AT,
  t.CHANNEL_ID,
  t.CHANNEL_TITLE,
  t.CATEGORY_ID,
  t.TRENDING_DATE,
  t.VIEW_COUNT,
  t.LIKES,
  t.DISLIKES,
  t.COMMENT_COUNT,
  t.COUNTRY,
  c.CATEGORY_TITLE,
  UUID_STRING() AS IDEAS
FROM TABLE_YOUTUBE_TRENDING_NORM t
LEFT JOIN TABLE_YOUTUBE_CATEGORY_NORM c
  ON t.COUNTRY = c.COUNTRY
 AND t.CATEGORY_ID = c.CATEGORY_ID;

-- --------------------------------------------------------------------------
-- 8) Sanity Check
-- --------------------------------------------------------------------------


SELECT 
    COUNT(DISTINCT country) AS num_countries,
    LISTAGG(DISTINCT country, ', ') WITHIN GROUP (ORDER BY country) AS countries
FROM table_youtube_final;

SELECT COUNT(*) AS FINAL_ROWS FROM TABLE_YOUTUBE_FINAL;  -- expect ~2,667,041
