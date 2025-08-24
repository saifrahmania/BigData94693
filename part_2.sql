/* ============================================================================
   part_2.sql — Data Cleaning
   Context: ASSIGNMENT_1.PUBLIC
   Required tables:
     - TABLE_YOUTUBE_CATEGORY (COUNTRY, CATEGORY_ID, CATEGORY_TITLE)
     - TABLE_YOUTUBE_FINAL    (all trending cols + CATEGORY_TITLE, IDEAS)
============================================================================ */

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ASSIGNMENT_1;
USE SCHEMA PUBLIC;

/* ───────────────────────────────────────────────
   Q1) CATEGORY_TITLEs with duplicates (ignoring ID)
   ─────────────────────────────────────────────── */
SELECT CATEGORY_TITLE
FROM TABLE_YOUTUBE_CATEGORY
GROUP BY CATEGORY_TITLE
HAVING COUNT(DISTINCT CATEGORY_ID) > 1
ORDER BY CATEGORY_TITLE;

/* ───────────────────────────────────────────────
   Q2) CATEGORY_TITLEs that appear in only ONE country
   ─────────────────────────────────────────────── */
SELECT
  CATEGORY_TITLE,
  MIN(COUNTRY) AS ONLY_COUNTRY
FROM TABLE_YOUTUBE_CATEGORY
GROUP BY CATEGORY_TITLE
HAVING COUNT(DISTINCT COUNTRY) = 1
ORDER BY CATEGORY_TITLE;

/* ───────────────────────────────────────────────
   Q3) Missing CATEGORY_TITLEs in FINAL
   ─────────────────────────────────────────────── */
SELECT
  COUNTRY,
  TRIM(TO_VARCHAR(CATEGORY_ID)) AS CATEGORY_ID_MISSING
FROM TABLE_YOUTUBE_FINAL
WHERE CATEGORY_TITLE IS NULL
GROUP BY COUNTRY, TRIM(TO_VARCHAR(CATEGORY_ID))
ORDER BY COUNTRY, CATEGORY_ID_MISSING;

/* ───────────────────────────────────────────────
   Q4) Update FINAL to fix missing CATEGORY_TITLEs
   ─────────────────────────────────────────────── */
UPDATE TABLE_YOUTUBE_FINAL AS T
SET CATEGORY_TITLE = C.CATEGORY_TITLE
FROM (
  SELECT
    UPPER(TRIM(COUNTRY))          AS COUNTRY_N,
    TRIM(TO_VARCHAR(CATEGORY_ID)) AS CATEGORY_ID_N,
    TRIM(CATEGORY_TITLE)          AS CATEGORY_TITLE
  FROM TABLE_YOUTUBE_CATEGORY
) AS C
WHERE T.CATEGORY_TITLE IS NULL
  AND UPPER(TRIM(T.COUNTRY))         = C.COUNTRY_N
  AND TRIM(TO_VARCHAR(T.CATEGORY_ID))= C.CATEGORY_ID_N;

-- Sanity: remaining NULLs
SELECT COUNT(*) AS NULL_CATEGORY_TITLES_AFTER_UPDATE
FROM TABLE_YOUTUBE_FINAL
WHERE CATEGORY_TITLE IS NULL;

/* ───────────────────────────────────────────────
   Q5) Videos missing CHANNEL_TITLE
   ─────────────────────────────────────────────── */
SELECT DISTINCT TITLE
FROM TABLE_YOUTUBE_FINAL
WHERE CHANNEL_TITLE IS NULL OR TRIM(CHANNEL_TITLE) = '';

/* ───────────────────────────────────────────────
   Q6) Delete bad VIDEO_ID = '#NAME?'
   ─────────────────────────────────────────────── */
DELETE FROM TABLE_YOUTUBE_FINAL
WHERE VIDEO_ID = '#NAME?';

/* ───────────────────────────────────────────────
   Q7) Find duplicates and store them
   Duplicates = same (VIDEO_ID, COUNTRY, TRENDING_DATE)
   Keep highest VIEW_COUNT
   ─────────────────────────────────────────────── */



SELECT 
    VIDEO_ID,
    COUNTRY,
    TRENDING_DATE,
    VIEW_COUNT,
    LIKES,
    DISLIKES,
    COMMENT_COUNT,
    COUNT(*) OVER (PARTITION BY VIDEO_ID, COUNTRY, TRENDING_DATE) AS duplicate_count
FROM TABLE_YOUTUBE_FINAL
QUALIFY duplicate_count > 1
ORDER BY COUNTRY, TRENDING_DATE, VIDEO_ID, VIEW_COUNT DESC;

SELECT 
    COUNT(*) AS total_duplicates
FROM (
    SELECT 
        VIDEO_ID,
        COUNTRY,
        TRENDING_DATE,
        ROW_NUMBER() OVER (
            PARTITION BY VIDEO_ID, COUNTRY, TRENDING_DATE 
            ORDER BY VIEW_COUNT DESC
        ) AS rn
    FROM TABLE_YOUTUBE_FINAL
)
WHERE rn > 1;

/* ───────────────────────────────────────────────
   Q8) Delete duplicates from FINAL
   ─────────────────────────────────────────────── */
DELETE FROM TABLE_YOUTUBE_FINAL
WHERE IDEAS IN (SELECT IDEAS FROM TABLE_YOUTUBE_DUPLICATES);

/* ______________________________________
   Check for Missing Category Titles (with example names)
   ______________________________________ */

SELECT
    country,
    title AS category_id_missing
FROM table_youtube_final
WHERE category_title IS NULL
GROUP BY country, title
ORDER BY country, category_id_missing;


SELECT 
    COUNT(*) AS total_duplicates
FROM (
    SELECT 
        VIDEO_ID,
        COUNTRY,
        TRENDING_DATE,
        ROW_NUMBER() OVER (
            PARTITION BY VIDEO_ID, COUNTRY, TRENDING_DATE 
            ORDER BY VIEW_COUNT DESC
        ) AS rn
    FROM TABLE_YOUTUBE_FINAL
)
WHERE rn > 1;


/* ============================================================================
   4. Updating Missing Category Titles
   We update table_youtube_final to replace NULL category_title values
   with the correct category_title from table_youtube_category.
============================================================================ */

UPDATE table_youtube_final AS t
SET category_title = c.category_title
FROM (
    SELECT
        UPPER(TRIM(country))               AS country_n,
        TRIM(TO_VARCHAR(category_id))      AS category_id_n,
        TRIM(category_title)               AS category_title
    FROM table_youtube_category
) AS c
WHERE t.category_title IS NULL
  AND UPPER(TRIM(t.country)) = c.country_n
  AND TRIM(TO_VARCHAR(t.category_id)) = c.category_id_n;

/* ============================================================================
   5. Videos Without Channel Titles
   Identify and flag videos that are missing channelTitle values
============================================================================ */

-- Count how many records are missing CHANNEL_TITLE
SELECT CHANNEL_TITLE, COUNT(*)
FROM TABLE_YOUTUBE_FINAL
GROUP BY CHANNEL_TITLE
ORDER BY COUNT(*) DESC;

-- Return distinct video titles that are missing a channel
SELECT COUNT(*) AS BLANK_OR_SPACES
FROM TABLE_YOUTUBE_FINAL
WHERE CHANNEL_TITLE IS NOT NULL
  AND REGEXP_REPLACE(CHANNEL_TITLE, '\\s', '') = '';




/* ───────────────────────────────────────────────
   Q9) Final validation
   Expect: 2,597,494 rows
   ─────────────────────────────────────────────── */
SELECT COUNT(*) AS FINAL_CLEAN_ROWS
FROM TABLE_YOUTUBE_FINAL;

SELECT
  CASE WHEN COUNT(*) = 2597493 THEN 'OK ✅ 2,597,493'
       ELSE 'MISMATCH ❌ Expected 2,597,493'
  END AS ROWCOUNT_STATUS
FROM TABLE_YOUTUBE_FINAL;
