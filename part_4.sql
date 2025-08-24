/* ============================================================================
   part_4.sql — Business Question (Assignment Part 4)
   Context: ASSIGNMENT_1.PUBLIC
   Goal:
     If launching a new YouTube channel tomorrow, which category
     (excluding 'Music' and 'Entertainment') should we choose for JP
     to appear in top trends? Will this work in every country?
     Also: measure "improbability" = strong in JP but weak elsewhere.
============================================================================ */

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ASSIGNMENT_1;
USE SCHEMA PUBLIC;

---------------------------------------------------------------
-- 🔹 Final Query 1: JP recommended category
---------------------------------------------------------------
WITH daily_top AS (
  SELECT country, DATE(trending_date) AS d, category_title, video_id, title, channel_title, view_count,
         ROW_NUMBER() OVER (PARTITION BY country, DATE(trending_date) ORDER BY view_count DESC, title) AS rn
  FROM TABLE_YOUTUBE_FINAL
),
daily_top1 AS (
  SELECT country, d, category_title, video_id, title, channel_title, view_count
  FROM daily_top
  WHERE rn = 1
),
jp_vs_world AS (
  SELECT TRIM(category_title) AS category_title,
         SUM(IFF(country = 'JP', 1, 0))  AS jp_top_days,
         SUM(IFF(country <> 'JP', 1, 0)) AS rest_top_days
  FROM daily_top1
  WHERE UPPER(TRIM(category_title)) NOT IN ('MUSIC','ENTERTAINMENT')
  GROUP BY TRIM(category_title)
),
jp_reco AS (
  SELECT category_title AS recommended_category
  FROM jp_vs_world
  QUALIFY ROW_NUMBER() OVER (
    ORDER BY jp_top_days DESC, (jp_top_days / NULLIF(rest_top_days,0)) DESC, category_title
  ) = 1
)
SELECT * 
FROM jp_reco
ORDER BY recommended_category;

---------------------------------------------------------------
-- 🔹 Final Query 2: How often chosen category tops each country
---------------------------------------------------------------
WITH daily_top AS (
  SELECT country, DATE(trending_date) AS d, category_title, video_id, title, channel_title, view_count,
         ROW_NUMBER() OVER (PARTITION BY country, DATE(trending_date) ORDER BY view_count DESC, title) AS rn
  FROM TABLE_YOUTUBE_FINAL
),
daily_top1 AS (
  SELECT country, d, category_title, video_id, title, channel_title, view_count
  FROM daily_top
  WHERE rn = 1
),
jp_vs_world AS (
  SELECT TRIM(category_title) AS category_title,
         SUM(IFF(country = 'JP', 1, 0))  AS jp_top_days,
         SUM(IFF(country <> 'JP', 1, 0)) AS rest_top_days
  FROM daily_top1
  WHERE UPPER(TRIM(category_title)) NOT IN ('MUSIC','ENTERTAINMENT')
  GROUP BY TRIM(category_title)
),
jp_reco AS (
  SELECT category_title AS recommended_category
  FROM jp_vs_world
  QUALIFY ROW_NUMBER() OVER (
    ORDER BY jp_top_days DESC, (jp_top_days / NULLIF(rest_top_days,0)) DESC, category_title
  ) = 1
),
country_daily_totals AS (
  SELECT country, COUNT(DISTINCT d) AS days_with_data
  FROM daily_top1
  GROUP BY country
),
chosen_cat_by_country AS (
  SELECT t.country, COUNT(*) AS days_chosen_cat_top
  FROM daily_top1 t
  JOIN jp_reco r
    ON TRIM(t.category_title) = r.recommended_category
  GROUP BY t.country
)
SELECT
  c.country,
  COALESCE(ch.days_chosen_cat_top, 0) AS days_chosen_cat_top,
  c.days_with_data,
  TRUNC((COALESCE(ch.days_chosen_cat_top, 0) / NULLIF(c.days_with_data,0)) * 100, 2) AS pct_of_days_top
FROM country_daily_totals c
LEFT JOIN chosen_cat_by_country ch
  ON c.country = ch.country
ORDER BY pct_of_days_top DESC, c.country;

---------------------------------------------------------------
-- 🔹 Final Query 3: JP top examples (evidence)
---------------------------------------------------------------
WITH daily_top AS (
  SELECT country, DATE(trending_date) AS d, category_title, video_id, title, channel_title, view_count,
         ROW_NUMBER() OVER (PARTITION BY country, DATE(trending_date) ORDER BY view_count DESC, title) AS rn
  FROM TABLE_YOUTUBE_FINAL
),
daily_top1 AS (
  SELECT country, d, category_title, video_id, title, channel_title, view_count
  FROM daily_top
  WHERE rn = 1
),
jp_vs_world AS (
  SELECT TRIM(category_title) AS category_title,
         SUM(IFF(country = 'JP', 1, 0))  AS jp_top_days,
         SUM(IFF(country <> 'JP', 1, 0)) AS rest_top_days
  FROM daily_top1
  WHERE UPPER(TRIM(category_title)) NOT IN ('MUSIC','ENTERTAINMENT')
  GROUP BY TRIM(category_title)
),
jp_reco AS (
  SELECT category_title AS recommended_category
  FROM jp_vs_world
  QUALIFY ROW_NUMBER() OVER (
    ORDER BY jp_top_days DESC, (jp_top_days / NULLIF(rest_top_days,0)) DESC, category_title
  ) = 1
)
SELECT
  t.d AS date,
  t.title,
  t.channel_title,
  t.view_count
FROM daily_top1 t
JOIN jp_reco r
  ON t.country = 'JP'
 AND TRIM(t.category_title) = r.recommended_category
ORDER BY t.view_count DESC
LIMIT 20;

---------------------------------------------------------------
-- 🔹 Extra Query: Improbability measure (JP vs Rest of World)
---------------------------------------------------------------
WITH daily_top AS (
  SELECT
    country,
    DATE(trending_date) AS d,
    category_title,
    video_id,
    ROW_NUMBER() OVER (
      PARTITION BY country, DATE(trending_date)
      ORDER BY view_count DESC
    ) AS rn
  FROM TABLE_YOUTUBE_FINAL
)
, daily_top1 AS (
  SELECT country, d, category_title, video_id
  FROM daily_top
  WHERE rn = 1
    AND category_title NOT ILIKE 'Music%'
    AND category_title NOT ILIKE 'Entertainment%'
)
, counts AS (
  SELECT country, category_title, COUNT(DISTINCT d) AS days_top
  FROM daily_top1
  GROUP BY country, category_title
)
, jp AS (
  SELECT category_title,
         days_top AS jp_days,
         (days_top / NULLIF((SELECT COUNT(DISTINCT d) 
                             FROM daily_top1 WHERE country='JP'),0)) * 100 AS pct_jp
  FROM counts WHERE country='JP'
)
, others AS (
  SELECT category_title,
         SUM(days_top) AS other_days,
         (SUM(days_top) / NULLIF((SELECT COUNT(DISTINCT d) 
                                  FROM daily_top1 WHERE country!='JP'),0)) * 100 AS pct_other
  FROM counts WHERE country!='JP'
  GROUP BY category_title
)
SELECT j.category_title, j.jp_days, j.pct_jp, o.other_days, o.pct_other
FROM jp j
JOIN others o USING (category_title)
ORDER BY j.pct_jp DESC, o.pct_other ASC;
