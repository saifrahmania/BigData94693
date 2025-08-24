/* ============================================================================
   part_3.sql — Data Analysis (Assignment Part 3, Expanded)
   Context: ASSIGNMENT_1.PUBLIC
   Table used: TABLE_YOUTUBE_FINAL
============================================================================ */

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ASSIGNMENT_1;
USE SCHEMA PUBLIC;

/* ───────────────────────────────────────────────────────────────
   Q1) 3 most viewed videos per country in Gaming category
       for trending_date = '2024-04-01'
   ─────────────────────────────────────────────────────────────── */
WITH daily AS (
  SELECT
    COUNTRY,
    VIDEO_ID,
    TITLE,
    CHANNEL_TITLE,
    VIEW_COUNT
  FROM TABLE_YOUTUBE_FINAL
  WHERE DATE(TRENDING_DATE) = '2024-04-01'
    AND CATEGORY_TITLE ILIKE 'Gaming%'
),
ranked AS (
  SELECT
    COUNTRY,
    VIDEO_ID,
    TITLE,
    CHANNEL_TITLE,
    VIEW_COUNT,
    ROW_NUMBER() OVER (PARTITION BY COUNTRY ORDER BY VIEW_COUNT DESC, TITLE) AS rk
  FROM daily
)
SELECT
  COUNTRY,
  TITLE,
  CHANNEL_TITLE,
  VIEW_COUNT,
  rk
FROM ranked
WHERE rk <= 3
ORDER BY COUNTRY, rk;

/* ───────────────────────────────────────────────────────────────
   Q2) Count DISTINCT videos with 'BTS' in title (case-insensitive)
   ─────────────────────────────────────────────────────────────── */
SELECT
  COUNTRY,
  COUNT(DISTINCT VIDEO_ID) AS ct
FROM TABLE_YOUTUBE_FINAL
WHERE TITLE ILIKE '%BTS%'
GROUP BY COUNTRY
ORDER BY ct DESC, COUNTRY;

/* ───────────────────────────────────────────────────────────────
   Q3) For each (country, year_month) in 2024,
       pick the most viewed video and compute likes_ratio (%)
   ─────────────────────────────────────────────────────────────── */
WITH y2024 AS (
  SELECT
    COUNTRY,
    TO_CHAR(DATE(TRENDING_DATE), 'YYYY-MM') AS year_month,
    VIDEO_ID,
    TITLE,
    CHANNEL_TITLE,
    CATEGORY_TITLE,
    VIEW_COUNT,
    LIKES
  FROM TABLE_YOUTUBE_FINAL
  WHERE EXTRACT(YEAR FROM DATE(TRENDING_DATE)) = 2024
),
best AS (
  SELECT
    COUNTRY,
    year_month,
    VIDEO_ID,
    TITLE,
    CHANNEL_TITLE,
    CATEGORY_TITLE,
    VIEW_COUNT,
    TRUNC( (LIKES / NULLIF(VIEW_COUNT, 0)) * 100, 2 ) AS likes_ratio
  FROM (
    SELECT
      COUNTRY,
      year_month,
      VIDEO_ID,
      TITLE,
      CHANNEL_TITLE,
      CATEGORY_TITLE,
      VIEW_COUNT,
      LIKES,
      ROW_NUMBER() OVER (
        PARTITION BY COUNTRY, year_month
        ORDER BY VIEW_COUNT DESC, TITLE
      ) AS rn
    FROM y2024
  )
  WHERE rn = 1
)
SELECT
  COUNTRY,
  year_month,
  TITLE,
  CHANNEL_TITLE,
  CATEGORY_TITLE,
  VIEW_COUNT,
  likes_ratio
FROM best
ORDER BY year_month, COUNTRY;

/* ───────────────────────────────────────────────────────────────
   Q4) From 2022 onward, per country find category with MOST DISTINCT videos
       + its percentage share of total distinct videos
   ─────────────────────────────────────────────────────────────── */
WITH post_2021 AS (
  SELECT DISTINCT
    COUNTRY,
    CATEGORY_TITLE,
    VIDEO_ID
  FROM TABLE_YOUTUBE_FINAL
  WHERE DATE(TRENDING_DATE) >= '2022-01-01'
),
per_cat AS (
  SELECT
    COUNTRY,
    CATEGORY_TITLE,
    COUNT(DISTINCT VIDEO_ID) AS total_category_video
  FROM post_2021
  GROUP BY COUNTRY, CATEGORY_TITLE
),
with_totals AS (
  SELECT
    p.*,
    SUM(total_category_video) OVER (PARTITION BY COUNTRY) AS total_country_video
  FROM per_cat p
),
winners AS (
  SELECT
    COUNTRY,
    CATEGORY_TITLE,
    total_category_video,
    TRUNC( (total_category_video / NULLIF(total_country_video, 0)) * 100, 2 ) AS percentage
  FROM (
    SELECT
      COUNTRY,
      CATEGORY_TITLE,
      total_category_video,
      total_country_video,
      ROW_NUMBER() OVER (
        PARTITION BY COUNTRY
        ORDER BY total_category_video DESC, CATEGORY_TITLE
      ) AS rn
    FROM with_totals
  )
  WHERE rn = 1
)
SELECT
  COUNTRY,
  CATEGORY_TITLE,
  total_category_video,
  percentage
FROM winners
ORDER BY CATEGORY_TITLE, COUNTRY;

/* ───────────────────────────────────────────────────────────────
   Q5) Channel with MOST DISTINCT videos overall
   ─────────────────────────────────────────────────────────────── */
SELECT
  CHANNEL_TITLE,
  COUNT(DISTINCT VIDEO_ID) AS distinct_videos
FROM TABLE_YOUTUBE_FINAL
GROUP BY CHANNEL_TITLE
QUALIFY ROW_NUMBER() OVER (
  ORDER BY COUNT(DISTINCT VIDEO_ID) DESC, CHANNEL_TITLE
) = 1;



/* ───────────────────────────────────────────────────────────────
   EXTRA 1) Top 5 categories by view count per country
   ─────────────────────────────────────────────────────────────── */
SELECT
  COUNTRY,
  CATEGORY_TITLE,
  SUM(VIEW_COUNT) AS total_views,
  ROW_NUMBER() OVER (PARTITION BY COUNTRY ORDER BY SUM(VIEW_COUNT) DESC) AS rk
FROM TABLE_YOUTUBE_FINAL
GROUP BY COUNTRY, CATEGORY_TITLE
QUALIFY rk <= 5
ORDER BY COUNTRY, rk;

/* ───────────────────────────────────────────────────────────────
   EXTRA 2) Most frequent channels in trending (top 10 overall)
   ─────────────────────────────────────────────────────────────── */
SELECT
  CHANNEL_TITLE,
  COUNT(*) AS times_trended
FROM TABLE_YOUTUBE_FINAL
GROUP BY CHANNEL_TITLE
ORDER BY times_trended DESC
LIMIT 10;

/* ───────────────────────────────────────────────────────────────
   EXTRA 3) Temporal trend: total views per month (all countries)
   ─────────────────────────────────────────────────────────────── */
SELECT
  TO_CHAR(DATE(TRENDING_DATE), 'YYYY-MM') AS year_month,
  SUM(VIEW_COUNT) AS total_views
FROM TABLE_YOUTUBE_FINAL
GROUP BY year_month
ORDER BY year_month;

/* ───────────────────────────────────────────────────────────────
   EXTRA 4) Outliers check — videos with unusually high likes_ratio
   ─────────────────────────────────────────────────────────────── */
SELECT
  VIDEO_ID,
  TITLE,
  CHANNEL_TITLE,
  COUNTRY,
  VIEW_COUNT,
  LIKES,
  TRUNC((LIKES / NULLIF(VIEW_COUNT,0)) * 100, 2) AS likes_ratio
FROM TABLE_YOUTUBE_FINAL
WHERE VIEW_COUNT > 1000000
  AND (LIKES / NULLIF(VIEW_COUNT,0)) * 100 > 50
ORDER BY likes_ratio DESC
LIMIT 20;

/* =============================================================
   Distribution of Categories Across Countries
   Example: Gaming-related videos on 2024-04-01
   ============================================================= */

WITH daily AS (
    SELECT
        country,
        video_id,
        title,
        channel_title,
        view_count
    FROM TABLE_YOUTUBE_FINAL
    WHERE DATE(trending_date) = '2024-04-01'
      AND category_title ILIKE 'Gaming%'
),
ranked AS (
    SELECT
        country,
        title,
        channel_title,
        view_count,
        ROW_NUMBER() OVER (
            PARTITION BY country
            ORDER BY view_count DESC
        ) AS rk
    FROM daily
)
SELECT
    country,
    title,
    channel_title,
    view_count,
    rk
FROM ranked
WHERE rk <= 3   -- top 3 per country
ORDER BY country, rk;

