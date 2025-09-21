CREATE OR REPLACE TABLE yt_clean AS
WITH base AS (
  SELECT
    video_id::VARCHAR                        AS video_id,
    channel_id::VARCHAR                      AS channel_id,
    channel_title::VARCHAR                   AS channel_title,
    video_title::VARCHAR                     AS video_title,
    video_description::VARCHAR               AS video_description,
    channel_description::VARCHAR             AS channel_description,
    video_default_thumbnail::VARCHAR         AS video_default_thumbnail,
    video_tags::VARCHAR                      AS video_tags,
    video_dimension::VARCHAR                 AS video_dimension,
    video_definition::VARCHAR                AS video_definition,
    video_trending_country::VARCHAR          AS video_trending_country,
    channel_country::VARCHAR                 AS channel_country,
    video_category_id::VARCHAR               AS video_category_id,
    try_cast(video_licensed_content AS BOOLEAN) AS video_licensed_content,
    try_cast(video_view_count AS BIGINT)         AS video_view_count,
    try_cast(video_like_count AS BIGINT)         AS video_like_count,
    try_cast(video_comment_count AS BIGINT)      AS video_comment_count,
    try_cast(channel_view_count AS BIGINT)       AS channel_view_count,
    try_cast(channel_subscriber_count AS BIGINT) AS channel_subscriber_count,
    try_cast(channel_video_count AS BIGINT)      AS channel_video_count,
    coalesce(
      try_strptime(video_published_at,   '%d/%m/%Y %H:%M'),
      try_strptime(video_published_at,   '%Y-%m-%d %H:%M:%S'),
      try_strptime(video_published_at,   '%Y-%m-%d')
    )::TIMESTAMP AS video_published_at,
    coalesce(
      try_strptime(channel_published_at, '%d/%m/%Y %H:%M'),
      try_strptime(channel_published_at, '%Y-%m-%d %H:%M:%S'),
      try_strptime(channel_published_at, '%Y-%m-%d')
    )::TIMESTAMP AS channel_published_at,
    CASE
  WHEN regexp_matches(CAST(video_trending__date AS VARCHAR),
       '^(?:[A-Za-zÀ-ÿ]+)\s+([0-9]{1,2})\s+([A-Za-zÀ-ÿ]+)\s+([0-9]{4})\s*$')
  THEN make_date(
    try_cast(regexp_extract(CAST(video_trending__date AS VARCHAR),
            '^(?:[A-Za-zÀ-ÿ]+)\s+([0-9]{1,2})\s+([A-Za-zÀ-ÿ]+)\s+([0-9]{4})\s*$', 3) AS INTEGER),
    CASE lower(regexp_extract(CAST(video_trending__date AS VARCHAR),
                '^(?:[A-Za-zÀ-ÿ]+)\s+([0-9]{1,2})\s+([A-Za-zÀ-ÿ]+)\s+([0-9]{4})\s*$', 2))
          WHEN 'janvier'   THEN 1
          WHEN 'février'   THEN 2
          WHEN 'fevrier'   THEN 2
          WHEN 'mars'      THEN 3
          WHEN 'avril'     THEN 4
          WHEN 'mai'       THEN 5
          WHEN 'juin'      THEN 6
          WHEN 'juillet'   THEN 7
          WHEN 'août'      THEN 8
          WHEN 'aout'      THEN 8
          WHEN 'septembre' THEN 9
          WHEN 'octobre'   THEN 10
          WHEN 'novembre'  THEN 11
          WHEN 'décembre'  THEN 12
          WHEN 'decembre'  THEN 12
          ELSE NULL
        END,
        try_cast(regexp_extract(CAST(video_trending__date AS VARCHAR),
                '^(?:[A-Za-zÀ-ÿ]+)\s+([0-9]{1,2})\s+([A-Za-zÀ-ÿ]+)\s+([0-9]{4})\s*$', 1) AS INTEGER)
      )
      ELSE NULL
    END AS video_trending_date,
    video_duration
  FROM youtube
)
SELECT
  base.*,
  CASE
    WHEN CAST(video_duration AS VARCHAR) LIKE 'PT%' THEN
      coalesce(try_cast(regexp_extract(CAST(video_duration AS VARCHAR), 'PT([0-9]+)H', 1) AS BIGINT), 0) * 3600 +
      coalesce(try_cast(regexp_extract(CAST(video_duration AS VARCHAR), 'PT(?:[0-9]+H)?([0-9]+)M', 1) AS BIGINT), 0) * 60 +
      coalesce(try_cast(regexp_extract(CAST(video_duration AS VARCHAR), 'PT(?:[0-9]+H)?(?:[0-9]+)M?([0-9]+)S', 1) AS BIGINT), 0)
    WHEN try_cast(video_duration AS TIME) IS NOT NULL THEN
      date_part('hour',   try_cast(video_duration AS TIME)) * 3600 +
      date_part('minute', try_cast(video_duration AS TIME)) * 60 +
      date_part('second', try_cast(video_duration AS TIME))
    ELSE NULL
  END AS video_duration_seconds
FROM base;

-- ============================================================
-- TABLE DES TAGS DÉNORMALISÉE
-- ============================================================

-- Créer une fonction pour splitter les tags
CREATE OR REPLACE MACRO split_tags(tag_string) AS 
    string_split(LOWER(TRIM(tag_string)), ',');

-- Table tags_videos (une ligne par tag)
CREATE OR REPLACE TABLE video_tags AS
WITH tags_split AS (
    SELECT 
        video_id,
        video_trending_date,
        UNNEST(split_tags(video_tags)) AS tag,
        ROW_NUMBER() OVER (PARTITION BY video_id, video_trending_date ORDER BY 1) AS tag_position
    FROM yt_clean
    WHERE video_tags IS NOT NULL AND video_tags != ''
)
SELECT 
    TRIM(tag) AS tag,
    video_id,
    video_trending_date,
FROM tags_split
WHERE LENGTH(TRIM(tag)) > 1
  AND TRIM(tag) NOT LIKE '%?%';