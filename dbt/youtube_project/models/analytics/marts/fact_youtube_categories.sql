{{ config(
    materialized='table',
    engine='SummingMergeTree()',
    order_by='(category_name, published_date)'
)}}

WITH video_data AS (
    SELECT 
        v.category_id,
        COALESCE(d.category_name, 'Unknown Category') AS category_name,
        COALESCE(d.category_group, 'Unknown Group') AS category_group,
        toDate(v.published_at) AS published_date,
        v.view_count,
        v.like_count,
        v.comment_count,
        v.duration_seconds,
        v.has_subtitles
    FROM {{ ref('int_youtube_videos_clean') }} v
    LEFT JOIN {{ ref('dim_youtube_categories') }} d
        ON v.category_id = d.category_id
    WHERE v.category_id IS NOT NULL 
)

SELECT
    category_name,
    category_group,
    published_date,
    COUNT() AS video_count,
    SUM(view_count) AS total_views,
    SUM(like_count) AS total_likes,
    SUM(comment_count) AS total_comments,
    AVG(duration_seconds) AS avg_duration_seconds,
    SUM(has_subtitles) AS videos_with_subtitles,
    ROUND(SUM(like_count) / nullIf(SUM(view_count), 0) * 100, 2) AS engagement_rate_percent
FROM video_data
GROUP BY category_name, category_group, published_date
HAVING total_views > 0  
ORDER BY total_views DESC