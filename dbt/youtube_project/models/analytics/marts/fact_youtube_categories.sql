{{ config(
    materialized='table',
    engine='SummingMergeTree()',
    order_by='(category_name, published_date)'
)}}

WITH video_data AS (
    SELECT 
        v.category_id,
        d.category_name,
        d.category_group,
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
    count() AS video_count,
    sum(view_count) AS total_views,
    sum(like_count) AS total_likes,
    sum(comment_count) AS total_comments,
    avg(duration_seconds) AS avg_duration_seconds,
    sum(has_subtitles) AS videos_with_subtitles,
    total_likes / nullIf(total_views, 0) * 100 AS engagement_rate_percent
FROM video_data
GROUP BY category_name, category_group, published_date
ORDER BY total_views DESC