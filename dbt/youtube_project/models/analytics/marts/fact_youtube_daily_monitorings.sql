{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='date',
    partition_by='toYYYYMM(date)'  
)}}

SELECT 
    toDate(published_at) AS date,
    COUNT(DISTINCT video_id) AS new_videos,
    COUNT(DISTINCT channel_id) AS active_channels,
    SUM (view_count) AS daily_views,
    SUM (like_count) AS daily_likes,
    SUM (comment_count) AS daily_comments,
    ROUND(SUM(like_count) / NULLIF(SUM(view_count), 0) * 100, 2) AS daily_engagement_rate_percent
FROM {{ ref('int_youtube_videos_clean') }}
GROUP BY toDate(published_at)
ORDER BY date