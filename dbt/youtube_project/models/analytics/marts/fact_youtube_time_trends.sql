{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(date)'
)}}

SELECT 
    toDate(published_at) AS date,
    toHour(published_at) AS hour,
    dayOfWeek(published_at) AS day_of_week,
    COUNT(DISTINCT video_id) AS videos_published,
    COUNT(DISTINCT channel_id) AS active_channels,
    SUM(view_count) AS total_views,
    AVG(view_count) AS avg_views_per_video,
    SUM(like_count) AS total_likes,
    AVG(if(view_count > 0, like_count / view_count * 100, 0)) AS avg_engagement_rate_percent
FROM {{ ref('int_youtube_videos_clean') }}
GROUP BY toDate(published_at), toHour(published_at), dayOfWeek(published_at)