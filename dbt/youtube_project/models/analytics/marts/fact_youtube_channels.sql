{{ config(
    materialized='table',
    engine='SummingMergeTree()',
    order_by='(channel_id, published_date)'
)}}

SELECT 
    channel_id,
    title,
    toDate(published_at) as published_date,
    count() AS videos_count,
    sum(view_count) AS total_views,
    sum(like_count) AS total_likes,
    sum(comment_count) AS total_comments,
    avg(duration_seconds) AS avg_duration_seconds,
    sum(has_subtitles) AS videos_with_subtitles,
    total_likes / nullIf(total_views, 0) * 100 AS engagement_rate_percentage 
FROM {{ ref('int_youtube_videos_clean')}}
GROUP BY channel_id, title, toDate(published_at)