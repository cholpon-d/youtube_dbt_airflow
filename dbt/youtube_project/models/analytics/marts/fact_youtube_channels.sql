{{ config(
    materialized='table',
    engine='SummingMergeTree()',
    order_by='(channel_id, published_date)'
)}}

{{ config(
    materialized='table',
    engine='SummingMergeTree()',
    order_by='(channel_id, published_date)'
)}}

SELECT 
    v.channel_id,
    COALESCE(
        NULLIF(d.channel_name, ''), 
        'unknown channel'
    ) AS channel_name,
    title,
    toDate(published_at) as published_date,
    count() AS videos_count,
    sum(view_count) AS total_views,
    sum(like_count) AS total_likes,
    sum(comment_count) AS total_comments,
    avg(duration_seconds) AS avg_duration_seconds,
    sum(has_subtitles) AS videos_with_subtitles,
    CASE 
        WHEN sum(view_count) > 0 
        THEN round((sum(like_count) / sum(view_count)) * 100, 2)
        ELSE 0 
    END AS engagement_rate_percentage
FROM {{ ref('int_youtube_videos_clean')}} v
LEFT JOIN {{ ref('dim_youtube_channels') }} d
    ON v.channel_id = d.channel_id
GROUP BY 
    v.channel_id, 
    COALESCE(NULLIF(d.channel_name, ''), 'unknown channel'), 
    v.title, 
    toDate(published_at)
ORDER BY total_views DESC