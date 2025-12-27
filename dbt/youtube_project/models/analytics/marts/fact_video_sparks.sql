{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(published_date, video_id)'
)}}

SELECT 
    v.video_id,
    v.channel_id,
    v.title,
    toDate(v.published_at) AS published_date,
    v.published_at,
    v.category_id,
    d.category_name,
    COALESCE(NULLIF(ch.channel_name, ''), 'unknown channel') AS channel_name,
    ch.subscriber_count,
    COALESCE(NULLIF(ch.country, ''), 'unknown country') AS channel_country,
    v.view_count AS views_day_1,
    v.like_count AS likes_day_1,
    v.comment_count AS comments_day_1,
    v.dislike_count AS dislikes_day_1,
    v.favorite_count AS favorites_day_1,
    v.duration_seconds,
    v.has_subtitles,
    v.topic_domains,
    v.load_ts,
    ROUND(if(v.view_count > 0, v.like_count / v.view_count, 0), 4) AS like_ratio_day_1,
    ROUND(if(v.like_count > 0, v.dislike_count / v.like_count, 0), 4) AS dislike_ratio_day_1,
    ROUND(if(v.view_count > 0, v.like_count / v.view_count * 100, 0), 2) AS like_percentage_day_1,
    multiIf(
        v.duration_seconds < 60, 'short',
        v.duration_seconds < 300, 'medium',
        v.duration_seconds < 600, 'long',
        'very_long'
    ) AS duration_category,
    multiIf(v.like_count >= 1000, 'viral',
            v.like_count >= 500, 'popular',
            v.like_count >= 100, 'good_start',
            'normal'
            ) AS popularity_tier_day_1,
    ROUND(if(v.duration_seconds > 0, v.view_count / v.duration_seconds, 0), 2) AS views_per_second,
    v.has_subtitles = 1 AS has_subtitles_flag,
    notEmpty(v.topic_domains) AS has_topic_domains_flag,
    ch.subscriber_count > 100000 AS is_large_channel
FROM {{ ref('int_youtube_videos_clean') }} v 
LEFT JOIN {{ ref('dim_youtube_categories') }} d 
    ON v.category_id = d.category_id
LEFT JOIN {{ ref('dim_youtube_channels') }} ch 
    ON v.channel_id = ch.channel_id
WHERE v.published_at >= now() - INTERVAL 2 DAY
  AND v.like_count >= 1000