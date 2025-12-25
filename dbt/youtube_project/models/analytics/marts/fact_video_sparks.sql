{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(published_date, video_id)'
)}}

WITH first_day_data AS(
    SELECT 
        v.video_id,
        v.channel_id,
        v.title,
        toDate(v.published_at) AS published_date,
        v.published_at,
        v.category_id,
        d.category_name,
        v.view_count,
        v.like_count,
        v.comment_count,
        v.dislike_count,
        v.duration_seconds,
        v.has_subtitles,
        v.topic_domains,
        v.load_ts
    FROM {{ ref('int_youtube_videos_clean') }} v 
    LEFT JOIN {{ ref('dim_youtube_categories') }} d 
        ON v.category_id = d.category_id
    WHERE v.published_at >= now() - INTERVAL 2 DAY
)

SELECT 
    video_id,
    channel_id,
    title,
    published_date,
    published_at,
    category_id,
    category_name,
    view_count AS views_day_1,
    like_count AS likes_day_1,
    comment_count AS comments_day_1,
    dislike_count AS dislikes_day_1,
    duration_seconds,
    has_subtitles,
    topic_domains,
    load_ts,
    if(view_count > 0, like_count / view_count, 0) AS like_ratio_day_1,
    if(like_count > 0, dislike_count / like_count, 0) AS dislike_ratio_day_1,
    multiIf(like_count >= 1000, 'viral',
            like_count >= 500, 'popular',
            like_count >= 100, 'good_start',
            'normal'
            ) AS popularity_tier_day_1
FROM first_day_data
WHERE like_count >= 1000