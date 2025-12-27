{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(views_rank)'
)}}

WITH ranked_videos AS(
    SELECT 
        v.video_id,
        v.channel_id,
        v.title,
        toDate(v.published_at) AS published_date,
        v.category_id,
        d.category_name,
        d.category_group,
        v.view_count,
        v.like_count,
        v.comment_count,
        v.dislike_count,
        v.duration_seconds,
        v.has_subtitles,

        ROW_NUMBER() OVER (ORDER BY v.view_count DESC) AS views_rank,
        ROW_NUMBER() OVER (ORDER BY v.like_count DESC) AS likes_rank,
        ROW_NUMBER() OVER (ORDER BY v.comment_count DESC) AS comments_rank,
        ROW_NUMBER() OVER (ORDER BY v.dislike_count DESC) AS dislikes_rank,
        ROW_NUMBER() OVER (ORDER BY (v.like_count * 0.5 + v.comment_count *1.5)DESC) AS engagement_rank,
        ROW_NUMBER() OVER (PARTITION BY v.category_id ORDER BY v.view_count DESC) AS views_rank_in_category,
        ROW_NUMBER() OVER (PARTITION BY v.category_id ORDER BY v.like_count DESC) AS likes_rank_in_category,

        v.view_count * 100.0 / SUM(view_count) OVER () AS views_percentage_of_total,
        v.like_count * 100.0 / nullIf(v.view_count, 0) AS like_rate_percent,
        dateDiff('hour', published_at, now()) AS hours_since_published,
        multiIf(
            v.duration_seconds < 60, 'short',
            v.duration_seconds < 300, 'medium',
            v.duration_seconds < 600, 'long',
            'very_long'
        ) AS duration_category
    FROM {{ ref('int_youtube_videos_clean') }} v
    LEFT JOIN {{ ref('dim_youtube_categories') }} d 
        ON v.category_id = d.category_id
    WHERE v.view_count > 0
)

SELECT 
    video_id,
    channel_id,
    title,
    published_date,
    category_id,
    category_name,
    category_group,
    view_count,
    like_count,
    comment_count,
    dislike_count,
    duration_seconds,
    has_subtitles,
    views_rank,
    likes_rank,
    comments_rank,
    dislikes_rank,
    engagement_rank,
    views_rank_in_category,
    likes_rank_in_category,
    ROUND(views_percentage_of_total, 4) AS views_percentage_of_total,
    ROUND(like_rate_percent, 2) AS like_rate_percent,
    hours_since_published,
    duration_category,
    views_rank <= 100 AS is_top_100_views,
    likes_rank <= 100 AS is_top_100_likes,
    engagement_rank <= 100 AS is_top_100_engagement,
    views_rank_in_category <= 10 AS is_top_10_views_in_category,

    multiIf(
        views_rank <= 10, 'top_10',
        views_rank <= 50, 'top_50',
        views_rank <= 100, 'top_100',
        views_rank <= 500, 'top_500',
        'other'
    ) AS popularity_group
FROM ranked_videos
ORDER BY views_rank DESC