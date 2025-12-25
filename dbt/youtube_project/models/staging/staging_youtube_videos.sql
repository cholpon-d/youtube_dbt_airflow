{{ config (materialized='table')}}

SELECT 
    video_id,
    channel_id,
    published_at::TIMESTAMP AS published_at,
    title,
    view_count,
    like_count,
    comment_count,
    dislike_count,
    favorite_count,
    duration_raw,
    has_caption,
    topic_categories,
    category_id,
    load_ts, 
    raw_payload 
FROM {{ source('youtube_raw', 'youtube_videos')}}
