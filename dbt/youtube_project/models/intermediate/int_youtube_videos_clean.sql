{{ config (materialized='view')}}

SELECT 
    video_id,
    channel_id,
    published_at,
    title,
    view_count,
    like_count,
    comment_count,
    load_ts 
FROM {{ ref('staging_youtube_videos')}}
WHERE title ~ '^[A-Za-zА-Яа-я0-9[:space:][:punct:]]+$'