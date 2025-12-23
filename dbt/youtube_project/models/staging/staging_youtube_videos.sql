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
FROM {{ source('youtube_raw', 'youtube_videos')}}
