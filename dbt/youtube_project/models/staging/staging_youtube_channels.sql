{{ config(materialized='table') }}

SELECT 
    channel_id,
    channel_name,
    description,
    published_at::TIMESTAMP AS published_at,
    country,
    view_count,
    subscriber_count,
    video_count,
    load_ts
FROM {{ source('youtube_raw', 'youtube_channels') }}