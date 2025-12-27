{{ config(materialized='table',
   engine='ReplacingMergeTree(load_ts)',
   order_by='channel_id'
 ) }}

 SELECT 
    channel_id,
    channel_name,
    country,
    view_count,
    subscriber_count,
    video_count,
    load_ts
FROM external_sources.postgres_youtube_channels
WHERE channel_name != '' AND channel_id IS NOT NULL