{{ config (
    materialized='table',
    engine='MergeTree',
    order_by=('published_at', 'channel_id'),
    partition_by='toYYYYMM(published_at)'
)}}

SELECT 
    video_id,
    channel_id,
    published_at,
    TRIM(title) AS title,
    view_count,
    like_count,
    comment_count,
    dislike_count,
    favorite_count,
    {{ iso8601_duration_to_seconds('duration_raw') }} AS duration_seconds,
    duration_raw AS duration_original,
    if(has_caption, 1, 0) as has_subtitles,
    if(view_count > 0, like_count / view_count, 0) as like_ratio,
    if(like_count > 0, dislike_count / like_count, 0) as dislike_ratio,
    arrayDistinct(
        arrayMap(
            x -> domain(x), 
            JSONExtractArrayRaw(raw_payload, 'topicCategories')
        )
    ) as topic_domains,
    category_id,
    JSONExtractArrayRaw(raw_payload, 'topicCategories') as topic_categories_raw,
    load_ts
FROM external_sources.postgres_staging_youtube_videos
WHERE video_id IS NOT NULL
  AND channel_id IS NOT NULL
  AND published_at IS NOT NULL
  AND view_count >= 0
  AND match(title, '^[A-Za-zА-Яа-я0-9[:space:][:punct:]]+$')