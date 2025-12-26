SELECT video_id, view_count
FROM {{ ref('int_youtube_videos_clean') }}
WHERE view_count < 0