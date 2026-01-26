CREATE DATABASE IF NOT EXISTS external_sources;

--DROP TABLE IF EXISTS external_sources.postgres_staging_youtube_videos;
postgres_staging_youtube_videos
CREATE TABLE external_sources.postgres_staging_youtube_videos
(
    video_id String,
    channel_id String,
    published_at DateTime,
    title String,
    view_count Int64,
    like_count Int64,
    comment_count Int64,
    dislike_count Int64,
    favorite_count Int64,
    duration_raw String,
    has_caption UInt8,
    category_id String,
    load_ts DateTime,
    raw_payload String
)
ENGINE = PostgreSQL('postgres:5432', 'youtube_raw', 'youtube_videos',
                    'raw_user', 'raw_password', 'raw');
                    
 
                   
--DROP TABLE IF EXISTS external_sources.postgres_youtube_channels;

CREATE TABLE external_sources.postgres_youtube_channels
(
    channel_id String,
    channel_name String,
    description String,
    published_at DateTime,
    country String,
    view_count Int64,
    subscriber_count Int64,
    video_count Int64,
    load_ts DateTime,
    raw_payload String
)
ENGINE = PostgreSQL('postgres:5432', 'youtube_raw', 'youtube_channels',
                    'raw_user', 'raw_password', 'raw');
