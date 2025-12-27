from datetime import datetime
import json
import logging
from airflow.hooks.base import BaseHook
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

def get_pg_connections():
    conn = BaseHook.get_connection("postgres_raw")

    return psycopg2.connect(
        host=conn.host,
        port=conn.port,
        dbname=conn.schema or conn.extra_dejson.get("database"),
        user=conn.login,
        password=conn.password,
    )

def create_raw_table():
    sql = """

    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.youtube_videos(
        load_ts TIMESTAMP NOT NULL,
        video_id TEXT PRIMARY KEY,
        channel_id TEXT,
        published_at TIMESTAMP,
        title TEXT,
        view_count BIGINT,
        like_count BIGINT,
        raw_payload JSONB,
        comment_count BIGINT,
        dislike_count BIGINT DEFAULT 0,
        favorite_count BIGINT DEFAULT 0,
        duration_raw TEXT,
        has_caption BOOLEAN,
        topic_categories JSONB,
        category_id TEXT
    );
    """

    try:
        with get_pg_connections() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
        logger.info('Table raw.youtube_videos is created')

    except Exception as e:
        logger.error(f'Error while creating table: {e}')
        raise 


def insert_videos(videos: list[dict]):
    if not videos:
        logger.info('No videos to insert')
        return
    
    rows = [
        (   
            v["load_ts"],
            v["video_id"],
            v["channel_id"],
            v["published_at"],
            v.get("title"),
            v.get("view_count", 0),
            v.get("like_count", 0),
            v.get("comment_count", 0),
            v.get("dislike_count", 0),
            v.get("favorite_count", 0),
            v.get("duration_raw"),
            v.get("has_caption"),
            json.dumps(v.get("topic_categories", [])),
            v.get("category_id"),
            json.dumps(v["raw_payload"])
        )
        for v in videos 
    ]

    sql = """
    INSERT INTO raw.youtube_videos
    (load_ts,
     video_id, 
     channel_id,
     published_at, 
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
     raw_payload)
    VALUES %s
    ON CONFLICT (video_id) DO NOTHING;
    """

    try:
        with get_pg_connections() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, rows)
      
        logger.info(f"Inserted {len(rows)} videos")
    except Exception as e:
        logger.error(f"Error while inserting: {e}")
        raise

def create_channels_table():
    sql = """
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.youtube_channels(
        channel_id TEXT PRIMARY KEY,
        channel_name TEXT NOT NULL,
        description TEXT,
        published_at TIMESTAMP,
        country TEXT,
        view_count BIGINT DEFAULT 0,
        subscriber_count BIGINT DEFAULT 0,
        video_count BIGINT DEFAULT 0,
        load_ts TIMESTAMP NOT NULL,

        CONSTRAINT valid_channel_id CHECK (channel_id ~ '^UC[a-zA-Z0-9_-]+$')
    );

    CREATE INDEX IF NOT EXISTS idx_channels_name ON raw.youtube_channels(channel_name);
    CREATE INDEX IF NOT EXISTS idx_channels_country ON raw.youtube_channels(country);
    CREATE INDEX IF NOT EXISTS idx_channels_updated ON raw.youtube_channels(load_ts);
    """

    try:
        with get_pg_connections() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
        logger.info("Table raw.youtube_channels is created/verirfied")

    except Exception as e:
        logger.error(f"Error while creating channels table: {e}")
        raise 


def get_existing_channel_ids(limit: int = 1000) -> list:
    sql = """
        SELECT DISTINCT v.channel_id
        FROM raw.youtube_videos v
        WHERE v.channel_id IS NOT NULL
            AND LENGTH(TRIM(v.channel_id)) > 0
        ORDER BY v.channel_id
        LIMIT %s;
    """
  
    try:
        with get_pg_connections() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (limit,))
                results = cur.fetchall()
                channel_ids = [row[0] for row in results if row[0]]
        
        logger.info(f"Retrieved {len(channel_ids)} unique channel IDs")
        return channel_ids
    except Exception as e:
        logger.error(f"Error fetching channel IDs: {e}")
        return []


def insert_channels(channels: list[dict]) -> int:
    if not channels:
        logger.info("No channels to insert")
        return 0

    rows = []
    for channel in channels:
        try:
            rows.append((
                channel.get("channel_id"),
                channel.get("channel_name", ""),
                channel.get("description", ""),
                channel.get("published_at"),
                channel.get("country"),
                channel.get("view_count", 0),
                channel.get("subscriber_count", 0),
                channel.get("video_count", 0),
                channel.get("load_ts", datetime.utcnow())
            )) 
        except Exception as e:
            logger.warning(f"Skipping malformed channel data: {e}")
            continue
    if not rows:
        logger.warning("No valid channel data to insert")
        return 0
    
    sql = """
    INSERT INTO raw.youtube_channels
    (channel_id, channel_name, description, published_at,
    country, view_count, subscriber_count, video_count, load_ts)
    VALUES %s
    ON CONFLICT (channel_id) DO UPDATE SET
        channel_name = EXCLUDED.channel_name,
        description = EXCLUDED.description,
        country = EXCLUDED.country,
        view_count = EXCLUDED.view_count,
        subscriber_count = EXCLUDED.subscriber_count,
        video_count = EXCLUDED.video_count,
        load_ts = EXCLUDED.load_ts
    WHERE raw.youtube_channels.load_ts < EXCLUDED.load_ts;
    """

    try:
        with get_pg_connections() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, rows, page_size=100)
                inserted_count = cur.rowcount

        logger.info(f"Inserted/updated {inserted_count} channels")
        return inserted_count
    
    except Exception as e:
        logger.error(f"Error inserting channels: {e}")
        raise

def get_channel_stats():
    """
    Returns statistics for monitoring
    """
    sql = """
    SELECT 
        COUNT(*) AS total_channels,
        COUNT(DISTINCT country) AS unique_countries,
        SUM(CASE WHEN channel_name IS NULL OR channel_name = '' THEN 1 ELSE 0 END) AS channels_without_name,
        MAX(load_ts) AS last_updated,
        AVG(subscriber_count) AS avg_subscribers
    FROM raw.youtube_channels;
    """ 

    try:
        with get_pg_connections() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                stats = cur.fetchone()

        return {
            "total_channels": stats[0],
            "unique_countries": stats[1],
            "channels_without_name": stats[2],
            "last_updated": stats[3],
            "avg_subscribers": float(stats[4] if stats[4] else 0)
        }
    except Exception as e:
        logger.error(f"Error fetching channel stats: {e}")
        return {}
    
def cleanup_old_channels(days_old: int= 90):
    """
    Removes channels that have not appeared in videos for more than N days.
    """
    sql = """
    DELETE FROM raw_youtube_channels c
    WHERE NOT EXISTS (
        SELECT 1 FROM raw.youtube_videos v
        WHERE v.channel_id = c.channel_id
        AND v.published_at > NOW() - INTERVAL '%s days'
        )
    AND c.load_ts < NOW() - INTERVAL '%s days'
    """

    try:
        with get_pg_connections() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (days_old, days_old))
                deleted_count = cur.rowcount

        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} old channels (older than {days_old} days)")
        return deleted_count
    
    except Exception as e:
        logger.error(f"Error cleaning up old channels: {e}")
        return 0
    
def export_channels_to_csv(filepath: str = '/tmp/youtube_channels.csv'):
    sql = "COPY (SELECT * FROM raw.youtube_channels ORDER BY channel_name) TO STDOUT WITH CSV HEADER;"

    try:
        with get_pg_connections() as conn:
            with conn.cursor() as cur:
                with open(filepath, 'w', encoding='utf-8') as f:
                    cur.copy_expert(sql, f)

        logger.info(f"Channel ecported to {filepath}")
        return filepath
    
    except Exception as e:
        logger.error(f"Error exporting channels: {e}")
        return None