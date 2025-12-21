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
        comment_count BIGINT,
        raw_payload JSONB
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