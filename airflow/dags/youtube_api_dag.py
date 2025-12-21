from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import requests 
import os 
from datetime import datetime
import logging 
from youtube_postgres import create_raw_table, insert_videos


logger = logging.getLogger(__name__)

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1
    }

@dag(
    dag_id="youtube_raw_dag",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    tags=["youtube", "raw"]
)

def youtube_raw():

    @task()
    def create_table_task():
        create_raw_table()

    @task()
    def fetch_videos():
        videos = []
        try:
            params = {
                "part": "snippet",
                "maxResults": 50,
                "order": "date",
                "type": "video",
                "publishedAfter": "2025-12-10T00:00:00Z",
                "q": "news|health|technology|music|sports|education|cosmetics",
                "key": YOUTUBE_API_KEY
            }
            next_page_token = None 
            while len(videos) < 200:
                if next_page_token:
                    params["pageToken"] = next_page_token

                response = requests.get(SEARCH_URL, params=params)
                response.raise_for_status()
                data = response.json()

                for item in data.get("items", []):
                    video_id = item.get("id", {}).get("videoId")
                    if not video_id:
                        continue
                    videos.append({
                        "video_id": video_id,
                        "channel_id": item["snippet"]["channelId"],
                        "published_at": item["snippet"]["publishedAt"],
                        "title": item["snippet"]["title"],
                        "raw_payload": item,
                        "load_ts": datetime.utcnow()
                    })
                
                next_page_token = data.get("nextPageToken")
                if not next_page_token:
                    break 
            logger.info(f"Fetched {len(videos)} videos")
        except Exception as e:
            logger.error(f"Failed fetching videos: {e}")
            raise 

        return videos
    
    @task 
    def fetch_video_stats(videos: list):
        enriched = []
        try:
            for i in range(0, len(videos), 50):
                batch = videos[i:i+50]
                video_ids = ",".join([v["video_id"] for v in batch])
                params = {
                    "part": "statistics",
                    "id": video_ids,
                    "key": YOUTUBE_API_KEY
                }
                response = requests.get(VIDEOS_URL, params=params)
                response.raise_for_status()
                data = response.json()

                stats_map = {v["id"]: v.get("statistics", {}) for v in data.get("items", [])}

                for v in batch:
                    stats = stats_map.get(v["video_id"], {})
                    v["view_count"] = int(stats.get("viewCount", 0))
                    v["like_count"] = int(stats.get("likeCount", 0))
                    v["comment_count"] = int(stats.get("commentCount", 0))
                    enriched.append(v)
            logger.info("Video statistics successfully retrieved")
        except Exception as e:
            logger.error(f"Error retrieving video statistics: {e}")
            raise
        return enriched
    
    @task()
    def save_videos(videos):
        insert_videos(videos)

    create_table_task()
    videos = fetch_videos()
    enriched = fetch_video_stats(videos)
    save_videos(enriched)

dag = youtube_raw()