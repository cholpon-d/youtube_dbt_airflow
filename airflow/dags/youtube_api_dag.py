from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException
import requests 
import os 
from datetime import datetime, timedelta, timezone
import logging 
from youtube_postgres import create_raw_table, insert_videos, check_video_data_exists

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
    start_date=datetime(2025, 12, 1),
    end_date=datetime(2025, 12, 31), 
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=3,
    concurrency=10,
    tags=["youtube", "raw"]
)
def youtube_raw():

    @task()
    def create_table_task():
        create_raw_table()

    @task()
    def fetch_videos():
        context = get_current_context()
        logical_date = context['logical_date']
        target_date = logical_date.date()

        if check_video_data_exists(target_date, threshold=20):
            logger.info(f"Data for {target_date} already sufficient. Skipping.")
            raise AirflowSkipException(f"Enough data for {target_date}")

        published_after = logical_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        published_before = (logical_date + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

        videos = []
        params = {
            "part": "snippet",
            "maxResults": 50,
            "order": "date",
            "type": "video",
            "publishedAfter": published_after,
            "publishedBefore": published_before,
            "q": "news|health|technology|music|movies|anime|sports|education|cosmetics|travel|cooking|fitness|vlog|gaming",
            "key": YOUTUBE_API_KEY
        }
        
        next_page_token = None 
        request_count = 0
        MAX_REQUESTS = 40 

        try:
            while len(videos) < 2000 and request_count < MAX_REQUESTS:
                if next_page_token:
                    params["pageToken"] = next_page_token

                response = requests.get(SEARCH_URL, params=params)
                response.raise_for_status()
                data = response.json()

                if not data.get("items"):
                    break

                for item in data.get("items", []):
                    video_id = item.get("id", {}).get("videoId")
                    if video_id:
                        videos.append({
                            "video_id": video_id,
                            "channel_id": item["snippet"]["channelId"],
                            "published_at": item["snippet"]["publishedAt"],
                            "title": item["snippet"]["title"],
                            "raw_payload": item,
                            "load_ts": datetime.now(timezone.utc)
                        })
                
                next_page_token = data.get("nextPageToken")
                if not next_page_token:
                    break 
                request_count += 1
            
            logger.info(f"Collected {len(videos)} videos for {target_date}")
            return videos
        except Exception as e:
            logger.error(f"API Error: {e}")
            raise 
    
    @task 
    def fetch_video_stats(videos: list):
        if not videos:
            return []
            
        enriched = []
        try:
            for i in range(0, len(videos), 50):
                batch = videos[i:i+50]
                video_ids = ",".join([v["video_id"] for v in batch])
                params = {
                    "part": "snippet,statistics,contentDetails,topicDetails",
                    "id": video_ids,
                    "key": YOUTUBE_API_KEY
                }
                
                response = requests.get(VIDEOS_URL, params=params)
                response.raise_for_status()
                data = response.json()
                
                video_map = {item["id"]: item for item in data.get("items", [])}

                for v in batch:
                    info = video_map.get(v["video_id"], {})
                    stats = info.get("statistics", {})
                    snippet = info.get("snippet", {})
                    details = info.get("contentDetails", {})
                    
                    v.update({
                        "view_count": int(stats.get("viewCount", 0)),
                        "like_count": int(stats.get("likeCount", 0)),
                        "comment_count": int(stats.get("commentCount", 0)),
                        "dislike_count": int(stats.get("dislikeCount", 0)),
                        "favorite_count": int(stats.get("favoriteCount", 0)),
                        "category_id": snippet.get("categoryId"),
                        "duration_raw": details.get("duration", "PT0S"),
                        "has_caption": details.get("caption", "false").lower() == "true",
                        "topic_categories": info.get("topicDetails", {}).get("topicCategories", [])
                    })
                    enriched.append(v)
            return enriched
        except Exception as e:
            logger.error(f"Enrichment error: {e}")
            raise

    @task()
    def save_videos(videos):
        if videos:
            insert_videos(videos)

    setup = create_table_task()
    raw_videos = fetch_videos()
    enriched_metadata = fetch_video_stats(raw_videos)
    saving = save_videos(enriched_metadata)
    setup >> raw_videos >> enriched_metadata >> saving

dag_instance = youtube_raw()