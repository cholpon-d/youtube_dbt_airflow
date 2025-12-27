from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
import requests 
import os 
from datetime import datetime, timedelta, timezone
import time
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
    start_date=datetime(2025, 12, 1),
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1,
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
            context = get_current_context()
            logical_date = context['logical_date']

            utc_now = datetime.now(timezone.utc)
            
            day_start = logical_date.replace(
                hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

            if day_start >= utc_now:
                day_start = utc_now - timedelta(days=1)
                day_start = day_start.replace(hour=0, minute=0, second=0, microsecond=0)  

            published_after = day_start.replace(tzinfo=None).isoformat() + "Z"   

            params = {
                "part": "snippet",
                "maxResults": 50,
                "order": "date",
                "type": "video",
                "publishedAfter": published_after,
                "q": "news|health|technology|music|movies|anime|sports|education|cosmetics|travel|cooking|fitness|vlog|gaming",
                "key": YOUTUBE_API_KEY
            }
            next_page_token = None 
            request_count = 0
            MAX_REQUESTS = 50
            while len(videos) < 2000 and request_count < MAX_REQUESTS:
                if next_page_token:
                    params["pageToken"] = next_page_token

                response = requests.get(SEARCH_URL, params=params)

                logger.info(f"Rate limit: {response.headers.get('X-Rate-Limit-Remaining')}")

                response.raise_for_status()
                data = response.json()

                if not data.get("items"):
                    logger.info(f"No videos found for {day_start.date()}")
                    return []

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

                request_count += 1
            logger.info(f"Request {request_count}: collected {len(videos)} videos for {day_start.date()}")

            time.sleep(1)

            logger.info(f"Total collected {len(videos)} videos for {day_start.date()}")
            return videos
        except Exception as e:
            logger.error(f"Failed fetching videos for {day_start.date() if 'day_start' in locals() else 'unknown date'}: {e}")
            raise 
    
    @task 
    def fetch_video_stats(videos: list):
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

                video_data_map = {item["id"]: item for item in data.get("items", [])}

                for v in batch:
                    data_for_video = video_data_map.get(v["video_id"], {})
                    stats = data_for_video.get("statistics", {})
                    v["view_count"] = int(stats.get("viewCount", 0))
                    v["like_count"] = int(stats.get("likeCount", 0))
                    v["comment_count"] = int(stats.get("commentCount", 0))
                    v["dislike_count"] = int(stats.get("dislikeCount", 0))
                    v["favorite_count"] = int(stats.get("favoriteCount", 0))
                    snippet = data_for_video.get("snippet", {})
                    v["category_id"] = snippet.get("categoryId")

                    content_details = data_for_video.get("contentDetails", {})
                    v["duration_raw"] = content_details.get("duration", "PT0S")
                    caption_status = content_details.get("caption", "false")
                    v["has_caption"] = caption_status.lower() == "true"
                    topic_details = data_for_video.get("topicDetails", {})
                    v["topic_categories"] = topic_details.get("topicCategories", [])

                    enriched.append(v)
            logger.info(f"Successfully enriched {len(enriched)} videos")
        except Exception as e:
            logger.error(f"Error retrieving extended video data: {e}")
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