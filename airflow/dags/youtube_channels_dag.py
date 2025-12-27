from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone
import requests
import os
import time
import logging
from youtube_postgres import create_channels_table, insert_channels, get_existing_channel_ids

logger = logging.getLogger(__name__)

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
CHANNELS_URL = "https://www.googleapis.com/youtube/v3/channels"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="youtube_channels_dag", 
    default_args=default_args,
    start_date=datetime(2025, 12, 1, tzinfo=timezone.utc),
    schedule_interval='0 3 * * 0',  
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "channels", "dimensions"]
)
def youtube_channels():
    
    @task()
    def setup_task():
        """Создаёт таблицу для каналов."""
        try:
            create_channels_table()
            logger.info("Channels table created/verified")
        except Exception as e:
            logger.error(f"Failed to create channels table: {e}")
            raise
    
    @task()
    def get_channel_ids():
        """Получает список channel_id из базы."""
        try:
            channel_ids = get_existing_channel_ids()
            total_channels = len(channel_ids)

            if total_channels == 0:
                logger.warning("No channel IDs found in database")
                return []
            
            max_channels_per_run = 300
            if total_channels > max_channels_per_run:
                logger.info(f"Found {total_channels} channels, limiting to {max_channels_per_run}")
                channel_ids = channel_ids[:max_channels_per_run]

            logger.info(f"Retrieved {len(channel_ids)} channel IDs for update")
            return channel_ids
            
        except Exception as e:
            logger.error(f"Failed to retrieve channel IDs: {e}")
            return [] 
    
    @task()
    def fetch_channel_data(channel_ids: list):

        if not channel_ids:
            logger.info("No channel IDs to fetch")
            return []
        
        channel_data = []
        failed_batches = []
        requested_count = len(channel_ids)

        try:
            for batch_idx, i in enumerate(range(0, len(channel_ids), 50), 1):
                batch = channel_ids[i:i + 50]

                try:
                    params = {
                        "part": "snippet,statistics",  
                        "id": ",".join(batch),
                        "key": YOUTUBE_API_KEY
                    }

                    logger.info(f"Fetching batch {batch_idx} ({len(batch)} channels)")
                    response = requests.get(CHANNELS_URL, params=params, timeout=30)
                    response.raise_for_status()

                    remaining_quota = response.headers.get("X-Rate-Limit-Remaining")
                    if remaining_quota:
                        logger.info(f"API quota remaining: {remaining_quota}")

                    data = response.json()
                    items = data.get("items", [])

                    returned_count = len(items)
                    requested_in_batch = len(batch)
                    
                    if requested_count < requested_in_batch:
                        requested_ids = set(batch)
                        returned_ids = set(item["id"] for item in items)
                        missing_ids = requested_ids - returned_ids

                        logger.warning(
                            f"Batch {batch_idx}:API returned only {returned_count} /{requested_in_batch} channels."
                            f"Missing ID {len(missing_ids)}"
                            f"First 5 missing: {list(missing_ids)[:5] if missing_ids else 'None'}"
                        )
                    if not items:
                        logger.warning(f"No data returned for batch {batch_idx}")
                        failed_batches.append(batch_idx)
                        continue

                    for item in items:
                        try:
                            snippet = item.get("snippet", {})
                            stats = item.get("statistics", {})

                            channel_info = {
                                "channel_id": item["id"],
                                "channel_name": snippet.get("title", "").strip(),
                                "description": snippet.get("description", "")[:1000],
                                "published_at": snippet.get("publishedAt"),
                                "country": snippet.get("country"),
                                "view_count": int(stats.get("viewCount", 0)),
                                "subscriber_count": int(stats.get("subscriberCount", 0)),
                                "video_count": int(stats.get("videoCount", 0)),
                                "load_ts": datetime.now(timezone.utc).isoformat()
                            }

                            if not channel_info["channel_id"] or not channel_info["channel_name"]:
                                logger.warning(f"Invalid channel data for ID: {channel_info.get('channel_id')}")
                                continue

                            channel_data.append(channel_info)

                        except (KeyError, ValueError, TypeError) as e:
                            logger.error(f"Error processing channel {item.get('id', 'unknown')}: {e}")
                            continue
                    
                    logger.info(f"Batch {batch_idx}: processed {len(items)} channels")

                    time.sleep(2) 

                except requests.exceptions.RequestException as e:
                    logger.error(f"API request failed for batch {batch_idx}: {e}")
                    failed_batches.append(batch_idx)
                    time.sleep(10)
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error in batch {batch_idx}: {e}")
                    failed_batches.append(batch_idx)
                    continue
            
            logger.info(f"Successfully fetched {len(channel_data)} channels")
            if failed_batches:
                logger.warning(f"Failed batches: {failed_batches}")
            if len(channel_data) < len(channel_ids):
                logger.warning(f"Fetched only {len(channel_data)} of {len(channel_ids)} requested channels")

            return channel_data
            
        except Exception as e:
            logger.error(f"Critical error in fetch_channel_data: {e}")
            raise 

    @task()
    def save_channels(channel_data: list):
        if not channel_data:
            logger.info("No channel data to save")
            return 0
        
        try:
            inserted_count = insert_channels(channel_data)
            logger.info(f"Successfully saved/updated {inserted_count} channels")

            return inserted_count
            
        except Exception as e:
            logger.error(f"Failed to save channels: {e}")
            raise
    
    @task()
    def final_report(inserted_count: int, channel_ids_list: list):
        try:
            total_requested = len(channel_ids_list) if channel_ids_list else 0 
            success_rate = (inserted_count / total_requested * 100) if total_requested > 0 else 0

            logger.info("=" * 50)
            logger.info("CHANNELS DAG EXECUTION REPORT")
            logger.info("=" * 50)
            logger.info(f"Total channels requested: {total_requested}")
            logger.info(f"Successfully processed: {inserted_count}")
            logger.info(f"Success rate: {success_rate:.1f}%")
            logger.info("=" * 50)

            if success_rate < 80:
                logger.warning("Low success rate detected")
            if inserted_count == 0:
                logger.error("No channels were processed")
                
        except Exception as e:
            logger.error(f"Error generating report: {e}")
    

    setup = setup_task()
    channel_ids = get_channel_ids()
    channel_data = fetch_channel_data(channel_ids)
    inserted_count = save_channels(channel_data)
    report = final_report(inserted_count, channel_ids)  
    
    setup >> channel_ids >> channel_data >> inserted_count >> report


dag = youtube_channels()