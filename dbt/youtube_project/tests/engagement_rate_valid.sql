SELECT channel_id, engagement_rate_percentage
FROM {{ ref('fact_youtube_channels') }}
WHERE engagement_rate_percentage < 0 OR engagement_rate_percentage > 100