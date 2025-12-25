{{ config(
    materialized='table',
    engine='ReplacingMergeTree(updated_at)',
    order_by='category_id'
)}}

SELECT 
    toString(category_id) AS category_id,
    toString(category_name) AS category_name,
    toString(category_group) AS category_group,
    if(is_active ='TRUE', 1, 0) as is_active_flag,
    priority AS display_priority,

    multiIf(category_group = 'Entertainment', 1,
            category_group = 'Sports & Gaming', 2,
            category_group = 'Education & News', 3,
            category_group = 'Lifestyle', 4,
            category_group = 'Content Creation', 5,
            6
    ) AS group_priority,
    if(category_group IN ('Entertainment', 'Sports & Gaming'), 1, 0) AS is_mainstream_flag,
    if(category_id IN ('10', '20', '17', '23', '24'), 1, 0) AS is_top_category_flag,
    multiIf(category_id = '10', 'Music',
            category_id = '20', 'Gaming',
            category_id = '17', 'Sports',
            category_id = '23', 'Comedy',
            category_id = '24', 'Entertainment',
            category_name
    )AS short_name,
    splitByChar('/', category_name)[1] AS primary_topic,
    length(category_name) < 20 AS is_short_name,
    now() AS updated_at
    FROM {{ ref('youtube_categories') }}

