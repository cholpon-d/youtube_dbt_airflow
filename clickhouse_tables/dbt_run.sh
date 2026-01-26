docker compose exec dbt bash

root@d05d2397d797:/usr/app/youtube_project# cd /usr/app/youtube_project

dbt seed --target clickhouse

dbt run --exclude staging.* --target clickhouse # create analytics schema and tables