{% macro iso8601_duration_to_seconds(duration_str) %}
    multiIf(
        empty({{ duration_str }}) OR {{ duration_str }} IS NULL, 0,
        coalesce(toInt32OrNull(regexpExtract({{ duration_str }}, '(\\d+)D', 1)), 0) * 86400 +
        coalesce(toInt32OrNull(regexpExtract({{ duration_str }}, '(\\d+)H', 1)), 0) * 3600 +
        coalesce(toInt32OrNull(regexpExtract({{ duration_str }}, '(\\d+)M', 1)), 0) * 60 +
        coalesce(toInt32OrNull(regexpExtract({{ duration_str }}, '(\\d+)S', 1)), 0)
    )
{% endmacro %}