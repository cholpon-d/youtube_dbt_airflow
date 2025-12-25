{% macro parse_and_format_duration(duration_str, output_format='seconds') %}
    {%- set seconds_macro = iso8601_duration_to_seconds(duration_str) -%}
    {{ format_duration(seconds_macro, output_format) }}
{% endmacro %}