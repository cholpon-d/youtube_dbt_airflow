{% macro format_duration(duration_seconds, format='seconds') %}
    {%- if format == 'seconds' -%}
        {{ duration_seconds }}
    {%- elif format == 'minutes' -%}
        round({{ duration_seconds }} / 60.0, 2)
    {%- elif format == 'human' -%}
        multiIf(
            {{ duration_seconds }} >= 3600,  
            format(
                '{}:{:02d}:{:02d}',
                floor({{ duration_seconds }} / 3600),
                floor(mod({{ duration_seconds }}, 3600) / 60),
                mod({{ duration_seconds }}, 60)
            ),
            format(
                '{}:{:02d}',
                floor({{ duration_seconds }} / 60), 
                mod({{ duration_seconds }}, 60)  
            )
        )
    {%- elif format == 'human_short' -%}
        multiIf(
            {{ duration_seconds }} >= 3600,
            format('{}h {}m', 
                floor({{ duration_seconds }} / 3600),
                floor(mod({{ duration_seconds }}, 3600) / 60)
            ),
            {{ duration_seconds }} >= 60,
            format('{}m {}s',
                floor({{ duration_seconds }} / 60),
                mod({{ duration_seconds }}, 60)
            ),
            format('{}s', {{ duration_seconds }})
        )
    {%- elif format == 'hours' -%}
        round({{ duration_seconds }} / 3600.0, 2)
    {%- elif format == 'hours_minutes' -%}
        round({{ duration_seconds }} / 3600.0, 1)
    {%- elif format == 'category' -%}
        multiIf(
            {{ duration_seconds }} < 60, 'Shorts (<1 мин)',
            {{ duration_seconds }} < 300, 'Clips (1-5 мин)',
            {{ duration_seconds }} < 600, 'Standarts (5-10 мин)',
            {{ duration_seconds }} < 1800, 'Longs (10-30 мин)',
                    )
    {%- else -%}
        {{ duration_seconds }}
    {%- endif -%}
{% endmacro %}