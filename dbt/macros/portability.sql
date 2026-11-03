{#
  Dialect-specific expressions live here, behind adapter.dispatch, so the models stay portable.
  Each macro has a DuckDB and a Snowflake implementation, and no default: a new engine must opt in.
#}

{# The UTC calendar date of a timezone-aware timestamp, whatever the session time zone is. #}
{% macro utc_date(expr) %}
  {{ return(adapter.dispatch('utc_date', 'recon')(expr)) }}
{% endmacro %}

{% macro duckdb__utc_date(expr) %}cast(timezone('UTC', {{ expr }}) as date){% endmacro %}

{% macro snowflake__utc_date(expr) %}cast(convert_timezone('UTC', {{ expr }}) as date){% endmacro %}


{# ISO day of week: Monday = 1 ... Sunday = 7. #}
{% macro iso_dow(expr) %}
  {{ return(adapter.dispatch('iso_dow', 'recon')(expr)) }}
{% endmacro %}

{% macro duckdb__iso_dow(expr) %}isodow({{ expr }}){% endmacro %}

{% macro snowflake__iso_dow(expr) %}dayofweekiso({{ expr }}){% endmacro %}


{# The as-of date from vars, as a DATE literal. #}
{% macro as_of_date() %}cast('{{ var("as_of") }}' as date){% endmacro %}
