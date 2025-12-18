select
    variable_name,
    variable,
    measurement_type,
    frequency,
    measure,
    unit,
    _effective_start_timestamp,
    _effective_end_timestamp
from {{ source('noaa_weather', 'NOAA_WEATHER_METRICS_ATTRIBUTES_PIT') }}
