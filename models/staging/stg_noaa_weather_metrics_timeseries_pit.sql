select
    date,
    datetime,
    variable_name,
    noaa_weather_station_id,
    variable,
    value,
    unit,
    _effective_start_timestamp,
    _effective_end_timestamp
from {{ source('noaa_weather', 'NOAA_WEATHER_METRICS_TIMESERIES_PIT') }}
where date >= dateadd(month, -1, current_date())
