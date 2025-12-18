-- Filter to current records only (most recent version of each station)
select
    noaa_weather_station_id,
    noaa_weather_station_name,
    country_geo_id,
    country_name,
    state_geo_id,
    state_name,
    zip_geo_id,
    zip_name,
    latitude,
    longitude,
    elevation,
    weather_station_network,
    associated_networks,
    world_meteorological_organization_id,
    source_data,
    _effective_start_timestamp,
    _effective_end_timestamp
from {{ source('noaa_weather', 'NOAA_WEATHER_STATION_INDEX_PIT') }}
where _effective_end_timestamp is null
   or _effective_end_timestamp > current_timestamp()
