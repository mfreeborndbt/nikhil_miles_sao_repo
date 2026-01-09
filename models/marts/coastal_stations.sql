-- Coastal weather stations (elevation near sea level, < 50m)
select
    noaa_weather_station_id,
    noaa_weather_station_name,
    country_name,
    state_name,
    latitude,
    longitude,
    elevation,
    weather_station_network
from {{ ref('stg_noaa_weather_station_index_pit') }}
where elevation is not null
  and elevation >= 0
  and elevation < 50
order by elevation, country_name, state_name



