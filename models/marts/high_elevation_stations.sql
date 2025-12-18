-- Weather stations at high elevation (above 2000m / ~6500ft)
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
where elevation >= 2000
order by elevation desc

