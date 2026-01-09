-- All US weather stations with their details
select
    noaa_weather_station_id,
    noaa_weather_station_name,
    state_name,
    zip_name,
    latitude,
    longitude,
    elevation,
    weather_station_network,
    world_meteorological_organization_id
from {{ ref('stg_noaa_weather_station_index_pit') }}
where country_name = 'United States'
order by state_name, noaa_weather_station_name



