-- Count of weather stations by country
select
    country_name,
    country_geo_id,
    count(distinct noaa_weather_station_id) as station_count,
    count(distinct state_name) as state_count,
    avg(latitude) as avg_latitude,
    avg(longitude) as avg_longitude,
    avg(elevation) as avg_elevation
from {{ ref('stg_noaa_weather_station_index_pit') }}
group by 1, 2
order by station_count desc

