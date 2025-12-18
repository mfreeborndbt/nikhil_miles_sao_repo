-- Count of weather stations by state
select
    country_name,
    state_name,
    count(distinct noaa_weather_station_id) as station_count,
    avg(elevation) as avg_elevation,
    min(elevation) as min_elevation,
    max(elevation) as max_elevation
from {{ ref('stg_noaa_weather_station_index_pit') }}
where state_name is not null
group by 1, 2
order by station_count desc

