-- Weather station network coverage analysis
select
    weather_station_network,
    count(distinct noaa_weather_station_id) as station_count,
    count(distinct country_name) as countries_covered,
    count(distinct state_name) as states_covered,
    avg(elevation) as avg_elevation,
    min(latitude) as min_latitude,
    max(latitude) as max_latitude,
    min(longitude) as min_longitude,
    max(longitude) as max_longitude
from {{ ref('stg_noaa_weather_station_index_pit') }}
where weather_station_network is not null
group by 1
order by station_count desc

