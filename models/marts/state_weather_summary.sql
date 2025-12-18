-- Weather summary aggregated by state
select
    st.country_name,
    st.state_name,
    ts.date,
    ts.variable_name,
    ts.unit,
    count(distinct ts.noaa_weather_station_id) as reporting_stations,
    min(ts.value) as min_value,
    max(ts.value) as max_value,
    avg(ts.value) as avg_value,
    sum(ts.value) as total_value
from {{ ref('stg_noaa_weather_metrics_timeseries_pit') }} ts
inner join {{ ref('stg_noaa_weather_station_index_pit') }} st
    on ts.noaa_weather_station_id = st.noaa_weather_station_id
where st.state_name is not null
group by 1, 2, 3, 4, 5

