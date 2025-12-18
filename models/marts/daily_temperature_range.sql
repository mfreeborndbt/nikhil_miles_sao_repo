-- Daily temperature range (difference between max and min) by station
with temp_readings as (
    select
        ts.date,
        ts.noaa_weather_station_id,
        st.noaa_weather_station_name,
        st.state_name,
        st.country_name,
        ts.value,
        ts.unit
    from {{ ref('stg_noaa_weather_metrics_timeseries_pit') }} ts
    left join {{ ref('stg_noaa_weather_station_index_pit') }} st
        on ts.noaa_weather_station_id = st.noaa_weather_station_id
    where lower(ts.variable_name) like '%temp%'
       or lower(ts.variable_name) like '%tmax%'
       or lower(ts.variable_name) like '%tmin%'
)

select
    date,
    noaa_weather_station_id,
    noaa_weather_station_name,
    state_name,
    country_name,
    min(value) as min_temp,
    max(value) as max_temp,
    max(value) - min(value) as temp_range,
    avg(value) as avg_temp,
    unit
from temp_readings
group by 1, 2, 3, 4, 5, unit
having max(value) - min(value) > 0
order by temp_range desc

