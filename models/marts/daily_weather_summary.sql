-- Daily weather summary with min, max, avg values per station and variable
select
    ts.date,
    ts.noaa_weather_station_id,
    st.noaa_weather_station_name,
    st.state_name,
    st.country_name,
    ts.variable_name,
    ts.unit,
    min(ts.value) as min_value,
    max(ts.value) as max_value,
    avg(ts.value) as avg_value,
    count(*) as reading_count
from {{ ref('stg_noaa_weather_metrics_timeseries_pit') }} ts
left join {{ ref('stg_noaa_weather_station_index_pit') }} st
    on ts.noaa_weather_station_id = st.noaa_weather_station_id
group by 1, 2, 3, 4, 5, 6, 7

