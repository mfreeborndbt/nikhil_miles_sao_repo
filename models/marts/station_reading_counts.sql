-- Count of readings per station in the past month
select
    ts.noaa_weather_station_id,
    st.noaa_weather_station_name,
    st.country_name,
    st.state_name,
    st.weather_station_network,
    count(*) as total_readings,
    count(distinct ts.date) as days_with_readings,
    count(distinct ts.variable_name) as unique_variables,
    min(ts.date) as first_reading_date,
    max(ts.date) as last_reading_date
from {{ ref('stg_noaa_weather_metrics_timeseries_pit') }} ts
left join {{ ref('stg_noaa_weather_station_index_pit') }} st
    on ts.noaa_weather_station_id = st.noaa_weather_station_id
group by 1, 2, 3, 4, 5
order by total_readings desc

