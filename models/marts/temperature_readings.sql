-- Temperature readings from the past month
select
    ts.date,
    ts.datetime,
    ts.noaa_weather_station_id,
    st.noaa_weather_station_name,
    st.state_name,
    st.country_name,
    st.latitude,
    st.longitude,
    ts.variable_name,
    ts.value as temperature_value,
    ts.unit
from {{ ref('stg_noaa_weather_metrics_timeseries_pit') }} ts
left join {{ ref('stg_noaa_weather_station_index_pit') }} st
    on ts.noaa_weather_station_id = st.noaa_weather_station_id
where lower(ts.variable_name) like '%temp%'
   or lower(ts.variable_name) like '%tmax%'
   or lower(ts.variable_name) like '%tmin%'

