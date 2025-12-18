-- Extreme weather readings (top and bottom 1% by variable)
with ranked_readings as (
    select
        ts.date,
        ts.noaa_weather_station_id,
        st.noaa_weather_station_name,
        st.state_name,
        st.country_name,
        ts.variable_name,
        ts.value,
        ts.unit,
        percent_rank() over (partition by ts.variable_name order by ts.value) as pct_rank
    from {{ ref('stg_noaa_weather_metrics_timeseries_pit') }} ts
    left join {{ ref('stg_noaa_weather_station_index_pit') }} st
        on ts.noaa_weather_station_id = st.noaa_weather_station_id
)

select
    date,
    noaa_weather_station_id,
    noaa_weather_station_name,
    state_name,
    country_name,
    variable_name,
    value,
    unit,
    case
        when pct_rank <= 0.01 then 'Bottom 1%'
        when pct_rank >= 0.99 then 'Top 1%'
    end as extreme_type
from ranked_readings
where pct_rank <= 0.01 or pct_rank >= 0.99
order by variable_name, pct_rank

