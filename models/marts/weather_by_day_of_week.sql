-- Weather patterns by day of week
select
    dayofweek(date) as day_of_week_num,
    dayname(date) as day_of_week_name,
    variable_name,
    unit,
    count(*) as reading_count,
    avg(value) as avg_value,
    min(value) as min_value,
    max(value) as max_value,
    stddev(value) as stddev_value
from {{ ref('stg_noaa_weather_metrics_timeseries_pit') }}
group by 1, 2, 3, 4
order by day_of_week_num, variable_name

