-- Catalog of all weather variables and their attributes
select
    variable_name,
    variable,
    measurement_type,
    frequency,
    measure,
    unit
from {{ ref('stg_noaa_weather_metrics_attributes_pit') }}
order by variable_name



