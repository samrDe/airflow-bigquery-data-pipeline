select *
from {{ ref('stg_airports') }}
where (longitude is not null and (longitude < -180 or longitude > 180))
   or (latitude is not null and (latitude < -90 or latitude > 90))
