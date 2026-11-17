select
    iso_country,
    airport_type,
    count(*) as airport_count,
    countif(iata_code is not null and trim(iata_code) != '') as records_with_iata
from {{ ref('stg_airports') }}
where iso_country is not null and airport_type is not null
group by iso_country, airport_type
