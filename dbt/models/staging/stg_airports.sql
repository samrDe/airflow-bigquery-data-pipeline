with source as (
    select * from {{ source('raw', 'raw_airports') }}
),
renamed as (
    select
        cast(ident as string) as ident,
        lower(trim(cast(type as string))) as airport_type,
        trim(cast(name as string)) as airport_name,
        safe_cast(elevation_ft as int64) as elevation_ft,
        cast(continent as string) as continent,
        upper(trim(cast(iso_country as string))) as iso_country,
        cast(iso_region as string) as iso_region,
        trim(cast(municipality as string)) as municipality,
        cast(gps_code as string) as gps_code,
        cast(iata_code as string) as iata_code,
        cast(local_code as string) as local_code,
        safe_cast(trim(split(cast(coordinates as string), ',')[safe_offset(0)]) as float64) as longitude,
        safe_cast(trim(split(cast(coordinates as string), ',')[safe_offset(1)]) as float64) as latitude
    from source
)
select * from renamed
