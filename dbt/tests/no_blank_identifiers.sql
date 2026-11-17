select *
from {{ ref('stg_airports') }}
where ident is null or trim(ident) = ''
