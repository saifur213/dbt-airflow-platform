{{
  config(
    materialized='view',
    tags=['staging', 'customers']
  )
}}

with source as (

    select * 
    from {{ source('raw', 'customers') }}

),

renamed as (

    select

        -- ids
        customer_id::varchar              as customer_id,
        external_id::uuid                 as external_id,

        -- attributes
        email::varchar(255)               as email,
        full_name::varchar(255)           as full_name,
        phone::varchar(50)                as phone,
        segment::varchar(50)              as segment,
        country_code::char(2)             as country_code,
        city::varchar(100)                as city,
        is_active::boolean                as is_active,

        -- timestamps
        created_at::timestamp             as created_at,
        updated_at::timestamp             as updated_at,

        -- deduplication watermark
        row_number() over (
            partition by customer_id
            order by updated_at desc
        ) as _row_num

    from source
    where customer_id is not null

)

select *
from renamed
where _row_num = 1