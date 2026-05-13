{{
  config(
    materialized='view',
    tags=['staging', 'orders']
  )
}}

with source as (
    select * from {{ source('raw', 'orders') }}
),

renamed as (
    select
        -- ids
        order_id::varchar          as order_id,
        customer_id::varchar       as customer_id,

        -- amounts (FIXED)
        order_total::numeric(18,2)  as order_total_usd,
        tax_amount::numeric(18,2)   as tax_amount_usd,

        -- timestamps
        created_at::timestamp       as created_at,
        updated_at::timestamp       as updated_at,

        -- metadata
        _extracted_at::timestamp    as _extracted_at,

        -- dedup: keep latest record per order
        row_number() over (
            partition by order_id
            order by updated_at desc
        ) as _row_num

    from source
    where order_id is not null
)

select *
from renamed
where _row_num = 1