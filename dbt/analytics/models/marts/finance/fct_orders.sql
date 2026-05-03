{{
  config(
    materialized='table',
    cluster_by=['created_at::date'],
    tags=['marts', 'finance', 'critical']
  )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        o.order_id,
        o.customer_id,
        c.customer_name,
        c.customer_segment,
        o.order_total_usd,
        o.tax_amount_usd,
        o.order_total_usd - o.tax_amount_usd as net_revenue_usd,
        o.created_at,
        date_trunc('month', o.created_at) as order_month
    from orders o
    left join customers c using (customer_id)
)

select * from final