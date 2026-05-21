import random
import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from faker import Faker

from config.default_args import DEFAULT_ARGS

logger = logging.getLogger(__name__)

fake = Faker()

ORDER_STATUSES = [
    "pending",
    "confirmed",
    "processing",
    "shipped",
    "delivered",
    "cancelled",
]

CURRENCIES = ["USD", "EUR", "BDT"]
COUNTRIES = ["US", "BD", "UK", "CA", "IN"]


def generate_orders(records_count=100, **kwargs):

    postgres = PostgresHook(postgres_conn_id="postgres_source")

    # Get existing customer IDs
    customer_query = """
        SELECT customer_id
        FROM public.customers
    """

    customers = postgres.get_records(customer_query)

    if not customers:
        raise Exception("No customers found")

    customer_ids = [row[0] for row in customers]

    rows = []

    for _ in range(records_count):

        status = random.choice(ORDER_STATUSES)

        order_total = round(random.uniform(100, 10000), 2)
        tax_amount = round(order_total * 0.1, 2)
        discount_amount = round(random.uniform(0, order_total * 0.2), 2)
        shipping_amount = round(random.uniform(20, 500), 2)

        order_ref = f"ORD-{fake.unique.random_number(digits=8)}"

        placed_at = fake.date_time_this_year()

        shipped_at = None
        delivered_at = None
        cancelled_at = None

        if status in ["shipped", "delivered"]:
            shipped_at = fake.date_time_between(
                start_date=placed_at,
                end_date="+5d"
            )

        if status == "delivered":
            delivered_at = fake.date_time_between(
                start_date=shipped_at,
                end_date="+5d"
            )

        if status == "cancelled":
            cancelled_at = fake.date_time_between(
                start_date=placed_at,
                end_date="+2d"
            )

        rows.append(
            (
                order_ref,
                random.choice(customer_ids),
                status,
                order_total,
                tax_amount,
                discount_amount,
                shipping_amount,
                random.choice(CURRENCIES),
                random.choice(COUNTRIES),
                fake.sentence(nb_words=6),
                placed_at,
                shipped_at,
                delivered_at,
                cancelled_at,
            )
        )

    insert_query = """
        INSERT INTO public.orders (
            order_ref,
            customer_id,
            status,
            order_total,
            tax_amount,
            discount_amount,
            shipping_amount,
            currency,
            shipping_country,
            notes,
            placed_at,
            shipped_at,
            delivered_at,
            cancelled_at
        )
        VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s
        )
    """

    postgres.insert_rows(
        table="public.orders",
        rows=rows,
        target_fields=[
            "order_ref",
            "customer_id",
            "status",
            "order_total",
            "tax_amount",
            "discount_amount",
            "shipping_amount",
            "currency",
            "shipping_country",
            "notes",
            "placed_at",
            "shipped_at",
            "delivered_at",
            "cancelled_at"
        ]
    )

    logger.info(f"{len(rows)} orders inserted")


with DAG(
    dag_id="generate_random_orders",
    default_args=DEFAULT_ARGS,
    description="Generate random order data",
    schedule_interval="*/5 * * * *",  # every 5 mins
    catchup=False,
    max_active_runs=1,
    tags=["mock-data", "orders"],
) as dag:

    generate_task = PythonOperator(
        task_id="generate_orders",
        python_callable=generate_orders,
        op_kwargs={
            "records_count": 50
        }
    )