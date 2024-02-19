from datetime import datetime
from prefect import task, flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from lib.sample_task import SampleTask

duration_small = (1,3)
duration_med = (3,6)
duration_large = (4, 9)
date_format = "%Y-%m-%d_%H-%M-%S"

small_task = SampleTask(*duration_small, operation="egress")
big_task = SampleTask(*duration_large, operation="egress")

@task(name="Orders and products JSON")
def ingest_root_table(logger):
    small_task.run("ROOT", logger)

@task(name="Product root")
def ingest_level_1_products(logger):
    big_task.run("PRODUCT", logger)


@task(name="Orders root")
def ingest_level_1_orders(logger):
    small_task.run("ORDERS", logger)

@task(name="Product details")
def ingest_level_2_product_details(logger):
    big_task.run("PRODUCT DEETS", logger)


@task(name="Order details")
def ingest_level_2_order_details(logger):
    small_task.run("ORDER DEETS", logger)


@task(name="Order detail items")
def ingest_level_3_order_detail_items(logger):
    small_task.run("ORDER DEETS ITEMS", logger)


@flow(name="Ingesting JSON schema for Products and Orders - Deps", log_prints=True, task_runner=ConcurrentTaskRunner(), flow_run_name=f"prod_orders_deps_{datetime.now().strftime(date_format)}")
def egress_hierarchy():
    logger = get_run_logger()
    root = ingest_root_table(logger)
    orders = ingest_level_1_orders.submit(logger, wait_for=[root])
    prod = ingest_level_1_products.submit(logger, wait_for=[root])
    order_details = ingest_level_2_order_details.submit(logger, wait_for=[orders])
    ingest_level_2_product_details.submit(logger, wait_for=[prod])
    ingest_level_3_order_detail_items.submit(logger, wait_for=[order_details])


if __name__ == "__main__":
    egress_hierarchy()
