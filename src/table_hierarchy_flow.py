import logging
from time import sleep
from random import randint
from prefect import task, flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner

duration_small = (1,3)
duration_med = (3,6)
duration_large = (4,9)

@task(name="Orders and products JSON")
def ingest_root_table(logger):
    logger.info(f"ROOT - Getting data")
    sleep(randint(*duration_small))
    logger.info(f"ROOT - Completed ingestion")

@task(name="Product root")
def ingest_level_1_products(logger):
    logger.info(f"PRODUCT - Getting data")
    sleep(randint(*duration_small))
    logger.info(f"PRODUCT - Completed ingesion")


@task(name="Orders root")
def ingest_level_1_orders(logger):
    logger.info(f"ORDERS - Getting data")
    sleep(randint(*duration_large))
    logger.info(f"ORDERS - Completed ingesion")

@task(name="Product details")
def ingest_level_2_product_details(logger):
    logger.info(f"PRODUCT DEETS - Getting data")
    sleep(randint(*duration_small))
    logger.info(f"PRODUCT DEETS - Completed ingesion")


@task(name="Order details")
def ingest_level_2_order_details(logger):
    logger.info(f"ORDER DEETS - Getting data")
    sleep(randint(*duration_small))
    logger.info(f"ORDER DEETS - Completed ingesion")


@task(name="Order detail items")
def ingest_level_3_order_detail_items(logger):
    logger.info(f"ORDER DEETS ITEMS - Getting data")
    sleep(randint(*duration_small))
    logger.info(f"ORDER DEETS ITEMS - Completed ingesion")


@flow(name="Ingesting JSON schema for Products and Orders - Deps", log_prints=True, task_runner=ConcurrentTaskRunner())
def ingest_hierarchy():
    logger = get_run_logger()
    root = ingest_root_table(logger)
    orders = ingest_level_1_orders(logger, wait_for=[root])
    prod = ingest_level_1_products(logger, wait_for=[root])
    order_details = ingest_level_2_order_details(logger, wait_for=[orders])
    ingest_level_2_product_details(logger, wait_for=[prod])
    ingest_level_3_order_detail_items(logger, wait_for=[order_details])


if __name__ == "__main__":
    ingest_hierarchy()
