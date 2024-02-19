from datetime import datetime
from prefect import task, flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from lib.sample_task import SampleTask
from src.product_ingress import ingress_to_lake
from src.product_transforms import transform_prod_and_orders
from src.table_hierarchy_flow import egress_hierarchy

date_format = "%Y-%m-%d_%H-%M-%S"

@flow(name="ETL for JSON schema for Products and Orders", log_prints=True, flow_run_name=f"etl_for_prod_and_orders{datetime.now().strftime(date_format)}")
def product_and_orders_etl():
    ingress_to_lake()
    transform_prod_and_orders()
    egress_hierarchy()

if __name__ == "__main__":
    product_and_orders_etl()