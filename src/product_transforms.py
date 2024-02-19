from datetime import datetime
from prefect import task, flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from lib.sample_task import SampleTask

date_format = "%Y-%m-%d_%H-%M-%S"

@task(name="Data aggregations")
def transform_data(logger):
    SampleTask(min_time=6,
               max_time=10,
               title="AGGREGATE DATA",
               operation="transform").run(logger=logger)


@flow(name="Aggregate JSON schema for Products and Orders", log_prints=True, flow_run_name=f"aggregate_prod_and_orders{datetime.now().strftime(date_format)}")
def transform_prod_and_orders():
    logger = get_run_logger()
    transform_data(logger)

if __name__ == "__main__":
    transform_prod_and_orders()