from datetime import datetime
from prefect import task, flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from lib.sample_task import SampleTask

date_format = "%Y-%m-%d_%H-%M-%S"

@task(name="Ingress to lake")
def ingest_to_lake(logger):
    SampleTask(min_time=6,
               max_time=10,
               title="LAKE INGRESS",
               operation="ingress").run(logger=logger)


@flow(name="Egressing JSON schema for Products and Orders", log_prints=True, flow_run_name=f"ingress_to_lake{datetime.now().strftime(date_format)}")
def ingress_to_lake():
    logger = get_run_logger()
    ingest_to_lake(logger)

if __name__ == "__main__":
    ingress_to_lake()