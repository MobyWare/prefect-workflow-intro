from prefect import task, flow


@task
def initial_task():
    print("Initial task completed")

@task
def next_task():
    print("Next task completed")

@task
def final_task():
    print("Next task completed")

@flow(log_prints=True)
def sample_flow():
    initial_task()
    next_task()
    final_task()


def test_installation():
    sample_flow()