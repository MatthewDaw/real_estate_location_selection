import os
import time

from google.cloud import batch_v1

parent = "projects/flowing-flame-464314-j5/locations/us-west3"
job_id = "flowing-flame-464314-j5-land-watch-url-loading-%i" % int(time.time())
request = batch_v1.CreateJobRequest(
    parent=parent,
    job_id=job_id,
    job=batch_v1.Job(
        name=f"{parent}/jobs/{job_id}",
        task_groups=[
            batch_v1.TaskGroup(
                task_count=1,
                parallelism=1,
                task_spec=batch_v1.TaskSpec(
                    compute_resource=batch_v1.ComputeResource(
                        cpu_milli=1000,
                        memory_mib=2048
                    ),
                    max_retry_count=10,
                    runnables=[
                        batch_v1.Runnable(
                            container=batch_v1.Runnable.Container(
                                image_uri="us-west3-docker.pkg.dev/flowing-flame-464314-j5/real-estate-location-selection/zillow-scraper:latest",
                                entrypoint="sh",
                                commands=[
                                    "-c",
                                    "Xvfb :99 -screen 0 1280x720x24 > /dev/null 2>&1 & sleep 2 && export DISPLAY=:99 && exec uv run real_estate_location_selection/scrapers/land_watch/run_land_watch_job_initialization.py"
                                ]
                            ),
                            environment=batch_v1.Environment(
                                variables={
                                    "PERSONAL_GOOGLE_CLOUD_DB_HOST": os.getenv("PERSONAL_GOOGLE_CLOUD_DB_HOST"),
                                    "PERSONAL_GOOGLE_CLOUD_DB_NAME": os.getenv("PERSONAL_GOOGLE_CLOUD_DB_NAME"),
                                    "PERSONAL_GOOGLE_CLOUD_DB_USER": os.getenv("PERSONAL_GOOGLE_CLOUD_DB_USER"),
                                    "PERSONAL_GOOGLE_CLOUD_DB_PASS": os.getenv("PERSONAL_GOOGLE_CLOUD_DB_PASS"),
                                    "EVOMI_USERNAME": os.getenv("EVOMI_USERNAME"),
                                    "EVOMI_PASSWORD": os.getenv("EVOMI_PASSWORD"),
                                }
                            ),
                        )
                    ],
                ),
            )
        ],
        allocation_policy=batch_v1.AllocationPolicy(
            instances=[
                batch_v1.AllocationPolicy.InstancePolicyOrTemplate(
                    policy=batch_v1.AllocationPolicy.InstancePolicy(
                        machine_type="e2-small",
                        provisioning_model="STANDARD"
                    )
                )
            ]
        ),
        logs_policy=batch_v1.LogsPolicy(destination="CLOUD_LOGGING"),
    ),
)
if __name__ == '__main__':
    client = batch_v1.BatchServiceClient()
    response = client.create_job(request=request)
    print(response)