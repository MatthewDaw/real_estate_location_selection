import os
import time

from google.cloud import batch_v1

parent = "projects/flowing-flame-464314-j5/locations/us-west3"
job_id = "flowing-flame-464314-j5-land-watch-collection-%i" % int(time.time())
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
                        cpu_milli=2000,
                        memory_mib=2048
                    ),
                    max_retry_count=10,
                    runnables=[
                        # Cloud SQL Proxy - runs in background
                        batch_v1.Runnable(
                            container=batch_v1.Runnable.Container(
                                image_uri="gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.8.0",
                                commands=[
                                    "/cloud-sql-proxy",
                                    "--structured-logs",
                                    "--unix-socket=/cloudsql",
                                    "flowing-flame-464314-j5:us-west3:matt-sandbox"
                                ]
                            ),
                            background=True  # Critical: runs in background
                        ),
                        # Your application - runs in foreground
                        batch_v1.Runnable(
                            container=batch_v1.Runnable.Container(
                                image_uri="us-west3-docker.pkg.dev/flowing-flame-464314-j5/real-estate-location-selection/land-watch-scraper:latest"
                            ),
                            environment=batch_v1.Environment(
                                variables={
                                    "PERSONAL_GOOGLE_CLOUD_DB_HOST": os.getenv("PERSONAL_GOOGLE_CLOUD_DB_HOST"),
                                    "PERSONAL_GOOGLE_CLOUD_DB_NAME": os.getenv("PERSONAL_GOOGLE_CLOUD_DB_NAME"),
                                    "PERSONAL_GOOGLE_CLOUD_DB_USER": os.getenv("PERSONAL_GOOGLE_CLOUD_DB_USER"),
                                    "PERSONAL_GOOGLE_CLOUD_DB_PASS": os.getenv("PERSONAL_GOOGLE_CLOUD_DB_PASS"),
                                    "EVOMI_USERNAME": os.getenv("EVOMI_USERNAME"),
                                    "EVOMI_PASSWORD": os.getenv("EVOMI_PASSWORD"),
                                    "INSTANCE_UNIX_SOCKET": "/cloudsql/flowing-flame-464314-j5:us-west3:matt-sandbox",
                                }
                            ),
                        )
                    ],
                    # Add shared volume for the Unix socket
                    volumes=[
                        batch_v1.Volume(
                            mount_path="/cloudsql"
                        )
                    ]
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

client = batch_v1.BatchServiceClient()
response = client.create_job(request=request)
print(response)