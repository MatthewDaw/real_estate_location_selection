import os
import time

from google.cloud import batch_v1

parent = "projects/flowing-flame-464314-j5/locations/us-central1"
job_id = "flowing-flame-464314-j5-rentsource-google-place-id-%i" % int(time.time())
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
                        batch_v1.Runnable(
                            container=batch_v1.Runnable.Container(
                                image_uri="us-central1-docker.pkg.dev/flowing-flame-464314-j5/apis/flowing-flame-464314-j5-google-place-id:latest"
                            ),
                            environment=batch_v1.Environment(
                                variables={
                                    "DB_HOST": os.getenv("DB_HOST"),
                                    "DB_NAME": os.getenv("DB_NAME"),
                                    "DB_USER": os.getenv("DB_USER"),
                                    "DB_PASS": os.getenv("DB_PASS"),
                                    "GOOGLE_API_KEY": os.getenv("GOOGLE_API_KEY"),
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
client = batch_v1.BatchServiceClient()
response = client.create_job(request=request)
print(response)