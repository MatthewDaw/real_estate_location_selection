

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
                task_count=20,
                parallelism=20,
                task_spec=batch_v1.TaskSpec(
                    max_retry_count=10,
                    compute_resource=batch_v1.ComputeResource(
                        cpu_milli="2000", memory_mib="2048"
                    ),
                    runnables=[
                        batch_v1.Runnable(
                            container=batch_v1.Runnable.Container(
                            image_uri="us-west3-docker.pkg.dev/flowing-flame-464314-j5/real-estate-location-selection/zillow-scraper:latest",
                            # Override the container's default entrypoint/command
                            entrypoint="sh",
                            commands=[
                                "-c",
                                "Xvfb :99 -screen 0 1280x720x24 > /dev/null 2>&1 & sleep 2 && export DISPLAY=:99 && exec uv run real_estate_location_selection/scrapers/zillow/run_landwatch_scraping.py"
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
                                    "INSTANCE_CONNECTION_NAME": "flowing-flame-464314-j5:us-central1:matt-sandbox",
                                    "BATCH_TASK_INDEX": "${BATCH_TASK_INDEX}",  # Task index for differentiation (0-19)
                                    "DISPLAY": ":99"  # Set display environment variable
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
                        provisioning_model="SPOT",
                        machine_type="e2-small",
                        boot_disk=batch_v1.AllocationPolicy.Disk(
                            size_gb=30,
                        ),
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
# import os
# import time
#
# from google.cloud import batch_v1
#
# parent = "projects/flowing-flame-464314-j5/locations/us-west3"
# job_id = "flowing-flame-464314-j5-land-watch-collection-%i" % int(time.time())
# request = batch_v1.CreateJobRequest(
#     parent=parent,
#     job_id=job_id,
#     job=batch_v1.Job(
#         name=f"{parent}/jobs/{job_id}",
#         task_groups=[
#             batch_v1.TaskGroup(
#                 task_count=2,
#                 parallelism=2,
#                 task_spec=batch_v1.TaskSpec(
#                     compute_resource=batch_v1.ComputeResource(
#                         cpu_milli=2000,
#                         memory_mib=2048
#                     ),
#                     max_retry_count=10,
#                     max_run_duration="86400s",
#                     runnables=[
#                         batch_v1.Runnable(
#                             container=batch_v1.Runnable.Container(
#                                 image_uri="us-west3-docker.pkg.dev/flowing-flame-464314-j5/real-estate-location-selection/zillow-scraper:latest",
#                                 entrypoint="sh",
#                                 commands=[
#                                     "-c",
#                                     "Xvfb :99 -screen 0 1280x720x24 > /dev/null 2>&1 & sleep 2 && export DISPLAY=:99 && exec uv run real_estate_location_selection/scrapers/zillow/run_landwatch_scraping.py"
#                                 ]
#                             ),
#                             environment=batch_v1.Environment(
#                                 variables={
#                                     "PERSONAL_GOOGLE_CLOUD_DB_HOST": os.getenv("PERSONAL_GOOGLE_CLOUD_DB_HOST"),
#                                     "PERSONAL_GOOGLE_CLOUD_DB_NAME": os.getenv("PERSONAL_GOOGLE_CLOUD_DB_NAME"),
#                                     "PERSONAL_GOOGLE_CLOUD_DB_USER": os.getenv("PERSONAL_GOOGLE_CLOUD_DB_USER"),
#                                     "PERSONAL_GOOGLE_CLOUD_DB_PASS": os.getenv("PERSONAL_GOOGLE_CLOUD_DB_PASS"),
#                                     "EVOMI_USERNAME": os.getenv("EVOMI_USERNAME"),
#                                     "EVOMI_PASSWORD": os.getenv("EVOMI_PASSWORD"),
#                                     "INSTANCE_CONNECTION_NAME": "flowing-flame-464314-j5:us-central1:matt-sandbox",
#                                     "BATCH_TASK_INDEX": "${BATCH_TASK_INDEX}",
#                                     "DISPLAY": ":99"
#                                 }
#                             ),
#                         )
#                     ],
#                 ),
#             )
#         ],
#         allocation_policy=batch_v1.AllocationPolicy(
#             instances=[
#                 batch_v1.AllocationPolicy.InstancePolicyOrTemplate(
#                     policy=batch_v1.AllocationPolicy.InstancePolicy(
#                         machine_type="e2-small",
#                         provisioning_model="SPOT",
#                         boot_disk=batch_v1.AllocationPolicy.Disk(
#                             type_="pd-standard",  # Use standard persistent disk instead of SSD
#                             size_gb=30  # Specify smaller disk size (default might be too large)
#                         )
#                     )
#                 )
#             ],
#             network=batch_v1.AllocationPolicy.NetworkPolicy(
#                 network_interfaces=[
#                     batch_v1.AllocationPolicy.NetworkInterface(
#                         network="projects/flowing-flame-464314-j5/global/networks/default",
#                         subnetwork="projects/flowing-flame-464314-j5/regions/us-west3/subnetworks/default",
#                         no_external_ip_address=True
#                     )
#                 ]
#             )
#         ),
#         logs_policy=batch_v1.LogsPolicy(destination="CLOUD_LOGGING"),
#     ),
# )
# client = batch_v1.BatchServiceClient()
# response = client.create_job(request=request)
# print(response)