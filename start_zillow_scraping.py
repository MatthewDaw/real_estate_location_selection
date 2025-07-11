import os
import time
import math
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import batch_v1
from google.protobuf import duration_pb2


class ZillowCollectionDispatcher:
    def __init__(self, project_id: str, task_count: int, provisioning_model: str = "SPOT"):
        self.project_id = project_id
        self.task_count = task_count
        self.provisioning_model = provisioning_model
        self.batch_client = batch_v1.BatchServiceClient()

        # Define regions and their subnetworks
        self.regions = {
            'us-west1': 'projects/flowing-flame-464314-j5/regions/us-west1/subnetworks/default-subnet',
            'us-west2': 'projects/flowing-flame-464314-j5/regions/us-west2/subnetworks/default-subnet',
            'us-west3': 'projects/flowing-flame-464314-j5/regions/us-west3/subnetworks/default-subnet',
            'us-central1': 'projects/flowing-flame-464314-j5/regions/us-central1/subnetworks/default-subnet',
            'us-east1': 'projects/flowing-flame-464314-j5/regions/us-east1/subnetworks/default-subnet',
            'us-east4': 'projects/flowing-flame-464314-j5/regions/us-east4/subnetworks/default-subnet',
        }

    def distribute_tasks(self) -> List[Tuple[str, int]]:
        """Distribute tasks evenly across regions"""
        regions_list = list(self.regions.keys())
        num_regions = len(regions_list)

        # Calculate base tasks per region and remainder
        base_tasks_per_region = self.task_count // num_regions
        remainder = self.task_count % num_regions

        distributions = []

        print(f"Distributing {self.task_count} tasks across {num_regions} regions...")

        for i, region in enumerate(regions_list):
            # Distribute remainder among first few regions
            tasks_for_region = base_tasks_per_region + (1 if i < remainder else 0)

            if tasks_for_region > 0:
                distributions.append((region, tasks_for_region))
                print(f"Assigning {tasks_for_region} tasks to {region}")

        return distributions

    def create_job_for_region(self, region: str, task_count: int) -> str:
        """Create a batch job for a specific region"""
        parent = f"projects/{self.project_id}/locations/{region}"
        job_id = f"zillow-collection-{region}-{int(time.time())}"

        # Get the appropriate subnetwork for this region
        subnetwork = self.regions[region]

        request = batch_v1.CreateJobRequest(
            parent=parent,
            job_id=job_id,
            job=batch_v1.Job(
                name=f"{parent}/jobs/{job_id}",
                task_groups=[
                    batch_v1.TaskGroup(
                        task_count=task_count,
                        parallelism=task_count,  # Run all tasks in parallel
                        task_spec=batch_v1.TaskSpec(
                            max_run_duration=duration_pb2.Duration(seconds=24 * 60 * 60),
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
                                            "Xvfb :99 -screen 0 1280x720x24 > /dev/null 2>&1 & sleep 2 && export DISPLAY=:99 && exec uv run real_estate_location_selection/scrapers/zillow/run_zillow_scraper.py"
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
                                            "BATCH_REGION": region,  # Add region info for debugging
                                            "BATCH_TASK_COUNT": str(task_count)
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
                                provisioning_model=self.provisioning_model
                            )
                        )
                    ],
                    network=batch_v1.AllocationPolicy.NetworkPolicy(
                        network_interfaces=[
                            batch_v1.AllocationPolicy.NetworkInterface(
                                network="projects/flowing-flame-464314-j5/global/networks/matt-default",
                                subnetwork=subnetwork,
                                no_external_ip_address=False
                            )
                        ]
                    ),
                ),
                logs_policy=batch_v1.LogsPolicy(destination="CLOUD_LOGGING"),
            ),
        )

        try:
            response = self.batch_client.create_job(request=request)
            print(f"Created job {job_id} in {region} with {task_count} tasks")
            return job_id
        except Exception as e:
            print(f"Failed to create job in {region}: {e}")
            return None

    def deploy_jobs(self) -> List[str]:
        """Deploy jobs across multiple regions"""
        distributions = self.distribute_tasks()

        if not distributions:
            print("No tasks could be distributed due to quota limitations")
            return []

        job_ids = []

        # Deploy jobs in parallel across regions
        with ThreadPoolExecutor(max_workers=len(distributions)) as executor:
            future_to_region = {
                executor.submit(self.create_job_for_region, region, task_count): region
                for region, task_count in distributions
            }

            for future in as_completed(future_to_region):
                region = future_to_region[future]
                try:
                    job_id = future.result()
                    if job_id:
                        job_ids.append(job_id)
                except Exception as e:
                    print(f"Job creation failed for region {region}: {e}")

        print(
            f"\nSuccessfully created {len(job_ids)} jobs across {len(set(dist[0] for dist in distributions))} regions")
        print("Job IDs:", job_ids)

        return job_ids


def main():
    # Configuration
    PROJECT_ID = "flowing-flame-464314-j5"
    TASK_COUNT = 32  # This is now a free variable - will be distributed as quota allows
    PROVISIONING_MODEL = "SPOT"  # Options: "STANDARD" or "SPOT"

    dispatcher = ZillowCollectionDispatcher(PROJECT_ID, TASK_COUNT, PROVISIONING_MODEL)
    job_ids = dispatcher.deploy_jobs()

    if job_ids:
        print(f"\nDeployment complete! {len(job_ids)} jobs created.")
        print("Monitor jobs with:")
        for job_id in job_ids:
            print(f"  gcloud batch jobs describe {job_id}")
    else:
        print("No jobs were created. Check quotas and try again.")


if __name__ == "__main__":
    main()