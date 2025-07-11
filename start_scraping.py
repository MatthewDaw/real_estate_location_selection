import os
import time
import math
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import batch_v1, compute_v1
from google.protobuf import duration_pb2


class UnifiedScraperDispatcher:
    def __init__(self, project_id: str, task_count: int, provisioning_model: str = "SPOT",
                 max_tasks_per_region: int = 8):
        self.project_id = project_id
        self.task_count = task_count
        self.provisioning_model = provisioning_model
        self.max_tasks_per_region = max_tasks_per_region
        self.batch_client = batch_v1.BatchServiceClient()
        self.compute_client = compute_v1.RegionsClient()

        # Define regions and their subnetworks
        self.regions = {
            'us-west1': 'projects/flowing-flame-464314-j5/regions/us-west1/subnetworks/default-subnet',
            'us-west2': 'projects/flowing-flame-464314-j5/regions/us-west2/subnetworks/default-subnet',
            'us-west3': 'projects/flowing-flame-464314-j5/regions/us-west3/subnetworks/default-subnet',
            'us-central1': 'projects/flowing-flame-464314-j5/regions/us-central1/subnetworks/default-subnet',
            'us-east1': 'projects/flowing-flame-464314-j5/regions/us-east1/subnetworks/default-subnet',
            'us-east4': 'projects/flowing-flame-464314-j5/regions/us-east4/subnetworks/default-subnet',
        }

        # Job configurations
        self.job_configs = {
            'landwatch': {
                'job_prefix': 'land-watch-collection',
                'script_path': 'real_estate_location_selection/scrapers/land_watch/run_land_watch_scraper.py'
            },
            'zillow': {
                'job_prefix': 'zillow-collection',
                'script_path': 'real_estate_location_selection/scrapers/zillow/run_zillow_scraper.py'
            }
        }

    def get_region_quota(self, region: str) -> int:
        """Get available IN_USE_ADDRESSES quota for a region"""
        try:
            request = compute_v1.GetRegionRequest(
                project=self.project_id,
                region=region
            )
            response = self.compute_client.get(request=request)

            for quota in response.quotas:
                if quota.metric == 'IN_USE_ADDRESSES':
                    available = quota.limit - quota.usage
                    return max(0, int(available))
            return 0
        except Exception as e:
            print(f"Error checking quota for region {region}: {e}")
            return 0

    def distribute_tasks(self) -> List[Tuple[str, int]]:
        """Distribute tasks across regions based on available quota and max_tasks_per_region limit"""
        region_quotas = {}
        total_available = 0

        print("Checking quotas across regions...")
        for region in self.regions.keys():
            quota = self.get_region_quota(region)
            # Cap quota at max_tasks_per_region to ensure we don't exceed the limit
            effective_quota = min(quota, self.max_tasks_per_region)
            region_quotas[region] = effective_quota
            total_available += effective_quota
            print(f"Region {region}: {quota} available addresses (capped at {effective_quota})")

        if total_available < self.task_count:
            print(
                f"Warning: Only {total_available} addresses available across all regions (with {self.max_tasks_per_region} max per region), but {self.task_count} tasks requested")
            print("Consider requesting quota increases, reducing task count, or increasing max_tasks_per_region")

        # Distribute tasks proportionally, but cap at available quota and max_tasks_per_region
        distributions = []
        remaining_tasks = self.task_count

        # Sort regions by available quota (descending)
        sorted_regions = sorted(region_quotas.items(), key=lambda x: x[1], reverse=True)

        for region, available_quota in sorted_regions:
            if remaining_tasks <= 0 or available_quota <= 0:
                continue

            # Take the minimum of remaining tasks, available quota, and max_tasks_per_region
            tasks_for_region = min(remaining_tasks, available_quota, self.max_tasks_per_region)

            if tasks_for_region > 0:
                distributions.append((region, tasks_for_region))
                remaining_tasks -= tasks_for_region
                print(f"Assigning {tasks_for_region} tasks to {region}")

        if remaining_tasks > 0:
            print(f"Warning: Could not assign {remaining_tasks} tasks due to quota/region limitations")

        return distributions

    def distribute_tasks_evenly(self) -> List[Tuple[str, int]]:
        """Distribute tasks evenly across regions (fallback method without quota checking)"""
        regions_list = list(self.regions.keys())
        num_regions = len(regions_list)

        # Calculate base tasks per region and remainder
        base_tasks_per_region = self.task_count // num_regions
        remainder = self.task_count % num_regions

        distributions = []

        print(
            f"Distributing {self.task_count} tasks evenly across {num_regions} regions (max {self.max_tasks_per_region} per region)...")

        for i, region in enumerate(regions_list):
            # Distribute remainder among first few regions
            tasks_for_region = base_tasks_per_region + (1 if i < remainder else 0)

            # Cap at max_tasks_per_region
            tasks_for_region = min(tasks_for_region, self.max_tasks_per_region)

            if tasks_for_region > 0:
                distributions.append((region, tasks_for_region))
                print(f"Assigning {tasks_for_region} tasks to {region}")

        # Check if we couldn't assign all tasks due to max_tasks_per_region limit
        total_assigned = sum(tasks for _, tasks in distributions)
        if total_assigned < self.task_count:
            print(
                f"Warning: Could only assign {total_assigned} of {self.task_count} tasks due to max_tasks_per_region limit of {self.max_tasks_per_region}")

        return distributions

    def create_job(self, region: str, task_count: int, job_type: str) -> str:
        """Create a batch job for a specific region and job type"""
        config = self.job_configs[job_type]
        parent = f"projects/{self.project_id}/locations/{region}"
        job_id = f"{config['job_prefix']}-{region}-{int(time.time())}"
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
                                            f"Xvfb :99 -screen 0 1280x720x24 > /dev/null 2>&1 & sleep 2 && export DISPLAY=:99 && exec uv run {config['script_path']}"
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
                                            "BATCH_TASK_COUNT": str(task_count),
                                            "JOB_TYPE": job_type  # Add job type for debugging
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
                ),
                logs_policy=batch_v1.LogsPolicy(destination="CLOUD_LOGGING"),
            ),
        )

        try:
            response = self.batch_client.create_job(request=request)
            print(f"Created {job_type} job {job_id} in {region} with {task_count} tasks")
            return job_id
        except Exception as e:
            print(f"Failed to create {job_type} job in {region}: {e}")
            return None

    def deploy_jobs(self, job_type: str, use_quota_checking: bool = True) -> List[str]:
        """Deploy jobs across multiple regions for a specific job type"""
        if use_quota_checking:
            try:
                distributions = self.distribute_tasks()
            except Exception as e:
                print(f"Quota checking failed: {e}. Falling back to even distribution.")
                distributions = self.distribute_tasks_evenly()
        else:
            distributions = self.distribute_tasks_evenly()

        if not distributions:
            print(f"No {job_type} tasks could be distributed")
            return []

        job_ids = []

        # Deploy jobs in parallel across regions
        with ThreadPoolExecutor(max_workers=len(distributions)) as executor:
            future_to_region = {
                executor.submit(self.create_job, region, task_count, job_type): region
                for region, task_count in distributions
            }

            for future in as_completed(future_to_region):
                region = future_to_region[future]
                try:
                    job_id = future.result()
                    if job_id:
                        job_ids.append(job_id)
                except Exception as e:
                    print(f"{job_type} job creation failed for region {region}: {e}")

        print(
            f"\nSuccessfully created {len(job_ids)} {job_type} jobs across {len(set(dist[0] for dist in distributions))} regions")
        print(f"{job_type} Job IDs:", job_ids)

        return job_ids

    def deploy_landwatch_jobs(self, use_quota_checking: bool = True) -> List[str]:
        """Deploy LandWatch jobs across multiple regions"""
        return self.deploy_jobs('landwatch', use_quota_checking)

    def deploy_zillow_jobs(self, use_quota_checking: bool = True) -> List[str]:
        """Deploy Zillow jobs across multiple regions"""
        return self.deploy_jobs('zillow', use_quota_checking)


def main():
    # Configuration
    PROJECT_ID = "flowing-flame-464314-j5"
    LANDWATCH_TASK_COUNT = 24  # Number of LandWatch tasks to distribute
    ZILLOW_TASK_COUNT = 32  # Number of Zillow tasks to distribute
    PROVISIONING_MODEL = "SPOT"  # Options: "STANDARD" or "SPOT"
    MAX_TASKS_PER_REGION = 8  # Maximum tasks per region to ensure we stay within quota
    USE_QUOTA_CHECKING = True  # Set to False if you don't have google-cloud-compute installed

    print("=== Deploying LandWatch Jobs ===")
    landwatch_dispatcher = UnifiedScraperDispatcher(
        PROJECT_ID,
        LANDWATCH_TASK_COUNT,
        PROVISIONING_MODEL,
        MAX_TASKS_PER_REGION
    )
    landwatch_job_ids = landwatch_dispatcher.deploy_landwatch_jobs(USE_QUOTA_CHECKING)

    print("\n=== Deploying Zillow Jobs ===")
    zillow_dispatcher = UnifiedScraperDispatcher(
        PROJECT_ID,
        ZILLOW_TASK_COUNT,
        PROVISIONING_MODEL,
        MAX_TASKS_PER_REGION
    )
    zillow_job_ids = zillow_dispatcher.deploy_zillow_jobs(USE_QUOTA_CHECKING)

    # Summary
    print(f"\n=== Deployment Summary ===")
    print(f"LandWatch jobs created: {len(landwatch_job_ids)}")
    print(f"Zillow jobs created: {len(zillow_job_ids)}")
    print(f"Total jobs created: {len(landwatch_job_ids) + len(zillow_job_ids)}")

    if landwatch_job_ids or zillow_job_ids:
        print("\nMonitor jobs with:")
        for job_id in landwatch_job_ids + zillow_job_ids:
            print(f"  gcloud batch jobs describe {job_id}")
    else:
        print("No jobs were created. Check quotas and try again.")


if __name__ == "__main__":
    main()
