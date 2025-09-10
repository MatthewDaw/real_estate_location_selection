import os
import time
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

        # Define regional groups in priority order (US first, then geographically closer regions)
        self.region_groups = {
            'us_primary': {
                'us-west1': f'projects/{project_id}/regions/us-west1/subnetworks/default-subnet',
                'us-west2': f'projects/{project_id}/regions/us-west2/subnetworks/default-subnet',
                'us-central1': f'projects/{project_id}/regions/us-central1/subnetworks/default-subnet',
                'us-east1': f'projects/{project_id}/regions/us-east1/subnetworks/default-subnet',
                'us-east4': f'projects/{project_id}/regions/us-east4/subnetworks/default-subnet',
            },
            'us_secondary': {
                'us-west3': f'projects/{project_id}/regions/us-west3/subnetworks/default-subnet',
                'us-west4': f'projects/{project_id}/regions/us-west4/subnetworks/default-subnet',
                'us-east5': f'projects/{project_id}/regions/us-east5/subnetworks/default-subnet',
                'us-south1': f'projects/{project_id}/regions/us-south1/subnetworks/default-subnet',
            },
            'north_america': {
                'northamerica-northeast1': f'projects/{project_id}/regions/northamerica-northeast1/subnetworks/default-subnet',
                # Montreal
                'northamerica-northeast2': f'projects/{project_id}/regions/northamerica-northeast2/subnetworks/default-subnet',
                # Toronto
            },
            'south_america': {
                'southamerica-east1': f'projects/{project_id}/regions/southamerica-east1/subnetworks/default-subnet',
                # São Paulo
                'southamerica-west1': f'projects/{project_id}/regions/southamerica-west1/subnetworks/default-subnet',
                # Santiago
            },
            'europe': {
                'europe-west1': f'projects/{project_id}/regions/europe-west1/subnetworks/default-subnet',  # Belgium
                'europe-west2': f'projects/{project_id}/regions/europe-west2/subnetworks/default-subnet',  # London
                'europe-west3': f'projects/{project_id}/regions/europe-west3/subnetworks/default-subnet',  # Frankfurt
                'europe-west4': f'projects/{project_id}/regions/europe-west4/subnetworks/default-subnet',  # Netherlands
                'europe-west6': f'projects/{project_id}/regions/europe-west6/subnetworks/default-subnet',  # Zurich
                'europe-west8': f'projects/{project_id}/regions/europe-west8/subnetworks/default-subnet',  # Milan
                'europe-west9': f'projects/{project_id}/regions/europe-west9/subnetworks/default-subnet',  # Paris
                'europe-west10': f'projects/{project_id}/regions/europe-west10/subnetworks/default-subnet',  # Berlin
                'europe-west12': f'projects/{project_id}/regions/europe-west12/subnetworks/default-subnet',  # Turin
                'europe-central2': f'projects/{project_id}/regions/europe-central2/subnetworks/default-subnet',
                # Warsaw
                'europe-north1': f'projects/{project_id}/regions/europe-north1/subnetworks/default-subnet',  # Finland
                'europe-southwest1': f'projects/{project_id}/regions/europe-southwest1/subnetworks/default-subnet',
                # Madrid
            },
            'asia_pacific': {
                'asia-east1': f'projects/{project_id}/regions/asia-east1/subnetworks/default-subnet',  # Taiwan
                'asia-east2': f'projects/{project_id}/regions/asia-east2/subnetworks/default-subnet',  # Hong Kong
                'asia-northeast1': f'projects/{project_id}/regions/asia-northeast1/subnetworks/default-subnet',  # Tokyo
                'asia-northeast2': f'projects/{project_id}/regions/asia-northeast2/subnetworks/default-subnet',  # Osaka
                'asia-northeast3': f'projects/{project_id}/regions/asia-northeast3/subnetworks/default-subnet',  # Seoul
                'asia-south1': f'projects/{project_id}/regions/asia-south1/subnetworks/default-subnet',  # Mumbai
                'asia-south2': f'projects/{project_id}/regions/asia-south2/subnetworks/default-subnet',  # Delhi
                'asia-southeast1': f'projects/{project_id}/regions/asia-southeast1/subnetworks/default-subnet',
                # Singapore
                'asia-southeast2': f'projects/{project_id}/regions/asia-southeast2/subnetworks/default-subnet',
                # Jakarta
                'australia-southeast1': f'projects/{project_id}/regions/australia-southeast1/subnetworks/default-subnet',
                # Sydney
                'australia-southeast2': f'projects/{project_id}/regions/australia-southeast2/subnetworks/default-subnet',
                # Melbourne
            },
            'middle_east_africa': {
                'me-west1': f'projects/{project_id}/regions/me-west1/subnetworks/default-subnet',  # Tel Aviv
                'me-central1': f'projects/{project_id}/regions/me-central1/subnetworks/default-subnet',  # Doha
                'me-central2': f'projects/{project_id}/regions/me-central2/subnetworks/default-subnet',  # Dammam
                'africa-south1': f'projects/{project_id}/regions/africa-south1/subnetworks/default-subnet',
                # Johannesburg
            }
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

    def get_prioritized_regions(self, enabled_groups: List[str] = None) -> Dict[str, str]:
        """Get regions in priority order (US first, then geographically closer)"""
        if enabled_groups is None:
            # Default to US regions first, then expand as needed
            enabled_groups = ['us_primary', 'us_secondary', 'north_america', 'south_america', 'europe', 'asia_pacific',
                              'middle_east_africa']

        prioritized_regions = {}
        for group_name in enabled_groups:
            if group_name in self.region_groups:
                prioritized_regions.update(self.region_groups[group_name])

        return prioritized_regions

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

    def get_region_group_for_region(self, region: str) -> str:
        """Get the group name for a given region"""
        for group_name, regions in self.region_groups.items():
            if region in regions:
                return group_name
        return "unknown"

    def distribute_tasks(self, enabled_groups: List[str] = None) -> List[Tuple[str, int]]:
        """Distribute tasks across regions based on priority order and available quota"""
        regions = self.get_prioritized_regions(enabled_groups)
        region_quotas = {}
        total_available = 0

        print("Checking quotas across regions in priority order...")

        # Check quotas for all regions, maintaining priority order
        for region in regions.keys():
            quota = self.get_region_quota(region)
            # Cap quota at max_tasks_per_region to ensure we don't exceed the limit
            effective_quota = min(quota, self.max_tasks_per_region)
            region_quotas[region] = effective_quota
            total_available += effective_quota
            group_name = self.get_region_group_for_region(region)
            print(f"Region {region} ({group_name}): {quota} available addresses (capped at {effective_quota})")

        if total_available < self.task_count:
            print(
                f"Warning: Only {total_available} addresses available across all regions (with {self.max_tasks_per_region} max per region), but {self.task_count} tasks requested")
            print("Consider requesting quota increases, reducing task count, or increasing max_tasks_per_region")

        # Distribute tasks in priority order
        distributions = []
        remaining_tasks = self.task_count

        # Process regions in the order they appear in the prioritized list
        for region in regions.keys():
            if remaining_tasks <= 0:
                break

            available_quota = region_quotas[region]
            if available_quota <= 0:
                continue

            # Take the minimum of remaining tasks, available quota, and max_tasks_per_region
            tasks_for_region = min(remaining_tasks, available_quota, self.max_tasks_per_region)

            if tasks_for_region > 0:
                group_name = self.get_region_group_for_region(region)
                distributions.append((region, tasks_for_region))
                remaining_tasks -= tasks_for_region
                print(f"Assigning {tasks_for_region} tasks to {region} ({group_name})")

        if remaining_tasks > 0:
            print(f"Warning: Could not assign {remaining_tasks} tasks due to quota/region limitations")

        return distributions

    def distribute_tasks_evenly(self, enabled_groups: List[str] = None) -> List[Tuple[str, int]]:
        """Distribute tasks evenly across regions (fallback method without quota checking)"""
        regions = self.get_prioritized_regions(enabled_groups)
        regions_list = list(regions.keys())
        num_regions = len(regions_list)

        if num_regions == 0:
            print("No regions available for task distribution")
            return []

        # Calculate base tasks per region and remainder
        base_tasks_per_region = self.task_count // num_regions
        remainder = self.task_count % num_regions

        distributions = []

        print(
            f"Distributing {self.task_count} tasks evenly across {num_regions} regions (max {self.max_tasks_per_region} per region)...")

        for i, region in enumerate(regions_list):
            # Distribute remainder among first few regions (which are highest priority)
            tasks_for_region = base_tasks_per_region + (1 if i < remainder else 0)

            # Cap at max_tasks_per_region
            tasks_for_region = min(tasks_for_region, self.max_tasks_per_region)

            if tasks_for_region > 0:
                group_name = self.get_region_group_for_region(region)
                distributions.append((region, tasks_for_region))
                print(f"Assigning {tasks_for_region} tasks to {region} ({group_name})")

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

        # Get the subnetwork for this region
        regions = self.get_prioritized_regions()
        subnetwork = regions.get(region)

        if not subnetwork:
            print(f"No subnetwork found for region {region}")
            return None

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
                                        image_uri="us-west3-docker.pkg.dev/flowing-flame-464314-j5/real-estate-location-selection/real-estate-scraping:latest",
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
                                            "JOB_TYPE": job_type,  # Add job type for debugging
                                            "REGION_GROUP": self.get_region_group_for_region(region)
                                            # Add region group for debugging
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
            group_name = self.get_region_group_for_region(region)
            print(f"Created {job_type} job {job_id} in {region} ({group_name}) with {task_count} tasks")
            return job_id
        except Exception as e:
            print(f"Failed to create {job_type} job in {region}: {e}")
            return None

    def deploy_jobs(self, job_type: str, use_quota_checking: bool = True, enabled_groups: List[str] = None) -> List[
        str]:
        """Deploy jobs across multiple regions for a specific job type"""
        if use_quota_checking:
            try:
                distributions = self.distribute_tasks(enabled_groups)
            except Exception as e:
                print(f"Quota checking failed: {e}. Falling back to even distribution.")
                distributions = self.distribute_tasks_evenly(enabled_groups)
        else:
            distributions = self.distribute_tasks_evenly(enabled_groups)

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

        # Print summary by region group
        region_groups_used = {}
        for region, task_count in distributions:
            group_name = self.get_region_group_for_region(region)
            if group_name not in region_groups_used:
                region_groups_used[group_name] = []
            region_groups_used[group_name].append((region, task_count))

        print(f"\nSuccessfully created {len(job_ids)} {job_type} jobs across {len(region_groups_used)} region groups:")
        for group_name, regions_info in region_groups_used.items():
            total_tasks = sum(task_count for _, task_count in regions_info)
            print(f"  {group_name}: {len(regions_info)} regions, {total_tasks} tasks")

        print(f"{job_type} Job IDs:", job_ids)
        return job_ids

    def deploy_landwatch_jobs(self, use_quota_checking: bool = True, enabled_groups: List[str] = None) -> List[str]:
        """Deploy LandWatch jobs across multiple regions"""
        return self.deploy_jobs('landwatch', use_quota_checking, enabled_groups)

    def deploy_zillow_jobs(self, use_quota_checking: bool = True, enabled_groups: List[str] = None) -> List[str]:
        """Deploy Zillow jobs across multiple regions"""
        return self.deploy_jobs('zillow', use_quota_checking, enabled_groups)


def main():
    # Configuration
    PROJECT_ID = "flowing-flame-464314-j5"
    LANDWATCH_TASK_COUNT = 0  # Number of LandWatch tasks to distribute
    ZILLOW_TASK_COUNT = 100  # Number of Zillow tasks to distribute
    PROVISIONING_MODEL = "SPOT"  # Options: "STANDARD" or "SPOT"
    MAX_TASKS_PER_REGION = 8  # Maximum tasks per region to ensure we stay within quota
    USE_QUOTA_CHECKING = True  # Set to False if you don't have google-cloud-compute installed

    # Define which region groups to use (in priority order)
    # Start with US regions, expand as needed
    ENABLED_GROUPS = ['us_primary', 'us_secondary']  # Start conservative
    # ENABLED_GROUPS = ['us_primary', 'us_secondary', 'north_america', 'south_america']  # Expand to Americas
    # ENABLED_GROUPS = None  # Use all available regions

    print("=== Deploying LandWatch Jobs ===")
    landwatch_dispatcher = UnifiedScraperDispatcher(
        PROJECT_ID,
        LANDWATCH_TASK_COUNT,
        PROVISIONING_MODEL,
        MAX_TASKS_PER_REGION
    )
    landwatch_job_ids = landwatch_dispatcher.deploy_landwatch_jobs(USE_QUOTA_CHECKING, ENABLED_GROUPS)

    print("\n=== Deploying Zillow Jobs ===")
    zillow_dispatcher = UnifiedScraperDispatcher(
        PROJECT_ID,
        ZILLOW_TASK_COUNT,
        PROVISIONING_MODEL,
        MAX_TASKS_PER_REGION
    )
    zillow_job_ids = zillow_dispatcher.deploy_zillow_jobs(USE_QUOTA_CHECKING, ENABLED_GROUPS)

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