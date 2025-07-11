#!/usr/bin/env python3
"""
Monitoring and management script for the enhanced scraping system
"""

import sys
import time
from datetime import datetime
from tabulate import tabulate
from scrapers.utils.big_query_wrapper import create_client


class ScrapingMonitor:
    """Monitor and manage the scraping system"""

    def __init__(self, project_id="flowing-flame-464314-j5", dataset_id="real_estate"):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = create_client(project=project_id)

    def get_comprehensive_status(self):
        """Get comprehensive status of the scraping system"""
        print("=== Scraping System Status ===")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        # Job processing statistics
        self._print_processing_stats()
        print()

        # Queue status
        self._print_queue_status()
        print()

        # Source table status
        self._print_source_table_status()
        print()

        # Active locks
        self._print_active_locks()
        print()

        # Recent errors
        self._print_recent_errors()

    def _print_processing_stats(self):
        """Print job processing statistics"""
        print("--- Job Processing Statistics (Last 24 Hours) ---")

        query = f"""
        SELECT 
            scraper_source,
            status,
            COUNT(*) as count,
            MIN(started_at) as earliest,
            MAX(started_at) as latest
        FROM `{self.project_id}.{self.dataset_id}.job_processing_log`
        WHERE started_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        GROUP BY scraper_source, status
        ORDER BY scraper_source, status
        """

        try:
            result = list(self.client.query(query).result())

            if result:
                headers = ["Scraper", "Status", "Count", "Earliest", "Latest"]
                rows = []
                for row in result:
                    rows.append([
                        row.scraper_source,
                        row.status,
                        row.count,
                        row.earliest.strftime('%H:%M:%S') if row.earliest else 'N/A',
                        row.latest.strftime('%H:%M:%S') if row.latest else 'N/A'
                    ])
                print(tabulate(rows, headers=headers, tablefmt="grid"))
            else:
                print("No processing activity in the last 24 hours")
        except Exception as e:
            print(f"Error getting processing stats: {e}")

    def _print_queue_status(self):
        """Print queue status"""
        print("--- Queue Activity (Last 24 Hours) ---")

        query = f"""
        SELECT 
            scraper_source,
            COUNT(*) as jobs_queued,
            MIN(queued_at) as earliest,
            MAX(queued_at) as latest
        FROM `{self.project_id}.{self.dataset_id}.job_queue_log`
        WHERE queued_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        GROUP BY scraper_source
        ORDER BY scraper_source
        """

        try:
            result = list(self.client.query(query).result())

            if result:
                headers = ["Scraper", "Jobs Queued", "Earliest", "Latest"]
                rows = []
                for row in result:
                    rows.append([
                        row.scraper_source,
                        row.jobs_queued,
                        row.earliest.strftime('%H:%M:%S') if row.earliest else 'N/A',
                        row.latest.strftime('%H:%M:%S') if row.latest else 'N/A'
                    ])
                print(tabulate(rows, headers=headers, tablefmt="grid"))
            else:
                print("No queue activity in the last 24 hours")
        except Exception as e:
            print(f"Error getting queue status: {e}")

    def _print_source_table_status(self):
        """Print source table status"""
        print("--- Source Table Status ---")

        # Zillow status
        zillow_query = f"""
        SELECT 
            state,
            COUNT(*) as total_urls,
            COUNTIF(scraped_at IS NOT NULL) as scraped,
            COUNTIF(scraped_at IS NULL) as pending
        FROM `{self.project_id}.{self.dataset_id}.zillow_urls`
        WHERE state IN ('UT', 'ID', 'NV', 'AZ', 'CO', 'WY')
        GROUP BY state
        ORDER BY state
        """

        # Landwatch status
        landwatch_query = f"""
        SELECT 
            state,
            COUNT(*) as total_urls,
            COUNTIF(scraped_at IS NOT NULL) as scraped,
            COUNTIF(scraped_at IS NULL) as pending
        FROM `{self.project_id}.{self.dataset_id}.landwatch_urls`
        WHERE state IN ('UT', 'ID', 'NV', 'WY', 'MT', 'NH', 'CO', 'AZ', 'NM', 'TX', 'OK', 'KS', 'NE', 'IA', 'IL', 'MO', 'IN', 'AR', 'LA', 'MS', 'MI')
        GROUP BY state
        ORDER BY state
        """

        print("\nZillow URLs:")
        try:
            result = list(self.client.query(zillow_query).result())
            if result:
                headers = ["State", "Total", "Scraped", "Pending", "% Complete"]
                rows = []
                for row in result:
                    pct_complete = (row.scraped / row.total_urls * 100) if row.total_urls > 0 else 0
                    rows.append([
                        row.state,
                        f"{row.total_urls:,}",
                        f"{row.scraped:,}",
                        f"{row.pending:,}",
                        f"{pct_complete:.1f}%"
                    ])
                print(tabulate(rows, headers=headers, tablefmt="grid"))
            else:
                print("No Zillow URLs found")
        except Exception as e:
            print(f"Error getting Zillow status: {e}")

        print("\nLandwatch URLs:")
        try:
            result = list(self.client.query(landwatch_query).result())
            if result:
                headers = ["State", "Total", "Scraped", "Pending", "% Complete"]
                rows = []
                for row in result:
                    pct_complete = (row.scraped / row.total_urls * 100) if row.total_urls > 0 else 0
                    rows.append([
                        row.state,
                        f"{row.total_urls:,}",
                        f"{row.scraped:,}",
                        f"{row.pending:,}",
                        f"{pct_complete:.1f}%"
                    ])
                print(tabulate(rows, headers=headers, tablefmt="grid"))
            else:
                print("No Landwatch URLs found")
        except Exception as e:
            print(f"Error getting Landwatch status: {e}")

    def _print_active_locks(self):
        """Print active locks"""
        print("--- Active Locks ---")

        query = f"""
        SELECT 
            lock_name,
            process_id,
            acquired_at,
            expires_at,
            TIMESTAMP_DIFF(expires_at, CURRENT_TIMESTAMP(), MINUTE) as minutes_until_expiry
        FROM `{self.project_id}.{self.dataset_id}.job_loader_locks`
        WHERE expires_at > CURRENT_TIMESTAMP()
        ORDER BY acquired_at
        """

        try:
            result = list(self.client.query(query).result())

            if result:
                headers = ["Lock Name", "Process ID", "Acquired", "Expires", "Min Until Expiry"]
                rows = []
                for row in result:
                    rows.append([
                        row.lock_name,
                        row.process_id,
                        row.acquired_at.strftime('%H:%M:%S'),
                        row.expires_at.strftime('%H:%M:%S'),
                        row.minutes_until_expiry
                    ])
                print(tabulate(rows, headers=headers, tablefmt="grid"))
            else:
                print("No active locks")
        except Exception as e:
            print(f"Error getting active locks: {e}")

    def _print_recent_errors(self):
        """Print recent errors"""
        print("--- Recent Errors (Last 2 Hours) ---")

        query = f"""
        SELECT 
            scraper_source,
            last_error,
            COUNT(*) as error_count,
            MAX(completed_at) as latest_error
        FROM `{self.project_id}.{self.dataset_id}.job_processing_log`
        WHERE status = 'failed' 
        AND completed_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
        AND last_error IS NOT NULL
        GROUP BY scraper_source, last_error
        ORDER BY latest_error DESC
        LIMIT 10
        """

        try:
            result = list(self.client.query(query).result())

            if result:
                headers = ["Scraper", "Error", "Count", "Latest"]
                rows = []
                for row in result:
                    error_msg = row.last_error[:60] + "..." if len(row.last_error) > 60 else row.last_error
                    rows.append([
                        row.scraper_source,
                        error_msg,
                        row.error_count,
                        row.latest_error.strftime('%H:%M:%S') if row.latest_error else 'N/A'
                    ])
                print(tabulate(rows, headers=headers, tablefmt="grid"))
            else:
                print("No recent errors")
        except Exception as e:
            print(f"Error getting recent errors: {e}")

    def get_hourly_processing_rate(self, hours=6):
        """Get processing rate over the last N hours"""
        print(f"--- Processing Rate (Last {hours} Hours) ---")

        query = f"""
        SELECT 
            scraper_source,
            EXTRACT(HOUR FROM started_at) as hour,
            COUNT(*) as jobs_processed,
            COUNTIF(status = 'completed') as jobs_completed,
            COUNTIF(status = 'failed') as jobs_failed
        FROM `{self.project_id}.{self.dataset_id}.job_processing_log`
        WHERE started_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
        GROUP BY scraper_source, EXTRACT(HOUR FROM started_at)
        ORDER BY scraper_source, hour
        """

        try:
            result = list(self.client.query(query).result())

            if result:
                headers = ["Scraper", "Hour", "Total", "Completed", "Failed", "Success %"]
                rows = []
                for row in result:
                    success_rate = (row.jobs_completed / row.jobs_processed * 100) if row.jobs_processed > 0 else 0
                    rows.append([
                        row.scraper_source,
                        f"{int(row.hour):02d}:00",
                        row.jobs_processed,
                        row.jobs_completed,
                        row.jobs_failed,
                        f"{success_rate:.1f}%"
                    ])
                print(tabulate(rows, headers=headers, tablefmt="grid"))
            else:
                print(f"No processing activity in the last {hours} hours")
        except Exception as e:
            print(f"Error getting processing rate: {e}")

    def clean_expired_processing_jobs(self):
        """Clean up expired processing jobs"""
        print("--- Cleaning Expired Processing Jobs ---")

        query = f"""
        UPDATE `{self.project_id}.{self.dataset_id}.job_processing_log`
        SET status = 'failed', 
            completed_at = CURRENT_TIMESTAMP(),
            last_error = 'Job expired - process likely crashed'
        WHERE status = 'processing' 
        AND expires_at < CURRENT_TIMESTAMP()
        """

        try:
            result = self.client.query(query).result()
            print("Successfully cleaned up expired processing jobs")
        except Exception as e:
            print(f"Error cleaning expired jobs: {e}")

    def clean_old_logs(self, days=30):
        """Clean up old log entries"""
        print(f"--- Cleaning Logs Older Than {days} Days ---")

        tables_to_clean = [
            f"{self.project_id}.{self.dataset_id}.job_processing_log",
            f"{self.project_id}.{self.dataset_id}.job_queue_log"
        ]

        for table in tables_to_clean:
            query = f"""
            DELETE FROM `{table}`
            WHERE started_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            OR queued_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            """

            try:
                result = self.client.query(query).result()
                print(f"Cleaned old entries from {table.split('.')[-1]}")
            except Exception as e:
                print(f"Error cleaning {table}: {e}")

    def get_performance_summary(self):
        """Get a performance summary"""
        print("=== Performance Summary ===")

        # Overall stats for last 24 hours
        query = f"""
        SELECT 
            scraper_source,
            COUNT(*) as total_jobs,
            COUNTIF(status = 'completed') as completed,
            COUNTIF(status = 'failed') as failed,
            COUNTIF(status = 'processing') as still_processing,
            AVG(TIMESTAMP_DIFF(COALESCE(completed_at, CURRENT_TIMESTAMP()), started_at, SECOND)) as avg_processing_time_seconds
        FROM `{self.project_id}.{self.dataset_id}.job_processing_log`
        WHERE started_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        GROUP BY scraper_source
        ORDER BY scraper_source
        """

        try:
            result = list(self.client.query(query).result())

            if result:
                headers = ["Scraper", "Total", "Completed", "Failed", "Processing", "Success %", "Avg Time (s)"]
                rows = []
                for row in result:
                    success_rate = (row.completed / row.total_jobs * 100) if row.total_jobs > 0 else 0
                    avg_time = row.avg_processing_time_seconds if row.avg_processing_time_seconds else 0
                    rows.append([
                        row.scraper_source,
                        row.total_jobs,
                        row.completed,
                        row.failed,
                        row.still_processing,
                        f"{success_rate:.1f}%",
                        f"{avg_time:.1f}"
                    ])
                print(tabulate(rows, headers=headers, tablefmt="grid"))
            else:
                print("No activity in the last 24 hours")
        except Exception as e:
            print(f"Error getting performance summary: {e}")

    def monitor_live(self, interval=30):
        """Monitor the system in real-time"""
        print("Starting live monitoring (Ctrl+C to stop)...")
        print(f"Refresh interval: {interval} seconds")
        print()

        try:
            while True:
                # Clear screen (works on most terminals)
                print("\033[2J\033[H")

                self.get_comprehensive_status()
                print(f"\nNext refresh in {interval} seconds...")
                time.sleep(interval)

        except KeyboardInterrupt:
            print("\nMonitoring stopped.")


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python monitor.py status              - Get comprehensive status")
        print("  python monitor.py rate [hours]        - Get processing rate")
        print("  python monitor.py summary             - Get performance summary")
        print("  python monitor.py clean-expired       - Clean expired processing jobs")
        print("  python monitor.py clean-logs [days]   - Clean old log entries")
        print("  python monitor.py live [interval]     - Live monitoring")
        sys.exit(1)

    monitor = ScrapingMonitor()
    command = sys.argv[1]

    if command == "status":
        monitor.get_comprehensive_status()

    elif command == "rate":
        hours = int(sys.argv[2]) if len(sys.argv) > 2 else 6
        monitor.get_hourly_processing_rate(hours)

    elif command == "summary":
        monitor.get_performance_summary()

    elif command == "clean-expired":
        monitor.clean_expired_processing_jobs()

    elif command == "clean-logs":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        monitor.clean_old_logs(days)

    elif command == "live":
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        monitor.monitor_live(interval)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()