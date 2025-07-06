from real_estate_location_selection.load_pub_sub import load_jobs_into_pub_sub

if __name__ == '__main__':
    # job_loader = DistributedJobLoader("flowing-flame-464314-j5", "landwatch",
    #                                   dataset_id="real_estate")
    load_jobs_into_pub_sub("landwatch",1000)
