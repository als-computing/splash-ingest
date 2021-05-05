import os
import time

import requests
from splash_ingest.server.model import Job, JobStatus


def create_job(url, api_key, file_path, mapping_name):
    request = {
        "file_path": file_path,
        "mapping_name": mapping_name,
        "ingest_types": [
            "scicat_databroker"
        ]
        }
    resp = requests.post(url + f"/?api_key={api_key}", json=request)
    if resp.ok:
        return resp.json()['job_id']

    else:
        raise Exception("job creation failed", resp.json()['detail'])

def check_job(url, api_key, job_id) -> Job:
    resp = requests.get(url + f"/{job_id}?api_key={api_key}")
    if resp.ok:
        return Job(**resp.json())

    else:
        raise Exception("job status check failed", resp.json()['detail'])


if __name__ == "__main__":

    import dotenv

    dotenv.load_dotenv()
    jobs_url = "http://localhost:8089/api/ingest/jobs"
    api_key = os.getenv("API_KEY")
    job_id = create_job(jobs_url, api_key, "/data/beamlines/als832/20210421_091523_test3.h5", "als832_dx_2")

    # poll for a change in job status
    for x in range(0, 60):
        job = check_job(jobs_url, api_key, job_id)
        if job.status == JobStatus.submitted or job.status == JobStatus.running:
            print(f"{job_id} status is {job.status}")
            time.sleep(3)
            continue
        else:
            print(f"{job_id} job completed process with status {job.status}")
            for status in job.status_history:
                print(f"  {status.time} {status.status}")
                if status.log:
                    for log in status.log.split("\\n"):
                        print(f"     {log}")
            break
