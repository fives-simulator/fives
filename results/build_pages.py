#!/usr/bin/env python3

import sys
from datetime import datetime
import os
import glob
import requests
from yaml import load, dump, CLoader, CDumper
from jinja2 import Environment, FileSystemLoader, select_autoescape

import zipfile

PROJECT_ID = "40922"  # Should never change?
BASE_URL = "https://gitlab.inria.fr/api/v4"
CI_PIPELINE_ID = os.getenv("CI_PIPELINE_ID", default="UNKNOWN_PIPELINE_ID")
PROJECT_URL = f"{BASE_URL}/projects/{PROJECT_ID}"
ARTEFACTS_DIR = "./public/static"
MAX_PIPELINES = int(os.getenv("MAX_PIPELINES", default=1))
PUBLIC_PROJECT_URL = os.getenv(
    "CI_PROJECT_URL", default="https://gitlab.inria.fr/jmonniot/storalloc_wrench"
)


def download_file(session, url, filename):
    """https://stackoverflow.com/questions/16694907/download-large-file-in-python-with-requests"""
    # NOTE the stream=True parameter below
    with session.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return filename


def get_pipelines(session):
    """Get the last {MAX_PIPELINES} successful pipelines (or less if the pipeline list is too short)
    NOTE : This is a generator!
    """

    unfiltered_pipelines = session.get(f"{PROJECT_URL}/pipelines").json()

    counter = 0
    for pipeline in unfiltered_pipelines:
        if counter >= MAX_PIPELINES:
            break
        if pipeline["status"] != "success":
            continue
        counter += 1
        yield pipeline


def get_pipeline_jobs(session, pipeline_id):
    """Get all jobs from a pipeline. All? Nope, just the two that contains artefacts that we'll be using!"""

    unfiltered_jobs = session.get(f"{PROJECT_URL}/pipelines/{pipeline_id}/jobs").json()

    calibration_job = {}
    analysis_job = {}

    for job in unfiltered_jobs:
        if job["status"] != "success":
            print(f"WARNING : Job {job['name']} not in 'success' state")
            # raise RuntimeError(f"This should not happen, job {job['id']} in successful pipeline {pipeline_id} is not successful")
        if job["name"] == "analysis-job":
            analysis_job = job
        if job["name"] == "calibration-job":
            calibration_job = job

    if not calibration_job or not analysis_job:
        raise RuntimeError(
            f"For pipeline {pipeline_id}, we're missing either the calibrated simulation job or the analysis job"
        )

    return (calibration_job, analysis_job)


def get_job_artifacts(session, job, artifacts_archive_name):
    """Get those images and config files and suffs"""

    try:
        os.mkdir(f"{ARTEFACTS_DIR}/{job['pipeline']['id']}")
    except:
        pass

    return download_file(
        session,
        f"{BASE_URL}/jobs/{job['id']}/artifacts",
        f"{ARTEFACTS_DIR}/{job['pipeline']['id']}/{artifacts_archive_name}.zip",
    )


def unzip_file(filename: str, pipeline_id):
    """Unzip file in place"""

    print(f"  ..Unzipping {filename} to {ARTEFACTS_DIR}")
    with zipfile.ZipFile(filename, "r") as zip_ref:
        zip_ref.extractall(f"{ARTEFACTS_DIR}/{pipeline_id}/")


def build_previous_result(result_pages: list, pipeline_id: str):
    """Build page with results from this run"""

    # Open the configuration calibrated during this pipeline
    calibrated_config = None
    try:
        with open(
            f"./public/static/{pipeline_id}/results/exp_configurations/calibrated_config.yaml",
            "r",
            encoding="utf-8",
        ) as config:
            calibrated_config = load(config, Loader=CLoader)
    except:
        calibrated_config = {}

    # Open the metrics from the analysis job
    metrics = None
    try:
        with open(
            f"./public/static/{pipeline_id}/results/{pipeline_id}_metrics.yaml",
            "r",
            encoding="utf-8",
        ) as metrics:
            metrics = load(metrics, Loader=CLoader)
    except:
        metrics = {}

    # Append everything to the same dictionnary
    metrics["pipeline_id"] = pipeline_id
    metrics["result_pages"] = result_pages
    metrics["calibrated_config"] = dump(
        calibrated_config, Dumper=CDumper, encoding=None
    )
    metrics["latest"] = "index.html"
    metrics["static_path"] = f"./static/{pipeline_id}/results/"
    if not "calibration_iter" in metrics:
        metrics["calibration_iter"] = "Unknown"
    if "commit_ts" not in metrics:
        metrics["commit_ts"] = "Unknown date"
    else:
        metrics["commit_ts"] = datetime.strptime(
            metrics["commit_ts"], "%Y-%m-%dT%H:%M:%S%z"
        ).strftime("%a %d %b %Y, %H:%M")
    print(metrics["commit_ts"])
    metrics["pstor_buff_size"] = calibrated_config["permanent_storage"][
        "io_buffer_size"
    ]
    metrics["stor_buff_size"] = calibrated_config["storage"]["io_buffer_size"]

    env = Environment(
        loader=FileSystemLoader("web_template"), autoescape=select_autoescape()
    )

    template = env.get_template("result_page.html")
    with open(f"public/{pipeline_id}.html", "w", encoding="utf-8") as rendered:
        rendered.write(template.render(metrics))


def build_current_results(result_pages: list):
    """Build page with results from this run"""

    # Open the configuration calibrated during this pipeline
    calibrated_config = None
    try:
        with open(
            f"./{CI_PIPELINE_ID}_calibrated_config.yaml", "r", encoding="utf-8"
        ) as config:
            calibrated_config = load(config, Loader=CLoader)
    except:
        calibrated_config = {}

    # Open the metrics from the analysis job
    metrics = None
    try:
        with open(f"./{CI_PIPELINE_ID}_metrics.yaml", "r", encoding="utf-8") as metrics:
            metrics = load(metrics, Loader=CLoader)
    except:
        metrics = {}

    # Append everything to the same dictionnary
    metrics["pipeline_id"] = CI_PIPELINE_ID
    metrics["result_pages"] = result_pages
    metrics["calibrated_config"] = dump(
        calibrated_config, Dumper=CDumper, encoding=None
    )
    metrics["latest"] = "index.html"
    metrics["static_path"] = "./static"
    if not "calibration_iter" in metrics:
        metrics["calibration_iter"] = "Unknown"
    if "commit_ts" not in metrics:
        metrics["commit_ts"] = "Unknown date"
    else:
        metrics["commit_ts"] = datetime.strptime(
            metrics["commit_ts"], "%Y-%m-%dT%H:%M:%S%z"
        ).strftime("%a %d %b %Y, %H:%M")

    metrics["pstor_buff_size"] = calibrated_config["permanent_storage"][
        "io_buffer_size"
    ]
    metrics["stor_buff_size"] = calibrated_config["storage"]["io_buffer_size"]

    print(metrics["commit_ts"])

    env = Environment(
        loader=FileSystemLoader("web_template"), autoescape=select_autoescape()
    )

    template = env.get_template("result_page.html")
    with open("public/index.html", "w", encoding="utf-8") as rendered:
        rendered.write(template.render(metrics))


def run(token):
    """Run the script !"""

    session = requests.Session()
    session.headers["PRIVATE-TOKEN"] = token

    fetched_pipelines = []
    pipeline_variables = {}

    for pipeline in get_pipelines(session):
        pipeline_id = pipeline["id"]
        fetched_pipelines.append(pipeline_id)
        print(f"Fetching pipeline {pipeline_id}")

        calibration_job, analysis_job = get_pipeline_jobs(session, pipeline["id"])
        print(
            f" Got calibration_job {calibration_job['id']} and analysis_job {analysis_job['id']}"
        )

        calibrated_config_path = get_job_artifacts(
            session, calibration_job, f"{pipeline_id}_calibrated_config"
        )
        print(f" Artifacts for calibration_job downloaded at {calibrated_config_path}")
        unzip_file(calibrated_config_path, pipeline_id)
        analysis_path = get_job_artifacts(
            session, analysis_job, f"{pipeline_id}_analysis"
        )
        print(f" Artifacts for analysis_job downloaded at {analysis_path}")
        unzip_file(analysis_path, pipeline_id)

        # Not used so far
        variables = {
            "commit_sha": analysis_job["commit"]["short_id"],
            "commit_ref": analysis_job["pipeline"]["ref"],
            "commit_ts": analysis_job["commit"]["committed_date"],
            "commit_description": analysis_job["commit"]["message"],
            "job_id": analysis_job["id"],
            "pipeline_url": analysis_job["pipeline"]["web_url"],
            "project_url": PUBLIC_PROJECT_URL,
        }
        pipeline_variables[pipeline_id] = variables

        os.remove(calibrated_config_path)
        os.remove(analysis_path)
        undesired_files = glob.glob(
            f"./public/static/{pipeline_id}/results/exp_configurations/exp_config*"
        )
        for rfile in undesired_files:
            print(f"Attempting to remove file {rfile}")
            os.remove(rfile)

    build_current_results(fetched_pipelines)

    for pipeline in fetched_pipelines:
        build_previous_result(fetched_pipelines, pipeline)


if __name__ == "__main__":
    TOKEN = sys.argv[1]
    run(TOKEN)
