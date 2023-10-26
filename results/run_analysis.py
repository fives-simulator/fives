#!/usr/bin/env python3

""" Analyse results from a simulation run with storalloc-wrench
    (using simulatedJobs result file)
"""

import os
import sys

from yaml import load, CLoader
import numpy as np
from scipy.stats import pearsonr


def cohend(data1: list, data2: list):
    """Compute a Cohen's d metric of two list of values"""
    n_data1, n_data2 = len(data1), len(data2)
    var1, var2 = np.var(data1, ddof=1), np.var(data2, ddof=1)
    global_var = np.sqrt(
        ((n_data1 - 1) * var1 + (n_data2 - 1) * var2) / (n_data1 + n_data2 - 2)
    )
    mean1, mean2 = np.mean(data1), np.mean(data2)
    return (mean1 - mean2) / global_var


def load_job_trace(job_trace_path: str):
    """Load simulated job trace from file"""

    results = None
    with open(job_trace_path, "r", encoding="utf-8") as job_results:
        results = load(job_results, Loader=CLoader)

    print(f"Loaded result dataset with {len(results)} jobs")
    return results


def compute_runtime_diff(jobs):
    ## Compute the run time differences for all jobs
    runtime_diffs = []
    sim_runtime = []
    real_runtime = []

    for job in jobs:
        runtime_diffs.append(abs(job["job_runtime_s"] - job["real_runtime_s"]))
        sim_runtime.append(job["job_runtime_s"])
        real_runtime.append(job["real_runtime_s"])

    mean_real_runtime = np.mean(real_runtime)
    mean_sim_runtime = np.mean(sim_runtime)

    mean_runtime_difference = np.mean(runtime_diffs)

    # Pearson's correlation
    runtime_corr, _ = pearsonr(sim_runtime, real_runtime)

    # Cohen's D
    runtime_cohen_d = cohend(sim_runtime, real_runtime)

    print(f"Mean runtime for simulation : {mean_sim_runtime}s")
    print(f"Mean runtime in traces : {mean_real_runtime}s")
    print(
        f"The mean run time difference between simulated and real values for all jobs is {mean_runtime_difference}s"
    )
    print(
        f"The Pearson's corr is {runtime_corr} (we want a correlation as high as possible)"
    )
    print(
        f"The Cohen d effect size is {runtime_cohen_d} (we want an effect size as low as possible, the use of the simulator should lead to values close to real world traces)"
    )


def analyse(trace):
    """Analyse traces"""

    results = load_job_trace(trace)

    compute_runtime_diff(results)


if __name__ == "__main__":
    trace_path = None
    if len(sys.argv) > 1:
        trace_path = sys.argv[1]
    elif os.getenv("STORALLOC_JOB_TRACE"):
        trace_path = os.getenv("STORALLOC_JOB_TRACE")
    else:
        print("No trace file to work with provided")
        sys.exit(0)

    analyse(trace_path)
