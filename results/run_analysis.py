#!/usr/bin/env python3

""" Analyse results from a simulation run with storalloc-wrench
    (using simulatedJobs result file)
"""

import os
import sys

from yaml import load, CLoader, dump, CDumper
import numpy as np
from scipy.stats import pearsonr

import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import Colormap
import statsmodels.api as sm

CI_COMMIT_REF_NAME = os.getenv("CI_COMMIT_REF_NAME", default="UNKNOWN_COMMIT_REF")
CI_COMMIT_SHORT_SHA = os.getenv("CI_COMMIT_SHORT_SHA", default="UNKNOWN_COMMIT_SHA")
CI_COMMIT_TIMESTAMP = os.getenv("CI_COMMIT_TIMESTAMP", default="UNKNOWN_COMMIT_TS")
CI_COMMIT_MESSAGE = os.getenv("CI_COMMIT_MESSAGE", default="UNKNOWN_COMMIT_MESSAGE")
CI_JOB_ID = os.getenv("CI_JOB_ID", default="UNKNOWN_JOB_ID")
CI_PIPELINE_ID = os.getenv("CI_PIPELINE_ID", default="UNKNOWN_PIPELINE_ID")
CI_PIPELINE_URL = os.getenv("CI_PIPELINE_URL", default="UNKNOWN_PIPELINE_URL")
CI_PROJECT_URL = os.getenv("CI_PROJECT_URL", default="UNKNOWN_PROJECT_URL")
CALIBRATION_RUNS = os.getenv("CALIBRATION_RUNS", default="UNKNOWN_CALIBRATION_ITER_COUNT")


REAL_COLOR = (0.1, 0.4, 0.8, 0.5)
SIM_COLOR = (1, 0.5, 0.2, 0.5)

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


def compute_runtime_diff(jobs, plotting=True):
    """ Compute data for runtime difference between real and simulated jobs.
    """

    print("###################################################################")
    print("# RUNTIMES ---\n")

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

    ztest_runtime_tstat, ztest_runtime_pvalue = sm.stats.ztest(sim_runtime, real_runtime, alternative="two-sided")
    
    print(
        f"  - Mean runtime for simulation : {mean_sim_runtime}s\n"+
        f"  - Mean runtime in traces : {mean_real_runtime}s\n" +
        f"  - The mean run time difference between simulated and " +
        f"real values for all jobs is {mean_runtime_difference}s\n" +
        f"  - The Pearson's corr is {runtime_corr} (we want a correlation as high as possible)\n"+
        f"  - The Cohen d effect size is {runtime_cohen_d} (we want an effect size as low as possible," +
        " the use of the simulator should lead to values close to real world traces)"
    )


    if plotting:
        print("   [Plotting runtime analysis]")

        fig, axs = plt.subplots(ncols=2)
        fig.set_tight_layout(tight=True)
        fig.set_figheight(10)
        fig.set_figwidth(20)

        max_target = max(max(real_runtime), max(sim_runtime))
        line = {"x": [0, max_target], "y": [0, max_target]}

        scatter = sns.scatterplot(x=real_runtime, y=sim_runtime, s=15, color=".15", ax=axs[0])
        target_line = sns.lineplot(line, x="x", y="y", color="red", linestyle="--",  ax=axs[0])
        scatter.set(xlabel="Real runtimes", ylabel="Simulated runtimes")

        hist_data = {"values": [], "Origin": []}
        hist_data["values"].extend(real_runtime)
        hist_data["values"].extend(sim_runtime)
        hist_data["Origin"].extend(["real" for i in range(len(real_runtime))])
        hist_data["Origin"].extend(["sim" for i in range(len(sim_runtime))])
        
        binwidth = 1000

        global_hist = sns.histplot(data=hist_data, x="values", hue="Origin", binwidth=binwidth, 
                                 ax=axs[1], multiple="dodge", palette={"real": REAL_COLOR, "sim": SIM_COLOR})
        global_hist.set(xlabel=f"Runtimes histogram (real / simulation) ; binwidth = {binwidth}s")


        plt.savefig(f"{CI_PIPELINE_ID}_runtime.pdf", dpi=300, format='pdf')
        plt.savefig(f"{CI_PIPELINE_ID}_runtime.png", dpi=300, format='png')

    return {
        "runtime_correlation": float(runtime_corr),
        "runtime_cohend_effect": float(runtime_cohen_d),
        "mean_real_runtime": float(mean_real_runtime),
        "mean_sim_runtime": float(mean_sim_runtime),
        "ztest_runtime_tstat": float(ztest_runtime_tstat),
        "ztest_runtime_pvalue": float(ztest_runtime_pvalue)
    }

def compute_iotime_diff(jobs, plotting=True):
    """ Compute data for IO durations differences between real and simulated jobs
    """

    print("###################################################################")
    print("# IO TIMES ---\n")

    # Mean diffs
    io_time_diff = []
    sim_io_time = []
    sim_read_time = []
    sim_write_time = []
    real_io_time = []
    real_read_time = []
    real_write_time = []

    for job in jobs:

        # "Real"
        r_io_time = (job["real_cReadTime_s"] + job["real_cWriteTime_s"] + job["real_cMetaTime_s"]) / job["sum_nprocs"] 
        real_io_time.append(r_io_time)
        real_read_time.append(job["real_cReadTime_s"] / job["sum_nprocs"] )
        real_write_time.append(job["real_cWriteTime_s"]  / job["sum_nprocs"] )

        # Simulated
        s_io_time = 0
        s_r_time = 0
        s_w_time = 0
        for action in job["actions"]:
            if action["act_type"] == "COMPUTE" or action["act_type"] == "SLEEP":
                continue
            if action["act_status"] != "COMPLETED":
                continue
            if action["act_type"] == "FILEREAD":
                s_r_time += (action["act_duration"] / action["nb_procs_io"])
            if action["act_type"] == "CUSTOM" and "write" in str(action["sub_job"]):  # only custom action out there is our custom write action
                s_w_time += (action["act_duration"] / action["nb_procs_io"])
            
        s_io_time = (s_r_time + s_w_time) 

        sim_io_time.append(s_io_time)
        sim_read_time.append(s_r_time)
        sim_write_time.append(s_w_time)

        io_time_diff.append(abs(s_io_time - r_io_time))


    mean_real_io_time = np.mean(real_io_time)
    mean_sim_iotime = np.mean(sim_io_time)
    mean_io_time_difference = np.mean(io_time_diff)

    # Pearson's correlation
    io_time_corr, _ = pearsonr(sim_io_time, real_io_time)

    # Cohen's D
    io_time_cohen_d = cohend(sim_io_time, real_io_time)

    ztest_iotime_tstat, ztest_iotime_pvalue = sm.stats.ztest(sim_io_time, real_io_time, alternative="two-sided")

    print(
        f"  - Mean IO time for simulation : {mean_sim_iotime}s\n" +
        f"  - Mean IO time in traces : {mean_real_io_time}s\n" +
        f"  - The mean IO time difference between simulated and real values for all jobs is {mean_io_time_difference}s " +
        "(we want a mean difference as close to 0 as possible)\n" +
        f"  - The Pearson's corr is {io_time_corr} (we want a correlation as high as possible)\n" +
        f"  - The Cohen d effect size is {io_time_cohen_d} (we want an effect size as low as possible, " +
        "the use of the simulator should lead to values close to real world traces)\n" +
        f"  - ztest tstat : {ztest_iotime_tstat}\n" + 
        f"  - ztest pvalue: {ztest_iotime_pvalue}\n")

    if plotting:
        print("    [Plotting io time analysis]")

        fig, axs = plt.subplots(ncols=2)
        fig.set_tight_layout(tight=True)
        fig.set_figheight(10)
        fig.set_figwidth(20)

        max_target = max(max(real_io_time), max(sim_io_time))
        line = {"x": [0, max_target], "y": [0, max_target]}

        scatter = sns.scatterplot(x=real_io_time, y=sim_io_time, s=40, color=".15", alpha=0.5, ax=axs[0])
        read_scatter = sns.scatterplot(x=real_read_time, y=sim_read_time, s=20, ax=axs[0], facecolors="red", marker="+", alpha=0.6)
        write_scatter = sns.scatterplot(x=real_write_time, y=sim_write_time, s=20, color=".10", ax=axs[0], facecolors="blue", marker="x", alpha=0.3)
        target_line = sns.lineplot(line, x="x", y="y", color="red", linestyle="--", ax=axs[0])
        scatter.set(xlabel="Real jobs (mean IO time per rank)", ylabel="Simulated jobs (mean IO time per simulated rank)")
        #axs[0].set_xscale('log')
        axs[0].set_xlim([0.0001, max_target*1.2])
        #axs[0].set_yscale('log')
        axs[0].set_ylim([0.0001, max_target*1.2])

        binwidth = 100

        hist_data = {"values": [], "Origin": []}
        hist_data["values"].extend(real_io_time)
        hist_data["values"].extend(sim_io_time)
        hist_data["Origin"].extend(["real" for i in range(len(real_io_time))])
        hist_data["Origin"].extend(["sim" for i in range(len(sim_io_time))])

        global_hist = sns.histplot(data=hist_data, x="values", hue="Origin", binwidth=binwidth, 
                                 ax=axs[1], multiple="dodge", palette={"real": REAL_COLOR, "sim": SIM_COLOR})
        global_hist.set(xlabel=f"IO times histogram (real / simulation) ; binwidth = {binwidth}s")

        plt.savefig(f"{CI_PIPELINE_ID}_iotime.pdf", dpi=300, format='pdf')
        plt.savefig(f"{CI_PIPELINE_ID}_iotime.png", dpi=300, format='png')

    return {
        "iotime_correlation": float(io_time_corr),
        "iotime_cohend_effect": float(io_time_cohen_d),
        "mean_real_iotime": float(mean_real_io_time),
        "mean_sim_iotime": float(mean_sim_iotime),
        "ztest_iotime_tstat": float(ztest_iotime_tstat),
        "ztest_iotime_pvalue": float(ztest_iotime_pvalue)
    }

def compute_iovolume_diff(jobs, plotting=True):
    """
    """

    print("###################################################################")
    print("# IO VOLUMES ---\n")

    ## Compute the IO volume differences and stats for all jobs (Here we're just checking that simulated values are coherent,
    ## as the simulation should always read / write the data volume specified in the dataset anyway.

    # Mean diffs
    io_volume_diff = []
    sim_io_volume_gb = []
    real_io_volume_gb = []

    for job in jobs:

        # Real:
        r_io_volume_gb = job["real_read_bytes"] / 1_000_000_000 + job["real_written_bytes"] / 1_000_000_000
        real_io_volume_gb.append(r_io_volume_gb)

        # Simulated:
        s_io_volume_gb = 0
        for action in job["actions"]:

            if (action["act_type"] == "FILEREAD" 
            or (action["act_type"] == "CUSTOM" and "write" in str(action["sub_job"])) 
            and action["act_status"] == "COMPLETED"):
                s_io_volume_gb += action["io_size_bytes"] / 1_000_000_000

        sim_io_volume_gb.append(s_io_volume_gb)

        io_volume_diff.append(abs(s_io_volume_gb - r_io_volume_gb))

    mean_io_volume_difference = np.mean(io_volume_diff)

    # Pearson's correlation
    io_vol_corr, _ = pearsonr(sim_io_volume_gb, real_io_volume_gb)

    # Cohen's D
    io_vol_cohen_d = cohend(sim_io_volume_gb, real_io_volume_gb)

    print(f"  - The mean IO volume difference between simulated and real values for all jobs is {mean_io_volume_difference}s (we want a mean difference as close to 0 as possible)")
    print(f"  - The Pearson's corr is {io_vol_corr} (we want a correlation as high as possible)")
    print(f"  - The Cohen d effect size is {io_vol_cohen_d} (we want an effect size as low as possible, the use of the simulator should lead to values close to real world traces)")

    if plotting:
        print("    [Plotting io volume analysis]")

        fig, axs = plt.subplots(ncols=2)
        fig.set_tight_layout(tight=True)
        fig.set_figheight(10)
        fig.set_figwidth(20)

        max_target = max(max(real_io_volume_gb), max(sim_io_volume_gb))
        line = {"x": [0, max_target], "y": [0, max_target]}

        scatter = sns.scatterplot(x=real_io_volume_gb, y=sim_io_volume_gb, s=15, color=".15", ax=axs[0])
        target_line = sns.lineplot(line, x="x", y="y", color="red", linestyle="--", linewidth=0.3, ax=axs[0])
        scatter.set(xlabel="Real", ylabel="Simulated")

        hist_data = {"values": [], "Origin": []}
        hist_data["values"].extend(real_io_volume_gb)
        hist_data["values"].extend(sim_io_volume_gb)
        hist_data["Origin"].extend(["real" for i in range(len(real_io_volume_gb))])
        hist_data["Origin"].extend(["sim" for i in range(len(sim_io_volume_gb))])

        binwidth = 100

        global_hist = sns.histplot(data=hist_data, x="values", hue="Origin", binwidth=binwidth, 
                                 ax=axs[1], multiple="dodge", palette={"real": REAL_COLOR, "sim": SIM_COLOR})
        global_hist.set(xlabel=f"IO volume histogram (real / simulation) ; binwidth = {binwidth}s")


        plt.savefig(f"{CI_PIPELINE_ID}_iovolume.pdf", dpi=300, format='pdf')
        plt.savefig(f"{CI_PIPELINE_ID}_iovolume.png", dpi=300, format='png')

    return {"iovolume_correlation": float(io_vol_corr), "iovolume_cohend_effect": float(io_vol_cohen_d), "mean_iovol_diff": float(mean_io_volume_difference)}

def trace_job_schedule(jobs):
    """ /!\ We need access to the dataset in order to get the origin time for all timestamps !
    """

    print("[Tracing job schedule]")

    fig, axs = plt.subplots(ncols=1)
    fig.set_tight_layout(tight=True)
    fig.set_figheight(14)
    fig.set_figwidth(20)

    jobs_scatter = {"x": [], "y": [], "IO (GB)": [], "Cores": []}
    lines = []
    lines_wait = []

    runtime_index = 0
    for job in jobs:
        jobs_scatter["x"].append(int(job["job_submit_ts"]))
        jobs_scatter["y"].append(runtime_index)
        jobs_scatter["IO (GB)"].append((int(job["real_read_bytes"]) + int(job["real_written_bytes"])) / 1000000000),
        jobs_scatter["Cores"].append(int(job["real_cores_used"]))

        lines.append({
                        "x": [int(job['job_start_ts']), int(job['job_end_ts'])],
                        "y": [runtime_index, runtime_index]
                    })
        lines_wait.append({
                        "x": [int(job['job_submit_ts']), int(job['job_start_ts'])],
                        "y": [runtime_index, runtime_index]
                    })
        runtime_index += 1


    for line in lines:
        sns.lineplot(line, x="x", y="y", color="green", linestyle="-", linewidth=1.4, ax=axs, zorder=5)
    for line in lines_wait:
        sns.lineplot(line, x="x", y="y", color="red", linestyle="dashed", linewidth=1.4, ax=axs, zorder=5)

    scatter = sns.scatterplot(
        data=jobs_scatter,
        x="x", y="y", hue="IO (GB)", size="Cores",
        palette="RdYlGn_r",
        sizes=(20, 200),
        ax=axs,
        zorder=10,
    )

    scatter.set(xlabel="Timestamps (s) inside simulation", ylabel="Number of jobs run (ordered by submit timestamp)")

    plt.savefig(f"{CI_PIPELINE_ID}_schedule.pdf", dpi=300, format='pdf')
    plt.savefig(f"{CI_PIPELINE_ID}_schedule.png", dpi=300, format='png')

def save_metrics_to_file(metrics: dict, filename):
    """Save computed metrics and used configuration to a yaml file"""

    variables = {
        "commit_sha": CI_COMMIT_SHORT_SHA,
        "commit_ref": CI_COMMIT_REF_NAME,
        "commit_ts": CI_COMMIT_TIMESTAMP,
        "commit_description": CI_COMMIT_MESSAGE,
        "job_id": CI_JOB_ID,
        "pipeline_id": CI_PIPELINE_ID,
        "pipeline_url": CI_PIPELINE_URL,
        "project_url": CI_PROJECT_URL,
        "calibration_iter": CALIBRATION_RUNS,
    }
    variables.update(metrics)

    print(variables)

    with open(filename, "w", encoding="utf-8") as metric_file:
        dump(variables, metric_file, Dumper=CDumper)


def analyse(trace, plotting=True):
    """Analyse traces"""

    sns.set_theme(style="ticks")

    results = load_job_trace(trace)

    metrics = {}

    metrics.update(compute_runtime_diff(results, plotting))

    metrics.update(compute_iovolume_diff(results))

    metrics.update(compute_iotime_diff(results, plotting))

    trace_job_schedule(results)

    save_metrics_to_file(metrics, f"{CI_PIPELINE_ID}_metrics.yaml")

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
