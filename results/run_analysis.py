#!/usr/bin/env python3

""" Analyse results from a simulation run with storalloc-wrench
    (using simulatedJobs result file)
"""

import os
import sys

from yaml import load, CLoader
import numpy as np
from scipy.stats import pearsonr

import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt

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

    print(
        f"  - Mean runtime for simulation : {mean_sim_runtime}s\n"+
        f"  - Mean runtime in traces : {mean_real_runtime}s\n" +
        f"  - The mean run time difference between simulated and " +
        "real values for all jobs is {mean_runtime_difference}s\n" +
        f"  - The Pearson's corr is {runtime_corr} (we want a correlation as high as possible)\n"+
        f"  - The Cohen d effect size is {runtime_cohen_d} (we want an effect size as low as possible," +
        " the use of the simulator should lead to values close to real world traces)"
    )


    if plotting:
        print("   [Plotting runtime analysis]")

        fig, axs = plt.subplots(ncols=3)
        fig.set_tight_layout(tight=True)
        fig.set_figheight(6)
        fig.set_figwidth(20)

        max_target = max(max(real_runtime), max(sim_runtime))
        line = {"x": [0, max_target], "y": [0, max_target]}

        scatter = sns.scatterplot(x=real_runtime, y=sim_runtime, s=15, color=".15", ax=axs[0])
        target_line = sns.lineplot(line, x="x", y="y", color="red", linestyle="--",  ax=axs[0])
        scatter.set(xlabel="Real runtimes", ylabel="Simulated runtimes")

        binwidth = 1000
        real_hist = sns.histplot(data=real_runtime, binwidth=binwidth, ax=axs[1], color=REAL_COLOR)
        real_hist.set(xlabel=f"Real runtime - binwidth = {binwidth}s")

        sim_hist = sns.histplot(data=sim_runtime, binwidth=binwidth, ax=axs[2], color=SIM_COLOR)
        sim_hist.set(xlabel=f"Simulated runtime - binwidth = {binwidth}s")

        plt.savefig("runtimes.pdf", format='pdf')
        plt.savefig("runtimes.png", format='png')

    print("###")

    return {"runtime_correlation": runtime_corr, "runtime_cohend_effect": runtime_cohen_d}

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
        r_io_time = (job["real_cReadTime_s"] + job["real_cWriteTime_s"] + job["real_cMetaTime_s"]) / job["real_cores_used"]
        real_io_time.append(r_io_time)
        real_read_time.append(job["real_cReadTime_s"] / job["real_cores_used"])
        real_write_time.append(job["real_cWriteTime_s"] / job["real_cores_used"])
        
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
                s_r_time += action["act_duration"]
            if action["act_type"] == "CUSTOM":  # only custom action here is our custom write action
                s_w_time += action["act_duration"]
            s_io_time += action["act_duration"]
        
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

    print(
        f"  - Mean IO time for simulation : {mean_sim_iotime}s\n" +
        f"  - Mean IO time in traces : {mean_real_io_time}s\n" + 
        f"  - The mean IO time difference between simulated and real values for all jobs is {mean_io_time_difference}s " +
        "(we want a mean difference as close to 0 as possible)\n" + 
        f"  - The Pearson's corr is {io_time_corr} (we want a correlation as high as possible)\n" + 
        f"  - The Cohen d effect size is {io_time_cohen_d} (we want an effect size as low as possible, " + 
        "the use of the simulator should lead to values close to real world traces)")

    if plotting:
        print("    [Plotting io time analysis]")
        
        fig, axs = plt.subplots(ncols=3)
        fig.set_tight_layout(tight=True)
        fig.set_figheight(6)
        fig.set_figwidth(20)

        max_target = max(max(real_io_time), max(sim_io_time))
        line = {"x": [0, max_target], "y": [0, max_target]}

        scatter = sns.scatterplot(x=real_io_time, y=sim_io_time, s=40, color=".15", alpha=0.5, ax=axs[0])
        read_scatter = sns.scatterplot(x=real_read_time, y=sim_read_time, s=20, ax=axs[0], facecolors="red", marker="+", alpha=0.6)
        write_scatter = sns.scatterplot(x=real_write_time, y=sim_write_time, s=20, color=".10", ax=axs[0], facecolors="blue", marker="x", alpha=0.3)
        target_line = sns.lineplot(line, x="x", y="y", color="red", linestyle="--", ax=axs[0])
        scatter.set(xlabel="Real jobs", ylabel="Simulated jobs")
        #axs[0].set_xscale('log')
        axs[0].set_xlim([0.0001, max_target*1.2])
        #axs[0].set_yscale('log')
        axs[0].set_ylim([0.0001, max_target*1.2])

        binwidth = 100
        real_hist = sns.histplot(data=real_io_time, binwidth=binwidth, ax=axs[1], color=REAL_COLOR)
        real_hist.set(xlabel=f"Real IO time - binwidth = {binwidth}s")

        sim_hist = sns.histplot(data=sim_io_time, binwidth=binwidth, ax=axs[2], color=SIM_COLOR)
        sim_hist.set(xlabel=f"Simulated IO time - binwidth = {binwidth}s")

        plt.savefig("iotimes.pdf", format='pdf')
        plt.savefig("iotimes.png", format='png')

    print("###")

    return {"iotime_correlation": io_time_corr, "iotime_cohend_effect": io_time_cohen_d}

def compute_iovolume_diff(jobs):
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
            
            if (action["act_type"] == "FILEREAD" or action["act_type"] == "CUSTOM") and action["act_status"] == "COMPLETED":
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

    print("###")
    return {"iovolume_correlation": io_vol_corr, "iovolume_cohend_effect": io_vol_cohen_d}


def analyse(trace, plotting=True):
    """Analyse traces"""

    sns.set_theme(style="ticks")

    results = load_job_trace(trace)

    compute_runtime_diff(results, plotting)

    compute_iotime_diff(results, plotting)

    compute_iovolume_diff(results)

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
