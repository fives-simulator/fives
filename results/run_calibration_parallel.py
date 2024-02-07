#!/usr/bin/env python3

"""
    Run a calibration process using the Ax framework.
    This script expects to find the compiled fives_wrench bin inside <repo_root>/build
    and a base configuration in <repo_root>/results/exp_configurations/

    It might take a very long time to run...

"""

import json
import random
import subprocess
import pathlib
import os
import multiprocessing
from time import sleep

import yaml
import numpy as np
from scipy.stats import pearsonr
import statsmodels.api as sm

from ax.service.ax_client import AxClient, ObjectiveProperties

# from ax.utils.measurement.synthetic_functions import hartmann6

CONFIGURATION_PATH = os.getenv(
    "CALIBRATION_CONFIG_PATH", default="./exp_configurations"
)
CONFIGURATION_BASE = os.getenv(
    "CALIBRATION_CONFIGURATION_BASE", default=f"{CONFIGURATION_PATH}/theta_config.yml"
)
DATASET_PATH = os.getenv("CALIBRATION_DATASET_PATH", default="./exp_datasets")
DATASET = os.getenv("CALIBRATION_DATASET", default="theta2022_month8_cat1")
DATASET_EXT = os.getenv("CALIBRATION_DATASET_EXT", default=".yaml")
BUILD_PATH = os.getenv("CALIBRATION_BUILD_PATH", default="../build")
CALIBRATION_RUNS = int(os.getenv("CALIBRATION_RUNS", default=10))
CFG_VERSION = os.getenv("CI_COMMIT_SHORT_SHA", default="0.0.1")


# Define the parameters that will be given to Ax for the optimization loop
# Bounds / value lists are not final
AX_PARAMS = [
    {
        # Lustre parameter - number of OSTs onto which parts of a file will be allocated
        "name": "stripe_count",
        "type": "range",
        "bounds": [1, 3],  # NOTE : never using all OSTs for any allocation so far
        "value_type": "int",
    },
    {
        # Proportion (%) of compute nodes from a job reservation which may participate in read I/Os
        "name": "io_read_node_ratio",
        "type": "range",
        "bounds": [0.1, 0.6],
        "digits": 1,
        "value_type": "float",
    },
    {
        # In how many files should the read volume of a job be split into?
        # (eventually multiplied by the stripe count during simulation)
        "name": "nb_files_per_read",
        "type": "range",
        "bounds": [1, 10],
        "value_type": "int",
    },
    {
        # Maximum number of compute node participating in read I/Os
        # (upper bound for io_read_node_ratio)
        "name": "max_read_node_cnt",
        "type": "range",
        "bounds": [1, 28],
        "value_type": "int",
    },
    {
        # BW threshold above which a different "stripe_count" parameter should be used for a job
        "name": "stripe_count_high_thresh_read",
        "type": "range",
        "bounds": [10e6, 450e6],
        "value_type": "int",
    },
    {
        # Special stripe_count coefficient used when a job exceeds "stripe_count_high_thresh_read"
        "name": "stripe_count_high_read_add",
        "type": "range",
        "bounds": [1, 3],
        "value_type": "int",
    },
    {
        # Read bandwidth (without interferences) for disks
        "name": "disk_rb",
        "type": "range",
        "bounds": [10, 4300],  # Aggregated read bw is 240 GBps for 56 OSSs
        "value_type": "int",
    },
    {
        # Interference model coefficient applied to the read bandwidth
        "name": "non_linear_coef_read",
        "type": "range",
        "bounds": [1.5, 20],
        "digits": 1,
        "value_type": "float",
    },
    {
        "name": "io_write_node_ratio",
        "type": "range",
        "bounds": [0.1, 0.6],
        "digits": 1,
        "value_type": "float",
    },
    {
        "name": "nb_files_per_write",
        "type": "range",
        "bounds": [1, 10],
        "value_type": "int",
    },
    {
        "name": "max_write_node_cnt",
        "type": "range",
        "bounds": [1, 28],
        "value_type": "int",
    },
    {
        "name": "stripe_count_high_thresh_write",
        "type": "range",
        "bounds": [10e6, 450e6],
        "value_type": "int",
    },
    {
        "name": "stripe_count_high_write_add",
        "type": "range",
        "bounds": [1, 3],
        "value_type": "int",
    },
    {
        "name": "disk_wb",
        "type": "range",
        "bounds": [10, 3000],  # Aggregated write bw is 172 GBps for 56 OSSs
        "value_type": "int",
    },
    {
        "name": "non_linear_coef_write",
        "type": "range",
        "bounds": [1.5, 20],
        "digits": 1,
        "value_type": "float",
    },
    {
        # Max. number of file parts on each OSTs, from a single file
        # (May lead to a dynamic update of the stripe_size if the default stripe_size is too small for a given file size)
        "name": "max_chunks_per_ost",
        "type": "range",
        "bounds": [8, 64],
        "value_type": "int",
    },
    {
        "name": "bandwidth_backbone_storage",
        "type": "range",
        "bounds": [100, 240],
        "value_type": "int",
    },
]

AX_READ_PARAMS = [
    {
        "name": "io_read_node_ratio",
        "type": "range",
        "bounds": [0.1, 0.6],
        "digits": 1,
        "value_type": "float",
    },
    {
        "name": "nb_files_per_read",
        "type": "range",
        "bounds": [1, 15],
        "value_type": "int",
    },
    {
        "name": "max_read_node_cnt",
        "type": "range",
        "bounds": [1, 28],
        "value_type": "int",
    },
    {
        "name": "stripe_count_high_thresh_read",
        "type": "range",
        "bounds": [10e6, 450e6],
        "value_type": "int",
    },
    {
        "name": "stripe_count_high_read_add",
        "type": "range",
        "bounds": [1, 4],
        "value_type": "int",
    },
    {
        "name": "disk_rb",
        "type": "range",
        "bounds": [10, 4300],  # Aggregated read bw is 240 GBps for 56 OSSs
        "value_type": "int",
    },
    {
        "name": "non_linear_coef_read",
        "type": "range",
        "bounds": [1.5, 20],
        "digits": 1,
        "value_type": "float",
    },
]

AX_WRITE_PARAMS = [
    {
        "name": "io_write_node_ratio",
        "type": "range",
        "bounds": [0.1, 0.6],
        "digits": 1,
        "value_type": "float",
    },
    {
        "name": "nb_files_per_write",
        "type": "range",
        "bounds": [1, 15],
        "value_type": "int",
    },
    {
        "name": "max_write_node_cnt",
        "type": "range",
        "bounds": [1, 28],
        "value_type": "int",
    },
    {
        "name": "stripe_count_high_thresh_write",
        "type": "range",
        "bounds": [10e6, 450e6],
        "value_type": "int",
    },
    {
        "name": "stripe_count_high_write_add",
        "type": "range",
        "bounds": [1, 4],
        "value_type": "int",
    },
    {
        "name": "disk_wb",
        "type": "range",
        "bounds": [10, 3000],  # Aggregated write bw is 172 GBps for 56 OSSs
        "value_type": "int",
    },
    {
        "name": "non_linear_coef_write",
        "type": "range",
        "bounds": [1.5, 20],
        "digits": 1,
        "value_type": "float",
    },
    {
        "name": "bandwidth_backbone_storage",
        "type": "range",
        "bounds": [100, 240],
        "value_type": "int",
    },
]


""" UNUSED PARAMS
    {
        "name": "permanent_storage_read_bw",
        "type": "range",
        "bounds": [10, 90],
        "value_type": "int",
    },
    {
        "name": "bandwidth_backbone_perm_storage",
        "type": "range",
        "bounds": [50, 100],
        "value_type": "int",
    },
    {
        "name": "bandwidth_backbone_storage",
        "type": "range",
        "bounds": [100, 240],
        "value_type": "int",
    },
    {
        "name": "preload_percent",
        "type": "choice",
        "is_ordered": True,
        "values": [0.1, 0.2, 0.3, 0.4],
        "value_type": "float",
    },
    {
        "name": "stripe_count",
        "type": "range",
        "bounds": [1, 1],  # NOTE : never using all OSTs for any allocation so far
        "value_type": "int",
    },
    {
        "name": "permanent_storage_write_bw",
        "type": "range",
        "bounds": [10, 90],
        "value_type": "int",
    },
    {
        "name": "stripe_size",
        "type": "choice",
        "values": [
            2097152,
            4194304,
            8388608,
            16777216,
            67108864,
            1073741824,
            2147483648,
        ],
        "is_ordered": True,
        "value_type": "int",
    },
    {
        "name": "disk_rb",
        "type": "range",
        "bounds": [10, 4300],  # Aggregated read bw is 240 GBps for 56 OSSs
        "value_type": "int",
    },
    {
        "name": "disk_wb",
        "type": "range",
        "bounds": [10, 3000],  # Aggregated write bw is 172 GBps for 56 OSSs
        "value_type": "int",
    },
    {
        "name": "non_linear_coef_read",
        "type": "range",
        "bounds": [1.5, 20],
        "digits": 1,
        "value_type": "float",
    },
    {
        "name": "non_linear_coef_write",
        "type": "range",
        "bounds": [1.5, 20],
        "digits": 1,
        "value_type": "float",
    },
    {
        "name": "max_chunks_per_ost",
        "type": "choice",
        "values": [28, 56],
        "value_type": "int",
    },


    {
        "name": "io_write_node_ratio",
        "type": "range",
        "bounds": [0.1, 0.6],
        "digits": 1,
        "value_type": "float",
    },
    {
        "name": "nb_files_per_write",
        "type": "range",
        "bounds": [1, 15],
        "value_type": "int",
    },
    {
        "name": "max_write_node_cnt",
        "type": "range",
        "bounds": [1, 28],
        "value_type": "int",
    },
        {
        "name": "stripe_count_high_thresh_write",
        "type": "range",
        "bounds": [10e6, 450e6],
        "value_type": "int",
    },
        {
        "name": "stripe_count_high_write_add",
        "type": "range",
        "bounds": [1, 4],
        "value_type": "int",
    },
        {
        "name": "disk_wb",
        "type": "range",
        "bounds": [10, 3000],  # Aggregated write bw is 172 GBps for 56 OSSs
        "value_type": "int",
    },
    {
        "name": "non_linear_coef_write",
        "type": "range",
        "bounds": [1.5, 20],
        "digits": 1,
        "value_type": "float",
    },
"""


def load_base_config(path: str):
    """Open configuration file that serves as base config, cleanup the dictionnary and return it"""
    # Start from a configuration base for the platform we experiment on
    yaml_config = None

    with open(path, "r", encoding="utf-8") as cfg_base:
        yaml_config = yaml.load(cfg_base, Loader=yaml.FullLoader)

    # Remove keys that are only used inside the file with Yaml anchors/aliases
    del yaml_config["storage"]["disk_templates"]
    del yaml_config["storage"]["node_templates"]

    return yaml_config


def cohend(data1: list, data2: list):
    """Compute a Cohen's d metric of two list of values"""
    n_data1, n_data2 = len(data1), len(data2)
    var1, var2 = np.var(data1, ddof=1), np.var(data2, ddof=1)
    global_var = np.sqrt(
        ((n_data1 - 1) * var1 + (n_data2 - 1) * var2) / (n_data1 + n_data2 - 2)
    )
    mean1, mean2 = np.mean(data1), np.mean(data2)
    return (mean1 - mean2) / global_var


def update_base_config(parametrization, base_config, cfg_name):
    """Update the base config with new values for parameters, as provided by Ax"""

    # Extract parameters proposed by Ax
    bandwidth_backbone_storage = 200
    if "bandwidth_backbone_storage" in parametrization:
        bandwidth_backbone_storage = parametrization.get("bandwidth_backbone_storage")

    bandwidth_backbone_perm_storage = 80
    if "bandwidth_backbone_perm_storage" in parametrization:
        bandwidth_backbone_perm_storage = parametrization.get(
            "bandwidth_backbone_perm_storage"
        )

    permanent_storage_read_bw = 79
    if "permanent_storage_read_bw" in parametrization:
        permanent_storage_read_bw = parametrization.get("permanent_storage_read_bw")

    permanent_storage_write_bw = 27
    if "permanent_storage_write_bw" in parametrization:
        permanent_storage_write_bw = parametrization.get("permanent_storage_write_bw")

    preload_percent = 0
    if "preload_percent" in parametrization:
        preload_percent = parametrization.get("preload_percent")

    disk_rb = 3187
    if "disk_rb" in parametrization:
        disk_rb = parametrization.get("disk_rb")

    disk_wb = 893
    if "disk_wb" in parametrization:
        disk_wb = parametrization.get("disk_wb")

    stripe_size = 16777216
    if "stripe_size" in parametrization:
        stripe_size = parametrization.get("stripe_size")

    stripe_count = 1
    if "stripe_count" in parametrization:
        stripe_count = parametrization.get("stripe_count")

    stripe_count_high_thresh_write = 215034750
    if "stripe_count_high_thresh_write" in parametrization:
        stripe_count_high_thresh_write = parametrization.get(
            "stripe_count_high_thresh_write"
        )

    stripe_count_high_thresh_read = 150e6
    if "stripe_count_high_thresh_read" in parametrization:
        stripe_count_high_thresh_read = parametrization.get(
            "stripe_count_high_thresh_read"
        )

    stripe_count_high_write_add = 3
    if "stripe_count_high_write_add" in parametrization:
        stripe_count_high_write_add = parametrization.get("stripe_count_high_write_add")

    stripe_count_high_read_add = 4
    if "stripe_count_high_read_add" in parametrization:
        stripe_count_high_read_add = parametrization.get("stripe_count_high_read_add")

    nb_files_per_read = 12
    if "nb_files_per_read" in parametrization:
        nb_files_per_read = parametrization.get("nb_files_per_read")

    io_read_node_ratio = 0.3
    if "io_read_node_ratio" in parametrization:
        io_read_node_ratio = parametrization.get("io_read_node_ratio")

    nb_files_per_write = 5
    if "nb_files_per_write" in parametrization:
        nb_files_per_write = parametrization.get("nb_files_per_write")

    io_write_node_ratio = 0.5
    if "io_write_node_ratio" in parametrization:
        io_write_node_ratio = parametrization.get("io_write_node_ratio")

    # Non-linear coefficient for altering read/write during concurrent disk access
    non_linear_coef_read = 1.8
    if "non_linear_coef_read" in parametrization:
        non_linear_coef_read = parametrization.get("non_linear_coef_read")

    non_linear_coef_write = 15.2
    if "non_linear_coef_write" in parametrization:
        non_linear_coef_write = parametrization.get("non_linear_coef_write")

    # Maximum number of file chunks on each OST used in striping
    max_chunks_per_ost = 28
    if "max_chunks_per_ost" in parametrization:
        max_chunks_per_ost = parametrization.get("max_chunks_per_ost")

    # Max. number of compute nodes used for reading
    max_read_node_cnt = 28
    if "max_read_node_cnt" in parametrization:
        max_read_node_cnt = parametrization.get("max_read_node_cnt")

    # Max. number of compute nodes used for writing
    max_write_node_cnt = 11
    if "max_write_node_cnt" in parametrization:
        max_write_node_cnt = parametrization.get("max_write_node_cnt")

    # Update config file according to parameters provided by Ax
    base_config["general"]["config_name"] = cfg_name
    base_config["general"]["config_version"] = CFG_VERSION
    base_config["general"]["preload_percent"] = preload_percent
    base_config["network"][
        "bandwidth_backbone_storage"
    ] = f"{bandwidth_backbone_storage}GBps"
    base_config["network"][
        "bandwidth_backbone_perm_storage"
    ] = f"{bandwidth_backbone_perm_storage}GBps"
    base_config["permanent_storage"]["read_bw"] = f"{permanent_storage_read_bw}GBps"
    base_config["permanent_storage"]["write_bw"] = f"{permanent_storage_write_bw}GBps"

    base_config["storage"]["read_variability"] = 1  # deactivated
    base_config["storage"]["write_variability"] = 1  # deactivated

    base_config["storage"]["nb_files_per_read"] = nb_files_per_read
    base_config["storage"]["io_read_node_ratio"] = io_read_node_ratio
    base_config["storage"]["nb_files_per_write"] = nb_files_per_write
    base_config["storage"]["io_write_node_ratio"] = io_write_node_ratio

    base_config["storage"]["non_linear_coef_read"] = non_linear_coef_read
    base_config["storage"]["non_linear_coef_write"] = non_linear_coef_write

    base_config["storage"]["max_read_node_cnt"] = max_read_node_cnt
    base_config["storage"]["max_write_node_cnt"] = max_write_node_cnt

    # WARINING : HERE WE SET THE SAME READ/WRITE BANDWIDTH FOR ALL DISKS
    # THIS WILL NOT ALWAYS BE THE CASE.
    for storage_node in base_config["storage"]["nodes"]:
        for disk in storage_node["template"]["disks"]:
            disk["template"]["read_bw"] = disk_rb
            disk["template"]["write_bw"] = disk_wb

    base_config["lustre"]["stripe_size"] = stripe_size
    base_config["lustre"]["stripe_count"] = stripe_count
    base_config["lustre"][
        "stripe_count_high_thresh_write"
    ] = stripe_count_high_thresh_write
    base_config["lustre"][
        "stripe_count_high_thresh_read"
    ] = stripe_count_high_thresh_read
    base_config["lustre"]["stripe_count_high_write_add"] = stripe_count_high_write_add
    base_config["lustre"]["stripe_count_high_read_add"] = stripe_count_high_read_add

    base_config["lustre"]["max_chunks_per_ost"] = max_chunks_per_ost


def save_exp_config(base_config, run_idx):
    """Save base_config to file"""

    # Save config as file with a unique name for each parameter set
    random_part = "".join(
        random.choices("A,B,C,D,E,F,0,1,2,3,4,5,6,7,8,9".split(","), k=5)
    )

    # print(f"Updated configuration : ")
    # print(json.dumps(base_config, indent=4))

    output_configuration = f"{CONFIGURATION_PATH}/exp_config_{run_idx}_{random_part}"

    with open(output_configuration, "w", encoding="utf-8") as exp_config:
        # print("Dumping configuration to " + output_configuration)
        yaml.dump(base_config, exp_config)

    return (output_configuration, random_part)


def process_results(result_filename: str):
    """Process results from experiment"""

    # Now exploit results
    results = None
    with open(f"./exp_results/{result_filename}", "r", encoding="utf-8") as job_results:
        results = yaml.load(job_results, Loader=yaml.CLoader)

    # IO TIME
    io_time_diff = []
    sim_io_time = []
    sim_read_time = []
    sim_write_time = []
    real_io_time = []
    real_read_time = []
    real_write_time = []

    for job in results:
        s_io_time = 0
        s_r_time = 0
        s_w_time = 0

        for action in job["actions"]:
            if (
                action["act_type"] == "COMPUTE"
                or action["act_type"] == "SLEEP"
                or action["act_status"] != "COMPLETED"
            ):
                continue
            if action["act_type"] == "FILEREAD":
                s_r_time += action["act_duration"] * action["nb_stripes"]
            if action["act_type"] == "CUSTOM" and "write" in str(action["sub_job"]):
                s_w_time += action["act_duration"] * action["nb_stripes"]

        if len(job["actions"]) != 0:
            r_io_time = job["real_cReadTime_s"] + job["real_cWriteTime_s"]
            real_io_time.append(r_io_time)
            real_read_time.append(job["real_cReadTime_s"])
            real_write_time.append(job["real_cWriteTime_s"])

            s_io_time = s_r_time + s_w_time
            sim_io_time.append(s_io_time)
            sim_read_time.append(s_r_time)
            sim_write_time.append(s_w_time)
            io_time_diff.append(abs(s_io_time - r_io_time))

    # Z-test (asserting statistical significance of the difference between means of real and simulated runtime / IO times)
    ztest_iotime_tstat, ztest_iotime_pvalue = sm.stats.ztest(
        sim_io_time, real_io_time, alternative="two-sided"
    )

    if abs(ztest_iotime_tstat) > 1.96 and ztest_iotime_pvalue < 0.01:
        print(
            "Statistically significant difference between simulated io time values and real io time values"
        )

    io_time_corr, _ = pearsonr(sim_io_time, real_io_time)
    read_time_corr, _ = pearsonr(sim_read_time, real_read_time)
    write_time_corr, _ = pearsonr(sim_write_time, real_write_time)
    io_time_cohen_d = cohend(sim_io_time, real_io_time)

    mean_io_diff = np.array(io_time_diff).mean()

    # return {"optimization_metric": (abs(1 - io_time_corr) + abs(io_time_cohen_d))}
    # return {"optimization_metric": abs(ztest_iotime_tstat)}
    # return {"optimization_metric": abs(1 - write_time_corr) + abs(1 - read_time_corr)}
    # return {"optimization_metric": abs(1 - write_time_corr)}
    # return {"optimization_metric": abs(1 - read_time_corr)}
    return {"optimization_metric": abs(mean_io_diff)}


def run_simulation(
    parametrization: dict,
    base_config: dict,
    run_idx: int,
    capture: bool,
    logs: bool = False,
):
    """Calibration function : extract parameters as provided by the optimization framework,
    update the base config and create a fitting configuration file, then run the simulation
    using the new configuration and always the same dataset.
    Eventually, compute metrics (correlation / cohens) on the results and output a cost.
    """

    # Config
    update_base_config(
        parametrization, base_config, f"Storalloc_CalibrationCfg__{run_idx}"
    )
    output_configuration, random_part = save_exp_config(base_config, run_idx)

    # Now run simulatin with the current configuration file
    command = [
        f"{BUILD_PATH}/fives_wrench",
        output_configuration,
        f"{DATASET_PATH}/{DATASET}{DATASET_EXT}",
        random_part,
        "--wrench-commport-pool-size=200000",
    ]
    if logs:
        command.extend(
            [
                "--wrench-full-log",
                "--log=fives_controller.threshold=debug",
                "--log=wrench_core_compound_storage_system.threshold=debug",
                "--log=wrench_core_logical_file_system.threshold=warning",
            ]
        )

    completed = subprocess.run(
        command,
        capture_output=capture,
        check=False,
    )

    print(
        f"Simulation with tag {random_part} has completed with status : {completed.returncode}"
    )
    if completed.returncode != 0:
        print(f"############## FAILED RUN {run_idx} OUTPUT ###########")
        print(completed.stdout)
        print(completed.stderr)
        print(f"############## FAILED RUN {run_idx} END OF OUTPUT ####")
        raise RuntimeError("Simulation did not complete")

    result_filename = (
        f"simulatedJobs_{DATASET}__"
        + f"{base_config['general']['config_name']}"
        + f"_{base_config['general']['config_version']}"
        + f"_{random_part}.yml"
    )
    print(f"Now looking for result file : {result_filename}")

    result_file = pathlib.Path(f"./{result_filename}")
    if not result_file.exists() or not result_file.is_file():
        raise RuntimeError(f"Result file {result_filename} was not found")

    print(result_file.resolve())
    subprocess.run(
        ["mv", result_file.resolve(), f"./exp_results/{result_filename}"],
        capture_output=True,
        check=True,
    )

    return result_filename


def run_default_simulation():
    default_params = {
        "backbone_bw": 240,
        "permanent_storage_read_bw": 90,
        "permanent_storage_write_bw": 90,
        "preload_percent": 0,
        "amdahl": 0.8,
        "disk_rb": 6000,
        "disk_wb": 3000,
        "stripe_size": 2097152,
        "stripe_count": 4,
        "nb_files_per_read": 2,
        "io_read_node_ratio": 0.1,
        "nb_files_per_write": 2,
        "io_write_node_ratio": 0.1,
    }

    # buggy run
    default_params = {
        "backbone_bw": 215,
        "permanent_storage_read_bw": 88,
        "permanent_storage_write_bw": 70,
        "preload_percent": 0,
        "amdahl": 0.42,
        "disk_rb": 2617,
        "disk_wb": 539,
        "stripe_size": 67108864,
        "stripe_count": 8,
        "nb_files_per_read": 1,
        "io_read_node_ratio": 0.59,
        "nb_files_per_write": 1,
        "io_write_node_ratio": 0.75,
    }

    base_config = load_base_config(CONFIGURATION_BASE)

    run_simulation(default_params, base_config, 0, False, True)


def run_trial(base_config, parameters, trial_index):
    print(f"Starting run #{trial_index}")
    results = {"trial_index": trial_index, "optimization_metric": None}
    try:
        data = run_simulation(parameters, base_config, trial_index, True)
        results["optimization_metric"] = process_results(data)["optimization_metric"]
    except Exception as e:
        print(f"{e}")
        print(f"==> Trial {trial_index} FAILED")

    print(f"## Results for trial {trial_index} == {results}")
    return results


def run_calibration(params_set):
    """Main calibration loop"""

    base_config = load_base_config(CONFIGURATION_BASE)

    print("## PARAMETERS IN USE FOR THIS CALIBRATION : ")
    for param in params_set:
        print(f">> {param['name']}")
    print("############################################")
    sleep(3)

    ax_client = AxClient()  # enforce_sequential_optimization=False)
    ax_client.create_experiment(
        name="StorallocWrench_ThetaExperiment",
        parameters=params_set,
        objectives={
            "optimization_metric": ObjectiveProperties(minimize=True),
        },
        parameter_constraints=[
            "disk_rb >= disk_wb",
            # "permanent_storage_read_bw >= permanent_storage_write_bw",
            "non_linear_coef_read <= non_linear_coef_write",
            # "permanent_storage_read_bw >= permanent_storage_write_bw",
            # "bandwidth_backbone_storage >= bandwidth_backbone_perm_storage",
        ],
        outcome_constraints=[],
    )

    parallelism = ax_client.get_max_parallelism()
    print("# Ax parallelism bounds are : ")
    print(parallelism)
    print(f"# This machine has {multiprocessing.cpu_count()} physical cores")

    # "Full" parallel part (random parameters)
    trials_parameters = []
    for _ in range(parallelism[0][0]):
        trials_parameters.append(ax_client.get_next_trial())

    parallel_pool_params = [
        (base_config, trial[0], trial[1]) for trial in trials_parameters
    ]
    print(
        f"Parallel pool params contains {len(parallel_pool_params)} tuples of parameters for the simulations runs"
    )

    cpu = min(multiprocessing.cpu_count() - 2, parallelism[0][1])
    cpu = min(
        cpu, 6
    )  # Attempt at mitigating runner limitation... (the f***** VM is damn too slow / buggy)
    print(
        f"### Running {cpu} simulation in parallel (max Ax // is {parallelism[0][1]} for the first {parallelism[0][0]} runs)"
    )

    # Full-parallel pool and loop
    with multiprocessing.Pool(cpu) as p:
        print("## Starting the first parallel pool")
        results = p.starmap(run_trial, parallel_pool_params)

        for res in results:
            if res["optimization_metric"] is not None:
                print(f"Recording trial success for trial {res['trial_index']}")
                ax_client.complete_trial(
                    trial_index=res["trial_index"], raw_data=res["optimization_metric"]
                )
            else:
                print("Recording trial failure")
                ax_client.log_trial_failure(trial_index=res["trial_index"])

    # Run n iterations with a reduced number of parallel simulations (this is the 'sequential' part)
    for i in range(CALIBRATION_RUNS):
        parameters, trial_index = ax_client.get_next_trial()
        res = run_trial(load_base_config(CONFIGURATION_BASE), parameters, trial_index)
        if res["optimization_metric"]:
            print(f"Recording trial success for trial {res['trial_index']}")
            ax_client.complete_trial(
                trial_index=res["trial_index"], raw_data=res["optimization_metric"]
            )
        else:
            print("Recording trial failure")
            ax_client.log_trial_failure(trial_index=res["trial_index"])

    best_parameters, values = ax_client.get_best_parameters()
    print("Best parameters found :")
    print(best_parameters)
    print("Other calibration values : ")
    print(values)

    # Output calibrated config file
    update_base_config(best_parameters, base_config, f"Fives_C_{DATASET}")
    print("Calibrated config :")
    print(json.dumps(base_config, indent=4))
    output_configuration = f"{CONFIGURATION_PATH}/calibrated_config.yaml"
    with open(output_configuration, "w", encoding="utf-8") as calibration_result:
        print("Dumping configuration to " + output_configuration)
        yaml.dump(base_config, calibration_result)

    print("CALIBRATION DONE")
    return 0


if __name__ == "__main__":
    # run_default_simulation()
    run_calibration(AX_PARAMS)
