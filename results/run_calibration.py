#!/usr/bin/env python3

"""
    Run a calibration process using the Ax framework.
    This script expects to find the compiled storalloc_wrench bin inside <repo_root>/build
    and a base configuration in <repo_root>/results/exp_configurations/

    It might take a very long time to run...

"""

import json
import random
import subprocess
import pathlib
import os

import yaml
import numpy as np
from scipy.stats import pearsonr

from ax.service.ax_client import AxClient, ObjectiveProperties

# from ax.utils.measurement.synthetic_functions import hartmann6

CONFIGURATION_PATH =  os.getenv("CALIBRATION_CONFIG_PATH", default="./exp_configurations")
CONFIGURATION_BASE = os.getenv("CALIBRATION_CONFIGURATION_BASE", default=f"{CONFIGURATION_PATH}/theta_config.yml")
DATASET_PATH = os.getenv("CALIBRATION_DATASET_PATH", default="./exp_datasets")
DATASET = os.getenv("CALIBRATION_DATASET", default="theta2022_week4_tiny")
DATASET_EXT = os.getenv("CALIBRATION_DATASET_EXT", default=".yaml")
BUILD_PATH = os.getenv("CALIBRATION_BUILD_PATH", default="../build")
CALIBRATION_RUNS = int(os.getenv("CALIBRATION_RUNS", default=25))

# Define the parameters that will be given to Ax for the optimization loop
# Bounds / value lists are not final
AX_PARAMS = [
    {
        "name": "backbone_bw",
        "type": "range",
        "bounds": [120, 240],  # Use large ranges
        "value_type": "int",
    },
    {
        "name": "permanent_storage_read_bw",
        "type": "range",
        "bounds": [5, 90],
        "value_type": "int",
    },
    {
        "name": "permanent_storage_write_bw",
        "type": "range",
        "bounds": [5, 90],
        "value_type": "int",
    },
    {
        "name": "preload_percent",
        "type": "choice",
        "is_ordered": True,
        "values": [0.1, 0.2, 0.3],
        "value_type": "float",
    },
    {
        "name": "amdahl",
        "type": "range",
        "bounds": [0.7, 1.0],
        "digits": 2,
        "value_type": "float",
    },
    {
        "name": "disk_rb",
        "type": "range",
        "bounds": [600, 6000],
        "value_type": "int",
    },
    {
        "name": "disk_wb",
        "type": "range",
        "bounds": [300, 3000],
        "value_type": "int",
    },
    {
        "name": "stripe_size",
        "type": "choice",
        "values": [2097152, 4194304, 8388608, 16777216, 67108864, 1073741824],
        "is_ordered": True,
        "value_type": "int",
    },
    {
        "name": "stripe_count",
        "type": "range",
        "bounds": [1, 8],  # NOTE : never using all OSTs for any allocation so far
        "value_type": "int",
    },
    {
        "name": "nb_files_per_read",
        "type": "choice",
        "values": [1, 2, 3, 4],
        "is_ordered": True,
        "value_type": "int",
    },
    {
        "name": "io_read_node_ratio",
        "type": "range",
        "bounds": [0.05, 0.2],
        "digits": 2,
        "value_type": "float",
    },
    {
        "name": "nb_files_per_write",
        "type": "choice",
        "values": [1, 2, 3, 4],
        "is_ordered": True,
        "value_type": "int",
    },
    {
        "name": "io_write_node_ratio",
        "type": "range",
        "bounds": [0.05, 0.2],
        "digits": 2,
        "value_type": "float",
    },
]


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


def update_base_config(parametrization, base_config):
    """Update the base config with new values for parameters, as provided by Ax"""

    # Extract parameters proposed by Ax
    backbone_bw = parametrization.get("backbone_bw")
    permanent_storage_read_bw = parametrization.get("permanent_storage_read_bw")
    permanent_storage_write_bw = parametrization.get("permanent_storage_write_bw")
    preload_percent = parametrization.get("preload_percent")
    amdahl = parametrization.get("amdahl")
    disk_rb = parametrization.get("disk_rb")
    disk_wb = parametrization.get("disk_wb")
    stripe_size = parametrization.get("stripe_size")
    stripe_count = parametrization.get("stripe_count")
    nb_files_per_read = parametrization.get("nb_files_per_read")
    io_read_node_ratio = parametrization.get("io_read_node_ratio")
    nb_files_per_write = parametrization.get("nb_files_per_write")
    io_write_node_ratio = parametrization.get("io_write_node_ratio")

    # Update config file according to parameters provided by Ax
    base_config["general"]["backbone_bw"] = f"{backbone_bw}GBps"
    base_config["general"][
        "permanent_storage_read_bw"
    ] = f"{permanent_storage_read_bw}GBps"
    base_config["general"][
        "permanent_storage_write_bw"
    ] = f"{permanent_storage_write_bw}GBps"
    base_config["general"]["preload_percent"] = preload_percent
    base_config["general"]["amdahl"] = amdahl
    base_config["general"]["non_linear_coef_read"] = 1  # deactivated
    base_config["general"]["non_linear_coef_write"] = 1  # deactivated
    base_config["general"]["read_variability"] = 1  # deactivated
    base_config["general"]["write_variability"] = 1  # deactivated

    base_config["general"]["nb_files_per_read"] = nb_files_per_read
    base_config["general"]["io_read_node_ratio"] = io_read_node_ratio
    base_config["general"]["nb_files_per_write"] = nb_files_per_write
    base_config["general"]["io_write_node_ratio"] = io_write_node_ratio

    # WARINING : HERE WE SET THE SAME READ/WRITE BANDWIDTH FOR ALL DISKS
    # THIS WILL NOT ALWAYS BE THE CASE.
    for storage_node in base_config["storage"]["nodes"]:
        for disk in storage_node["template"]["disks"]:
            disk["template"]["read_bw"] = disk_rb
            disk["template"]["write_bw"] = disk_wb

    base_config["lustre"]["stripe_size"] = stripe_size
    base_config["lustre"]["stripe_count"] = stripe_count


def save_exp_config(base_config, run_idx):
    """Save base_config to file"""

    # Save config as file with a unique name for each parameter set
    random_part = "".join(
        random.choices("A,B,C,D,E,F,0,1,2,3,4,5,6,7,8,9".split(","), k=4)
    )

    print(f"Updated configuration : ")
    print(json.dumps(base_config, indent=4))

    output_configuration = f"{CONFIGURATION_PATH}/exp_config_{run_idx}_{random_part}"

    with open(output_configuration, "w", encoding="utf-8") as exp_config:
        print("Dumping configuration to " + output_configuration)
        yaml.dump(base_config, exp_config)

    return (output_configuration, random_part)


def process_results(result_filename: str):
    """Process results from experiment"""

    # Now exploit results
    results = None
    with open(f"./exp_results/{result_filename}", "r", encoding="utf-8") as job_results:
        results = yaml.load(job_results, Loader=yaml.CLoader)

    # RUNTIME
    runtime_diffs = []
    sim_runtime = []
    real_runtime = []

    # IO TIME
    io_time_diff = []
    sim_io_time = []
    sim_read_time = []
    sim_write_time = []
    real_io_time = []
    real_read_time = []
    real_write_time = []

    for job in results:
        # RUNTIME
        runtime_diffs.append(abs(job["job_runtime_s"] - job["real_runtime_s"]))
        sim_runtime.append(job["job_runtime_s"])
        real_runtime.append(job["real_runtime_s"])

        # IO TIME
        r_io_time = (
            job["real_cReadTime_s"] + job["real_cWriteTime_s"] + job["real_cMetaTime_s"]
        ) / job["real_cores_used"]
        real_io_time.append(r_io_time)
        real_read_time.append(job["real_cReadTime_s"] / job["real_cores_used"])
        real_write_time.append(job["real_cWriteTime_s"] / job["real_cores_used"])

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
                s_r_time += action["act_duration"]
                s_io_time += action["act_duration"]
            if action["act_type"] == "CUSTOM":
                s_w_time += action["act_duration"]
                s_io_time += action["act_duration"]

        sim_io_time.append(s_io_time)
        sim_read_time.append(s_r_time)
        sim_write_time.append(s_w_time)
        io_time_diff.append(abs(s_io_time - r_io_time))

    runtime_corr, _ = pearsonr(sim_runtime, real_runtime)
    runtime_cohen_d = cohend(sim_runtime, real_runtime)
    io_time_corr, _ = pearsonr(sim_io_time, real_io_time)
    io_time_cohen_d = cohend(sim_io_time, real_io_time)

    return {
        "optimization_metric": (
            abs(1 - runtime_corr)
            + abs(1 - io_time_corr)
            + abs(runtime_cohen_d)
            + abs(io_time_cohen_d)
        )
    }


def run_simulation(parametrization: dict, base_config: dict, run_idx: int, capture: bool, logs: bool = False):
    """Calibration function : extract parameters as provided by the optimization framework,
    update the base config and create a fitting configuration file, then run the simulation
    using the new configuration and always the same dataset.
    Eventually, compute metrics (correlation / cohens) on the results and output a cost.
    """

    # Config
    update_base_config(parametrization, base_config)
    output_configuration, random_part = save_exp_config(base_config, run_idx)

    # Now run simulatin with the current configuration file
    command = [
        f"{BUILD_PATH}/storalloc_wrench",
        output_configuration,
        f"{DATASET_PATH}/{DATASET}{DATASET_EXT}",
        random_part,
    ]
    if logs:
        command.extend([                
            "--wrench-full-log",
            "--log=storalloc_controller.threshold=debug",
            "--log=wrench_core_compound_storage_system.threshold=debug",
            "--log=wrench_core_logical_file_system.threshold=warning",
        ])

    completed = subprocess.run(
        command,
        capture_output=capture,
        check=False,
    )

    print(
        f"Simulation with tag {random_part} has completed with status : {completed.returncode}"
    )
    if completed.returncode != 0:
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


def run_calibration():
    """Main calibration loop"""

    base_config = load_base_config(CONFIGURATION_BASE)

    ax_client = AxClient()
    ax_client.create_experiment(
        name="StorallocWrench_ThetaExperiment",
        parameters=AX_PARAMS,
        objectives={
            "optimization_metric": ObjectiveProperties(minimize=True, threshold=0.2),
        },
        parameter_constraints=[
            "disk_rb >= disk_wb",
            "permanent_storage_read_bw >= permanent_storage_write_bw",
        ],
        outcome_constraints=[],
    )

    for i in range(CALIBRATION_RUNS):
        parameters, trial_index = ax_client.get_next_trial()
        data = None
        try:
            data = run_simulation(parameters, base_config, i, True)
            processed_results = process_results(data)
        except RuntimeError as err:
            print(err)
            ax_client.log_trial_failure(trial_index=trial_index)
            continue
        else:
            ax_client.complete_trial(
                trial_index=trial_index, raw_data=processed_results
            )

    best_parameters, values = ax_client.get_best_parameters()
    print("Best parameters found :")
    print(best_parameters)
    means, covariances = values
    print("Means : ")
    print(means)
    print("Covariances : ")
    print(covariances)

    # Output calibrated config file
    calibrated_config = update_base_config(best_parameters, base_config)
    print("Calibrated config :")
    print(json.dumps(calibrated_config, indent=4))
    output_configuration = f"{CONFIGURATION_PATH}/calibration_config.yaml"
    with open(output_configuration, "w", encoding="utf-8") as calibration_result:
        print("Dumping configuration to " + output_configuration)
        yaml.dump(calibrated_config, calibration_result)

    print("CALIBRATION DONE")


if __name__ == "__main__":
    # run_default_simulation()
    run_calibration()
