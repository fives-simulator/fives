#!/usr/bin/env python3

"""
    Run a calibration process using the Ax framework.
    This script expects to find the compiled fives bin inside <repo_root>/build
    and a base configuration in <repo_root>/results/exp_configurations/

    It might take a very long time to run...

"""

import json
import subprocess
import pathlib
import os
import multiprocessing
from time import sleep
import datetime as dt

import yaml
import numpy as np
from scipy.stats import pearsonr, ttest_rel, wilcoxon
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
DATASET = os.getenv("CALIBRATION_DATASET", default="theta2022_aggMonth11_cat1")
DATASET_EXT = os.getenv("CALIBRATION_DATASET_EXT", default=".yaml")
BUILD_PATH = os.getenv("CALIBRATION_BUILD_PATH", default="../build")
CALIBRATION_RUNS = int(os.getenv("CALIBRATION_RUNS", default=25))
CFG_VERSION = os.getenv("CI_COMMIT_SHORT_SHA", default="0.0.1")
MAX_PARALLELISM = os.getenv("MAX_PARALLELISM", default=1)

now = dt.datetime.now()
today = f"{now.year}-{now.month}-{now.day}"
min_in_day = ((now.timestamp() % 86400) / 60)
CALIBRATION_UID = f"{today}-{min_in_day:.0f}"

PARAMETERS = [
        # Read params
        { "name": "nb_files_per_read", "type": "range", "bounds": [1, 10], "value_type": "int" },
        { "name": "stripe_count_high_thresh_read", "type": "range", "bounds": [10e6, 100e6], "value_type": "int" },
        { "name": "read_node_thres", "type": "range", "bounds": [1e6, 50e6], "value_type": "int" },
        { "name": "stripe_count_high_read_add", "type": "range", "bounds": [1, 4], "value_type": "int" },
        { "name": "disk_rb", "type": "range", "bounds": [1000, 4300], "value_type": "int" },
        { "name": "non_linear_coef_read", "type": "range", "bounds": [1, 50], "value_type": "float", "digits": 1 },
        { "name": "static_read_overhead_seconds", "type": "range", "bounds": [0, 50], "value_type": "int" },
        # Write params
        { "name": "nb_files_per_write", "type": "range", "bounds": [1, 10], "value_type": "int" },
        { "name": "stripe_count_high_thresh_write", "type": "range", "bounds": [10e6, 100e6], "value_type": "int" },
        { "name": "write_node_thres", "type": "range", "bounds": [1e6, 50e6], "value_type": "int" },
        { "name": "stripe_count_high_write_add", "type": "range", "bounds": [1, 4], "value_type": "int" },
        { "name": "disk_wb", "type": "range", "bounds": [500, 3500], "value_type": "int" },
        { "name": "non_linear_coef_write", "type": "range", "bounds": [1, 50], "value_type": "float", "digits": 1 },
        { "name": "static_write_overhead_seconds", "type": "range", "bounds": [0, 50], "value_type": "int" },
        # Misc
        { "name": "stripe_count", "type": "range", "bounds":[1, 4], "value_type": "int" },
        { "name": "max_chunks_per_ost", "type": "range", "bounds":[8, 64], "value_type": "int" },
        { "name": "bandwidth_backbone_storage", "type": "range", "bounds":[100, 240], "value_type": "int" },
        # # Unused
        # RangeParameter(name="permanent_storage_read_bw", lower=10, upper=90, parameter_type=ParameterType.INT),
        # RangeParameter(name="permanent_storage_write_bw", lower=10, upper=90, parameter_type=ParameterType.INT),
        # RangeParameter(name="bandwidth_backbone_perm_storage", lower=50, upper=100, parameter_type=ParameterType.INT),
        # ChoiceParameter(name="stripe_size", values=[
        #     2097152, 4194304, 8388608, 16777216, 67108864, 1073741824, 2147483648,
        # ], parameter_type=ParameterType.INT),
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


def update_base_config(parametrization, base_config, cfg_name):
    """Update the base config with new values for parameters, as provided by Ax"""

    # Update config file according to parameters provided by Ax
    base_config["general"]["config_name"] = cfg_name 
    base_config["general"]["config_version"] = CFG_VERSION

    # Network bandwidths    
    if "bandwidth_backbone_storage" in parametrization:
        bandwidth_backbone_storage = parametrization.get("bandwidth_backbone_storage")
        base_config["network"]["bandwidth_backbone_storage"] = f"{bandwidth_backbone_storage}GBps"

    if "bandwidth_backbone_perm_storage" in parametrization:
        bandwidth_backbone_perm_storage = parametrization.get("bandwidth_backbone_perm_storage")
        base_config["network"]["bandwidth_backbone_perm_storage"] = f"{bandwidth_backbone_perm_storage}GBps"

    # External storage R/W bandwidths
    if "permanent_storage_read_bw" in parametrization:
        permanent_storage_read_bw = parametrization.get("permanent_storage_read_bw")
        base_config["permanent_storage"]["read_bw"] = f"{permanent_storage_read_bw}GBps"
    
    if "permanent_storage_write_bw" in parametrization:
        permanent_storage_write_bw = parametrization.get("permanent_storage_write_bw")
        base_config["permanent_storage"]["write_bw"] = f"{permanent_storage_write_bw}GBps"

    # Number of preload jobs in proportion to the number of jobs in the simulated dataset
    if "preload_percent" in parametrization:
        base_config["general"]["preload_percent"] = parametrization.get("preload_percent")

    # Lustre parameter, stripe_size (can be dynamically overriden during sim)
    if "stripe_size" in parametrization:
        stripe_size = parametrization.get("stripe_size")
        base_config["lustre"]["stripe_size"] = stripe_size

    # Lustre parameter, stripe_count (number of OST used for load-balancing parts of a file)
    if "stripe_count" in parametrization:
        stripe_count = parametrization.get("stripe_count")
        base_config["lustre"]["stripe_count"] = stripe_count

    # Cumulated read mean bandwidth threshold between static and dynamic stripe_count for jobs
    if "stripe_count_high_thresh_read" in parametrization:
        stripe_count_high_thresh_read = parametrization.get("stripe_count_high_thresh_read")
        base_config["lustre"]["stripe_count_high_thresh_read"] = stripe_count_high_thresh_read

    # Cumulated write mean bandwidth threshold between static and dynamic stripe_count for jobs
    if "stripe_count_high_thresh_write" in parametrization:
        stripe_count_high_thresh_write = parametrization.get("stripe_count_high_thresh_write")
        base_config["lustre"]["stripe_count_high_thresh_write"] = stripe_count_high_thresh_write
    
    # Base stripe_count value for dynamic stripe_count model (read)
    if "stripe_count_high_read_add" in parametrization:
        stripe_count_high_read_add = parametrization.get("stripe_count_high_read_add")
        base_config["lustre"]["stripe_count_high_read_add"] = stripe_count_high_read_add

    # Base stripe_count value for dynamic stripe_count model (write)
    if "stripe_count_high_write_add" in parametrization:
        stripe_count_high_write_add = parametrization.get("stripe_count_high_write_add")
        base_config["lustre"]["stripe_count_high_write_add"] = stripe_count_high_write_add

    # Max number of parts of each file to place on an OST (simulation performance limitation)
    if "max_chunks_per_ost" in parametrization:
        max_chunks_per_ost = parametrization.get("max_chunks_per_ost")
        base_config["lustre"]["max_chunks_per_ost"] = max_chunks_per_ost

    # Read bandwidth of individual PFS disks
    if "disk_rb" in parametrization:
        disk_rb = parametrization.get("disk_rb")
        for storage_node in base_config["storage"]["nodes"]:
            for disk in storage_node["template"]["disks"]:
                disk["template"]["read_bw"] = disk_rb

    # Write bandwidth of individual PFS disks
    if "disk_wb" in parametrization:
        disk_wb = parametrization.get("disk_wb")
        for storage_node in base_config["storage"]["nodes"]:
            for disk in storage_node["template"]["disks"]:
                disk["template"]["write_bw"] = disk_wb

    # Base file number for read action (part of dynamic model)
    if "nb_files_per_read" in parametrization:
        base_config["storage"]["nb_files_per_read"] = parametrization.get("nb_files_per_read")

    # Base file number for write action (part of dynamic model)
    if "nb_files_per_write" in parametrization:
        base_config["storage"]["nb_files_per_write"] = parametrization.get("nb_files_per_write")

    # Disk bandwidth degradation model calibrated coefficient for reads
    if "non_linear_coef_read" in parametrization:
        non_linear_coef_read = parametrization.get("non_linear_coef_read")
        base_config["storage"]["non_linear_coef_read"] = non_linear_coef_read
    
    # Disk bandwidth degradation model calibrated coefficient for writes
    if "non_linear_coef_write" in parametrization:
        non_linear_coef_write = parametrization.get("non_linear_coef_write")
        base_config["storage"]["non_linear_coef_write"] = non_linear_coef_write

    # Cumulated read mean bandwidth threshold between static and dynamic number of I/O nodes for jobs
    if "read_node_thres" in parametrization:
        read_node_thres = parametrization.get("read_node_thres")
        base_config["storage"]["read_node_thres"] = read_node_thres

    # Cumulated write mean bandwidth threshold between static and dynamic number of I/O nodes for jobs
    if "write_node_thres" in parametrization:
        write_node_thres = parametrization.get("write_node_thres")
        base_config["storage"]["write_node_thres"] = write_node_thres

    if "static_read_overhead_seconds" in parametrization:
        static_read_overhead_seconds = parametrization.get("static_read_overhead_seconds")
        base_config["storage"]["static_read_overhead_seconds"] = static_read_overhead_seconds

    if "static_write_overhead_seconds" in parametrization:
        static_write_overhead_seconds = parametrization.get("static_write_overhead_seconds")
        base_config["storage"]["static_write_overhead_seconds"] = static_write_overhead_seconds


def save_exp_config(base_config, run_idx):
    """Save base_config to file"""

    # print(f"Updated configuration : ")
    # print(json.dumps(base_config, indent=4))

    output_configuration = f"{CONFIGURATION_PATH}/exp_config_{CALIBRATION_UID}_{run_idx}"

    with open(output_configuration, "w", encoding="utf-8") as exp_config:
        print("  Dumping configuration to " + output_configuration)
        yaml.dump(base_config, exp_config)

    return output_configuration


def process_results(result_filename: str):
    """Process results from experiment"""

    # Now exploit results
    results = None
    with open(f"./exp_results/{result_filename}", "r", encoding="utf-8") as job_results:
        results = yaml.load(job_results, Loader=yaml.CLoader)

    # IO TIME
    io_time_error = []
    io_time_gt0_error = []
    io_time_lt0_error = []
    io_time_squared_error = []
    io_time_abs_error = []
    io_time_abs_pct_error = []

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

            error = r_io_time - s_io_time
            io_time_error.append(r_io_time - s_io_time)
            if error >= 0:
                io_time_gt0_error.append(error)
            else:
                io_time_lt0_error.append(error)
            io_time_squared_error.append(pow(error, 2))
            io_time_abs_error.append(abs(error))
            io_time_abs_pct_error.append((abs(error) / r_io_time))
        else:
            raise RuntimeError(f"Job {job['job_uid']} has 0 read or write action. This should not happen.")

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
    ttest_io_time = ttest_rel(real_io_time, sim_io_time, alternative="two-sided")
    wilcoxon_io_time = wilcoxon(io_time_error, alternative="two-sided")

    me = np.array(io_time_error).mean()
    mgt0e = np.array(io_time_gt0_error).mean()
    mlt0e = abs(np.array(io_time_lt0_error).mean())
    mse = np.array(io_time_squared_error).mean()
    mae = np.array(io_time_abs_error).mean()
    mae_pct = np.array(io_time_abs_pct_error).mean()

    # return {"optimization_metric": (abs(1 - io_time_corr) + abs(io_time_cohen_d))}
    # return {"optimization_metric": abs(ztest_iotime_tstat)}
    # return {"optimization_metric": abs(1 - write_time_corr) + abs(1 - read_time_corr)}
    # return {"optimization_metric": abs(1 - write_time_corr)}
    # return {"optimization_metric": abs(1 - read_time_corr)}
    # return {"optimization_metric": abs(ttest_io_time.statistic)}
    # return {"optimization_metric": abs(wilcoxon_io_time.statistic)}
    return {"optimization_metric": mae_pct}
    # return {"optimization_metric": ((1 - write_time_corr) + (1 - read_time_corr)) * mean_io_diff_pct }


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
        parametrization, base_config, f"Storalloc_ParaCalib{CALIBRATION_UID}__{run_idx}"
    )
    output_configuration = save_exp_config(base_config, run_idx)
    tag = f"{CALIBRATION_RUNS}_{run_idx}"

    # Now run simulatin with the current configuration file
    command = [
        f"{BUILD_PATH}/fives",
        output_configuration,
        f"{DATASET_PATH}/{DATASET}{DATASET_EXT}",
        tag,
        "--wrench-commport-pool-size=1000000",
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
        f"Simulation with tag {tag} has completed with status : {completed.returncode}"
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
        + f"_{tag}.yml"
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


def evaluate(parameters, trial_index):
    """Run a simulation with the parameter set provided by Ax and return results."""
    print(f"Starting run #{trial_index}")
    base_config = load_base_config(CONFIGURATION_BASE)
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
            "non_linear_coef_read <= non_linear_coef_write",
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
        (trial[0], trial[1]) for trial in trials_parameters
    ]
    print(
        f"Parallel pool params contains {len(parallel_pool_params)} tuples of parameters for the simulations runs"
    )

    cpu = min(multiprocessing.cpu_count() - 2, parallelism[0][1])
    cpu = min(
        cpu, MAX_PARALLELISM
    )  # Attempt at mitigating runner limitation... (the f***** VM is damn too slow / buggy)
    print(
        f"### Running {cpu} simulation in parallel (max Ax // is {parallelism[0][1]} for the first {parallelism[0][0]} runs)"
    )

    failed_attempts = 0
    # Full-parallel pool and loop
    with multiprocessing.Pool(cpu) as p:
        print("## Starting the first parallel pool")
        results = p.starmap(evaluate, parallel_pool_params)

        for res in results:
            if res["optimization_metric"] is not None:
                print(f"Recording trial success for trial {res['trial_index']}")
                ax_client.complete_trial(
                    trial_index=res["trial_index"], raw_data=res["optimization_metric"]
                )
            else:
                print("Recording trial failure")
                ax_client.log_trial_failure(trial_index=res["trial_index"])
                failed_attempts += 1

    # Run n iterations with a reduced number of parallel simulations (this is the 'sequential' part)
    for i in range(CALIBRATION_RUNS):
        parameters, trial_index = ax_client.get_next_trial()
        res = evaluate(parameters, trial_index)
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
    output_configuration = f"{CONFIGURATION_PATH}/calibrated_config_p{CALIBRATION_UID}.yaml"
    with open(output_configuration, "w", encoding="utf-8") as calibration_result:
        print("Dumping configuration to " + output_configuration)
        yaml.dump(base_config, calibration_result)

    # Keep trace of the calibration env.
    calib_settings = {
        "params": params_set, 
        "iterations": CALIBRATION_RUNS, 
        "calibration_dataset": DATASET, 
        "base_config": load_base_config(CONFIGURATION_BASE),
        "failed_calibration_runs": failed_attempts,
    }
    with open(f"parallel_calibration{CALIBRATION_UID}_settings.yaml", "w", encoding="utf-8") as calibration_out:
        yaml.dump(calib_settings, calibration_out)

    print("CALIBRATION DONE")
    return 0


if __name__ == "__main__":
    run_calibration(PARAMETERS)
