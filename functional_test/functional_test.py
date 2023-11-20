#!/usr/bin/env python3

"""
    Storalloc-Wrench
    Functional testing

    This script expects to find the `storalloc_wrench` binary in
    <repository_root>/build/, data file in <repository_root>/data
    and config files in <repository_root>/configs. It should be run
    from the functionnal_test directory
"""

import sys
import subprocess
import pathlib
import random
import yaml

import numpy as np
from scipy.stats import pearsonr
from colorama import Fore, Back, Style

BASE_PATH = ".."

STORALLOC = f"{BASE_PATH}/build/storalloc_wrench"
CONFIG = "theta_config.yml"
CONFIG_PATH = f"{BASE_PATH}/configs/{CONFIG}"
DATA = "theta2022_week4_trunc"
DATA_PATH = f"{BASE_PATH}/data/test_data/{DATA}.yaml"
SIMULATION_LOGS = "./simulation_logs.txt"


def cohend(d1, d2):
    """Cohen's d computation"""
    n1, n2 = len(d1), len(d2)
    s1, s2 = np.var(d1, ddof=1), np.var(d2, ddof=1)
    s = np.sqrt(((n1 - 1) * s1 + (n2 - 1) * s2) / (n1 + n2 - 2))
    u1, u2 = np.mean(d1), np.mean(d2)
    return (u1 - u2) / s


def analyse_jobs(file_path: pathlib.Path):
    """Process 'simulatedJobs_* dataset and check that values are correct"""

    jobs = None
    with open(file_path, "r", encoding="utf-8") as job_file:
        jobs = yaml.load(job_file, Loader=yaml.CLoader)

    job_count = len(jobs)

    print(Fore.CYAN + f"  - Loaded dataset with {job_count} jobs" + Style.RESET_ALL)

    errors = 0
    warnings = 0
    last_submit_ts = 0
    for job in jobs:
        if job["job_status"] != "COMPLETED":
            print(
                Fore.RED
                + f"  [ERROR] Job {job['job_uid']} is not in COMPLETED state"
                + Style.RESET_ALL
            )
            errors += 1
        if job["job_submit_ts"] < last_submit_ts:
            print(
                Fore.RED
                + f"  [ERROR] Job {job['job_uid']} has a submit TS lower than previous job"
                + Style.RESET_ALL
            )
            errors += 1
        last_submit_ts = job["job_submit_ts"]
        if job["job_end_ts"] <= job["job_submit_ts"]:
            print(
                Fore.RED
                + f"  [ERROR] Job {job['job_uid']} has an end TS lower or equal to its submit TS"
                + Style.RESET_ALL
            )
            errors += 1
        if job["real_runtime_s"] <= 0:
            print(
                Fore.RED
                + f"  [ERROR] Job {job['job_uid']} has a real runtime <= 0"
                + Style.RESET_ALL
            )
            errors += 1
        if job["real_waiting_time_s"] < 0:
            print(
                Fore.RED
                + f"  [ERROR] Job {job['job_uid']} has a real waitime < 0"
                + Style.RESET_ALL
            )
            errors += 1
        if job["real_read_bytes"] != 0 and job["real_cReadTime_s"] <= 1:
            print(
                Fore.RED
                + f"  [ERROR] Job {job['job_uid']} amount of read bytes and read time don't seem to match"
                + Style.RESET_ALL
            )
            errors += 1
        if job["real_written_bytes"] != 0 and job["real_cWriteTime_s"] <= 1:
            print(
                Fore.RED
                + f"  [ERROR] Job {job['job_uid']} amount of write bytes and write time don't seem to match"
                + Style.RESET_ALL
            )
            errors += 1
        if job["approx_cComputeTime_s"] > job["real_runtime_s"]:
            print(
                Fore.RED
                + f"  [ERROR] For job {job['job_uid']}, estimated compute time seems off."
                + Style.RESET_ALL
            )
            errors += 1
        if job["job_start_ts"] < job["job_submit_ts"]:
            print(
                Fore.RED
                + f"  [ERROR] Job {job['job_uid']} has a start TS lower than its submit TS"
                + Style.RESET_ALL
            )
            errors += 1
        if (
            abs(job["job_runtime_s"] - job["real_runtime_s"])
            > job["real_runtime_s"] / 2
        ):
            print(
                Fore.YELLOW
                + f"  [WARNING] Job {job['job_uid']} have wildly different real ({job['real_runtime_s']}) and sim ({job['job_runtime_s']}) runtimes"
                + Style.RESET_ALL
            )
            warnings += 1
        if not job["actions"]:
            print(
                Fore.RED
                + f"  [ERROR] Job {job['job_uid']} has no recorded actions"
                + Style.RESET_ALL
            )
            errors += 1

        for action in job["actions"]:
            if action["act_start_ts"] >= action["act_end_ts"]:
                print(
                    Fore.RED
                    + f"  [ERROR] In job {job['job_uid']}, action {action['act_name']} has a start TS >= to its end TS"
                    + Style.RESET_ALL
                )
                errors += 1
            if action["act_start_ts"] < job["job_start_ts"]:
                print(
                    Fore.RED
                    + f"  [ERROR] In job {job['job_uid']}, action {action['act_name']} starts before it's enclosing job"
                    + Style.RESET_ALL
                )
                errors += 1
            if action["act_end_ts"] > job["job_end_ts"]:
                print(
                    Fore.RED
                    + f"  [ERROR] In job {job['job_uid']}, action {action['act_name']} ends after it's enclosing job"
                    + Style.RESET_ALL
                )
                errors += 1
            if (
                abs(
                    action["act_duration"]
                    - (action["act_end_ts"] - action["act_start_ts"])
                )
                > 0.1
            ):
                print(
                    Fore.RED
                    + f"  [ERROR] In job {job['job_uid']}, action {action['act_name']} duration doesn't match other TS"
                    + Style.RESET_ALL
                )
                errors += 1
            if action["act_status"] != "COMPLETED":
                print(
                    Fore.RED
                    + f"  [ERROR] In job {job['job_uid']}, action {action['act_name']} did not complete"
                    + Style.RESET_ALL
                )
                errors += 1

    # Compute some values to check if the simulation was way too far off from reality or not.
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
    runtime_corr, _ = pearsonr(sim_runtime, real_runtime)
    runtime_cohen_d = cohend(sim_runtime, real_runtime)

    if abs(mean_runtime_difference) > (mean_real_runtime / 2):
        print(
            Fore.YELLOW
            + f"  [WARNING] Simulated mean runtime is off from real mean runtime by more than 50% of the real mean runtime"
            + Style.RESET_ALL
        )
        warnings += 1
    if runtime_corr < 0.95:
        print(
            Fore.YELLOW
            + f"  [WARNING] Correlation of simulated and real runtimes is awfully bad ({runtime_corr})"
            + Style.RESET_ALL
        )
        warnings += 1
    if abs(runtime_cohen_d) > 0.2:
        print(
            Fore.YELLOW
            + f"  [WARNING] The Cohen's d effect of obtaining runtime values by simulation rather than using the real values is more than 0.2 ({runtime_cohen_d})"
            + Style.RESET_ALL
        )
        warnings += 1

    io_volume_diff = []
    sim_io_volume_gb = []
    real_io_volume_gb = []

    for job in jobs:
        # Real:
        r_io_volume_gb = (
            job["real_read_bytes"] / 1_000_000_000
            + job["real_written_bytes"] / 1_000_000_000
        )
        real_io_volume_gb.append(r_io_volume_gb)
        # Simulated:
        s_io_volume_gb = 0
        for action in job["actions"]:
            if (
                action["act_type"] == "FILEREAD" or action["act_type"] == "CUSTOM"
            ) and action["act_status"] == "COMPLETED":
                s_io_volume_gb += action["io_size_bytes"] / 1_000_000_000
        sim_io_volume_gb.append(s_io_volume_gb)
        io_volume_diff.append(abs(s_io_volume_gb - r_io_volume_gb))

    mean_io_volume_sim = np.mean(sim_io_volume_gb)
    mean_io_volume_real = np.mean(real_io_volume_gb)
    mean_io_volume_difference = np.mean(io_volume_diff)
    io_vol_corr, _ = pearsonr(sim_io_volume_gb, real_io_volume_gb)
    io_vol_cohen_d = cohend(sim_io_volume_gb, real_io_volume_gb)

    if (
        abs(np.sum(sim_io_volume_gb) - np.sum(real_io_volume_gb))
        > np.sum(real_io_volume_gb) * 0.01
    ):
        print(
            Fore.RED
            + f"  [ERROR] Simulated total volume of read/written GB is off from real volume by more than 1% of the real volume"
            + Style.RESET_ALL
        )
        print(
            Fore.RED
            + f"          Sim={sim_io_volume_gb} vs Real={real_io_volume_gb}"
            + Style.RESET_ALL
        )
        errors += 1
    if abs(mean_io_volume_difference) > (mean_io_volume_real * 0.01):
        print(
            Fore.RED
            + f"  [ERROR] Simulated mean IO volume is off from real mean IO volume by more than 1% of the real mean IO vol."
            + Style.RESET_ALL
        )
        print(
            Fore.RED
            + f"          Sim={mean_io_volume_sim} vs Real={mean_io_volume_real}"
            + Style.RESET_ALL
        )
        errors += 1
    if io_vol_corr < 0.999:
        print(
            Fore.RED
            + f"  [ERROR] Correlation of simulated and real IO volumes is awfully bad ({io_vol_corr})"
            + Style.RESET_ALL
        )
        errors += 1
    if abs(io_vol_cohen_d) > 0.0001:
        print(
            Fore.RED
            + f"  [ERROR] The Cohen's d effect of obtaining IO volume values by simulation rather than using the real values is more than 0.0001 ({io_vol_cohen_d})"
            + Style.RESET_ALL
        )
        errors += 1

    if warnings != 0 or errors != 0:
        print(
            Fore.RED
            + f"  {warnings} WARNINGS and {errors} ERRORS found while checking {file_path}"
            + Style.RESET_ALL
        )
    else:
        print(
            Fore.GREEN
            + f"  No warnings or errors triggered while analysing {file_path}"
            + Style.RESET_ALL
        )

    return (warnings, errors)


def analyse_actions(file_path: pathlib.Path):
    """Analyse actions traces"""

    actions = None
    with open(file_path, "r", encoding="utf-8") as action_file:
        actions = yaml.load(action_file, Loader=yaml.CLoader)

    yaml_config = None
    with open(CONFIG_PATH, "r", encoding="utf-8") as cfg:
        yaml_config = yaml.load(cfg, Loader=yaml.FullLoader)

    action_count = len(actions)
    print(
        Fore.CYAN + f"  - Loaded dataset with {action_count} actions" + Style.RESET_ALL
    )

    errors = 0
    warnings = 0
    last_ts = 0
    last_action_name = ""
    actions_dict = {}
    storage_services = {}

    for act in actions:
        name = act["action_name"]

        if act["ts"] < last_ts:
            print(
                Fore.RED
                + f"  [ERROR] Action {name} has an incorrect / misordered TS"
                + Style.RESET_ALL
            )
            errors += 1
        if act["volume_change_bytes"] == 0:
            print(
                Fore.RED
                + f"  [ERROR] Action {name} has 0B of volume change"
                + Style.RESET_ALL
            )
            errors += 1

        # Checking all traces for a given action
        if name != last_action_name and last_action_name != "":
            nb_files = len(actions_dict[last_action_name]["files"])
            if actions_dict[last_action_name]["trace_count"] != nb_files:
                print(
                    Fore.RED
                    + f"  [ERROR] Action {last_action_name} has {actions_dict[last_action_name]['trace_count']}"
                    + f" for only {nb_files} different files"
                    + Style.RESET_ALL
                )
                errors += 1

            nb_allocations_total = sum(storage_services.values())
            if nb_allocations_total != nb_files:
                print(
                    Fore.RED
                    + f"  [ERROR] Action {last_action_name} has {nb_files} files "
                    + f" for only {nb_allocations_total} on storage services"
                    + Style.RESET_ALL
                )
                errors += 1

            if yaml_config["allocator"] == "lustre":
                if len(storage_services) != yaml_config["lustre"]["stripe_count"]:
                    print(
                        Fore.RED
                        + f"  [ERROR] Action {last_action_name} has files on "
                        + f" {len(storage_services)} different storage services, but "
                        + f" configured stripe_count was {yaml_config['lustre']['stripe_count']}"
                        + Style.RESET_ALL
                    )
                    errors += 1

            actions_dict.clear()
            storage_services.clear()

        if not name in actions_dict:
            actions_dict[name] = {
                "trace_count": 1,
                "volume_change_bytes": act["volume_change_bytes"],
                "storage_services": set(),
                "files": set(),
                "type": act["action_type"],
                "parent_job": act["action_job"],
            }
            actions_dict[name]["storage_services"].add(act["storage_service"])
            actions_dict[name]["files"].add(act["filename"])

        else:
            if act["action_type"] != actions_dict[name]["type"]:
                print(
                    Fore.RED
                    + f"  [ERROR] Action {name} is listed multiple times with different types"
                    + Style.RESET_ALL
                )
                errors += 1
            if act["volume_change_bytes"] != actions_dict[name]["volume_change_bytes"]:
                print(
                    Fore.RED
                    + f"  [ERROR] Action {name} is listed multiple times with different volume_change_bytes"
                    + Style.RESET_ALL
                )
                errors += 1
            if act["action_job"] != actions_dict[name]["parent_job"]:
                print(
                    Fore.RED
                    + f"  [ERROR] Action {name} is listed multiple times with different parent job"
                    + Style.RESET_ALL
                )
                errors += 1

            actions_dict[name]["trace_count"] += 1
            actions_dict[name]["storage_services"].add(act["storage_service"])
            actions_dict[name]["files"].add(act["filename"])

        # Update known storage services values
        if not act["storage_service"] in storage_services:
            storage_services[act["storage_service"]] = 1
        else:
            storage_services[act["storage_service"]] += 1

    return (warnings, errors)


def run():
    """
    Run full simulation and do some introspection in the result files
    """

    failed = False

    print(f"# Starting test... Opening config file {CONFIG_PATH}")
    yaml_config = None
    with open(CONFIG_PATH, "r", encoding="utf-8") as cfg:
        yaml_config = yaml.load(cfg, Loader=yaml.FullLoader)

    rand_part = "test_" + "".join(
        random.choices(
            "A,B,C,D,E,F,0,1,2,3,4,5,6,7,8,9,0".split(","),
            k=4,
        )
    )

    print(
        f"# Running storalloc simulation with data '{DATA_PATH}' and config '{CONFIG_PATH}'..."
    )
    with open(SIMULATION_LOGS, "w", encoding="utf-8") as output_file:
        completed = subprocess.run(
            [
                STORALLOC,
                CONFIG_PATH,
                DATA_PATH,
                rand_part,
                "--log=storalloc_main.threshold=debug",
                "--log=storalloc_controller.threshold=debug",
                "--log=storalloc_allocator.threshold=debug",
                "--log=wrench_core_compound_storage_system.threshold=debug",
            ],
            stdout=output_file,
            stderr=output_file,
        )

        if completed.returncode != 0:
            print(
                Fore.RED
                + f"[ERROR] Simulation did not complete. Return code = {completed.returncode}"
                + Style.RESET_ALL
            )
            return 1

        print(
            Fore.GREEN
            + f"  - [OK] Simulation with tag {rand_part} has completed with status : {completed.returncode}"
            + Style.RESET_ALL
        )

    ## Job file
    simulatedJobs_filename = f"simulatedJobs_{DATA}__{yaml_config['general']['config_name']}_{yaml_config['general']['config_version']}_{rand_part}.yml"
    print(f"# Looking for result file : {simulatedJobs_filename}")

    simJobs_file = pathlib.Path(f"./{simulatedJobs_filename}")
    if not simJobs_file.exists() or not simJobs_file.is_file():
        print(f"Result file {simulatedJobs_filename} was not found")
        return 1

    warnings, errors = analyse_jobs(simJobs_file)
    if errors:
        print(Fore.RED + f"  - [NOK]" + Style.RESET_ALL)
        failed = True
    elif warnings:
        print(Fore.YELLOW + f"  - [OK::WARN]" + Style.RESET_ALL)
    else:
        print(Fore.GREEN + f"  - [OK]" + Style.RESET_ALL)

    ## IO traces
    ioactions_filename = (
        "io_actions_ts_"
        + DATA
        + "__"
        + f"{yaml_config['general']['config_name']}_{yaml_config['general']['config_version']}_{rand_part}.yml"
    )
    print(f"# Looking for result file : {ioactions_filename}")

    ioactions_file = pathlib.Path(f"./{ioactions_filename}")
    if not ioactions_file.exists() or not ioactions_file.is_file():
        print(f"Result file {ioactions_filename} was not found")
        return 1

    warnings, errors = analyse_actions(ioactions_file)
    if errors:
        print(Fore.RED + f"  - [NOK]" + Style.RESET_ALL)
        failed = True
    elif warnings:
        print(Fore.YELLOW + f"  - [OK::WARN]" + Style.RESET_ALL)
    else:
        print(Fore.GREEN + f"  - [OK]" + Style.RESET_ALL)

    ## Storage services traces
    sstraces_filename = (
        "storage_services_operations_"
        + DATA
        + f"__{yaml_config['general']['config_name']}_{yaml_config['general']['config_version']}_"
        + rand_part
        + ".csv"
    )
    print(f"# Looking for result file : {sstraces_filename}")

    sstraces_file = pathlib.Path(f"./{sstraces_filename}")
    if not sstraces_file.exists() or not sstraces_file.is_file():
        print(f"Result file {sstraces_file} was not found")
        return 1
    print(Fore.GREEN + f"  - [OK]" + Style.RESET_ALL)

    ## CLEANUP
    del_result = subprocess.run(["rm", simJobs_file.resolve()], capture_output=True)
    if del_result.returncode != 0:
        print("job file was not deleted successfuly")
        return 1

    del_result = subprocess.run(["rm", ioactions_file.resolve()], capture_output=True)
    if del_result.returncode != 0:
        print("io actions trace file was not deleted successfuly")
        return 1

    del_result = subprocess.run(["rm", sstraces_file.resolve()], capture_output=True)
    if del_result.returncode != 0:
        print("storage service traces file was not deleted successfuly")
        return 1

    if failed:
        return 1
    return 0


if __name__ == "__main__":
    returncode = run()
    sys.exit(returncode)