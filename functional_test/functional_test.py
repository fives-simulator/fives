#!/usr/bin/env python3

"""
    Storalloc-Wrench
    Functional testing

    This script expects to find the `storalloc_wrench` binary in
    <repository_root>/build/, data file in <repository_root>/data
    and config files in <repository_root>/configs. It should be run
    from the functionnal_test directory
"""

import subprocess
import pathlib
import random
import yaml

BASE_PATH = ".."

STORALLOC = f"{BASE_PATH}/build/storalloc_wrench"
CONFIG = "theta_config.yml"
CONFIG_PATH = f"{BASE_PATH}/configs/{CONFIG}"
DATA = "theta2022_week4_trunc"
DATA_PATH = f"{BASE_PATH}/data/test_data/{DATA}.yaml"


def run():
    """
    Run full simulation and do some introspection in the result files
    """
    yaml_config = None
    with open(CONFIG_PATH, "r", encoding="utf-8") as cfg:
        yaml_config = yaml.load(cfg, Loader=yaml.FullLoader)

    rand_part = "test_" + "".join(
        random.choices(
            "A,B,C,D,E,F,0,1,2,3,4,5,6,7,8,9,0".split(","),
            k=4,
        )
    )

    completed = subprocess.run(
        [STORALLOC, CONFIG_PATH, DATA_PATH, rand_part, "--wrench-full-log"]
    )
    print(
        f"Simulation with tag {rand_part} has completed with status : {completed.returncode}"
    )
    if completed.returncode != 0:
        print("ERROR : Simulation did not complete")
        return 1

    ## Job file
    simulatedJobs_filename = f"simulatedJobs_{DATA}__{yaml_config['general']['config_name']}_{yaml_config['general']['config_version']}_{rand_part}.yml"
    print(f"Now looking for result file : {simulatedJobs_filename}")

    simJobs_file = pathlib.Path(f"./{simulatedJobs_filename}")
    if not simJobs_file.exists() or not simJobs_file.is_file():
        print(f"Result file {simulatedJobs_filename} was not found")
        return 1

    ## IO traces
    ioactions_filename = (
        "io_actions_ts_"
        + DATA
        + "__"
        + f"{yaml_config['general']['config_name']}_{yaml_config['general']['config_version']}_{rand_part}.yml"
    )
    print(f"Now looking for result file : {ioactions_filename}")

    ioactions_file = pathlib.Path(f"./{ioactions_filename}")
    if not ioactions_file.exists() or not ioactions_file.is_file():
        print(f"Result file {ioactions_filename} was not found")
        return 1

    ## Storage services traces
    sstraces_filename = (
        "storage_services_operations_"
        + DATA
        + f"__{yaml_config['general']['config_name']}_{yaml_config['general']['config_version']}_"
        + rand_part
        + ".csv"
    )
    print(f"Now looking for result file : {sstraces_filename}")

    sstraces_file = pathlib.Path(f"./{sstraces_filename}")
    if not sstraces_file.exists() or not sstraces_file.is_file():
        print(f"Result file {sstraces_file} was not found")
        return 1


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


    return 0


if __name__ == "__main__":
    run()
