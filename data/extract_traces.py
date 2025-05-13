#!/usr/bin/env python3

"""
    Extract part of a Darshan trace dataset and output a YAML file with relevant data for Fives
    ---
    KerData - INRIA RBA
"""

import sys
import argparse
import datetime as dt
import logging
import calendar
import yaml
import pandas as pd

pd.options.mode.chained_assignment = None  # default='warn'


DARSHAN_COLUMNS = [
    "COBALT_JOBID",
    "RUN_DATE_ID",
    "USER_ID",
    "EXE_NAME_GENID",
    "QUEUED_TIMESTAMP",
    "START_TIMESTAMP",
    "END_TIMESTAMP",
    "NODES_USED",
    "CORES_USED",
    "NPROCS",
    "RUN_TIME",
    "TOTAL_POSIX_OPENS",
    "RUNTIME_SECONDS",
    "TOTAL_POSIX_READS",
    "TOTAL_POSIX_WRITES",
    "TOTAL_POSIX_BYTES_READ",
    "TOTAL_POSIX_BYTES_WRITTEN",
    "TOTAL_POSIX_F_READ_TIME",
    "TOTAL_POSIX_F_WRITE_TIME",
    "TOTAL_POSIX_F_META_TIME",
    "TOTAL_MPIIO_INDEP_READS",
    "TOTAL_MPIIO_INDEP_WRITES",
    "TOTAL_MPIIO_COLL_READS",
    "TOTAL_MPIIO_COLL_WRITES",
    "TOTAL_MPIIO_NB_READS",
    "TOTAL_MPIIO_NB_WRITES",
    "TOTAL_MPIIO_BYTES_READ",
    "TOTAL_MPIIO_BYTES_WRITTEN",
    "TOTAL_MPIIO_F_READ_TIME",
    "TOTAL_MPIIO_F_WRITE_TIME",
    "TOTAL_MPIIO_F_META_TIME",
    "TOTAL_STDIO_OPENS",
    "TOTAL_STDIO_READS",
    "TOTAL_STDIO_WRITES",
    "TOTAL_STDIO_BYTES_READ",
    "TOTAL_STDIO_BYTES_WRITTEN",
    "TOTAL_STDIO_F_READ_TIME",
    "TOTAL_STDIO_F_WRITE_TIME",
    "TOTAL_STDIO_F_META_TIME",
]


def parse_args():
    """Parse command line arguments :
    * -d for the main Darshan traces file
    * -c for the related composite file
    * -m to extract only the data related to a month in particular
    * -v for debug informations
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--darshan", help="Path of the Darshan data file")
    parser.add_argument("-c", "--composite", help="Path of the Composite data file")
    parser.add_argument(
        "-m", "--month", help="Specify a specific month to extract ([1,12])"
    )
    parser.add_argument(
        "-v", "--verbose", help="Display debug information", action="store_true"
    )

    args = parser.parse_args()

    if not args.darshan or not args.composite:
        parser.print_usage()
        print("Error: arguments --darshan (-d) and --composite (-c) are mandatory!")
        sys.exit(1)
    else:
        data_file_darshan = args.darshan
        data_file_composite = args.composite

    if args.month:
        try:
            month = int(args.month)
            if month < 1 or month > 12:
                raise RuntimeError("Month should be between 1 and 12")
        except ValueError:
            parser.print_usage()
            print(
                "Error: arguments --month (-m) must be an integer in the range [1,12]!"
            )
            sys.exit(1)
    else:
        month = 0

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="[D] %(message)s")

    return (data_file_darshan, data_file_composite, month)


def main():
    """Main parsing"""

    data_file_darshan, data_file_composite, month = parse_args()

    traces_darshan = pd.read_csv(data_file_darshan, low_memory=False)
    traces_composite = pd.read_csv(data_file_composite, low_memory=False)

    # Left joint the two dataframes
    traces = traces_darshan.merge(traces_composite, on="COBALT_JOBID", how="left")

    # Filter data: keep a subset of useful columns and remove empty rows
    filtered_cols = traces.columns.to_list()[11:186]
    non_empty_rows = (
        traces[filtered_cols].apply(pd.to_numeric).any(axis="columns").to_list()
    )
    subset_traces = traces[non_empty_rows]

    # Filter and keep relevant data
    workload_traces = subset_traces[DARSHAN_COLUMNS]
    # Reformat timestamps and dates
    workload_traces.RUN_DATE_ID = pd.to_datetime(
        workload_traces.RUN_DATE_ID, format="%Y%m%d"
    )
    workload_traces.QUEUED_TIMESTAMP = pd.to_datetime(
        workload_traces.QUEUED_TIMESTAMP, format="%Y-%m-%d %H:%M:%S"
    )
    workload_traces.START_TIMESTAMP = pd.to_datetime(
        workload_traces.START_TIMESTAMP, format="%Y-%m-%d %H:%M:%S"
    )
    workload_traces.END_TIMESTAMP = pd.to_datetime(
        workload_traces.END_TIMESTAMP, format="%Y-%m-%d %H:%M:%S"
    )

    """
    diff = workload_traces.query("RUN_TIME != RUNTIME_SECONDS")
    diff["COMPUTED_RUNTIME"] = diff["END_TIMESTAMP"] - diff["START_TIMESTAMP"]
    print(
        diff[
            [
                "RUN_TIME",
                "RUNTIME_SECONDS",
                "COMPUTED_RUNTIME",
                "START_TIMESTAMP",
                "END_TIMESTAMP",
            ]
        ]
    )
    return 0
    """

    # Filter on month, if necessary
    if month != 0:
        current_traces = workload_traces[
            (workload_traces["RUN_DATE_ID"].dt.month == month)
        ]
    else:
        current_traces = workload_traces

    # Keep only relevant IO DATA (not sure why we're doing this)
    io_data = current_traces[
        [
            "COBALT_JOBID",
            "NPROCS",
            "QUEUED_TIMESTAMP",
            "START_TIMESTAMP",
            "END_TIMESTAMP",
            "NODES_USED",
            "CORES_USED",
            "TOTAL_MPIIO_BYTES_WRITTEN",
            "TOTAL_MPIIO_BYTES_READ",
            "TOTAL_MPIIO_F_WRITE_TIME",
            "RUNTIME_SECONDS",  # USED TO BE RUN_TIME
        ]
    ]

    # Add custom metrics
    io_data.loc[:, ("RatioWriteTimeRunTime")] = (
        io_data.TOTAL_MPIIO_F_WRITE_TIME / io_data.NPROCS
    ) / io_data.RUNTIME_SECONDS  # Used to be RUN_TIME
    io_data.loc[:, ("WAITING_TIME")] = (
        io_data.START_TIMESTAMP - io_data.QUEUED_TIMESTAMP
    )

    print(str(io_data.shape[0]) + " x " + str(io_data.shape[1]))
    # Select I/O intensive jobs
    #   - 10% of the time spent writing data or
    #   - At least 10GB read or written
    io_jobs = io_data.loc[
        (io_data["RatioWriteTimeRunTime"] >= 0.1)
        | (io_data["TOTAL_MPIIO_BYTES_WRITTEN"] >= 10000000000)
        | (io_data["TOTAL_MPIIO_BYTES_READ"] >= 10000000000)
    ]

    # Sort by queued TS, and calculate diff between consecutive jobs
    io_jobs = io_jobs.sort_values("QUEUED_TIMESTAMP", axis=0)
    io_jobs["SLEEP_BETWEEN_JOBS"] = io_jobs["QUEUED_TIMESTAMP"].diff()
    io_jobs["SLEEP_BETWEEN_JOBS"] = io_jobs["SLEEP_BETWEEN_JOBS"].fillna(
        dt.timedelta(seconds=0)
    )

    starting_date = io_jobs["QUEUED_TIMESTAMP"].iloc[0]
    print(starting_date)
    print(starting_date.timestamp())
    starting_ts = dt.timedelta(seconds=starting_date.timestamp())

    io_jobs.iloc[0, io_jobs.columns.get_loc("SLEEP_BETWEEN_JOBS")] = starting_ts

    io_jobs["DUPLICATED_ID"] = io_jobs.duplicated(keep=False, subset=["COBALT_JOBID"])
    # print(duplicated_job_id_idx.head())

    class AddtoUnique:
        """Dirty class to re-index jobs which share the same ID"""

        idx = 0

        @staticmethod
        def index(uid, cond):
            """Return the string with an added suffix if needed"""
            if cond:
                AddtoUnique.idx = (AddtoUnique.idx + 1) % 1000
                return str(uid) + "-" + str(AddtoUnique.idx)
            return str(uid) + "-0"

    io_jobs["COBALT_JOBID"] = io_jobs.apply(
        lambda x: AddtoUnique.index(x["COBALT_JOBID"], x["DUPLICATED_ID"]), axis=1
    )

    print(str(io_jobs.shape[0]) + " x " + str(io_jobs.shape[1]))

    d_io_jobs = {"jobs": []}

    for _, row in io_jobs.iterrows():
        d_io_jobs["jobs"].append(
            {
                "id": str(row["COBALT_JOBID"]),
                "MPIprocs": int(row["NPROCS"]),
                "submissionTime": row["QUEUED_TIMESTAMP"].strftime("%Y-%m-%d %H:%M:%S"),
                "startTime": row["START_TIMESTAMP"].strftime("%Y-%m-%d %H:%M:%S"),
                "endTime": row["END_TIMESTAMP"].strftime("%Y-%m-%d %H:%M:%S"),
                "waitingTime": str(row["WAITING_TIME"]),
                "sleep_simulation": int(row["SLEEP_BETWEEN_JOBS"].total_seconds()),
                "nodesUsed": int(row["NODES_USED"]),
                "coresUsed": int(row["CORES_USED"]),
                "writtenBytes": int(row["TOTAL_MPIIO_BYTES_WRITTEN"]),
                "readBytes": int(row["TOTAL_MPIIO_BYTES_READ"]),
                "runTime": int(row["RUNTIME_SECONDS"]),  # USED TO BE RUN_TIME
            }
        )

    with open(
        "IOJobs" + calendar.month_abbr[month] + ".yml", "w", encoding="utf-8"
    ) as yamlfile:
        yaml.dump(d_io_jobs, yamlfile)
        print(
            calendar.month_name[month]
            + " IO intensive jobs extracted successfully to IOJobs"
            + calendar.month_abbr[month]
            + ".yml"
        )


if __name__ == "__main__":

    main()
