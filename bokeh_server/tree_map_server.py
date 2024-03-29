#!/usr/bin/env python3

""" TreeMap server for visualisation of storage state after each
    IO event (write / copy / delete) in the simulation.
"""

import random
import sys

import pandas as pd
import matplotlib

from bokeh.layouts import column
from bokeh.models import (
    ColumnDataSource,
    Slider,
    HoverTool,
    Div,
)
from bokeh.plotting import figure
from bokeh.palettes import viridis
from bokeh.server.server import Server
from bokeh.transform import factor_cmap, linear_cmap

from squarify import normalize_sizes, squarify


X, Y, W, H = 0, 0, 1800, 900
INPUTFILE = "timestamped_io_operations.csv"

ACTIONS_TYPE_TO_STRING = {
    1: "Read - Start",
    2: "Read - End",
    3: "Write - Start",
    4: "Write - End",
    5: "CopyToCSS - Start",
    6: "CopyToCSS - End",
    7: "CopyFromCSS - Start",
    8: "CopyFromCss- End",
    9: "Delete - Start",
    10: "Delete - End",
    11: "Simulation Start",
}

CUSTOM_DTYPES = {
    "ts": "Float64",
    "storage_service_name": "string",
    "storage_hostname": "string",
    "disk_id": "string",
    "disk_capacity": "UInt64",
    "disk_free_space": "UInt64",
    "file_name": "string",
}

DISKS = None
CURRENT_INDEX = 0


def load_traces(file: str = INPUTFILE):
    """Import traces from Wrench app"""

    if len(sys.argv) == 2:
        file = sys.argv[1]

    ts_traces = pd.read_csv(file, sep=",", header=0, dtype=CUSTOM_DTYPES)
    return ts_traces


def preprocess_traces(traces: pd.DataFrame):
    """One-shot pre-processing on imported traces"""

    # Make sure traces are ordered by timestamp
    ts_traces = traces.sort_values(by="ts")
    # Compute percentage of free space for each disk at every timestep
    ts_traces["percent_free"] = ts_traces["disk_free_space"].mul(100)
    ts_traces["percent_free"] = ts_traces["percent_free"] / ts_traces["disk_capacity"]
    # Add a column with capcity in TB
    ts_traces["disk_capacity_tb"] = (
        ts_traces["disk_capacity"] / 1000 / 1000 / 1000 / 1000
    ).round(decimals=2)

    return ts_traces


def treemap(df, norm_column, x_coord, y_coord, delta_x, delta_y):
    """Compute blocks coordinates for the treemap"""
    sub_df = df.copy()  # nlargest(N, col)
    normed = normalize_sizes(sub_df[norm_column], delta_x, delta_y)
    blocks = squarify(normed, x_coord, y_coord, delta_x, delta_y)
    blocks_df = pd.DataFrame.from_dict(blocks).set_index(sub_df.index)
    return sub_df.join(blocks_df, how="left").reset_index()


def compute_dfs_treemap(df: pd.DataFrame, ts_index: int):
    """Prepare generic source from dataframe"""

    updt_traces = df[df["ts"] == ts_index]
    updt_traces = updt_traces[
        [
            "storage_hostname",
            "action_name",
            "disk_id",
            "disk_capacity",
            "disk_free_space",
            "percent_free",
            "disk_capacity_tb",
            "file_name",
        ]
    ]
    updt_traces_by_server = (
        updt_traces[["storage_hostname", "disk_id", "disk_capacity"]]
        .groupby("storage_hostname")
        .sum("disk_capacity")
        .sort_values(["storage_hostname"])
    )

    blocks_by_server = treemap(updt_traces_by_server, "disk_capacity", X, Y, W, H)

    dfs = []
    for _, (storage_server, _, x, y, dx, dy) in blocks_by_server.iterrows():
        df = updt_traces[updt_traces.storage_hostname == storage_server]
        df = df.sort_values(["disk_capacity"])
        dfs.append(treemap(df, "disk_capacity", x, y, dx, dy))
    blocks = pd.concat(dfs)
    blocks["ytop"] = blocks.y + blocks.dy

    internal_blocks = blocks.copy()
    internal_blocks["x"] = internal_blocks["x"] + 5
    internal_blocks["y"] = internal_blocks["y"] + 5
    internal_blocks["dx"] = internal_blocks["dx"] - 10
    internal_blocks["dy"] = internal_blocks["dy"] - 10

    internal_blocks = internal_blocks.sort_values(
        ["storage_hostname", "disk_id", "file_name"]
    )
    internal_blocks = internal_blocks.reset_index(drop=True)
    internal_blocks = internal_blocks.drop("index", axis=1)

    return (blocks_by_server, internal_blocks)


def update_file_name(filename: str, previous_filename: str, action: int):
    ret = ""
    if previous_filename == "NoFile" or previous_filename == "":
        ret = filename
    elif action in [4, 9]:
        ret = previous_filename  # No update of the file name on write - end and delete -start
    elif previous_filename == filename and action != 10:
        print(f"Filename {filename} unchanged")
        ret = filename
    elif action == 10:  # delete
        print("In delete action")
        found = previous_filename.find(filename)
        if found == -1:
            print(f"Filename {filename} not found")
            print(f"Previous filename : {previous_filename}")
            ret = previous_filename
        else:
            print(f"Filename {filename} found")
            print(f"Previous filename : {previous_filename}")
            ret = (
                previous_filename[0:found] + previous_filename[found + len(filename) :]
            )
    elif action in [1, 2, 7, 8]:  # no new file on CSS, do not update filename
        print(f"No update (action 1, 2, 7, 8)")
        ret = previous_filename
    else:
        print(f"Appending filename {filename}")
        ret = previous_filename + ", " + filename

    print(f"# New filename : {ret}")
    return ret


def bkapp(doc):
    traces = load_traces()
    ptraces = preprocess_traces(traces)

    # List unique TS in the traces
    UNIQUE_TS = pd.Series(ptraces["ts"].unique()).sort_values()

    # Get the total number of ts
    ptraces_by_ts = ptraces.groupby("ts")
    keys = [key for key, _ in ptraces_by_ts]
    TRACES_COUNT = len(keys)

    # Get the total number of server names and their list
    ptraces_by_hostname = ptraces.groupby("storage_hostname")
    SERVER_NAMES = [key for key, _ in ptraces_by_hostname]
    NB_SERVERS = len(SERVER_NAMES)

    # Get a color map with 100 colors ranging from green to red
    colors = ["#3aeb34", "#ffba26", "#ff2a26"][::-1]
    cmap = matplotlib.colors.LinearSegmentedColormap.from_list(
        "cmap_green_red", colors, N=98
    )
    hex_color_map = [matplotlib.colors.rgb2hex(cmap(i)) for i in range(cmap.N)]

    global DISKS
    servers, DISKS = compute_dfs_treemap(ptraces, UNIQUE_TS[0])
    servers_source = ColumnDataSource(servers)
    disks_source = ColumnDataSource(DISKS)

    p = figure(
        width=W,
        height=H,
        x_axis_location=None,
        y_axis_location=None,
    )
    p.x_range.range_padding = p.y_range.range_padding = 0
    p.grid.grid_line_color = None

    p.title = "View of Storage Resources Usage"
    p.title_location = "left"
    p.title.text_font_size = "25px"
    p.title.text_font_size = "25px"
    p.title.align = "right"
    p.title.background_fill_color = "darkgrey"
    p.title.text_color = "white"

    # Servers
    fill_colormap = list(viridis(NB_SERVERS))
    random.shuffle(fill_colormap)
    b = p.block(
        "x",
        "y",
        "dx",
        "dy",
        source=servers_source,
        line_width=2,
        line_color="black",
        fill_alpha=1,
        fill_color=factor_cmap("storage_hostname", "Turbo256", SERVER_NAMES),
    )

    # Disks
    disk_blk = p.block(
        "x",
        "y",
        "dx",
        "dy",
        source=disks_source,
        line_width=1,
        line_color="black",
        fill_alpha=0.7,
        fill_color=linear_cmap(
            "percent_free",
            hex_color_map,
            1,
            99.9,
            high_color="white",
            low_color="black",
        ),
    )
    # p.text('x', 'ytop', x_offset=2, y_offset=5, text="disk_id", source=disks_source,
    # text_font_size="8pt", text_baseline="top")
    # capa_text = p.text('x', 'ytop', x_offset=2, y_offset=20, text="disk_capacity_tb", source=disks_source,
    # text_font_size="10pt", text_baseline="top", text_align="left")

    #
    # p.text('x', 'y', x_offset=2, text="storage_hostname", source=servers_source,
    #       text_font_size="14pt", text_color="white")

    hover = HoverTool(
        name="ytd_ave",
        tooltips=[
            ("Storage service", "@storage_hostname"),
            ("Disk", "@disk_id"),
            ("Free space (%)", "@percent_free"),
            ("Free space (Bytes)", "@disk_free_space"),
            ("Latest File part(s)", "@{file_name}"),
            ("Latest Action", "@action_name"),
        ],
    )
    hover.renderers = [disk_blk]
    p.add_tools(hover)

    # Slider for trace id control
    trace_id = Slider(
        title="trace", value=0, start=0, end=(TRACES_COUNT - 1), step=1, width=1800
    )
    plain_text = Div(text=f"{disks_source}")

    def update_data(attrname, old, new):
        global CURRENT_INDEX

        # Get the new slider value
        new_index = trace_id.value

        start_index, end_index = 0, new_index + 1
        if new_index > CURRENT_INDEX:
            start_index = CURRENT_INDEX + 1

        for idx in range(start_index, end_index):
            updt_traces = ptraces[ptraces["ts"] == UNIQUE_TS[idx]]
            updt_traces = updt_traces[
                [
                    "storage_hostname",
                    "action_name",
                    "disk_id",
                    "disk_capacity",
                    "disk_free_space",
                    "percent_free",
                    "disk_capacity_tb",
                    "file_name",
                ]
            ].sort_values(["storage_hostname", "disk_id", "file_name"])
            updt_traces = updt_traces.reset_index()
            updt_traces = updt_traces.drop("index", axis=1)
            updt_traces = updt_traces.set_index(keys=["storage_hostname", "disk_id"])
            updt_traces = updt_traces.groupby(["storage_hostname", "disk_id"]).agg(
                {
                    "disk_capacity": "first",
                    "action_name": "first",
                    "disk_free_space": "first",
                    "percent_free": "first",
                    "disk_capacity_tb": "first",
                    "disk_capacity": "first",
                    "file_name": ", ".join,
                }
            )

            action_type = updt_traces["action_name"][0]

            global DISKS
            DISKS = DISKS.set_index(keys=["storage_hostname", "disk_id"])
            DISKS.update(updt_traces)
            """
            temp_file_name = DISKS["file_name"].copy(deep=True)
            DISKS.update(updt_traces)
            DISKS["previous_file_name"] = temp_file_name
            DISKS["file_name"] = DISKS.apply(
                lambda x: update_file_name(
                    x.file_name, x.previous_file_name, action_type
                ),
                axis=1,
            )
            """
            DISKS = DISKS.reset_index()
            disks_source.data = DISKS

        plain_text.text = f"Current TS: {UNIQUE_TS[new_index]} - Action type: {ACTIONS_TYPE_TO_STRING[action_type]}"
        CURRENT_INDEX = new_index

    trace_id.on_change("value", update_data)

    doc.add_root(column(p, column(trace_id, plain_text)))


server = Server({"/": bkapp}, num_procs=1)
server.start()

if __name__ == "__main__":
    print("Starting Bokeh Application on 'http://localhost:5006/'")
    server.io_loop.add_callback(server.show, "/")
    server.io_loop.start()
