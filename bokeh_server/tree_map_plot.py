#!/usr/bin/env python3

""" TreeMap server for visualisation of storage state after each
    IO event (write / copy / delete) in the simulation.
"""

import random
import sys

import pandas as pd
import matplotlib

from bokeh.models import (
    HoverTool,
    Div,
)
from bokeh.plotting import figure, show
from bokeh.palettes import viridis, interp_palette, Turbo256
from bokeh.server.server import Server
from bokeh.transform import factor_cmap, linear_cmap

from squarify import normalize_sizes, squarify


X, Y, W, H = 0, 0, 1800, 900

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
    "action_name": "string",
    "storage_service_name": "string",
    "storage_hostname": "string",
    "disk_id": "string",
    "disk_capacity": "UInt64",
    "disk_file_count": "UInt64",
    "disk_free_space": "UInt64",
    "file_name": "string",
    "parts_count": "UInt64",
}

DISKS = None


def load_traces(file: str):
    """Import traces from Wrench app"""
    print(f"Loading CSV traces from file {file}")
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

def remove_stripe_part(name):
    stripeindex = name.find("_stripe")
    if stripeindex != -1:
        return name[:stripeindex]
    return name


def compute_dfs_treemap(df: pd.DataFrame):
    """Prepare generic source from dataframe"""

    traces = df[
        [
            "storage_hostname",
            "action_name",
            "disk_id",
            "disk_capacity",
            "disk_free_space",
            "disk_file_count",
            "percent_free",
            "disk_capacity_tb",
            "file_name",
        ]
    ]

    traces["file_name"] = traces["file_name"].apply(remove_stripe_part)

    traces_by_server_and_disk = traces.groupby(["storage_hostname", "disk_id"]).agg(
        disk_capacity = pd.NamedAgg(column="disk_capacity", aggfunc="first"),               # Disk capacity (fixed)
        max_fileparts_count = pd.NamedAgg(column="disk_file_count", aggfunc="max"),         # Maximum number of file parts on the disk at any time during the simulation
        min_free_space_bytes = pd.NamedAgg(column="disk_free_space", aggfunc="min"),        # Minimum amount of free bytes on disk at any time during the simulation
        min_free_space_percent = pd.NamedAgg(column="percent_free", aggfunc="min"),         # Minimum percent of free space on disk at any time during the simulation
        unique_file_count = pd.NamedAgg(column="file_name", aggfunc="nunique"),             # Total number of unique files from which at least one part/stripe have been placed on the disk
    )

    treemap_server_disk = treemap(traces_by_server_and_disk, "disk_capacity", X, Y, W, H)

    treemap_server_disk["x"] = treemap_server_disk["x"] + 5
    treemap_server_disk["y"] = treemap_server_disk["y"] + 5
    treemap_server_disk["dx"] = treemap_server_disk["dx"] - 10
    treemap_server_disk["dy"] = treemap_server_disk["dy"] - 10

    return treemap_server_disk 


def plot(trace_file):
    
    traces = load_traces(trace_file)
    ptraces = preprocess_traces(traces)

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

    blocks = compute_dfs_treemap(ptraces)

    p = figure(
        width=W+20,
        height=H+20,
        x_axis_location=None,
        y_axis_location=None,
    )
    p.x_range.range_padding = p.y_range.range_padding = 0.05
    p.grid.grid_line_color = None

    p.title = "Storage resources usage summary"
    p.title_location = "left"
    p.title.text_font_size = "25px"
    p.title.text_font_size = "25px"
    p.title.align = "right"
    p.title.background_fill_color = "darkgrey"
    p.title.text_color = "white"

    # Servers
    oss = p.block(
        "x",
        "y",
        "dx",
        "dy",
        source=blocks,
        line_width=4,
        line_color=factor_cmap("storage_hostname", interp_palette(Turbo256, NB_SERVERS), SERVER_NAMES),
        fill_alpha=0.7,
        fill_color=linear_cmap(
            "min_free_space_percent",
            hex_color_map,
            1,
            99.9,
            high_color="white",
            low_color="black",
        ),
    )

    hover = HoverTool(
        name="ytd_ave",
        tooltips=[
            ("Storage service", "@storage_hostname"),
            ("Disk ID", "@disk_id"),
            ("Disk total capacity", "@disk_capacity"),
            ("Min Free space (%)", "@min_free_space_percent"),
            ("Min Free space (Bytes)", "@min_free_space_bytes"),
            ("Unique files count", "@unique_file_count"),
        ],
    )
    hover.renderers = [oss]
    p.add_tools(hover)

    show(p)


if __name__ == "__main__":
    
    if len(sys.argv) < 2:
        print("Missing path of CSV file for plotting")
        sys.exit(1)

    plot(sys.argv[1])
