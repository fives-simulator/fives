#!/usr/bin/env python3

import sys
import yaml
import drawsvg as draw
import datetime as dt
import matplotlib as mpl


WIDTH = 1800
PAD = 30
Y_OFFSET = 6


# Colors per action
C_COPY_IN = "#fcba03"
C_READ = "#38a816"
C_WRITE = "#3774c4"
C_COPY_OUT = "#6d1d8f"
C_DELETE_IN = "#ff6038"
C_DELETE_OUT = "#b52a35"

def act_to_job_scale(timestamp, global_start, global_end):
    return timestamp * ((WIDTH - 2*PAD) / (global_end - global_start)) + PAD - ((WIDTH - 2*PAD) / (global_end - global_start)) * global_start
        


def plot(job):

    rows = len(job["actions"])
    sorted_rows = sorted(job["actions"], key=lambda action: action["act_start_ts"])
    HEIGHT = (rows + 10) * Y_OFFSET

    d = draw.Drawing(WIDTH, HEIGHT, origin=(0,0))
    d.append(draw.Rectangle(0, 0, WIDTH + 2*PAD, HEIGHT, fill="#444444ff"))
        
    # Job total runtime
    d.append(draw.Lines(PAD, Y_OFFSET * 6, WIDTH - PAD, Y_OFFSET * 6, close=False, stroke="white", stroke_width=8))

    # Vertical boundaries
    d.append(draw.Lines(PAD + 1, Y_OFFSET * 10, PAD + 1, HEIGHT - Y_OFFSET, close=False, stroke="white", stroke_width=1, fill="white"))
    d.append(draw.Lines(WIDTH - PAD -1, Y_OFFSET * 10, WIDTH - PAD-1, HEIGHT - Y_OFFSET, close=False, stroke="white", stroke_width=1))

    job_sim_start = job["job_start_ts"]
    job_sim_end = job["job_end_ts"]   

    current_exec = ""

    current_y = Y_OFFSET * 10
    for action in sorted_rows:

        x1 = act_to_job_scale(action["act_start_ts"],job_sim_start, job_sim_end)
        x2 = act_to_job_scale(action["act_end_ts"], job_sim_start, job_sim_end) 

        exec_pos = action["act_name"].find("exec")
        exec_id = action["act_name"][int(exec_pos) + 4 : action["act_name"].find("_", exec_pos)]

        if exec_id != current_exec:
            current_exec = exec_id
            d.append(draw.Text(f"R#{exec_id}", font_size=12, x=x1, y=current_y - 8, font_family='Roboto', fill='#ddddddff' ))
            d.append(draw.Lines(x1, current_y - 4, x1, HEIGHT - Y_OFFSET, close=False, stroke="white", stroke_width=1, fill="white"))


        # Set colors by action
        color = "red"
        if action["act_type"] == "FILEREAD":
            color = C_READ
        elif action["act_type"] == "CUSTOM":
            color = C_WRITE
        elif action["act_type"] == "FILECOPY" and action["copy_direction"] == "css_to_sss":
            color = C_COPY_OUT    
        elif action["act_type"] == "FILECOPY" and action["copy_direction"] == "sss_to_css":
            color = C_COPY_IN 
        elif action["act_type"] == "FILEDELETE" and "output" in action["file_name"]:
            color = C_DELETE_OUT
        elif action["act_type"] == "FILEDELETE" and "nput" in action["file_name"]:
            color = C_DELETE_IN
        else:
            print(action)

        d.append(draw.Lines(x1,
                            current_y, 
                            x2, 
                            current_y, 
                            close=False, 
                            stroke=color, 
                            stroke_width=Y_OFFSET - 2, 
                            stroke_opacity=0.8))
        current_y += Y_OFFSET
    
    path = f"./sim_time_plots/{job['job_uid']}.svg"
    with open(path, "w") as plot:
        plot.write(d.as_svg())


def run(job_file: str, job_id: int):
    """Run script and output plot"""

    print(f"- Running on file : {job_file}")
    
    jobs = []
    with open(job_file, "r", encoding="utf-8") as yaml_jobs:
        jobs = yaml.load(yaml_jobs, Loader=yaml.CLoader)

    if job_id > 0:
        print(f"- Plotting job id {job_id}")
        for job in jobs:
            if job["job_uid"] == job_id:
                plot(job)
                break
        else:
            print(f"Job with ID {job_id} not found in provided job file")
    elif job_id == 0:
        print("- Plotting all jobs from file (each in their own SVG)  -- NOT IMPLEMENTED YET")
    else: 
        print(f"- Plotting {abs(job_id)} jobs at random  -- NOT IMPLEMENTED YET")


if __name__ == "__main__":
    
    if len(sys.argv) < 3:
        print(f"USAGE : {sys.argv[0]} <simulated_job_file>.yaml <job_id>")
        sys.exit(1)

    job_file = sys.argv[1]
    job_id = int(sys.argv[2])

    run(job_file, job_id)
    sys.exit(0)