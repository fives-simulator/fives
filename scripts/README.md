# Scripts and utilities

This directory contains scripts surrounding the calibration and results analysis processes for Fives.
Note that some scripts are not used anymore but were still preserved in case they become relavant again (instead of having to explore long lost commits looking for them).

Fairly short / minimalist inventory:

- `run_calibration_parallel.py` is the most usually used calibration script. Parellelization mostly depends on your local node memory (count up to 15-20GB per simulation)
- `run_analysis.py` is the most commonly used analysis script (for the first validation step: ensuring volumes and run times are ~equal between simulation and reality and assessing whether IO times/durations are way off or not between simulation and reality)
- `build_pages.py`, along with the content of `web_template` used to be triggered by CI, after a small scale validation, as sort of regression test. It's currently broken, but could be easily fixed.
- `run_calibration_[xxx]`  (others than `parallel` version) are other calibration scripts trying to employ various other methodologies (different base configurations / set of parameters / Multi-objectives optimization / etc). **None of them are currently up to date**
- The few notebooks were primarly created with small prototyping in mind, none of them is currently useful, but some things could probably be salvaged.

## How to use?

Simlinks to the right scripts were added to the `../results` directory, and these scripts should usually be run inside `../results` (a few hardcoded paths inside...).
In particular, a straightforward calibration attempt would call `./run_calibration_parallel.py` from `../results`, after potentially updating details inside the scripts (see the constants at the beginning, to reference the dataset to be used for calibration, for instance, or the // level).
After the calibration completes, one an run Fives using the calibrated config file, and then run `./run_analysis.py` on the output of the simulation.