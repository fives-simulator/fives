#!/usr/bin/bash

# Run simulation analysis for 12 independent outputs of Fives, one per month, on a single job category.
# This script assumes a calibration run the month of November, that is, an input
# dataset of jobs named "theta2022_aggMonth11_cat1.yaml"

DATA_DIR=datasets_955  # update according to the experiment ID
EXP_UID=para955       # update according to the experiment ID
CALIBRATION_CAT=1
CAT=1

## Update overhead read and write values in script before running
for i in {1..12}
do
    echo "Running analysis for month $i"

    CI_PIPELINE_ID=${EXP_UID}_month$i \
        ./run_analysis.py \
${DATA_DIR}/simulatedJobs_theta2022_aggMonth${i}_cat1__Fives_C_theta2022_aggMonth11_cat${CALIBRATION_CAT}_0.0.1_month${i}.yml;
    mkdir -p ${DATA_DIR}/analysis_month${i}_cat${CAT};
    files="./${EXP_UID}_month$i*";
    mv $files ${DATA_DIR}/analysis_month${i}_cat${CAT};

done
