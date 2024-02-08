# Fives

Fives is a WRENCH-based HPC storage system simulator, with a focus on studying storage allocations for HPC jobs on homogeneous or heterogeneous resources.

This is the updated, C++, version of StorAlloc, now backed by the [WRENCH](https://wrench-project.org/) and [Simgrid](https://framagit.org/simgrid/simgrid) libraries. 
If you are looking for the original Python version, it is available [here](https://github.com/hephtaicie/storalloc).

## Summary

Fives schedules the execution of jobs from a dataset of I/O traces onto a high-level abstraction of an HPC machine. The modelled platform consists of a compute partition and a storage partition. Fives may be configured in order to customize many aspects the simulation, from the HPC platform to the jobs' I/O models and the simulated parallel file system (PFS) allocation model.

The usual input data comes from a composition of HPC Resource Manager and I/O traces (typically [Darshan](https://www.mcs.anl.gov/research/projects/darshan/) traces).
Commonly used traces include datasets from [Theta@ANL](https://reports.alcf.anl.gov/data/index.html) and [Bluewaters@NCSA](https://bluewaters.ncsa.illinois.edu/data-sets) (Darshan + Torque).

The result of a simulation is a set of timestamped execution traces, with emphasis on either the jobs execution (and their different IO or compute actions), or on the storage resources (number of allocations on each disk, used capacity, etc). Scripts for the analysis of these traces are available in the `./results` and `.bokeh_server` directories.

## Building Fives

### Dependencies 

- WRENCH (07/02/24: at the moment you need to use the `storalloc` branch, but changes are periodically merged to `master`)
- yaml-cpp (https://github.com/jbeder/yaml-cpp)

### Build

```bash
mkdir build && cd build
cmake ..
make
```

## Running Fives

Building the project generate a `fives` binary. Usage is :

`./fives <configuration file> <job dataset> <experiment tag> [wrench / simgrid CLI options]`

E.g.:

```bash
cd build
./fives ../configs/lustre_config_hdd.yml ../data/IOJobsTest_6_LustreSim.yml test_lustre
```

## Tests

### Building the tests

Still inside the build directory

`make unit_tests`

### Running 

`./unit_tests`
