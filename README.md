https://gitlab.inria.fr/jmonniot/storalloc_wrench/badges/master/pipeline.svg

# StorAlloc

This is the updated, C++, version of StorAlloc, now backed by the [WRENCH library](https://wrench-project.org/). If you are looking for the original Python version, it is available [here](https://github.com/hephtaicie/storalloc).

## TL;DR

StorAlloc is a simulator with a focus on studying storage allocations for HPC jobs on heterogenous resources.

The usual input data is [Darshan](https://www.mcs.anl.gov/research/projects/darshan/) traces, enriched with some standard informations extracted from related resource manager traces.
For instance, we use traces from [Theta@ANL](https://reports.alcf.anl.gov/data/index.html) traces (Darshan + Cobalt) or [Bluewaters@NCSA](https://bluewaters.ncsa.illinois.edu/data-sets) (Darshan + Torque).

StorAlloc replays the execution of jobs from the input dataset on a high-level abstraction of an HPC machine. It has options to customize many aspects of the HPC platform (thanks to the programmatic platforms offered by [Simgrid](https://simgrid.org/doc/latest/Platform_cpp.html)), but the main interest is directed towards representing storage resources. In that regard, the configuration file allows to easily describe multple kinds of disks and multiple kinds of storage nodes which use one or many different disks.

The result of a simulation is a set of timestamped execution traces, with emphasis on either the jobs execution (and their different IO or compute actions), or on the storage resources (number of allocations on each disk, used capacity, etc). Scripts (wip) for the analysis of these traces are available in the `./results` and `.bokeh_server` directories.
 
## Build

### Dependencies 

- WRENCH (20/10/23: at the moment you need to use the `storalloc` branch, but changes are periodically merged to `master`)
- yaml-cpp (https://github.com/jbeder/yaml-cpp)

### Build

Nothing fancy.

```bash
mkdir build && cd build
cmake ..
make -j 8
```

## Run

Building the project generate a `storalloc_wrench` binary. Usage is :

`./storalloc_wrench <configuration> <job dataset> [<tag included in the result files name>]`

For instance

```bash
cd build
./storalloc_wrench ../configs/lustre_config_hdd.yml ../data/IOJobsTest_6_LustreSim.yml"
```

## Tests

### Building the tests

Inside the build directory

`make -j8 unit_tests`

### Running 

`./unit_tests`
