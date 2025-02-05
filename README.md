# Fives

Fives is a WRENCH-based HPC storage system simulator, with a focus on studying storage allocations for HPC jobs on homogeneous or heterogeneous resources.

This is the updated, C++, version of StorAlloc, now backed by the [WRENCH](https://wrench-project.org/) and [Simgrid](https://framagit.org/simgrid/simgrid) libraries. 
If you are looking for the original Python version, it is available [here](https://github.com/hephtaicie/storalloc).

Current version is `0.1.0`.

## Summary

Fives schedules the execution of jobs from a dataset of I/O traces onto a high-level abstraction of an HPC machine. The modelled platform consists of a compute partition and a storage partition. Fives may be configured in order to customize many aspects the simulation, from the HPC platform to the jobs' I/O models and the simulated parallel file system (PFS) allocation model.

The usual input data comes from a composition of HPC Resource Manager and I/O traces (typically [Darshan](https://www.mcs.anl.gov/research/projects/darshan/) traces).
Commonly used traces include datasets from [Theta@ANL](https://reports.alcf.anl.gov/data/index.html) and [Bluewaters@NCSA](https://bluewaters.ncsa.illinois.edu/data-sets) (Darshan + Torque).

The result of a simulation is a set of timestamped execution traces, with emphasis on either the jobs execution (and their different IO or compute actions), or on the storage resources (number of allocations on each disk, used capacity, etc). Scripts for the analysis of these traces are available in the `./results` and `.bokeh_server` directories.

## Building Fives

### System dependencies

```
cmake >= 3.14
gcc/g++ >= 7.0
boost >= 1.70 (or at least v1.59)
```

Note: For some reasons, while the project should also build with clang/clang++, it seems that some the dependencies don't really like it.

### Project dependencies

There are two ways to build `Fives`. 
You may manually install the dependencies system-wide, or you can let CMake fetch and install locally all depenencies (except boost, as previously noted, which we consider to always be a system dependency).

#### Manual setup

Manual setup is mostly useful for deployment use cases. For development use cases, see the next item, *Automatic setup*.
When installing dependencies manually, you need:

```
simgrid (v3.36) @ https://framagit.org/simgrid/simgrid
fsmod (v0.2) @ https://github.com/simgrid/file-system-module
wrench (origin/master branch, or at least release v2.5) @ https://github.com/wrench-project/wrench
```

Note that **simgrid** `>= v3.36` and **fsmod** `>= v0.2` are current requirements of `WRENCH`.
In addition, **WRENCH** requires **nlohmann/json** (`>= v3.11.0` @ https://github.com/nlohmann/json)

Eventually, **Fives** also requires jbder/yaml-cpp (`>= 0.8.0` @ https://github.com/jbeder/yaml-cpp)

Once all dependencies are installed, you can use configure the project using:

```
mkdir build && cd build
cmake -DUSE_SYSTEM_LIBS=ON ..
```

If the dependencies are correctly installed, this should find all required librairies and include directories.
Then, go to the next step, `Building Fives`.

#### Automatic setup

Dependencies can be setup locally, entirely using CMake, thanks to an extensive use of `FetchContent` and `ExternalProject` modules.

The advantage of setting up **Fives** this way is that some features may rely on cutting edge / experimental versions or branches of **WRENCH** and **SimGrid**, and the development process in particular often requires to work simultaneously with **SimGrid**, **WRENCH** and **Fives** source codes.
Having all dependencies built in-project from source allows for a tighter control of the different versions that may be needed, and let us easily switch from a stable to an experimental version while making sure Fives is always built against the intended dependencies.

To build everything from source, use:

```
mkdir build && cd build
cmake -DBUILD_DEPENDENCIES=ON -DNUM_CORES=4 ..
```

**Note**: This configuration / build process mixes `FetchContent` and `ExternalProject` modules. 
This is usually a pretty bad idea: these modules don't work in the same CMake steps (`FetchContent` runs at config time, while `ExternalProject` is a build time module).
However in this case, `ExternalProject` is our best option because it lets us define custom source / build directories for **SimGrid**, **FSMod** and **WRENCH**.
Doing so, when you run the build, the `external/` directory at project root will be populated with the git clones of our three dev dependencies, and build will occur in source (or in `build/` in source).
You can include these source directories to your development workspace and cmake will rebuild the dependencies if you make any change to their sources.

**Note**: The first build will take several minutes, as both **SimGrid** and **WRENCH** are rather large projects that need to be downloaded and compiled from scracth. The `NUM_CORES` variable can be passed to CMake to define how many cores should be used in the builds (`cmake -DBUILD_DEPENDENCIES=ON -DNUM_CORES=8..` for 8 cores ; default is 3). **DO NOT** use `--parallel=X` in the next step, with `cmake --build .`, as it seems to 

### Building Fives

Building **Fives** depends on how you configured the dependencies. If you opted for manual setup, only `Fives` now needs to be compiled and linked. Otherwise, all dependencies will need to be compiled, linked and installed from scratch at least once. If you are building the dependencies inside the project and want some parallelism, use

Run:

```
cmake --build .
```

If you went for automatic setup, once the project has been built once, you can use:

```
cmake --build . --target fives  # `--target` tells cmake to not attempt to build any dependency 
```

This will skip any checks on the dependencies (which is fine as long as you do not update their sources obviously).

In any case you should end up with a `fives` binary available in `build/`.


### Running Fives

Building the project generate a `fives` binary. Usage is :

`./fives <configuration file> <job dataset> <experiment tag> [wrench / simgrid CLI options]`

E.g.:

```bash
cd build
./fives ../configs/lustre_config_hdd.yml ../data/IOJobsTest_6_LustreSim.yml test_lustre
```

You can run **Fives** without any argument to get the help message about arguments, and a version information about Fives itself, and the version of SimGrid and Wrench that were used if you chose the automatic dependencies setup.
If you used system libraries, dependencies version will just print `SYSTEM`.

Whenever in doubt, you can also use `ldd fives` to see which SimGrid and FSMod library are linked by default (WRENCH won't appear as it is static library).
Another useful check can be to look at the files `./build/CMakeFiles/fives.dir/compiler_depend.make` and `./build/compile_commands.json`, where all includes for the current build of Fives should be listed (and should be local to the project for the most part in case of automatic setup, or should rely on system libraries exclusively with the manual setup).



### Building / running tests

A specific target exists for unit tests (not part of `all` target): `unit_tests`. It is available if the `SETUP_GTEST` option is on, which is the default.
Whether you are using manual or automatic dependencies setup, run:

```
cmake --build . --target=unit_tests
```

Then, run unit tests with:

```
./unit_tests
```

Note: The full simulation test may consume a lot of RAM and run for quite a long time. In some cases you may want to skip it and run instead:

```
./unit_tests --gtest_filter="-*lustreFullSim*"
```

Note: Obviously tests need to be rebuild if Fives code is updated.