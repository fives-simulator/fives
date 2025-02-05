# Project and dependencies setup on G5K environement

*Grid5000 (G5K) is the INRIA's main experimental platform at the time of writing (see https://www.grid5000.fr). This procedure contains the steps taken to install and configure Fives and its dependencies on a G5K node, when used for tests and automated experiments. It could also easily be adapted to most other systems.*


### 

In this guide, we'll be using a debian system, specifically `debian11-big`, a Debian bullseye image with developement packages, system tools editors and shells (see [G5K Env](https://www.grid5000.fr/w/Generated/Environments)).


### Dependencies

#### Boost

Installed through the package manager (in our case, it is already part of `debian11-big`, but you can optionally check that it is recent enough)

```bash
apt update
apt install libboost-all-dev
```

#### Google Test

Although google test is an external dependency for dev environments, it is always integrated with our own CMake build.

Google generally recommends compiling googletest with the same compiler / compiler flags as the project being tested. System-wide installation remains an option, but it may lead to weird bugs.
In this project, we always add google test using cmake FetchContent module, as long as the 'SETUP_GTEST' option is true (default).

#### SimGrid

Tag used (v3.36) corresponds to the recommended version at the time of writing, don't forget to update according to the value used in this project's CMakeLists.txt.

```bash
git clone --depth=1 --branch v3.36 https://framagit.org/simgrid/simgrid.git
cd simgrid
cmake .
make -j8
sudo make install
```

#### FSMod

FSMod is a SimGrid module, required by WRENCH. 

```bash
git clone --depth=1 --branch=v0.2 https://github.com/simgrid/file-system-module
cd file-system-module/
mkdir build && cd build/
cmake ..
make -j8
sudo make install
```

#### nlohmann/json

Go-to C++ JSON library, required by WRENCH.
Tag used (v3.11.3) is the most recent release at the time of writing. Don't forget to update according to the value used in this project's CMakeLists.txt.

Note: nlohmann/json is a header-only library, no build is required at this step (except for tests). 

```bash
git clone  --depth=1 --branch=v3.11.3 https://github.com/nlohmann/json/
cd json
sudo cp -r single_include/nlohmann /usr/local/include/
```

#### WRENCH


```bash
git clone --depth=1 --branch=master https://github.com/wrench-project/wrench.git
cd wrench && mkdir wrench
cmake ..
make -j8
sudo make install
```

#### jbeder/yaml-cpp

```bash
git clone --depth=1 --branch=0.8.0 https://github.com/jbeder/yaml-cpp
cd yaml-cpp
mkdir build && cd build
cmake ..
make -j8
sudo make install
```

### FIVES

```bash
git clone --depth=1 --branch=master https://gitlab.inria.fr/Kerdata/Kerdata-Codes/fives.git
cd fives
mkdir build && build
cmake -DUSE_SYSTEM_LIBS=ON ..   # note the cmake option, because we installed dependencies beforehand
make -j8
make -j8 unit_tests
```


