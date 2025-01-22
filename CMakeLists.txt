cmake_minimum_required(VERSION 3.14)
project(fives)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Module includes
include(FetchContent)
include(ExternalProject)

# Fives version
set(FIVES_VERSION_MAJOR "0")
set(FIVES_VERSION_MINOR "1")
set(FIVES_VERSION_PATCH "0")

# Commit/tag for each dependency to build from source
set(NLOHMANNJSON_GIT_REF "v3.11.3")
set(YAMLCPP_GIT_REF "0.8.0")
set(GOOGLETEST_GIT_REF "main")                  # googletest recommands to  always use the latest commit on main
set(SIMGRID_GIT_REF "v3.36")
set(FSMOD_GIT_REF "v0.2")                       # or maybe we should use 'main'
set(WRENCH_GIT_REF "v2.5")

# Prepare and set placeholder for downloading / building / installing external dependencies such as SimGrid and WRENCH
set(EXTERNAL_DIR ${CMAKE_SOURCE_DIR}/external CACHE PATH "Directory for external dependencies")
set(LOCAL_INSTALL_DIR ${EXTERNAL_DIR}/install_deps CACHE PATH "Local installation directory for dependencies")
file(MAKE_DIRECTORY ${EXTERNAL_DIR})
file(MAKE_DIRECTORY ${LOCAL_INSTALL_DIR})

# Number of cores to use to build projects.
set(NUM_CORES "5") 

#### Setup dependencies
# -> Boost is supposed to be installed system-wide by the user (outside of this repository),
#    because it's very heavy and not version-critical
# -> Similarly, nlohmann/json, yaml-cpp and GTest are added to the project via simple FetchContent
#    because we don't need to access/modify/rebuild these sources during dev
# -> Lastly, SimGrid, FSMod and WRENCH are added via ExternalProject, so that we have full control over
#    their sources and are able to update / rebuild them manually if needed.
# Note that some optional dependencies (for instance for WRENCH) are not required by this project and thus ignored.

# Find Boost (and silencing a warning coming from a deprecated FindBoost module)
if(POLICY CMP0167)
  cmake_policy(SET CMP0167 NEW)
endif()
find_package(Boost REQUIRED)

# nlohmann/json - no need to manually rebuild during dev, so we don't care where cmake builds and install this
FetchContent_Declare(
    json
    GIT_REPOSITORY      https://github.com/nlohmann/json/
    GIT_TAG             ${NLOHMANNJSON_GIT_REF}
)
FetchContent_MakeAvailable(json)

# jbeder/yaml-cpp - no need to manually rebuild during dev, so we don't care where cmake builds and install this
FetchContent_Declare(
    yaml
    GIT_REPOSITORY      https://github.com/jbeder/yaml-cpp
    GIT_TAG             ${YAMLCPP_GIT_REF}
)
FetchContent_MakeAvailable(yaml)

# Google test - no need to manually rebuild during dev, so we don't care where cmake builds and install this
FetchContent_Declare(
    gtest
    GIT_REPOSITORY      https://github.com/google/googletest
    GIT_TAG             ${GOOGLETEST_GIT_REF}
)
FetchContent_MakeAvailable(gtest)

# SimGrid - builds in source, as mandated by the project at this time
ExternalProject_Add(
    simgrid
    GIT_REPOSITORY https://framagit.org/simgrid/simgrid.git
    GIT_TAG        ${SIMGRID_GIT_REF}
    GIT_SHALLOW    ON                                                   # Comment out if entire repository is required instead
    SOURCE_DIR     ${EXTERNAL_DIR}/simgrid
    INSTALL_DIR    ${LOCAL_INSTALL_DIR}
    CMAKE_ARGS     -DCMAKE_INSTALL_PREFIX=${LOCAL_INSTALL_DIR}
    BUILD_COMMAND ${CMAKE_COMMAND} --build . --parallel ${NUM_CORES}    # Force multicore build
    BUILD_IN_SOURCE ON
)
add_library(simgrid_lib SHARED IMPORTED)
set_target_properties(simgrid_lib PROPERTIES
    IMPORTED_LOCATION ${LOCAL_INSTALL_DIR}/lib/libsimgrid.so
    INTERFACE_INCLUDE_DIRECTORIES ${LOCAL_INSTALL_DIR}/include
)

# SimGrid file-system-module (fsmod) - builds in separate `build` dir, which requires a custom CONFIGURE_COMMAND
ExternalProject_Add(
    fsmod
    GIT_REPOSITORY https://github.com/simgrid/file-system-module
    GIT_TAG        ${FSMOD_GIT_REF}
    GIT_SHALLOW    ON                                                                   # Comment out if entire repository is required instead
    SOURCE_DIR     ${EXTERNAL_DIR}/fsmod
    INSTALL_DIR    ${LOCAL_INSTALL_DIR}
    BINARY_DIR     ${EXTERNAL_DIR}/fsmod/build
    PATCH_COMMAND  ${CMAKE_COMMAND} -E make_directory ${EXTERNAL_DIR}/fsmod/build       # Add our build directory to the local sources
    CMAKE_ARGS     -DCMAKE_INSTALL_PREFIX=${LOCAL_INSTALL_DIR}
    BUILD_COMMAND ${CMAKE_COMMAND} --build . --parallel ${NUM_CORES}                    # Force multicore build
)
add_library(fsmod_lib SHARED IMPORTED)
set_target_properties(fsmod_lib PROPERTIES
    IMPORTED_LOCATION ${LOCAL_INSTALL_DIR}/lib/libfsmod.so
    INTERFACE_INCLUDE_DIRECTORIES ${LOCAL_INSTALL_DIR}/include
)

# WRENCH - builds in separate `build` dir, which requires a custom CONFIGURE_COMMAND
ExternalProject_Add(
    wrench
    GIT_REPOSITORY https://github.com/wrench-project/wrench
    GIT_TAG        ${WRENCH_GIT_REF}
    GIT_SHALLOW    ON                                                                   # Comment out if entire repository is required instead
    DEPENDS        fsmod simgrid
    SOURCE_DIR     ${EXTERNAL_DIR}/wrench
    INSTALL_DIR    ${LOCAL_INSTALL_DIR}
    BINARY_DIR     ${EXTERNAL_DIR}/wrench/build
    PATCH_COMMAND  ${CMAKE_COMMAND} -E make_directory ${EXTERNAL_DIR}/wrench/build      # Add our build directory to the local sources
    CMAKE_ARGS     -DCMAKE_INSTALL_PREFIX=${LOCAL_INSTALL_DIR} -DFSMOD_PATH=${LOCAL_INSTALL_DIR} -DSimGrid_PATH=${LOCAL_INSTALL_DIR}
    BUILD_COMMAND  ${CMAKE_COMMAND} --build . --parallel ${NUM_CORES}                   # Force multicore build
)
add_library(wrench_lib STATIC IMPORTED)
set_target_properties(wrench_lib PROPERTIES
    IMPORTED_LOCATION ${LOCAL_INSTALL_DIR}/lib/libwrench.a
    INTERFACE_INCLUDE_DIRECTORIES ${LOCAL_INSTALL_DIR}/include
)

# Specific target for dependencies, so that they can be build apart from fives (and the opposite as well)
add_custom_target(dependencies DEPENDS nlohmann_json::nlohmann_json gtest yaml-cpp simgrid fsmod wrench)

# Main target

# Set the list of source files
set(SOURCE_FILES
        include/Controller.h
        include/Simulator.h
        include/Platform.h
        include/AllocationStrategy.h
        include/JobDefinition.h
        include/ConfigDefinition.h
        include/Utils.h
        include/Constants.h
        src/Controller.cpp
        src/Simulator.cpp
        src/Platform.cpp
        src/AllocationStrategy.cpp
        src/JobDefinition.cpp
        src/ConfigDefinition.cpp
        src/Utils.cpp
        src/main.cpp
)

# Necessary because of the 'ExternalProject'-based dependencies
include_directories(${LOCAL_INSTALL_DIR}/include)
link_directories(${LOCAL_INSTALL_DIR}/lib)

add_executable(
    fives 
    ${SOURCE_FILES}
) 
set_target_properties(fives PROPERTIES COMPILE_FLAGS "-O3")
target_link_libraries(
        fives
        PRIVATE
        wrench_lib
        simgrid_lib
        fsmod_lib
        yaml-cpp
)

target_include_directories(
        fives
        PRIVATE
        ./include/
        ${LOCAL_INSTALL_DIR}/include
)

install(TARGETS fives DESTINATION bin)