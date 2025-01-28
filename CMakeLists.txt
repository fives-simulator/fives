cmake_minimum_required(VERSION 3.14)

project(fives)

# Project options
# Whether to build dependencies (simgrid, wrench, ...) inside project tree 
option(BUILD_DEPENDENCIES "Build dependencies (local to project)" OFF)
# Whether to use sustem libs (simgrid, wrench, ...) instead of local build of dependencies
option(USE_SYSTEM_LIBS "Use system libraries instead of local dependencies" OFF)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/CMakeModules")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON) # Enforce C++ 17 as strict requirement

# Make Release the default build type (set -DCMAKE_BUILD_TYPE=debug at build type to build a debug version)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Fives version
set(FIVES_VERSION_MAJOR "0")
set(FIVES_VERSION_MINOR "1")
set(FIVES_VERSION_PATCH "0")
if (${FIVES_VERSION_PATCH} EQUAL "0")
    set(FIVES_RELEASE_VERSION "${FIVES_VERSION_MAJOR}.${FIVES_VERSION_MINOR}")
else ()
    set(FIVES_RELEASE_VERSION "${FIVES_VERSION_MAJOR}.${FIVES_VERSION_MINOR}.${FIVES_VERSION_PATCH}")
endif ()

# Commit/tag version for each dependency to build from source
set(NLOHMANNJSON_GIT_REF "v3.11.3")
set(YAMLCPP_GIT_REF "0.8.0")
set(GOOGLETEST_GIT_REF "main")                  # googletest recommands to  always use the latest commit on main
set(SIMGRID_GIT_REF "v3.36")
set(FSMOD_GIT_REF "v0.2")                       # or maybe we should use 'main'
set(WRENCH_GIT_REF "origin/master")             # we should/could be using an official release, but we often need cutting edge WRENCH fixes/features

# WARNING : this is the date at CONFIG time (when 'cmake ..' is run, not when building the project)
string(TIMESTAMP TODAY "%Y%m%d")
message(STATUS "Timestamp of configuration ${TODAY}")

# Prepare and set placeholder for downloading / building / installing external dependencies such as SimGrid and WRENCH
set(EXTERNAL_DIR ${CMAKE_SOURCE_DIR}/external CACHE PATH "Directory for external dependencies")
set(LOCAL_INSTALL_DIR ${EXTERNAL_DIR}/install_deps CACHE PATH "Top level local installation directory for dependencies")
set(LOCAL_INSTALL_LIB ${LOCAL_INSTALL_DIR}/lib CACHE PATH "Local path for libraries from dependencies")
set(LOCAL_INSTALL_INCLUDE ${LOCAL_INSTALL_DIR}/include CACHE PATH "Local path for includes from dependencies")
file(MAKE_DIRECTORY ${EXTERNAL_DIR})
file(MAKE_DIRECTORY ${LOCAL_INSTALL_DIR})
file(MAKE_DIRECTORY ${LOCAL_INSTALL_LIB})
file(MAKE_DIRECTORY ${LOCAL_INSTALL_INCLUDE})

# Number of cores to use to build projects.
set(NUM_CORES "3") 

#### Setup dependencies
# -> Boost is supposed to be always installed system-wide by the user (outside of this repository),
#    because it's very heavy and not version-critical for us
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

# Fetch / Build other dependencies if required.
if (NOT USE_SYSTEM_LIBS AND BUILD_DEPENDENCIES)

    # Module includes
    include(FetchContent)
    include(ExternalProject)

    set(FETCHCONTENT_TRY_FIND_PACKAGE_MODE "NEVER")

    # nlohmann/json - no need to manually rebuild during dev, so we don't care where cmake builds and install this
    FetchContent_Declare(
        nlohmann_json
        GIT_REPOSITORY      https://github.com/nlohmann/json/
        GIT_TAG             ${NLOHMANNJSON_GIT_REF}
    )

    # jbeder/yaml-cpp - no need to manually rebuild during dev, so we don't care where cmake builds and install this
    FetchContent_Declare(
        yaml-cpp
        GIT_REPOSITORY      https://github.com/jbeder/yaml-cpp
        GIT_TAG             ${YAMLCPP_GIT_REF}
        CMAKE_ARGS          -DYAML_CPP_BUILD_TOOLS=OFF -DYAML_CPP_BUILD_CONTRIB=OFF
    )

    # Google test - no need to manually rebuild during dev, so we don't care where cmake builds and install this
    FetchContent_Declare(
        gtest
        GIT_REPOSITORY      https://github.com/google/googletest
        GIT_TAG             ${GOOGLETEST_GIT_REF}
    )
    FetchContent_MakeAvailable(nlohmann_json yaml-cpp gtest)

    # SimGrid - builds in source, as mandated by the project at this time
    ExternalProject_Add(
        simgrid
        GIT_REPOSITORY https://framagit.org/simgrid/simgrid.git
        GIT_TAG        ${SIMGRID_GIT_REF}
        GIT_SHALLOW    ON                                                   # Comment out if entire repository is required instead
        SOURCE_DIR     ${EXTERNAL_DIR}/simgrid
        INSTALL_DIR    ${LOCAL_INSTALL_DIR}
        CMAKE_ARGS     -DCMAKE_INSTALL_PREFIX=${LOCAL_INSTALL_DIR}
        BUILD_COMMAND ${CMAKE_COMMAND} --build ${EXTERNAL_DIR}/simgrid --parallel ${NUM_CORES}    # Force multicore build
        BUILD_IN_SOURCE ON
    )
    add_library(simgrid_lib SHARED IMPORTED)
    set_target_properties(simgrid_lib PROPERTIES
        IMPORTED_LOCATION ${LOCAL_INSTALL_LIB}/libsimgrid.so
        INTERFACE_INCLUDE_DIRECTORIES ${LOCAL_INSTALL_INCLUDE}
    )

    # SimGrid file-system-module (fsmod) - builds in separate `build` dir, which requires a custom CONFIGURE_COMMAND
    ExternalProject_Add(
        fsmod
        GIT_REPOSITORY https://github.com/simgrid/file-system-module
        GIT_TAG        ${FSMOD_GIT_REF}
        GIT_SHALLOW    ON                                                                   # Comment out if entire repository is required instead
        DEPENDS        simgrid
        SOURCE_DIR     ${EXTERNAL_DIR}/fsmod
        INSTALL_DIR    ${LOCAL_INSTALL_DIR}
        BINARY_DIR     ${EXTERNAL_DIR}/fsmod/build
        PATCH_COMMAND  ${CMAKE_COMMAND} -E make_directory ${EXTERNAL_DIR}/fsmod/build       # Add our build directory to the local sources
        CMAKE_ARGS     -DCMAKE_INSTALL_PREFIX=${LOCAL_INSTALL_DIR} -Denable_python=OFF
        CMAKE_ARGS     -Denable_python=OFF
        BUILD_COMMAND ${CMAKE_COMMAND} --build . --parallel ${NUM_CORES}                    # Force multicore build
    )
    add_library(fsmod_lib SHARED IMPORTED)
    set_target_properties(fsmod_lib PROPERTIES
        IMPORTED_LOCATION ${LOCAL_INSTALL_LIB}/libfsmod.so
        INTERFACE_INCLUDE_DIRECTORIES ${LOCAL_INSTALL_INCLUDE}
    )

    # WRENCH - builds in separate `build` dir, which requires a custom CONFIGURE_COMMAND
    ExternalProject_Add(
        wrench
        GIT_REPOSITORY https://github.com/wrench-project/wrench
        GIT_TAG        ${WRENCH_GIT_REF}
        GIT_SHALLOW    ON                                                                   # Comment out if entire repository is required instead
        DEPENDS        simgrid fsmod
        SOURCE_DIR     ${EXTERNAL_DIR}/wrench
        INSTALL_DIR    ${LOCAL_INSTALL_DIR}
        BINARY_DIR     ${EXTERNAL_DIR}/wrench/build
        PATCH_COMMAND  ${CMAKE_COMMAND} -E make_directory ${EXTERNAL_DIR}/wrench/build      # Add our build directory to the local sources
        CMAKE_ARGS     -DCMAKE_INSTALL_PREFIX=${LOCAL_INSTALL_DIR} -DFSMOD_PATH=${LOCAL_INSTALL_DIR} -DSimGrid_PATH=${LOCAL_INSTALL_DIR}
        CMAKE_ARGS     -DFSMOD_PATH=${LOCAL_INSTALL_DIR} -DSimGrid_PATH=${LOCAL_INSTALL_DIR}
        BUILD_COMMAND  ${CMAKE_COMMAND} --build . --parallel ${NUM_CORES}                   # Force multicore build
    )
    add_library(wrench_lib STATIC IMPORTED)
    set_target_properties(wrench_lib PROPERTIES
        IMPORTED_LOCATION ${LOCAL_INSTALL_LIB}/libwrench.a 
        INTERFACE_INCLUDE_DIRECTORIES ${LOCAL_INSTALL_INCLUDE}
    )

    set(SIMGRID_GIT_REF ${simgrid_libsimgrid_version})
    set(FSMOD_GIT_REF ${fsmod_})                       
    set(WRENCH_GIT_REF ${wrench_WRENCH_RELEASE_VERSION})
    add_custom_target(dependencies DEPENDS nlohmann_json gtest yaml-cpp simgrid_lib fsmod_lib wrench_lib)

elseif(USE_SYSTEM_LIBS)

    if (USE_SYSTEM_LIBS AND NOT BUILD_DEPENDENCIES)
        message(STATUS "USE_SYSTEM_LIBS is ON, and BUILD_DEPENDENCIES is OFF, attempting to use system libs (make sure the installed versions are correct)")
    else()
        message(WARNING "BUILD_DEPENDENCIES is ON, but USE_SYSTEM_LIBS is also ON. No dependencies will be built.")
    endif()

else()

    message(ERROR "USE_SYSTEM_LIBS and BUILD_DEPENDENCIES are OFF, we can't build Fives.")

endif() 


# Main target: Fives

# Set the list of source files
set(HEADER_FILES
    include/Controller.h
    include/Simulator.h
    include/Platform.h
    include/AllocationStrategy.h
    include/JobDefinition.h
    include/ConfigDefinition.h
    include/Utils.h
    include/Constants.h
    include/BuildInfo.h         # Generated by CMake from include/BuildInfo.h.in
)

set(SOURCE_FILES
    src/Controller.cpp
    src/Simulator.cpp
    src/Platform.cpp
    src/AllocationStrategy.cpp
    src/JobDefinition.cpp
    src/ConfigDefinition.cpp
    src/Utils.cpp
    src/main.cpp
)

set(TEST_FILES
    test/main.cpp
    test/FunctionalTest.cpp
    test/AllocationStrategyTest.cpp
    test/UtilsTest.cpp
    test/include/TestWithFork.h
    test/include/UniqueTmpPathPrefix.h
    test/include/TestConstants.h
)

# Necessary because of the 'ExternalProject'-based dependencies
include_directories(${LOCAL_INSTALL_DIR}/include)
link_directories(${LOCAL_INSTALL_DIR}/lib)

configure_file(include/BuildInfo.h.in ${CMAKE_SOURCE_DIR}/include/BuildInfo.h)

add_executable(
    fives 
    ${HEADER_FILES}
    ${SOURCE_FILES}
)

add_definitions(
    "-Wall -Wno-unused-variable -Wno-unused-private-field"
)

target_compile_options(fives PRIVATE
    $<$<CONFIG:Debug>:-g -O0>
    $<$<CONFIG:Release>:-O3>
)

# Add target libraries depending on whether we build evertyhting inside the repo or 
# using system-wide dependencies
if (USE_SYSTEM_LIBS)

    # Find SimGrid
    if (DEFINED SimGrid_PATH)
        message(STATUS "Looking for SimGrid at ${SimGrid_PATH}")
        if (NOT EXISTS "${SimGrid_PATH}")
            message(FATAL_ERROR "The specified SimGrid_PATH doesn't exit")
        else()
            file(GLOB LIBSIMGRID_FILES
                    "${SimGrid_PATH}/lib/libsimgrid.so"
                    "${SimGrid_PATH}/lib/libsimgrid.dylib"
                )
            if (NOT LIBSIMGRID_FILES)
                message(FATAL_ERROR "The specified SimGrid_PATH doesn't seem to contain a valid SimGrid installation")
            else()
                set(CMAKE_PREFIX_PATH ${SimGrid_PATH} ${CMAKE_PREFIX_PATH})
            endif()
        endif()
    endif()
    find_package(SimGrid REQUIRED)

    # Find SimGrid's FS module
    if (DEFINED FSMOD_PATH)
        message(STATUS "Looking for FSMod at ${FSMOD_PATH}")
        if (NOT EXISTS "${FSMOD_PATH}")
            message(FATAL_ERROR "The specified FSMOD_PATH doesn't exit")
        else()
            file(GLOB LIBFSMOD_FILES
                    "${FSMOD_PATH}/lib/libfsmod.so"
                    "${FSMOD_PATH}/lib/libfsmod.dylib"
                    )
            if (NOT LIBFSMOD_FILES)
                message(FATAL_ERROR "The specified FSMOD_PATH doesn't seem to contain a valid FSMod installation")
            else()
                set(CMAKE_PREFIX_PATH ${FSMOD_PATH} ${CMAKE_PREFIX_PATH})
            endif()
        endif()
    endif()
    find_package(FSMod REQUIRED)

    # Find WRENCH
    if (DEFINED WRENCH_PATH)
        message(STATUS "Looking for WRENCH at ${WRENCH_PATH}")
        if (NOT EXISTS "${WRENCH_PATH}")
            message(FATAL_ERROR "The specified WRENCH_PATH doesn't exit")
        else()
            file(GLOB LIBFSMOD_FILES
                    "${WRENCH_PATH}/lib/libwrench.a"
                    )
            if (NOT LIBFSMOD_FILES)
                message(FATAL_ERROR "The specified WRENCH_PATH doesn't seem to contain a valid WRENCH installation")
            else()
                set(CMAKE_PREFIX_PATH ${WRENCH_PATH} ${CMAKE_PREFIX_PATH})
            endif()
        endif()
    endif()
    find_package(WRENCH REQUIRED)

    find_library(YAML_CPP_LIBRARY NAMES yaml-cpp)

    target_link_libraries(fives
            ${WRENCH_LIBRARY}
            ${SimGrid_LIBRARY}
            ${YAML_CPP_LIBRARY}
            ${FSMOD_LIBRARY}
    )
    target_include_directories(fives PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        /usr/include,  
        /opt/simgrid, # suggested by simgrid as installation directory
        ${SimGrid_INCLUDE_DIR} 
        ${Boost_INCLUDE_DIR} 
        ${FSMOD_INCLUDE_DIR} 
        ${WRENCH_INCLUDE_DIR} 
        /usr/include /usr/local/include /opt/local/include
    )

else()

    target_link_libraries(fives PRIVATE
        wrench_lib
        simgrid_lib
        fsmod_lib
        yaml-cpp
    )

    target_include_directories(fives PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${LOCAL_INSTALL_DIR}/include
    )

endif()

install(TARGETS fives DESTINATION bin)


##### TEST 

# find_library(GTEST_LIBRARY NAMES gtest)
# if(GTEST_LIBRARY)

#     add_definitions(-DGTEST_USED)

#     add_executable(
#         unit_tests
#         EXCLUDE_FROM_ALL    # don't build as part of the 'all' default target
#         ${TEST_FILES}
#         ${SOURCE_FILES}
#         ${HEADER_FILES}
#     )

#     target_link_libraries(unit_tests
#         gtest
#         wrench_lib
#         simgrid_lib
#         fsmod_lib
#         yaml-cpp
#         pthread # Necessary on Debian for some reason
#     )

#   set_target_properties(unit_tests PROPERTIES COMPILE_FLAGS "-g -O0 --coverage")
#   set_target_properties(unit_tests PROPERTIES LINK_FLAGS "--coverage")
#   add_custom_command(TARGET unit_tests POST_BUILD COMMAND find . -name *.gcda -delete)

# endif()

