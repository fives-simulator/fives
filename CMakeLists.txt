cmake_minimum_required(VERSION 3.18)
message(STATUS "Cmake version ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}")

project(fives)
set(FIVES_VERSION_MAJOR "0")
set(FIVES_VERSION_MINOR "0")
set(FIVES_VERSION_PATCH "2")
set(FIVES_VERSION_EXTRA "dev")

add_definitions("-Wall -Wno-unused-variable -Wno-unused-private-field")

if (ENABLE_BATSCHED)
    add_definitions(-DENABLE_BATSCHED)
endif ()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/CMakeModules/")

# Default to release build
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

find_package(SimGrid REQUIRED)
find_package(Boost REQUIRED)

if (DEFINED FSMOD_PATH)
    if (NOT EXISTS "${FSMOD_PATH}")
        message(FATAL_ERROR "The specified FSMOD_PATH doesn't exit")
    else()
        file(GLOB LIBFSMOD_FILES
                "${FSMOD_PATH}/liblibfsmod.so"
                "${FSMOD_PATH}/lib/libfsmod.dylib"
                )
        if (NOT LIBFSMOD_FILES)
            message(FATAL_ERROR "The specified FSMOD_PATH doesn't seem to contain a valid FSMod installation")
        else()
            set(CMAKE_PREFIX_PATH ${FSMOD_PATH} ${CMAKE_PREFIX_PATH})
        endif()
    endif()
endif()
#find_package(FSMod REQUIRED)


# include directories for dependencies and WRENCH libraries
include_directories(include/ /usr/local/include /opt/local/include /opt/simgrid/include /usr/local/include/wrench ${SimGrid_INCLUDE_DIR} ${Boost_INCLUDE_DIR})

# source files
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
)

# Wrench library and dependencies
find_library(WRENCH_LIBRARY NAMES wrench)
find_library(YAML_CPP_LIBRARY NAMES yaml-cpp)
find_library(ZMQ_LIBRARY NAMES zmq)
find_library(FSMOD_LIBRARY NAMES fsmod)

# generating the executable
set(CMAKE_CXX_STANDARD 17)
add_executable(fives src/main.cpp ${SOURCE_FILES})

if (ENABLE_BATSCHED)
    target_link_libraries(fives
            ${WRENCH_LIBRARY}
            ${SimGrid_LIBRARY}
            ${ZMQ_LIBRARY}
            ${YAML_CPP_LIBRARY}
            ${FSMOD_LIBRARY}
    )
else()
    target_link_libraries(fives
            ${WRENCH_LIBRARY}
            ${SimGrid_LIBRARY}
            ${YAML_CPP_LIBRARY}
            ${FSMOD_LIBRARY}
    )
endif()

set_target_properties(fives PROPERTIES COMPILE_FLAGS "-O3")
install(TARGETS fives DESTINATION bin)


find_library(GTEST_LIBRARY NAMES gtest)
if(GTEST_LIBRARY)

  add_definitions(-DGTEST_USED)

  add_executable(
    unit_tests
    EXCLUDE_FROM_ALL
    test/main.cpp
    test/FunctionalTest.cpp
    test/AllocationStrategyTest.cpp
    test/UtilsTest.cpp
    test/include/TestWithFork.h
    test/include/UniqueTmpPathPrefix.h
    test/include/TestConstants.h
    ${SOURCE_FILES}
  )
  if (ENABLE_BATSCHED)
    target_link_libraries(unit_tests
      ${GTEST_LIBRARY}
      ${WRENCH_LIBRARY}
      ${SimGrid_LIBRARY}
      ${ZMQ_LIBRARY}
      ${YAML_CPP_LIBRARY}
      ${FSMOD_LIBRARY}
      pthread # Necessary on debian for some reason
  )
  else()
    target_link_libraries(unit_tests
      ${GTEST_LIBRARY}
      ${WRENCH_LIBRARY}
      ${SimGrid_LIBRARY}
      ${YAML_CPP_LIBRARY}
      ${FSMOD_LIBRARY}
      pthread # Necessary on Debian for some reason
    )
  endif()


  set_target_properties(unit_tests PROPERTIES COMPILE_FLAGS "-g -O0 --coverage")
  set_target_properties(unit_tests PROPERTIES LINK_FLAGS "--coverage")
  add_custom_command(TARGET unit_tests COMMAND find . -name *.gcda -delete)

endif()
