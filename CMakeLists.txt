cmake_minimum_required(VERSION 3.18)
message(STATUS "Cmake version ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}")

project(storalloc_wrench)
set(STORALLOC_VERSION_MAJOR "0")
set(STORALLOC_VERSION_MINOR "0")
set(STORALLOC_VERSION_PATCH "2")
set(STORALLOC_VERSION_EXTRA "dev")

add_definitions("-Wall -Wno-unused-variable -Wno-unused-private-field")

if (ENABLE_BATSCHED)
    add_definitions(-DENABLE_BATSCHED)
endif ()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/CMakeModules/")

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

find_package(SimGrid REQUIRED)
find_package(Boost REQUIRED)

# include directories for dependencies and WRENCH libraries
include_directories(include/ /usr/local/include /opt/local/include /opt/local/include/simgrid /usr/local/include/wrench ${SimGrid_INCLUDE_DIR} ${Boost_INCLUDE_DIR})

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

# generating the executable
set(CMAKE_CXX_STANDARD 17)
add_executable(storalloc_wrench src/main.cpp ${SOURCE_FILES})

if (ENABLE_BATSCHED)
    target_link_libraries(storalloc_wrench
            ${WRENCH_LIBRARY}
            ${SimGrid_LIBRARY}
            ${ZMQ_LIBRARY}
            ${YAML_CPP_LIBRARY}
    )
else()
    target_link_libraries(storalloc_wrench
            ${WRENCH_LIBRARY}
            ${SimGrid_LIBRARY}
            ${YAML_CPP_LIBRARY}
    )
endif()

set_target_properties(storalloc_wrench PROPERTIES COMPILE_FLAGS "-pg -O3")
install(TARGETS storalloc_wrench DESTINATION bin)


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
  )
  else()
    target_link_libraries(unit_tests
      ${GTEST_LIBRARY}
      ${WRENCH_LIBRARY}
      ${SimGrid_LIBRARY}
      ${YAML_CPP_LIBRARY}
    )
  endif()


  set_target_properties(unit_tests PROPERTIES COMPILE_FLAGS "-g -O0 --coverage")
  set_target_properties(unit_tests PROPERTIES LINK_FLAGS "--coverage")
  add_custom_command(TARGET unit_tests COMMAND find . -name *.gcda -delete)

endif()
