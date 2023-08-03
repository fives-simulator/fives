cmake_minimum_required(VERSION 3.20)
message(STATUS "Cmake version ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}")

project(storalloc_wrench)
set(STORALLOC_VERSION_MAJOR "0")
set(STORALLOC_VERSION_MINOR "0")
set(STORALLOC_VERSION_PATCH "2")
set(STORALLOC_VERSION_EXTRA "dev")

add_definitions("-Wall -Wno-unused-variable -Wno-unused-private-field")



set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/CMakeModules/")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
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
        src/Controller.cpp
        src/Simulator.cpp
        src/Platform.cpp
        src/AllocationStrategy.cpp
        src/JobDefinition.cpp
        src/ConfigDefinition.cpp
        src/Utils.cpp
)

# wrench library and dependencies
find_library(WRENCH_LIBRARY NAMES wrench)
find_library(YAML_CPP_LIBRARY NAMES yaml-cpp)
#find_library(WRENCH_PEGASUS_WORKFLOW_PARSER_LIBRARY NAMES wrenchpegasusworkflowparser)
#find_library(PUGIXML_LIBRARY NAMES pugixml)

# generating the executable
set(CMAKE_CXX_STANDARD 17)
add_executable(storalloc_wrench src/main.cpp ${SOURCE_FILES})

if (ENABLE_BATSCHED)
    target_link_libraries(storalloc_wrench
            ${WRENCH_LIBRARY}
#            ${WRENCH_PEGASUS_WORKFLOW_PARSER_LIBRARY}
            ${SimGrid_LIBRARY}
            ${PUGIXML_LIBRARY}
            ${YAML_CPP_LIBRARY}
            -lzmq )
else()
    target_link_libraries(storalloc_wrench
            ${WRENCH_LIBRARY}
#            ${WRENCH_PEGASUS_WORKFLOW_PARSER_LIBRARY}
            ${SimGrid_LIBRARY}
            ${PUGIXML_LIBRARY}
            ${YAML_CPP_LIBRARY}
            )
endif()

install(TARGETS storalloc_wrench DESTINATION bin)


find_library(GTEST_LIBRARY NAMES gtest)
if(GTEST_LIBRARY)

  add_definitions(-DGTEST_USED)

  add_executable(
    unit_test
    EXCLUDE_FROM_ALL
    test/main.cpp
    test/FunctionalTest.cpp
    test/AllocationStrategyTest.cpp
    test/UtilsTest.cpp
    test/include/TestWithFork.h
    test/include/UniqueTmpPathPrefix.h
    ${SOURCE_FILES}
  )
  target_link_libraries(
    unit_test
    ${GTEST_LIBRARY}
    ${WRENCH_LIBRARY} ${SimGrid_LIBRARY} ${PUGIXML_LIBRARY} ${YAML_CPP_LIBRARY}
  )

  set_target_properties(unit_test PROPERTIES COMPILE_FLAGS "-g -O0 --coverage")
  set_target_properties(unit_test PROPERTIES LINK_FLAGS "--coverage")
  add_custom_command(TARGET unit_test COMMAND find . -name *.gcda -delete)

endif()
