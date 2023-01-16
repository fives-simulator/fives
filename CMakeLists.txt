cmake_minimum_required(VERSION 3.2)
message(STATUS "Cmake version ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}")

project(storalloc_wrench)

add_definitions("-Wall -Wno-unused-variable -Wno-unused-private-field")

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/CMakeModules/")

find_package(SimGrid REQUIRED)
find_package(Boost REQUIRED)

# include directories for dependencies and WRENCH libraries
include_directories(include/ /usr/local/include /opt/local/include /usr/local/include/wrench ${SimGrid_INCLUDE_DIR} ${Boost_INCLUDE_DIR})

# source files
set(SOURCE_FILES
        include/Controller.h
        include/Platform.h
        include/JobDefinition.h
        include/ConfigDefinition.h
        src/Controller.cpp
        src/Simulator.cpp
        src/Platform.cpp
        src/JobDefinition.cpp
        src/ConfigDefinition.cpp
)

# wrench library and dependencies
find_library(WRENCH_LIBRARY NAMES wrench)
find_library(YAML_CPP_LIBRARY NAMES yaml-cpp)
#find_library(WRENCH_PEGASUS_WORKFLOW_PARSER_LIBRARY NAMES wrenchpegasusworkflowparser)
#find_library(PUGIXML_LIBRARY NAMES pugixml)

# generating the executable
add_executable(storalloc_wrench ${SOURCE_FILES})

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
