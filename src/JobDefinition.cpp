#include "JobDefinition.h"

#include "yaml-cpp/yaml.h"
#include <iostream>
#include <string>
#include <wrench-dev.h>

WRENCH_LOG_CATEGORY(Fives_jobs, "Log category for Fives Job parser");

YAML::Node YAML::convert<fives::DarshanRecord>::encode(const fives::DarshanRecord &rhs) {
    YAML::Node node;

    node.push_back(rhs.id);
    node.push_back(rhs.readBytes);
    node.push_back(rhs.writtenBytes);
    node.push_back(rhs.nprocs);
    node.push_back(rhs.runtime);
    node.push_back(rhs.sleepDelay);
    node.push_back(rhs.dStartTime);
    node.push_back(rhs.dEndTime);

    return node;
}

bool YAML::convert<fives::DarshanRecord>::decode(const Node &node, fives::DarshanRecord &rhs) {

    rhs.id = node["id"].as<unsigned int>();
    rhs.nprocs = node["nprocs"].as<unsigned int>();
    rhs.readBytes = node["readBytes"].as<double>();
    rhs.writtenBytes = node["writtenBytes"].as<double>();
    rhs.runtime = node["runtime"].as<uint64_t>();
    rhs.dStartTime = node["dStartTime"].as<uint64_t>();
    rhs.dEndTime = node["dEndTime"].as<uint64_t>();
    rhs.sleepDelay = node["sleepDelay"].as<uint64_t>();

    return true;
}

bool fives::operator==(const fives::YamlJob &lhs, const fives::YamlJob &rhs) {
    // Note : not all fields are used.. but this should be way enough
    return (
        lhs.id == rhs.id &&
        lhs.coresUsed == rhs.coresUsed &&
        lhs.coreHoursReq == rhs.coreHoursReq &&
        lhs.coreHoursUsed == rhs.coreHoursUsed &&
        lhs.nodesUsed == rhs.nodesUsed &&
        lhs.readBytes == rhs.readBytes &&
        lhs.writtenBytes == rhs.writtenBytes &&
        lhs.readTimeSeconds == rhs.readTimeSeconds &&
        lhs.writeTimeSeconds == rhs.writeTimeSeconds &&
        lhs.metaTimeSeconds == rhs.metaTimeSeconds &&
        lhs.runtimeSeconds == rhs.runtimeSeconds &&
        lhs.walltimeSeconds == rhs.walltimeSeconds &&
        lhs.waitingTimeSeconds == rhs.waitingTimeSeconds &&
        lhs.sleepSimulationSeconds == rhs.sleepSimulationSeconds &&
        lhs.startTime == rhs.startTime &&
        lhs.endTime == rhs.endTime &&
        lhs.submissionTime == rhs.submissionTime);
}

YAML::Node YAML::convert<fives::YamlJob>::encode(const fives::YamlJob &rhs) {

    YAML::Node node;
    node.push_back(rhs.id);

    node.push_back(rhs.coresUsed);
    node.push_back(rhs.coreHoursReq);
    node.push_back(rhs.coreHoursUsed);
    node.push_back(rhs.nodesUsed);

    node.push_back(rhs.readBytes);
    node.push_back(rhs.writtenBytes);
    node.push_back(rhs.readTimeSeconds);
    node.push_back(rhs.writeTimeSeconds);
    node.push_back(rhs.metaTimeSeconds);

    node.push_back(rhs.runtimeSeconds);
    node.push_back(rhs.waitingTimeSeconds);
    node.push_back(rhs.walltimeSeconds);
    node.push_back(rhs.sleepSimulationSeconds);

    node.push_back(rhs.submissionTime);
    node.push_back(rhs.startTime);
    node.push_back(rhs.endTime);

    return node;
}

bool YAML::convert<fives::YamlJob>::decode(const YAML::Node &node, fives::YamlJob &rhs) {

    if (!(node.Type() == YAML::NodeType::Map) || node.size() != 22) {
        WRENCH_WARN("Invalid node format or incorrect number of keys in node map");
        return false;
    }

    rhs.id = node["id"].as<std::string>();
    rhs.coresUsed = node["coresUsed"].as<unsigned int>();
    if (rhs.coresUsed == 0) {
        WRENCH_WARN("coresUsed <= 0 for job  %s", rhs.id.c_str());
        return false;
    }
    rhs.coreHoursReq = node["coreHoursReq"].as<double>();
    rhs.coreHoursUsed = node["coreHoursUsed"].as<double>();
    if (rhs.coreHoursUsed == 0) {
        WRENCH_WARN("coreHoursUsed <= 0 for job %s", rhs.id.c_str());
    }
    rhs.nodesUsed = node["nodesUsed"].as<unsigned int>();
    if (rhs.nodesUsed == 0) {
        WRENCH_WARN("nodesUsed <= 0 for job %s", rhs.id.c_str());
        return false;
    }

    // Total io operations sizes and durations.
    rhs.readBytes = node["readBytes"].as<uint64_t>();
    rhs.writtenBytes = node["writtenBytes"].as<uint64_t>();
    if ((rhs.readBytes == 0) and (rhs.writtenBytes == 0)) {
        WRENCH_WARN("read and written bytes == 0 for job %s", rhs.id.c_str());
        return false;
    }
    rhs.readTimeSeconds = node["readTimeSeconds"].as<double>();
    rhs.writeTimeSeconds = node["writeTimeSeconds"].as<double>();
    rhs.metaTimeSeconds = node["metaTimeSeconds"].as<double>();
    if ((rhs.readTimeSeconds < 0) or (rhs.writeTimeSeconds < 0) or (rhs.metaTimeSeconds < 0)) {
        WRENCH_WARN("read, written or meta time < 0 for job %s", rhs.id.c_str());
        return false;
    }

    rhs.runtimeSeconds = node["runtimeSeconds"].as<unsigned int>();
    if (rhs.runtimeSeconds == 0) {
        WRENCH_WARN("runtimeSeconds == 0 for job %s", rhs.id.c_str());
        return false;
    }

    // Waiting time between this job submission and start
    rhs.waitingTimeSeconds = node["waitingTimeSeconds"].as<unsigned int>();
    // Waiting time before submitting this job after the previous one was submitted
    rhs.sleepSimulationSeconds = node["sleepSimulationSeconds"].as<unsigned int>();
    rhs.walltimeSeconds = node["walltimeSeconds"].as<unsigned int>();

    // String timedates
    rhs.submissionTime = node["submissionTime"].as<std::string>();
    rhs.startTime = node["startTime"].as<std::string>();
    rhs.endTime = node["endTime"].as<std::string>();

    for (const auto &node : node["runs"]) {
        rhs.runs.push_back(node.as<fives::DarshanRecord>());
    }
    rhs.runsCount = node["runsCount"].as<unsigned int>();

    rhs.cumulReadBW = node["cumul_read_bw"].as<double>();
    rhs.cumulWriteBW = node["cumul_write_bw"].as<double>();

    rhs.category = node["category"].as<unsigned int>();

    return true;
}