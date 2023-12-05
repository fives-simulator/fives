#include "JobDefinition.h"

#include "yaml-cpp/yaml.h"
#include <iostream>
#include <string>
#include <wrench-dev.h>

WRENCH_LOG_CATEGORY(storalloc_jobs, "Log category for storalloc config");

YAML::Node YAML::convert<storalloc::DarshanRecord>::encode(const storalloc::DarshanRecord &rhs) {
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

bool YAML::convert<storalloc::DarshanRecord>::decode(const Node &node, storalloc::DarshanRecord &rhs) {

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

bool storalloc::operator==(const storalloc::YamlJob &lhs, const storalloc::YamlJob &rhs) {
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

YAML::Node YAML::convert<storalloc::YamlJob>::encode(const storalloc::YamlJob &rhs) {

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

bool YAML::convert<storalloc::YamlJob>::decode(const YAML::Node &node, storalloc::YamlJob &rhs) {

    if (!(node.Type() == YAML::NodeType::Map) || node.size() != 19) {
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
        WRENCH_WARN("coreHoursUsed <= 0 for job %s This might be a data error in the dataset", rhs.id.c_str());
    }
    rhs.nodesUsed = node["nodesUsed"].as<unsigned int>();
    if (rhs.nodesUsed == 0) {
        WRENCH_WARN("nodesUsed <= 0 for job %s", rhs.id.c_str());
        return false;
    }

    // Total io operations sizes and durations.
    rhs.readBytes = node["readBytes"].as<uint64_t>();
    rhs.writtenBytes = node["writtenBytes"].as<uint64_t>();
    if ((rhs.readBytes == 0) or rhs.writtenBytes == 0)
        WRENCH_INFO("read or written bytes == 0 for job %s", rhs.id.c_str());
    rhs.readTimeSeconds = node["readTimeSeconds"].as<double>();
    rhs.writeTimeSeconds = node["writeTimeSeconds"].as<double>();
    rhs.metaTimeSeconds = node["metaTimeSeconds"].as<double>();

    rhs.runtimeSeconds = node["runtimeSeconds"].as<unsigned int>();
    if (rhs.runtimeSeconds == 0) {
        WRENCH_WARN("runtimeSeconds <= 0 for job %s", rhs.id.c_str());
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

    // Individual Darshan records

    for (YAML::const_iterator it = node["runs"].begin(); it != node["runs"].end(); ++it) {
        rhs.runs.push_back(it->as<storalloc::DarshanRecord>());
    }

    return true;
}

bool storalloc::operator==(const storalloc::JobsStats &lhs, const storalloc::JobsStats &rhs) {
    // Note: once again we're not using every field here, because it seems overkill..
    return (
        lhs.job_count == rhs.job_count &&
        lhs.first_ts == rhs.first_ts &&
        lhs.last_ts == rhs.last_ts &&
        lhs.mean_cores_used == rhs.mean_cores_used &&
        lhs.mean_nodes_used == rhs.mean_nodes_used &&
        lhs.median_cores_used == rhs.median_cores_used &&
        lhs.median_nodes_used == rhs.median_nodes_used &&
        lhs.mean_read_tbytes == rhs.mean_read_tbytes &&
        lhs.mean_written_tbytes == rhs.mean_written_tbytes &&
        lhs.median_read_tbytes == rhs.median_read_tbytes &&
        lhs.median_written_tbytes == rhs.median_written_tbytes);
}

YAML::Node YAML::convert<storalloc::JobsStats>::encode(const storalloc::JobsStats &rhs) {

    YAML::Node node;
    node.push_back(rhs.first_ts);
    node.push_back(rhs.last_ts);
    node.push_back(rhs.duration);
    node.push_back(rhs.mean_runtime_s);
    node.push_back(rhs.median_runtime_s);
    node.push_back(rhs.var_runtime_s);
    node.push_back(rhs.max_runtime_s);
    node.push_back(rhs.min_runtime_s);
    node.push_back(rhs.min_interval_s);
    node.push_back(rhs.max_interval_s);
    node.push_back(rhs.mean_interval_s);
    node.push_back(rhs.var_interval_s);
    node.push_back(rhs.median_interval_s);
    node.push_back(rhs.job_count);
    node.push_back(rhs.mean_cores_used);
    node.push_back(rhs.mean_nodes_used);
    node.push_back(rhs.median_cores_used);
    node.push_back(rhs.median_nodes_used);
    node.push_back(rhs.var_nodes_used);
    node.push_back(rhs.max_nodes_used);
    node.push_back(rhs.mean_read_tbytes);
    node.push_back(rhs.mean_written_tbytes);
    node.push_back(rhs.median_read_tbytes);
    node.push_back(rhs.median_written_tbytes);
    node.push_back(rhs.max_read_tbytes);
    node.push_back(rhs.max_written_tbytes);
    node.push_back(rhs.var_read_tbytes);
    node.push_back(rhs.var_written_tbytes);
    node.push_back(rhs.mean_jobs_per_hour);

    return node;
}

bool YAML::convert<storalloc::JobsStats>::decode(const YAML::Node &node, storalloc::JobsStats &rhs) {

    if (!(node.Type() == YAML::NodeType::Map)) {
        WRENCH_WARN("Invalid node format for dataset (Header)");
        return false;
    }

    if (node.size() != 29) {
        WRENCH_WARN("Incorrect number of keys in node map (header)");
        return false;
    }

    rhs.first_ts = node["first_ts"].as<uint64_t>();
    rhs.last_ts = node["last_ts"].as<uint64_t>();
    if (rhs.last_ts <= rhs.first_ts) {
        WRENCH_WARN("First TS in dataset is >= to the last TS");
        return false;
    }
    rhs.duration = node["duration"].as<uint64_t>();
    if (rhs.duration != (rhs.last_ts - rhs.first_ts)) {
        WRENCH_WARN("Duration and (last ts - first ts) do not match");
        return false;
    }
    rhs.mean_runtime_s = node["mean_runtime_s"].as<uint64_t>();
    rhs.median_runtime_s = node["median_runtime_s"].as<uint64_t>();
    rhs.var_runtime_s = node["var_runtime_s"].as<uint64_t>();
    rhs.max_runtime_s = node["max_runtime_s"].as<uint64_t>();
    rhs.min_runtime_s = node["min_runtime_s"].as<uint64_t>();
    rhs.min_interval_s = node["min_interval_s"].as<uint64_t>();
    rhs.max_interval_s = node["max_interval_s"].as<uint64_t>();
    rhs.mean_interval_s = node["mean_interval_s"].as<uint64_t>();
    rhs.median_interval_s = node["median_interval_s"].as<uint64_t>();
    rhs.var_interval_s = node["var_interval_s"].as<uint64_t>();
    rhs.job_count = node["job_count"].as<unsigned int>();
    rhs.mean_cores_used = node["mean_cores_used"].as<unsigned int>();
    rhs.mean_nodes_used = node["mean_nodes_used"].as<unsigned int>();
    rhs.median_cores_used = node["median_cores_used"].as<unsigned int>();
    rhs.median_nodes_used = node["median_nodes_used"].as<unsigned int>();
    rhs.max_nodes_used = node["max_nodes_used"].as<unsigned int>();
    rhs.var_nodes_used = node["var_nodes_used"].as<unsigned int>();
    rhs.mean_read_tbytes = node["mean_read_tbytes"].as<double>();
    rhs.mean_written_tbytes = node["mean_written_tbytes"].as<double>();
    rhs.var_written_tbytes = node["var_written_tbytes"].as<double>();
    rhs.var_read_tbytes = node["var_read_tbytes"].as<double>();
    rhs.median_read_tbytes = node["median_read_tbytes"].as<double>();
    rhs.median_written_tbytes = node["median_written_tbytes"].as<double>();
    rhs.max_read_tbytes = node["max_read_tbytes"].as<double>();
    rhs.max_written_tbytes = node["max_written_tbytes"].as<double>();
    rhs.mean_jobs_per_hour = node["mean_jobs_per_hour"].as<double>();

    return true;
}