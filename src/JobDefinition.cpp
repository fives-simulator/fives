/**
 *  This is an entry for a job in our YAML data file.
 *  This header defines a simple structure to map to this kind of job schema
 *
 *- MPIprocs: 2048
    coresUsed: 8192
    endTime: '2020-10-30 18:16:05'
    id: 476279
    nodesUsed: 128
    readBytes: 549755813888
    runTime: 166
    startTime: '2020-10-30 17:49:59'
    submissionTime: '2020-10-30 16:26:10'
    waitingTime: 0 days 01:23:49
    writtenBytes: 0
*/

#include "JobDefinition.h"
#include "yaml-cpp/yaml.h"
#include <iostream>
#include <string>

bool storalloc::operator==(const storalloc::YamlJob &lhs, const storalloc::YamlJob &rhs) {
    return (
        lhs.id == rhs.id &&
        // lhs.nprocs == rhs.nprocs &&
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
        lhs.approxComputeTimeSeconds == rhs.approxComputeTimeSeconds &&
        lhs.walltimeSeconds == rhs.walltimeSeconds &&
        lhs.waitingTimeSeconds == rhs.waitingTimeSeconds &&
        lhs.sleepSimulationSeconds == rhs.sleepSimulationSeconds &&
        lhs.startTime == rhs.startTime &&
        lhs.endTime == rhs.endTime &&
        lhs.submissionTime == rhs.submissionTime &&
        lhs.model == rhs.model);
}

YAML::Node YAML::convert<storalloc::YamlJob>::encode(const storalloc::YamlJob &rhs) {

    YAML::Node node;
    node.push_back(rhs.id);
    // node.push_back(rhs.nprocs);

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
    node.push_back(rhs.approxComputeTimeSeconds);
    node.push_back(rhs.waitingTimeSeconds);
    node.push_back(rhs.walltimeSeconds);
    node.push_back(rhs.sleepSimulationSeconds);

    node.push_back(rhs.submissionTime);
    node.push_back(rhs.startTime);
    node.push_back(rhs.endTime);

    node.push_back(storalloc::JobTypeTranslations[rhs.model]);

    return node;
}

bool YAML::convert<storalloc::YamlJob>::decode(const YAML::Node &node, storalloc::YamlJob &rhs) {

    if (!(node.Type() == YAML::NodeType::Map) || node.size() != 19) {
        std::cerr << "Invalid node format or incorrect number of keys in node map" << std::endl;
        return false;
    }

    rhs.id = node["id"].as<std::string>();
    // rhs.nprocs = node["nprocs"].as<int>();
    //  if (rhs.nprocs <= 0) {
    //     std::cerr << "nprocs <= 0 on node " << node["id"] << std::endl;
    //     return false;
    //  }
    rhs.coresUsed = node["coresUsed"].as<int>();
    if (rhs.coresUsed <= 0) {
        std::cerr << "coresUsed <= 0 on node " << node["id"] << std::endl;
        return false;
    }
    rhs.coreHoursReq = node["coreHoursReq"].as<double>();
    rhs.coreHoursUsed = node["coreHoursUsed"].as<double>();
    rhs.nodesUsed = node["nodesUsed"].as<int>();
    if (rhs.nodesUsed <= 0) {
        std::cerr << "nodesUsed <= 0 on node " << node["id"] << std::endl;
        return false;
    }

    // Total io operations sizes and durations.
    rhs.readBytes = node["readBytes"].as<long>();
    rhs.writtenBytes = node["writtenBytes"].as<long>();
    rhs.readTimeSeconds = node["readTimeSeconds"].as<double>();
    rhs.writeTimeSeconds = node["writeTimeSeconds"].as<double>();
    rhs.metaTimeSeconds = node["metaTimeSeconds"].as<double>();

    rhs.runtimeSeconds = node["runtimeSeconds"].as<int>();
    if (rhs.runtimeSeconds <= 0) {
        std::cerr << "runtimeSeconds <= 0 on node " << node["id"] << std::endl;
        return false;
    }

    // Computed approximate compute time (based on runtime - Darshan traced IO time)
    rhs.approxComputeTimeSeconds = node["approxComputeTimeSeconds"].as<double>();

    // Waiting time between job submission and start
    rhs.waitingTimeSeconds = node["waitingTimeSeconds"].as<int>();
    // Waiting time before submitting this job after the previous one was submitted
    rhs.sleepSimulationSeconds = node["sleepSimulationSeconds"].as<int>();
    rhs.walltimeSeconds = node["walltimeSeconds"].as<int>();

    // String timedates
    rhs.submissionTime = node["submissionTime"].as<std::string>();
    rhs.startTime = node["startTime"].as<std::string>();
    rhs.endTime = node["endTime"].as<std::string>();

    auto model_str = node["model"].as<std::string>();

    if (model_str == "RCW") {
        rhs.model = storalloc::JobType::ReadComputeWrite;
    } else if (model_str == "RC") {
        rhs.model = storalloc::JobType::ReadCompute;
    } else if (model_str == "CW") {
        rhs.model = storalloc::JobType::ComputeWrite;
    } else if (model_str == "C") {
        rhs.model = storalloc::JobType::Compute;
    } else if (model_str == "RW") {
        rhs.model = storalloc::JobType::ReadWrite;
    } else {
        std::cerr << "Invalide job model for job " << node["id"] << std::endl;
        return false;
    }

    return true;
}

bool storalloc::operator==(const storalloc::JobsStats &lhs, const storalloc::JobsStats &rhs) {
    // Note; we're note using every structure field here, because it seems overkill..
    return (
        lhs.job_count == rhs.job_count &&
        lhs.first_ts == rhs.first_ts &&
        lhs.last_ts == rhs.last_ts &&
        lhs.mean_cores_used == rhs.mean_cores_used &&
        lhs.mean_nodes_used == rhs.mean_nodes_used &&
        lhs.median_cores_used == rhs.median_cores_used &&
        lhs.median_nodes_used == rhs.median_nodes_used &&
        lhs.mean_read_bytes == rhs.mean_read_bytes &&
        lhs.mean_written_bytes == rhs.mean_written_bytes &&
        lhs.median_read_bytes == rhs.median_read_bytes &&
        lhs.median_written_bytes == rhs.median_written_bytes);
}

YAML::Node YAML::convert<storalloc::JobsStats>::encode(const storalloc::JobsStats &rhs) {

    YAML::Node node;
    node.push_back(rhs.first_ts);
    node.push_back(rhs.last_ts);
    node.push_back(rhs.duration);
    node.push_back(rhs.mean_runtime_s);
    node.push_back(rhs.median_runtime_s);
    node.push_back(rhs.runtime_s_norm_var);
    node.push_back(rhs.max_runtime_s);
    node.push_back(rhs.min_runtime_s);
    node.push_back(rhs.job_count);
    node.push_back(rhs.mean_cores_used);
    node.push_back(rhs.mean_nodes_used);
    node.push_back(rhs.median_cores_used);
    node.push_back(rhs.median_nodes_used);
    node.push_back(rhs.mean_read_bytes);
    node.push_back(rhs.mean_written_bytes);
    node.push_back(rhs.median_read_bytes);
    node.push_back(rhs.median_written_bytes);
    node.push_back(rhs.mean_jobs_per_hour);

    return node;
}

bool YAML::convert<storalloc::JobsStats>::decode(const YAML::Node &node, storalloc::JobsStats &rhs) {

    if (!(node.Type() == YAML::NodeType::Map)) {
        std::cerr << "Invalid node format for dataset (Header)" << std::endl;
        return false;
    }

    if (node.size() != 18) {
        std::cerr << "Incorrect number of keys in node map (header)" << std::endl;
        return false;
    }

    rhs.first_ts = node["first_ts"].as<uint64_t>();
    rhs.last_ts = node["last_ts"].as<uint64_t>();
    rhs.duration = node["duration"].as<uint64_t>();
    rhs.mean_runtime_s = node["mean_runtime_s"].as<uint64_t>();
    rhs.median_runtime_s = node["median_runtime_s"].as<uint64_t>();
    rhs.runtime_s_norm_var = node["runtime_s_norm_var"].as<uint64_t>();
    rhs.max_runtime_s = node["max_runtime_s"].as<uint64_t>();
    rhs.min_runtime_s = node["min_runtime_s"].as<uint64_t>();
    rhs.job_count = node["job_count"].as<int>();
    rhs.mean_cores_used = node["mean_cores_used"].as<int>();
    rhs.mean_nodes_used = node["mean_nodes_used"].as<int>();
    rhs.median_cores_used = node["median_cores_used"].as<int>();
    rhs.median_nodes_used = node["median_nodes_used"].as<int>();
    rhs.mean_read_bytes = node["mean_read_bytes"].as<uint64_t>();
    rhs.mean_written_bytes = node["mean_written_bytes"].as<uint64_t>();
    rhs.median_read_bytes = node["median_read_bytes"].as<uint64_t>();
    rhs.median_written_bytes = node["median_written_bytes"].as<uint64_t>();
    rhs.mean_jobs_per_hour = node["mean_jobs_per_hour"].as<double>();

    return true;
}
