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

#ifndef JOBDEFINITION_H
#define JOBDEFINITION_H

#include "yaml-cpp/yaml.h"
#include <string>

namespace storalloc {

    enum JobType {
        ReadComputeWrite,  // Copy data from permanent storage, read, compute, write results, copy results to permanent storage
        ComputeWrite,      // Compute, write results, copy results to permanent storage
        ReadCompute,       // Copy from permanent storage, read, compute
        ReadWrite,         // For a few jobs having large IO but marginal compute time (or incorrectly computed )
        NReadComputeWrite, // Copy/Read/Compute/Write/Copy cycle n times
        Compute,           // Compute only (or marginal IO)
    };
    const std::array<std::string, 6> JobTypeTranslations = {"RCW", "CW", "RC", "RW", "nRCW", "C"};

    /**
     * @brief Structure for data from the header of the job file,
     *        which contain statistics over the entire dataset of jobs.
     *        These stats are useful for creating a few 'preload' jobs before
     *        replaying the actual dataset, in order to simulate existing load
     *        on the platform.
     */
    struct JobsStats {
        uint64_t first_ts;
        uint64_t last_ts;
        uint64_t duration;
        uint64_t mean_runtime_s;
        uint64_t median_runtime_s;
        uint64_t runtime_s_norm_var;
        uint64_t max_runtime_s;
        uint64_t min_runtime_s;
        int job_count;
        int mean_cores_used;
        int mean_nodes_used;
        int median_cores_used;
        int median_nodes_used;
        uint64_t mean_read_bytes;
        uint64_t mean_written_bytes;
        uint64_t median_read_bytes;
        uint64_t median_written_bytes;
        double mean_jobs_per_hour;
    };

    bool operator==(const JobsStats &lhs, const JobsStats &rhs);

    /**
     * @brief Structure for data from each job in the dataset.
     *
     */
    struct YamlJob {
        std::string id;
        // int nprocs;
        int coresUsed;
        double coreHoursReq;
        double coreHoursUsed;
        int nodesUsed;
        long readBytes;
        long writtenBytes;
        int runtimeSeconds;
        int walltimeSeconds;
        int waitingTimeSeconds;
        int sleepSimulationSeconds;
        std::string submissionTime;
        std::string startTime;
        std::string endTime;
        double readTimeSeconds;
        double writeTimeSeconds;
        double metaTimeSeconds;
        double approxComputeTimeSeconds;
        JobType model;
    };

    bool operator==(const YamlJob &lhs, const YamlJob &rhs);
} // namespace storalloc

namespace YAML {

    template <>
    struct convert<storalloc::YamlJob> {
        static Node encode(const storalloc::YamlJob &rhs);
        static bool decode(const Node &node, storalloc::YamlJob &rhs);
    };

    template <>
    struct convert<storalloc::JobsStats> {
        static Node encode(const storalloc::JobsStats &rhs);
        static bool decode(const Node &node, storalloc::JobsStats &rhs);
    };

} // namespace YAML

#endif // JOBDEFINITION_H