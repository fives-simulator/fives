/**
 *  Definitions of a job and the header of a job dataset
 *
 */

#ifndef JOBDEFINITION_H
#define JOBDEFINITION_H

#include "yaml-cpp/yaml.h"
#include <string>

namespace storalloc {

    /**
     * @brief High-level job types (in terms of IO vs compute behaviour). Loosely based on job characteristics.
     */
    enum JobType {
        ReadComputeWrite,  // Copy data from permanent storage, read, compute, write results, copy results to permanent storage
        ComputeWrite,      // Compute, write results, copy results to permanent storage
        ReadCompute,       // Copy from permanent storage, read, compute
        ReadWrite,         // For a few jobs having large IO but marginal compute time (or incorrectly computed )
        NReadComputeWrite, // Copy/Read/Compute/Write/Copy cycle n times - Not implemented in simulation yet
        Compute,           // Compute only (or marginal IO)
    };
    const std::array<std::string, 6> JobTypeTranslations = {"RCW", "CW", "RC", "RW", "nRCW", "C"};

    /**
     * @brief Structure for data from the header of the job file,
     *        which contain statistics over the entire dataset of jobs.
     *        These stats are useful for creating a few 'preload' jobs using probability distributions
     *        before replaying the actual dataset, in order to simulate existing load on the platform.
     */
    struct JobsStats {
        uint64_t first_ts;          // 'Start' TS of the first job to start in the dataset
        uint64_t last_ts;           // 'End' TS of the last job to complete in the dataset
        uint64_t duration;          // last_ts - first_ts
        uint64_t mean_runtime_s;    // Mean runtime in seconds for all jobs     - note : some outlier triming might have been performed
        uint64_t median_runtime_s;  // Median runtime in seconds for all jobs
        uint64_t var_runtime_s;     // Job runtime variance
        uint64_t max_runtime_s;     // Longest runtime in s
        uint64_t min_runtime_s;     // Shortest runtime in s
        uint64_t mean_interval_s;   // Mean interval between two jobs, in s     - note : some outlier triming might have been performed
        uint64_t median_interval_s; // Median interval between two jobs, in s
        uint64_t max_interval_s;    // Longest interval between two jobs, in s
        uint64_t min_interval_s;    // Shortest interval between two jobs, in s
        uint64_t var_interval_s;    // Interval variance
        unsigned int job_count;     // Number of jobs in dataset
        unsigned int mean_cores_used;
        unsigned int mean_nodes_used;
        unsigned int var_nodes_used;
        unsigned int median_cores_used;
        unsigned int median_nodes_used;
        unsigned int max_nodes_used;
        double mean_read_tbytes;
        double mean_written_tbytes;
        double var_read_tbytes;
        double var_written_tbytes;
        double median_read_tbytes;
        double median_written_tbytes;
        double max_read_tbytes;
        double max_written_tbytes;
        double mean_jobs_per_hour; // Over the entire duration of the dataset (in number of hours), mean number of jobs concurrently running
    };

    bool operator==(const JobsStats &lhs, const JobsStats &rhs);

    struct DarshanRecord {
        unsigned int id;
        unsigned int nprocs;
        uint64_t readBytes;
        uint64_t writtenBytes;
        uint64_t runtime;
        uint64_t dStartTime;
        uint64_t dEndTime;
        uint64_t sleepDelay;
    };

    /**
     * @brief Structure representing a single job
     *
     */
    struct YamlJob {
        std::string id;         // ID of the job is usually the same as in the original dataset
        unsigned int coresUsed; // Total number of cores used (usually n° of nodes * cores per node)
        double coreHoursReq;    // Core hours requested at job creation
        double coreHoursUsed;   // Core hours actually used by job
        unsigned int nodesUsed; // Number of nodes used by job
        uint64_t readBytes;
        uint64_t writtenBytes;
        unsigned int runtimeSeconds;
        unsigned int walltimeSeconds;
        unsigned int waitingTimeSeconds;     // Waiting time between job submission and job start
        unsigned int sleepSimulationSeconds; // Time to wait in simulation before submitting this job after the previous job in the dataset was submitted
        std::string submissionTime;          // Submission timestamp as ISO string ()
        std::string startTime;               // Start timestamp as ISO string
        std::string endTime;                 // End timestamp as ISO string
        double readTimeSeconds;              // Cumulative read time from all core doing any kind of IO reads during the job's execution
        double writeTimeSeconds;             // Cumulative write time from all core doing any kind of IO writes during the job's execution
        double metaTimeSeconds;              // Cumulative meta time from all core doing any kind of IO meta ops during the job's execution
        std::vector<DarshanRecord> runs;
        unsigned int runsCount;
        double cumulReadBW;
        double cumulWriteBW;
        unsigned int category;
        // unsigned int sum_nprocs;
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

    template <>
    struct convert<storalloc::DarshanRecord> {
        static Node encode(const storalloc::DarshanRecord &rhs);
        static bool decode(const Node &node, storalloc::DarshanRecord &rhs);
    };

} // namespace YAML

#endif // JOBDEFINITION_H