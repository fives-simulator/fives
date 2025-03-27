/**
 *  Definitions of a job and the header of a job dataset
 *
 */

#ifndef JOBDEFINITION_H
#define JOBDEFINITION_H

#include "yaml-cpp/yaml.h"
#include <string>

namespace fives {

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

    struct DarshanRecord {
        unsigned int id;
        unsigned int nprocs;
        uint64_t readBytes;
        uint64_t writtenBytes;
        uint64_t runtime;
        uint64_t dStartTime;
        uint64_t dEndTime;
        uint64_t sleepDelay;
        unsigned int unique_files;
        unsigned int traced_file_accesses;
        unsigned int files_accessed_by_all_procs;
        unsigned int files_accessed_by_one_proc;
        double readTimeSeconds;
        double writeTimeSeconds;
        double metaTimeSeconds;
        int mpiio_used;

        // unique read / written file and unique read/write operations
        uint32_t read_files_count;
        uint32_t read_operations;
        uint32_t write_operations;
        uint32_t written_files_count;
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
    };

    bool operator==(const YamlJob &lhs, const YamlJob &rhs);
} // namespace fives

namespace YAML {

    template <>
    struct convert<fives::YamlJob> {
        static Node encode(const fives::YamlJob &rhs);
        static bool decode(const Node &node, fives::YamlJob &rhs);
    };

    template <>
    struct convert<fives::DarshanRecord> {
        static Node encode(const fives::DarshanRecord &rhs);
        static bool decode(const Node &node, fives::DarshanRecord &rhs);
    };

} // namespace YAML

#endif // JOBDEFINITION_H