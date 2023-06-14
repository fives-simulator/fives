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

#include <string>
#include "yaml-cpp/yaml.h"


namespace storalloc
{
    enum JobType {
        ReadComputeWrite,   // Copy data from permanent storage, read, compute, write results, copy results to permanent storage 
        ComputeWrite,       // Compute, write results, copy results to permanent storage    
        ReadCompute,        // Copy from permanent storage, read, compute
        NReadComputeWrite,  // Copy/Read/Compute/Write/Copy cycle n times
        Compute,            // Compute only (or marginal IO)
    };

    struct YamlJob {
        std::string id;
        int mpiProcs;
        int coresUsed;
        int nodesUsed;
        long readBytes;
        long writtenBytes;
        int runTime;
        std::string startTime;
        int sleepTime;
        std::string endTime;
        std::string submissionTime;
        std::string waitingTime;   
    };
    
    bool operator==(const YamlJob& lhs, const YamlJob& rhs);
} // namespace storalloc

namespace YAML {

    template <>
    struct convert<storalloc::YamlJob> {
        static Node encode(const storalloc::YamlJob& rhs);
        static bool decode(const Node& node, storalloc::YamlJob& rhs);
    };

}


#endif // JOBDEFINITION_H