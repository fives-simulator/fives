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
    
    struct YamlJob {

        int id;
        int mpiProcs;
        int coresUsed;
        int nodesUsed;
        long readBytes;
        long writtenBytes;
        int runTime;
        std::string startTime;
        std::string endTime;
        std::string submissionTime;
        std::string waitingTime;   
    };

    constexpr bool operator==(const YamlJob& lhs, const YamlJob& rhs) { 
        return (
            lhs.id == rhs.id && 
            lhs.mpiProcs == rhs.mpiProcs &&
            lhs.coresUsed == rhs.coresUsed &&
            lhs.nodesUsed == rhs.nodesUsed &&
            lhs.readBytes == rhs.readBytes &&
            lhs.writtenBytes == rhs.writtenBytes &&
            lhs.runTime == rhs.runTime &&
            lhs.startTime == rhs.startTime &&
            lhs.endTime == rhs.endTime &&
            lhs.submissionTime == rhs.submissionTime &&
            lhs.waitingTime == rhs.waitingTime
        );
    }

} // namespace storalloc

namespace YAML {
    template<>
        struct convert<storalloc::YamlJob> {
            static Node encode(const storalloc::YamlJob& rhs) {
                Node node;
                node.push_back(rhs.id);
                node.push_back(rhs.mpiProcs);
                node.push_back(rhs.coresUsed);
                node.push_back(rhs.nodesUsed);
                node.push_back(rhs.readBytes);
                node.push_back(rhs.writtenBytes);
                node.push_back(rhs.runTime);
                node.push_back(rhs.startTime);
                node.push_back(rhs.endTime);
                node.push_back(rhs.submissionTime);
                node.push_back(rhs.waitingTime);
            return node;
        }

        static bool decode(const Node& node, storalloc::YamlJob& rhs) {
            if(!(node.Type() == YAML::NodeType::Map) || node.size() != 11) {
                return false;
            }

            rhs.id = node["id"].as<int>();
            rhs.mpiProcs = node["MPIprocs"].as<int>();
            rhs.coresUsed = node["coresUsed"].as<int>();
            rhs.nodesUsed = node["nodesUsed"].as<int>();
            rhs.readBytes = node["readBytes"].as<long>();
            rhs.writtenBytes = node["writtenBytes"].as<long>();
            rhs.runTime = node["runTime"].as<int>();
            rhs.startTime = node["startTime"].as<std::string>();
            rhs.endTime = node["endTime"].as<std::string>();
            rhs.submissionTime = node["submissionTime"].as<std::string>();
            rhs.waitingTime = node["waitingTime"].as<std::string>();

            return true;
        }
    };
}


#endif // JOBDEFINITION_H