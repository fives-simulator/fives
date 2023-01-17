/** 
 *  This is an entry for a job in our YAML config file.
 *  
*/


#ifndef CFGDEFINITION_H
#define CFGDEFINITION_H

#include <string>
#include "yaml-cpp/yaml.h"

namespace storalloc
{

    struct DiskTemplate {
        std::string id;
        int capacity;
        int read_bw;
        int write_bw;
        std::string mount_prefix;

        std::string to_string() const;
    };

    struct DiskEntry {
        int qtt;
        DiskTemplate tpl;

        std::string to_string() const;
    };

    struct NodeTemplate {
        std::string id;
        std::vector<DiskEntry> disks;

        std::string to_string() const;
    };

    struct NodeEntry {
        int qtt;
        NodeTemplate tpl;

        std::string to_string() const;
    };

    struct Config {
        std::string config_name;
        std::string config_version;
        long bw;
        unsigned int d_groups;
        unsigned int d_group_links;
        unsigned int d_chassis;
        unsigned int d_chassis_links;
        unsigned int d_routers;
        unsigned int d_router_links;
        unsigned int d_nodes;
        std::map<std::string, DiskTemplate> disk_templates;
        std::map<std::string, NodeTemplate> node_templates;
        std::vector<NodeEntry> nodes;
    };

    bool operator==(const Config& lhs, const Config& rhs);
} // namespace storalloc

namespace YAML {

    template <>
    struct convert<storalloc::Config> {
        static bool decode(const Node& node, storalloc::Config& rhs);
    };
    
} // namespace YAML

#endif // CFGDEFINITION_H