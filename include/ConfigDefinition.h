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
        int nb_storage_nodes;
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