/** 
 *  This is an entry for a job in our YAML config file.
 *  
*/

#include "ConfigDefinition.h"
#include <string>
#include "yaml-cpp/yaml.h"


bool storalloc::operator==(const storalloc::Config& lhs, const storalloc::Config& rhs) { 
    return (
        lhs.config_name == rhs.config_name &&
        lhs.config_version == rhs.config_version
    );
}

bool YAML::convert<storalloc::Config>::decode(const YAML::Node& node, storalloc::Config& rhs) {
    
    rhs.config_name = node["general"]["config_name"].as<std::string>();
    rhs.config_version = node["general"]["config_version"].as<std::string>();
    rhs.bw = node["general"]["bandwidth"].as<int>() * 1'000'000'000;
    rhs.d_groups = node["dragonfly"]["groups"].as<int>();
    rhs.d_group_links = node["dragonfly"]["group_links"].as<int>();
    rhs.d_chassis = node["dragonfly"]["chassis"].as<int>();
    rhs.d_chassis_links = node["dragonfly"]["chassis_links"].as<int>();
    rhs.d_routers = node["dragonfly"]["routers"].as<int>();
    rhs.d_router_links = node["dragonfly"]["router_links"].as<int>();
    rhs.d_nodes = node["dragonfly"]["nodes"].as<int>();
    rhs.nb_storage_nodes = node["storage"]["nb_nodes"].as<int>();

    return true;
}
