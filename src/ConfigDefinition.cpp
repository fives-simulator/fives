/** 
 *  This is an entry for a job in our YAML config file.
 *  
*/

#include "ConfigDefinition.h"
#include <string>
#include <iostream>
#include <map>
#include "yaml-cpp/yaml.h"


std::string storalloc::DiskTemplate::to_string() const {
    return "Disk " + this->id + " - " + std::to_string(this->capacity) + " GB";
}

std::string storalloc::DiskEntry::to_string() const {
    return "Set of  " + std::to_string(this->qtt) + " disks using template " + this->tpl.id;
}

std::string storalloc::NodeTemplate::to_string() const {
    return "Node " + this->id + " - with " + std::to_string(this->disks.size()) + " disks";
}

std::string storalloc::NodeEntry::to_string() const {
    return "Set of  " + std::to_string(this->qtt) + " nodes using template " + this->tpl.id;
}


bool storalloc::operator==(const storalloc::Config& lhs, const storalloc::Config& rhs) { 
    return (
        lhs.config_name == rhs.config_name &&
        lhs.config_version == rhs.config_version
    );
}

bool YAML::convert<storalloc::Config>::decode(const YAML::Node& ynode, storalloc::Config& rhs) {
    
    try {

        // General
        rhs.config_name = ynode["general"]["config_name"].as<std::string>();
        rhs.config_version = ynode["general"]["config_version"].as<std::string>();
        rhs.bw = ynode["general"]["bandwidth"].as<int>() * 1'000'000'000;
        
        // Dragonfly
        rhs.d_groups = ynode["dragonfly"]["groups"].as<int>();
        rhs.d_group_links = ynode["dragonfly"]["group_links"].as<int>();
        rhs.d_chassis = ynode["dragonfly"]["chassis"].as<int>();
        rhs.d_chassis_links = ynode["dragonfly"]["chassis_links"].as<int>();
        rhs.d_routers = ynode["dragonfly"]["routers"].as<int>();
        rhs.d_router_links = ynode["dragonfly"]["router_links"].as<int>();
        rhs.d_nodes = ynode["dragonfly"]["nodes"].as<int>();

        // Storage
        for (const auto node : ynode["storage"]["nodes"]) {
    
            storalloc::NodeEntry node_entry = {};
            node_entry.qtt = node["quantity"].as<int>();

            auto yaml_node_template = node["template"];
            node_entry.tpl.id = yaml_node_template["id"].as<std::string>();

            // Loop through disk entries in node template
            for (const auto & yaml_disk_entry : yaml_node_template["disks"]) {
                
                storalloc::DiskEntry disk_entry = {};
                disk_entry.qtt = yaml_disk_entry["quantity"].as<int>();
                disk_entry.tpl.id = yaml_disk_entry["template"]["id"].as<std::string>();
                disk_entry.tpl.capacity = yaml_disk_entry["template"]["capacity"].as<int>();
                disk_entry.tpl.read_bw = yaml_disk_entry["template"]["read_bw"].as<int>();
                disk_entry.tpl.write_bw = yaml_disk_entry["template"]["write_bw"].as<int>();
                disk_entry.tpl.mount_prefix = yaml_disk_entry["template"]["mount_prefix"].as<std::string>();
            
                // Add the disk entry to the node template
                node_entry.tpl.disks.push_back(disk_entry);
                // Add the node template to the list of node templates
                rhs.node_templates[node_entry.tpl.id] = node_entry.tpl;
                // Add the disk template to the list of disks templates
                rhs.disk_templates[disk_entry.tpl.id] = disk_entry.tpl;

            }

            rhs.nodes.push_back(node_entry);

        }

    } catch(YAML::ParserException& e) {
        std::cout << e.what() << std::endl;
    }

    return true;
}
