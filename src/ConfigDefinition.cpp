/**
 *  This is an entry for a job in our YAML config file.
 *
 */

#include "ConfigDefinition.h"
#include "yaml-cpp/yaml.h"
#include <iostream>
#include <map>
#include <string>

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

bool storalloc::operator==(const storalloc::Config &lhs, const storalloc::Config &rhs) {
    return (
        lhs.config_name == rhs.config_name &&
        lhs.config_version == rhs.config_version);
}

bool YAML::convert<storalloc::Config>::decode(const YAML::Node &ynode, storalloc::Config &rhs) {

    try {

        // General
        rhs.config_name = ynode["general"]["config_name"].as<std::string>();
        rhs.config_version = ynode["general"]["config_version"].as<std::string>();
        rhs.bkbone_bw = ynode["general"]["backbone_bw"].as<std::string>();
        rhs.perm_storage_r_bw = ynode["general"]["permanent_storage_read_bw"].as<std::string>();
        rhs.perm_storage_w_bw = ynode["general"]["permanent_storage_write_bw"].as<std::string>();
        rhs.perm_storage_capa = ynode["general"]["permanent_storage_capacity"].as<std::string>();
        rhs.max_stripe_size = ynode["general"]["max_stripe_size"].as<unsigned int>();

        // Dragonfly
        rhs.d_groups = ynode["dragonfly"]["groups"].as<int>();
        rhs.d_group_links = ynode["dragonfly"]["group_links"].as<int>();
        rhs.d_chassis = ynode["dragonfly"]["chassis"].as<int>();
        rhs.d_chassis_links = ynode["dragonfly"]["chassis_links"].as<int>();
        rhs.d_routers = ynode["dragonfly"]["routers"].as<int>();
        rhs.d_router_links = ynode["dragonfly"]["router_links"].as<int>();
        rhs.d_nodes = ynode["dragonfly"]["nodes"].as<int>();
        rhs.core_count = ynode["dragonfly"]["core_count"].as<int>();
        rhs.ram = ynode["dragonfly"]["ram"].as<std::string>();
        rhs.local_storage = ynode["dragonfly"]["node_local_storage"]["enabled"].as<bool>();
        rhs.ls_disks = ynode["dragonfly"]["node_local_storage"]["nb_disks"].as<int>();
        rhs.ls_disks_capa = ynode["dragonfly"]["node_local_storage"]["capacity"].as<std::string>();
        rhs.ls_disks_read_bw = ynode["dragonfly"]["node_local_storage"]["read_bw"].as<std::string>();
        rhs.ls_disks_write_bw = ynode["dragonfly"]["node_local_storage"]["write_bw"].as<std::string>();

        // Storage
        for (const auto node : ynode["storage"]["nodes"]) {

            storalloc::NodeEntry node_entry = {};
            node_entry.qtt = node["quantity"].as<int>();

            auto yaml_node_template = node["template"];
            node_entry.tpl.id = yaml_node_template["id"].as<std::string>();

            // Loop through disk entries in node template
            for (const auto &yaml_disk_entry : yaml_node_template["disks"]) {

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

        // Allocator
        auto alloc = ynode["allocator"].as<std::string>();
        if (alloc == "lustre") {
            rhs.allocator = storalloc::AllocatorType::Lustre;

            // Load specific config
            if (ynode["lustre"]["lq_threshold_rr"].IsDefined())
                rhs.lustre.lq_threshold_rr = ynode["lustre"]["lq_threshold_rr"].as<long long unsigned int>();
            if (ynode["lustre"]["lq_prio_free"].IsDefined())
                rhs.lustre.lq_prio_free = ynode["lustre"]["lq_prio_free"].as<long long unsigned int>();
            if (ynode["lustre"]["max_nb_ost"].IsDefined())
                rhs.lustre.max_nb_ost = ynode["lustre"]["max_nb_ost"].as<long long unsigned int>();
            if (ynode["lustre"]["max_inodes"].IsDefined())
                rhs.lustre.max_inodes = ynode["lustre"]["max_inodes"].as<long long unsigned int>();
            if (ynode["lustre"]["stripe_size"].IsDefined())
                rhs.lustre.stripe_size = ynode["lustre"]["stripe_size"].as<long long unsigned int>();

        } else if (alloc == "rr") {
            rhs.allocator = storalloc::AllocatorType::GenericRR;
        }

    } catch (YAML::ParserException &e) {
        std::cout << e.what() << std::endl;
        return false;
    }

    return true;
}
