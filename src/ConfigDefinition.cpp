#include "ConfigDefinition.h"

#include "yaml-cpp/yaml.h"
#include <iostream>
#include <map>
#include <string>
#include <wrench-dev.h>

WRENCH_LOG_CATEGORY(storalloc_config, "Log category for storalloc config");

std::string storalloc::DiskTemplate::to_string() const {
    return "Disk " + this->id + " - " + std::to_string(this->capacity) + " GB with mount prefix " + this->mount_prefix;
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

bool operator==(const storalloc::Config &lhs, const storalloc::Config &rhs) {
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
        rhs.preload_percent = ynode["general"]["preload_percent"].as<float>();
        if (ynode["general"]["amdahl"].IsDefined()) {
            auto amdhal = ynode["general"]["amdahl"].as<float>();
            if ((amdhal > 1) or (amdhal < 0)) {
                WRENCH_WARN("amdhal parameter should be bounded in [0, 1]");
                return false;
            }
            rhs.amdahl = ynode["general"]["amdahl"].as<float>();
        } else {
            WRENCH_INFO("Using default value for amdahl config (0.7)");
            rhs.amdahl = 0.7; // "sensible" default value ?
        }
        if (ynode["general"]["walltime_extension"].IsDefined()) {
            rhs.walltime_extension = ynode["general"]["walltime_extension"].as<float>();
        } else {
            WRENCH_INFO("Not using walltime extension");
            rhs.walltime_extension = 1; // no walltime_extension
        }

        if ((not ynode["general"]["non_linear_coef_read"].IsDefined()) or (not ynode["general"]["non_linear_coef_write"].IsDefined())) {
            WRENCH_INFO("Using default values for non linear coefs for disk bandwidth (1) - Only the number of concurrent activities matters.");
            rhs.non_linear_coef_read = 1;
            rhs.non_linear_coef_write = 1;
        } else {
            rhs.non_linear_coef_read = ynode["general"]["non_linear_coef_read"].as<float>();
            rhs.non_linear_coef_write = ynode["general"]["non_linear_coef_write"].as<float>();
        }
        if ((not ynode["general"]["read_variability"].IsDefined()) or (not ynode["general"]["write_variability"].IsDefined())) {
            WRENCH_INFO("Disks won't have any normal distributed variability applied to their bandwidth");
            rhs.read_variability = 1;
            rhs.write_variability = 1;
        } else {
            rhs.read_variability = ynode["general"]["read_variability"].as<float>();
            rhs.write_variability = ynode["general"]["write_variability"].as<float>();
        }

        // Dragonfly compute zone
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

        // Storage system
        for (const auto node : ynode["storage"]["nodes"]) {

            storalloc::NodeEntry node_entry = {};
            node_entry.qtt = node["quantity"].as<unsigned int>();

            auto yaml_node_template = node["template"];
            node_entry.tpl.id = yaml_node_template["id"].as<std::string>();

            // Loop through disk entries in node template
            for (const auto &yaml_disk_entry : yaml_node_template["disks"]) {

                storalloc::DiskEntry disk_entry = {};
                disk_entry.qtt = yaml_disk_entry["quantity"].as<int>();
                disk_entry.tpl.id = yaml_disk_entry["template"]["id"].as<std::string>();
                disk_entry.tpl.capacity = yaml_disk_entry["template"]["capacity"].as<unsigned int>();
                disk_entry.tpl.read_bw = yaml_disk_entry["template"]["read_bw"].as<unsigned int>();
                disk_entry.tpl.write_bw = yaml_disk_entry["template"]["write_bw"].as<unsigned int>();
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

        // Allocator callback
        auto alloc = ynode["allocator"].as<std::string>();
        if (alloc == "lustre") {
            rhs.allocator = storalloc::AllocatorType::Lustre;

            auto lustreConfig = storalloc::LustreConfig();

            // Load specific config or default values.
            if (ynode["lustre"]["lq_threshold_rr"].IsDefined()) {
                lustreConfig.lq_threshold_rr = ynode["lustre"]["lq_threshold_rr"].as<uint64_t>();
            } else {
                WRENCH_INFO("Using default value for lustre.lq_threshold_rr : %lu", lustreConfig.lq_threshold_rr);
            }
            if (ynode["lustre"]["lq_prio_free"].IsDefined()) {
                lustreConfig.lq_prio_free = ynode["lustre"]["lq_prio_free"].as<uint64_t>();
            } else {
                WRENCH_INFO("Using default value for lustre.lq_prio_free : %lu", lustreConfig.lq_prio_free);
            }
            if (ynode["lustre"]["max_nb_ost"].IsDefined()) {
                lustreConfig.max_nb_ost = ynode["lustre"]["max_nb_ost"].as<uint64_t>();
            } else {
                WRENCH_INFO("Using default value for lustre.max_nb_ost : %lu", lustreConfig.max_nb_ost);
            }
            if (ynode["lustre"]["max_inodes"].IsDefined()) {
                lustreConfig.max_inodes = ynode["lustre"]["max_inodes"].as<uint64_t>();
            } else {
                WRENCH_INFO("Using default value for lustre.max_inodes : %lu", lustreConfig.max_inodes);
            }
            if (ynode["lustre"]["stripe_size"].IsDefined()) {
                lustreConfig.stripe_size = ynode["lustre"]["stripe_size"].as<uint64_t>();
            } else {
                WRENCH_INFO("Using default value for lustre.stripe_size : %lu", lustreConfig.stripe_size);
            }
            if (ynode["lustre"]["stripe_count"].IsDefined()) {
                lustreConfig.stripe_count = ynode["lustre"]["stripe_count"].as<uint64_t>();
            } else {
                WRENCH_INFO("Using default value for lustre.stripe_count : %lu", lustreConfig.stripe_count);
            }
            if (ynode["lustre"]["max_chunks_per_ost"].IsDefined()) {
                lustreConfig.max_chunks_per_ost = ynode["lustre"]["max_chunks_per_ost"].as<uint64_t>();
            } else {
                WRENCH_INFO("Using default value for lustre.max_chunks_per_ost : %lu", lustreConfig.max_chunks_per_ost);
            }

            rhs.lustre = lustreConfig;

        } else if (alloc == "rr") {
            rhs.allocator = storalloc::AllocatorType::GenericRR;
        }
    } catch (YAML::ParserException &e) {
        WRENCH_WARN("Unable to parse configuration file : %s", e.what());
        return false;
    }

    WRENCH_INFO("Configuration successfully parsed");
    return true;
}