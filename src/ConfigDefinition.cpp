#include "ConfigDefinition.h"

#include "yaml-cpp/yaml.h"
#include <iostream>
#include <map>
#include <string>
#include <wrench-dev.h>

WRENCH_LOG_CATEGORY(fives_config, "Log category for Fives Config parser");

std::string fives::DiskTemplate::to_string() const {
    return "Disk " + this->id + " - " + std::to_string(this->capacity) + " GB with mount prefix " + this->mount_prefix;
}

std::string fives::DiskEntry::to_string() const {
    return "Set of  " + std::to_string(this->qtt) + " disks using template " + this->tpl.id;
}

std::string fives::NodeTemplate::to_string() const {
    return "Node " + this->id + " - with " + std::to_string(this->disks.size()) + " disks";
}

std::string fives::NodeEntry::to_string() const {
    return "Set of  " + std::to_string(this->qtt) + " nodes using template " + this->tpl.id;
}

bool operator==(const fives::Config &lhs, const fives::Config &rhs) {
    return (
        lhs.config_name == rhs.config_name &&
        lhs.config_version == rhs.config_version);
}

bool YAML::convert<fives::Config>::decode(const YAML::Node &ynode, fives::Config &rhs) {

    try {
        // General
        rhs.config_name = ynode["general"]["config_name"].as<std::string>();
        rhs.config_version = ynode["general"]["config_version"].as<std::string>();
        rhs.max_stripe_size = ynode["general"]["max_stripe_size"].as<unsigned int>();
        rhs.preload_percent = ynode["general"]["preload_percent"].as<float>();

        if (ynode["general"]["allowed_failure_percent"].IsDefined()) {
            rhs.allowed_failure_percent = ynode["general"]["allowed_failure_percent"].as<float>();
        }

        if (ynode["general"]["testing"].IsDefined()) {
            rhs.testing = ynode["general"]["testing"].as<bool>();
        }
        if (ynode["general"]["debug"].IsDefined()) {
            rhs.debug = ynode["general"]["debug"].as<bool>();
        }
        if (ynode["general"]["walltime_extension"].IsDefined()) {
            rhs.walltime_extension = ynode["general"]["walltime_extension"].as<float>();
        } else {
            WRENCH_INFO("Not using walltime extension");
            rhs.walltime_extension = 1; // no walltime_extension
        }

        // Network:
        rhs.net.bw_backbone = ynode["network"]["bandwidth_backbone"].as<std::string>();
        rhs.net.bw_backbone_storage = ynode["network"]["bandwidth_backbone_storage"].as<std::string>();
        rhs.net.bw_backbone_perm_storage = ynode["network"]["bandwidth_backbone_perm_storage"].as<std::string>();
        rhs.net.bw_backbone_ctrl = ynode["network"]["bandwidth_backbone_ctrl"].as<std::string>();
        rhs.net.link_latency = ynode["network"]["link_latency"].as<std::string>();

        // Torus
        rhs.compute.max_compute_nodes = ynode["torus"]["max_compute_nodes"].as<unsigned int>();
        rhs.compute.core_count = ynode["torus"]["core_count"].as<int>();
        rhs.compute.ram = ynode["torus"]["ram"].as<unsigned int>();
        rhs.compute.flops = ynode["torus"]["flops"].as<std::string>();

        // Storage system (PFS)
        rhs.stor.read_variability = ynode["storage"]["read_variability"].as<float>();
        if (rhs.stor.read_variability < 0 || rhs.stor.read_variability > 1) {
            WRENCH_WARN("read_variability should be bounded in [0, 1]");
            return false;
        }
        rhs.stor.write_variability = ynode["storage"]["write_variability"].as<float>();
        if (rhs.stor.write_variability < 0 || rhs.stor.write_variability > 1) {
            WRENCH_WARN("write_variability should be bounded in [0, 1]");
            return false;
        }
        rhs.stor.non_linear_coef_read = ynode["storage"]["non_linear_coef_read"].as<float>();
        if (rhs.stor.non_linear_coef_read < 0) {
            WRENCH_WARN("non_linear_coef_read should be > 0");
            return false;
        }
        rhs.stor.non_linear_coef_write = ynode["storage"]["non_linear_coef_write"].as<float>();
        if (rhs.stor.non_linear_coef_write < 0) {
            WRENCH_WARN("non_linear_coef_write should be > 0");
            return false;
        }
        rhs.stor.read_node_param = ynode["storage"]["read_node_param"].as<float>();   // currently threshold on the cumul BW (r/w)
        rhs.stor.write_node_param = ynode["storage"]["write_node_param"].as<float>(); // currently threshold on the cumul BW (r/w)

        rhs.stor.io_buffer_size = ynode["storage"]["io_buffer_size"].as<std::string>();

        rhs.stor.read_bytes_preload_thres = ynode["storage"]["read_bytes_preload_thres"].as<uint64_t>();
        rhs.stor.write_bytes_copy_thres = ynode["storage"]["write_bytes_copy_thres"].as<uint64_t>();

        // rhs.stor.cleanup_threshold = ynode["storage"]["cleanup_threshold"].as<float>();

        for (const auto node : ynode["storage"]["nodes"]) {

            fives::NodeEntry node_entry = {};
            node_entry.qtt = node["quantity"].as<unsigned int>();

            auto yaml_node_template = node["template"];
            node_entry.tpl.id = yaml_node_template["id"].as<std::string>();

            // Loop through disk entries in node template
            for (const auto &yaml_disk_entry : yaml_node_template["disks"]) {

                fives::DiskEntry disk_entry = {};
                disk_entry.qtt = yaml_disk_entry["quantity"].as<unsigned int>();
                disk_entry.tpl.id = yaml_disk_entry["template"]["id"].as<std::string>();
                disk_entry.tpl.capacity = yaml_disk_entry["template"]["capacity"].as<unsigned int>();
                disk_entry.tpl.read_bw = yaml_disk_entry["template"]["read_bw"].as<unsigned int>();
                disk_entry.tpl.write_bw = yaml_disk_entry["template"]["write_bw"].as<unsigned int>();
                disk_entry.tpl.mount_prefix = yaml_disk_entry["template"]["mount_prefix"].as<std::string>();

                // Add the disk entry to the node template
                node_entry.tpl.disks.push_back(disk_entry);
                // Add the node template to the list of node templates
                rhs.stor.node_templates[node_entry.tpl.id] = node_entry.tpl;
                // Add the disk template to the list of disks templates
                rhs.stor.disk_templates[disk_entry.tpl.id] = disk_entry.tpl;
            }

            rhs.stor.nodes.push_back(node_entry);
        }

        // Storage system (permanent)
        rhs.pstor.r_bw = ynode["permanent_storage"]["read_bw"].as<std::string>();
        rhs.pstor.w_bw = ynode["permanent_storage"]["write_bw"].as<std::string>();
        rhs.pstor.capa = ynode["permanent_storage"]["capacity"].as<std::string>();
        rhs.pstor.mount_prefix = ynode["permanent_storage"]["mount_prefix"].as<std::string>();
        rhs.pstor.read_path = ynode["permanent_storage"]["read_path"].as<std::string>();
        rhs.pstor.write_path = ynode["permanent_storage"]["write_path"].as<std::string>();
        rhs.pstor.disk_id = ynode["permanent_storage"]["disk_id"].as<std::string>();
        rhs.pstor.io_buffer_size = ynode["permanent_storage"]["io_buffer_size"].as<std::string>();

        // Allocator callback
        auto alloc = ynode["allocator"].as<std::string>();
        if (alloc == "lustre") {
            rhs.allocator = fives::AllocatorType::Lustre;

            auto lustreConfig = fives::LustreConfig();

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

            // STRIPE_COUNT FOR HIGH BW (WRITES)
            if (ynode["lustre"]["stripe_count_high_thresh_write"].IsDefined()) {

                lustreConfig.stripe_count_high_thresh_write = ynode["lustre"]["stripe_count_high_thresh_write"].as<uint64_t>();

                if (ynode["lustre"]["stripe_count_high_write_add"].IsDefined()) {
                    lustreConfig.stripe_count_high_write_add = ynode["lustre"]["stripe_count_high_write_add"].as<uint64_t>();
                } else {
                    WRENCH_INFO("Using default value for lustre.stripe_count_high_write_add : %u", lustreConfig.stripe_count_high_write_add);
                }
            } else {
                WRENCH_INFO("No value set for 'stripe_count_high_thresh_write' (ignoring setting and alt. stripe_count)");
            }

            // STRIPE_COUNT FOR HIGH BW (READS)
            if (ynode["lustre"]["stripe_count_high_thresh_read"].IsDefined()) {

                lustreConfig.stripe_count_high_thresh_read = ynode["lustre"]["stripe_count_high_thresh_read"].as<uint64_t>();

                if (ynode["lustre"]["stripe_count_high_read_add"].IsDefined()) {
                    lustreConfig.stripe_count_high_read_add = ynode["lustre"]["stripe_count_high_read_add"].as<uint64_t>();
                } else {
                    WRENCH_INFO("Using default value for lustre.stripe_count_high_read_add : %u", lustreConfig.stripe_count_high_read_add);
                }

            } else {
                WRENCH_INFO("No value set for 'stripe_count_high_thresh_read' (ignoring setting and alt. stripe_count)");
            }
            if (ynode["lustre"]["max_chunks_per_ost"].IsDefined()) {
                lustreConfig.max_chunks_per_ost = ynode["lustre"]["max_chunks_per_ost"].as<uint64_t>();
            } else {
                WRENCH_INFO("Using default value for lustre.max_chunks_per_ost : %lu", lustreConfig.max_chunks_per_ost);
            }

            rhs.lustre = lustreConfig;

        } else if (alloc == "rr") {
            rhs.allocator = fives::AllocatorType::GenericRR;
        }

        // Output (for metric files)
        rhs.out.io_actions_prefix = ynode["outputs"]["io_actions_prefix"].as<std::string>();
        rhs.out.job_filename_prefix = ynode["outputs"]["job_filename_prefix"].as<std::string>();
        rhs.out.storage_svc_prefix = ynode["outputs"]["storage_svc_prefix"].as<std::string>();
        if ((rhs.out.io_actions_prefix == "") || (rhs.out.job_filename_prefix == "") || (rhs.out.storage_svc_prefix == "")) {
            WRENCH_WARN("At least one of the filename prefix in the configuration is an empty string");
            return false;
        }

    } catch (YAML::ParserException &e) {
        WRENCH_WARN("Unable to parse configuration file : %s", e.what());
        return false;
    }

    WRENCH_INFO("Configuration successfully parsed");
    return true;
}