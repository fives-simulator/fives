/**
 *  Definitions of configuration blocks to interface the Yaml configuration file and the simulation.
 *
 */

#ifndef CFGDEFINITION_H
#define CFGDEFINITION_H

#include "yaml-cpp/yaml.h"
#include <cstdint>
#include <string>

namespace fives {

    /**
     * @brief Configuration specific to the Lustre Allocator
     *        Some of the fields are directly correlated to Lustre's internal parameters
     *        Others (max_chunks_per_ost) are used to speed up simulation at the cost of accuracy
     *
     *        Also note that stripe_size and stripe_count are normally selected on a per-file /
     *        per-directory basis by the clients, but we're are not expecting to have this
     *        information in the simulation, and set it system-wide in the configuration instead.
     */
    struct LustreConfig {
        uint64_t lq_threshold_rr = 43;      // Internal Lustre parameter (not often changed)
        uint64_t lq_prio_free = 232;        // Internal Lustre parameter (not often changed)
        uint64_t max_nb_ost = 2000;         // Lustre limit - not really used so far
        uint64_t max_inodes = (1ULL << 32); // Unix limit - not really used so far
        uint64_t stripe_size = 2097152;     // 2MiB by default
        uint64_t stripe_count = 1;          // One OST by default
        uint64_t max_chunks_per_ost = 10;   // Maximum number of chunks per OST for each allocation (for simulation speed-up purpose)
        uint16_t stripe_count_high_write_add = 2;
        uint16_t stripe_count_high_read_add = 2;
        uint64_t stripe_count_high_thresh_write = 0;
        uint64_t stripe_count_high_thresh_read = 0;
    };

    /**
     * @brief Definition of a single disk type (or array of disks).
     */
    struct DiskTemplate {
        std::string id;           // Can be used in simulation to name the disks
        unsigned int capacity;    // In GB
        unsigned int read_bw;     // In MBps
        unsigned int write_bw;    // In MBps
        std::string mount_prefix; // Disk mount point in a storage service

        std::string to_string() const;
    };

    /**
     * @brief Placeholder for a quantity of a given disk type, inside a storage node
     *        This is the actual structure used when creating the disks of a storage node.
     */
    struct DiskEntry {
        unsigned int qtt; // Number of disks
        DiskTemplate tpl; // Type of disks

        std::string to_string() const;
    };

    /**
     * @brief Definition of a storage node type
     */
    struct NodeTemplate {
        std::string id;               // Can be used in simulation to name the node
        std::vector<DiskEntry> disks; // Array of disks quantity / type found inside the node

        std::string to_string() const;
    };

    /**
     * @brief Placeholder for a quantity of a given node type. One to many of such
     *        structures make up for a given storage system (depending on homogeneity
     *        or heterogeneity of storage resources).
     */
    struct NodeEntry {
        unsigned int qtt;
        NodeTemplate tpl;

        std::string to_string() const;
    };

    /**
     *  AllocatorType selection. Currently used inside the configuration itself, to make
     *  sure all required parameters are provided
     */
    enum AllocatorType {
        Lustre,    // 'lustre' in config
        GenericRR, // 'rr' in config
    };

    struct NetworkCfg {
        std::string bw_backbone;
        std::string bw_backbone_storage;
        std::string bw_backbone_perm_storage;
        std::string bw_backbone_ctrl;
        std::string link_latency;
    };

    struct StorageCfg {

        float read_variability;      // Mean of the normal distribution used in disk read bandwidth variability - eg. 0.8
        float write_variability;     // Mean of the normal distribution used in disk write bandwidth variability - eg. 0.6
        float non_linear_coef_read;  // Reduction coefficient for read bandwidth of disks in presence of concurrent read - eg. 0.8
        float non_linear_coef_write; // Reduction coefficient for read bandwidth of disks in presence of concurrent read - eg. 0.6

        unsigned int nb_files_per_read;  // How many files should be used to represent the read amount of each job
        unsigned int nb_files_per_write; // How many files should be used to represent the write amount of each job

        uint64_t read_node_thres;
        uint64_t write_node_thres;

        // Parameters for Gompertz function used to determine how many
        // nodes are participating in the I/O operations of a job.
        float read_node_inflection_param; // Typically between 1 and 10000
        float read_node_rate_param;       // TYpically between 1e-2 and 1
        float write_node_inflection_param;
        float write_node_rate_param;

        // Parameters for Gompertz function used to determine how many
        // nodes are participating in the I/O operations of a job.
        float read_sc_inflection_param; // Typically between 1 and 10000
        float read_sc_rate_param;       // TYpically between 1e-2 and 1
        float write_sc_inflection_param;
        float write_sc_rate_param;

        std::string io_buffer_size;

        uint64_t read_bytes_preload_thres;
        uint64_t write_bytes_copy_thres;

        unsigned int static_read_overhead_seconds;
        unsigned int static_write_overhead_seconds;

        float cleanup_threshold;

        std::map<std::string, DiskTemplate> disk_templates;
        std::map<std::string, NodeTemplate> node_templates;
        std::vector<NodeEntry> nodes;
    };

    struct PermStorageCfg {
        std::string r_bw; // Read bandwidth of the external storage zone (outside HPC machine) - eg. "1000MBps"
        std::string w_bw; // Write bandwidth of the external storage zone (outside HPC machine) - eg. "1000MBps"
        std::string capa; // Capacity of external storage (currently doesn't matter as long as it's not limiting) - eg. "10000TB"
        std::string mount_prefix;
        std::string read_path;
        std::string write_path;
        std::string disk_id;
        std::string io_buffer_size;
    };

    struct ComputeCfg {
        unsigned int d_groups;          // Dragonfly compute zone sizing
        unsigned int d_group_links;     // Dragonfly compute zone sizing
        unsigned int d_chassis;         // Dragonfly compute zone sizing
        unsigned int d_chassis_links;   // Dragonfly compute zone sizing
        unsigned int d_routers;         // Dragonfly compute zone sizing
        unsigned int d_router_links;    // Dragonfly compute zone sizing
        unsigned int d_nodes;           // Dragonfly compute zone sizing
        unsigned int max_compute_nodes; // Max number of compute nodes to create, regardless of the total dragonfly size
        unsigned int core_count;        // Core count on each compute node - eg. 64
        unsigned int ram;               // Ram on each compute node - eg. '192' (in GB)
        std::string flops;

        bool local_storage;            // Enable or disable node local storage creation on compute nodes - NOT IMPLEMENTED in simulation
        unsigned int ls_disks;         // Node local storage settings
        std::string ls_disks_capa;     // Node local storage settings
        std::string ls_disks_read_bw;  // Node local storage settings
        std::string ls_disks_write_bw; // Node local storage settings
    };

    struct OutputCfg {
        std::string job_filename_prefix;
        std::string io_actions_prefix;
        std::string storage_svc_prefix;
    };

    /**
     * @brief Main configuration structure
     */
    struct Config {
        std::string config_name;
        std::string config_version;
        unsigned int max_stripe_size; // Maximum stripe size in bytes (when files are stripped by the CSS, and not inside a specifid allocator)
        float preload_percent;        // Number of preload jobs to create from job dataset, as a percentage of the number of jobs in the dataset
        bool testing = false;         // Whether the simulation should run in testing mode or not (used in unit tests)
        bool debug = false;           // Whether to run the simulation in debug mode (NOT IMPLEMENTED YET)
        float walltime_extension;     // Coefficient to increase or decrease the original walltime of jobs - eg. 1.2
        float allowed_failure_percent = 0.1;

        ComputeCfg compute;
        NetworkCfg net;
        StorageCfg stor;
        PermStorageCfg pstor;
        OutputCfg out;

        enum AllocatorType allocator;
        struct LustreConfig lustre; // May be empty if 'allocator' is not 'Lustre'
    };

    bool operator==(const Config &lhs, const Config &rhs);
} // namespace fives

namespace YAML {

    template <>
    struct convert<fives::Config> {
        static bool decode(const Node &node, fives::Config &rhs);
    };

} // namespace YAML

#endif // CFGDEFINITION_H