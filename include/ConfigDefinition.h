/**
 *  This is an entry for a job in our YAML config file.
 *
 */

#ifndef CFGDEFINITION_H
#define CFGDEFINITION_H

#include "yaml-cpp/yaml.h"
#include <cstdint>
#include <string>

namespace storalloc {

    /**
     * @brief Configuration specific to the Lustre Allocator
     */
    struct LustreConfig {
        uint64_t lq_threshold_rr = 43;
        uint64_t lq_prio_free = 232;
        uint64_t max_nb_ost = 2000;
        uint64_t max_inodes = (1ULL << 32);
        uint64_t stripe_size = 50000000;
    };

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

    enum AllocatorType {
        Lustre,    // 'lustre' in config
        GenericRR, // 'rr in config
    };

    struct Config {
        std::string config_name;
        std::string config_version;
        std::string bkbone_bw;
        std::string perm_storage_r_bw;
        std::string perm_storage_w_bw;
        std::string perm_storage_capa;
        unsigned int max_stripe_size;
        unsigned int d_groups;
        unsigned int d_group_links;
        unsigned int d_chassis;
        unsigned int d_chassis_links;
        unsigned int d_routers;
        unsigned int d_router_links;
        unsigned int d_nodes;
        unsigned int core_count; // eg. 64
        std::string ram;         // eg. '192GB'
        bool local_storage;
        unsigned int ls_disks;
        std::string ls_disks_capa;
        std::string ls_disks_read_bw;
        std::string ls_disks_write_bw;
        std::map<std::string, DiskTemplate> disk_templates;
        std::map<std::string, NodeTemplate> node_templates;
        std::vector<NodeEntry> nodes;
        enum AllocatorType allocator;
        struct LustreConfig lustre;
    };

    bool operator==(const Config &lhs, const Config &rhs);
} // namespace storalloc

namespace YAML {

    template <>
    struct convert<storalloc::Config> {
        static bool decode(const Node &node, storalloc::Config &rhs);
    };

} // namespace YAML

#endif // CFGDEFINITION_H