#ifndef ALLOCATION_STRATEGY_H
#define ALLOCATION_STRATEGY_H

#include <cmath>
#include <memory>

#include <wrench-dev.h>

#include "ConfigDefinition.h"

namespace storalloc {

    class GenericRRAllocator : public wrench::StorageAllocator {

    public:
        std::vector<std::shared_ptr<wrench::FileLocation>> allocate(
            const std::shared_ptr<wrench::DataFile> &file,
            const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
            const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
            const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations) override;
    };

    struct ba_min_max {
        uint64_t min;
        uint64_t max;
    };

    struct striping {
        uint64_t stripe_size_b;           //  Size of a stripe in bytes (ie. how many bytes to write/read to/from an OST before using the next one in the current selection)
        unsigned int stripes_per_ost = 1; //  Number of stripes on each OST (used only in PATTERN_OVERSTRIPING case, default to 1)
        unsigned int stripes_count;       //  Number of OSTs used (on how many OSTs to load balance the reads / writes)
    };

    class LustreAllocator : public wrench::StorageAllocator {

    public:
        LustreAllocator(std::shared_ptr<Config> config) : config(config), prio_wide(256 - config->lustre.lq_prio_free){};

        /** @brief  Main entry point for the implementation of Lustre allocation strategy
         *          which redirects either to the Round-Robin allocator (lustreRRStrategy)
         *          or the weighted allocator (lustreWeightedStrategy)
         *
         *          The choice of the allocator is decided by the imbalance in terms of free space
         *          between "OST" (modeled by SimpleStorageServices with one disk in our case)
         */
        std::vector<std::shared_ptr<wrench::FileLocation>> allocate(
            const std::shared_ptr<wrench::DataFile> &file,
            const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
            const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
            const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations) override;

        std::vector<std::shared_ptr<wrench::FileLocation>> lustreRRAllocator(
            const std::shared_ptr<wrench::DataFile> &file,
            const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
            const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
            const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations);

        std::vector<std::shared_ptr<wrench::FileLocation>> lustreWeightedAllocator(
            const std::shared_ptr<wrench::DataFile> &file,
            const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
            const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
            const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations);

        /**
         *  HELPER FUNCTION FOR LUSTRE STRATEGIES
         */

        striping lustreComputeStriping(uint64_t file_size_b, size_t number_of_OSTs);

        std::vector<std::shared_ptr<wrench::FileLocation>> lustreCreateFileParts(const std::string &file_id, std::map<int, std::shared_ptr<wrench::StorageService>> temp_allocations, storalloc::striping &striping);

        ba_min_max lustreComputeMinMaxUtilization(const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &);

        bool lustreUseRR(struct ba_min_max ba_min_max);

        std::vector<std::shared_ptr<wrench::StorageService>> lustreRROrderServices(const std::map<std::string, int> &hostname_to_service_count,
                                                                                   const std::vector<std::shared_ptr<wrench::StorageService>> &disk_level_services);

        bool lustreOstIsUsed(const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping, const std::shared_ptr<wrench::StorageService> ost);

        uint64_t lustreComputeOstPenalty(uint64_t free_space_b, uint64_t free_inode_count, double active_service_count);

        uint64_t lustreComputeOstWeight(uint64_t free_space_b, uint64_t free_inode_count, uint64_t ost_penalty);

        uint64_t lustreComputeOssPenalty(uint64_t free_space_b, uint64_t free_inode_count, size_t ost_count, size_t oss_count);

        // shared config from main simulation
        std::shared_ptr<Config> config = nullptr;

        uint64_t prio_wide;
    };

}; // namespace storalloc

#endif // ALLOCATION_STRATEGY_H