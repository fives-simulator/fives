#ifndef ALLOCATION_STRATEGY_H
#define ALLOCATION_STRATEGY_H

#include <cmath>
#include <memory>

#include <wrench-dev.h>

#include "ConfigDefinition.h"

namespace fives {

    class GenericRRAllocator {

    public:
        std::vector<std::shared_ptr<wrench::FileLocation>> operator()(
            const std::shared_ptr<wrench::DataFile> &file,
            const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
            const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
            const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations,
            unsigned int stripe_count);
    };

    struct ba_min_max {
        uint64_t min;
        uint64_t max;
    };

    struct striping {
        uint64_t stripe_size_b;       //  Size of a stripe in bytes (ie. how many bytes to write/read to/from an OST before using the next one in the current selection)
        uint64_t max_stripes_per_ost; //  Don't mistake with Lustre's own 'stripes_per_ost' used for overstriping purpose
        uint64_t stripes_count;       //  Number of OSTs used (on how many OSTs to load balance the reads / writes)
    };

    class LustreAllocator {

    public:
        LustreAllocator(std::shared_ptr<fives::Config> config) : config(config), prio_wide(256 - config->lustre.lq_prio_free) {}

        /** @brief  Main entry point for the implementation of Lustre allocation strategy
         *          which redirects either to the Round-Robin allocator (lustreRRStrategy)
         *          or the weighted allocator (lustreWeightedStrategy)
         *
         *          The choice of the allocator is decided by the imbalance in terms of free space
         *          between "OST" (modeled by SimpleStorageServices with one disk in our case)
         */
        std::vector<std::shared_ptr<wrench::FileLocation>> operator()(const std::shared_ptr<wrench::DataFile> &file,
                                                                      const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
                                                                      const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
                                                                      const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations,
                                                                      unsigned int stripe_count);

        std::vector<std::shared_ptr<wrench::FileLocation>> lustreRRAllocator(
            const std::shared_ptr<wrench::DataFile> &file,
            const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
            const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
            const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations,
            unsigned int stripe_count);

        std::vector<std::shared_ptr<wrench::FileLocation>> lustreWeightedAllocator(
            const std::shared_ptr<wrench::DataFile> &file,
            const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
            const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
            const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations,
            unsigned int stripe_count) const;

        /**
         *  HELPER FUNCTION FOR LUSTRE STRATEGIES
         */

        striping lustreComputeStriping(uint64_t file_size_b, size_t number_of_OSTs, unsigned int stripe_count) const;

        std::vector<std::shared_ptr<wrench::FileLocation>> lustreCreateFileParts(const std::shared_ptr<wrench::DataFile> &file,
                                                                                 std::vector<std::shared_ptr<wrench::StorageService>> selectedOSTs,
                                                                                 uint64_t stripeSize) const;

        ba_min_max lustreComputeMinMaxUtilization(const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &) const;

        bool lustreUseRR(struct ba_min_max ba_min_max) const;

        std::vector<std::shared_ptr<wrench::StorageService>> lustreRROrderServices(const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources) const;

        bool lustreOstIsUsed(const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
                             const std::shared_ptr<wrench::StorageService> ost) const;

        uint64_t lustreComputeOstPenalty(uint64_t free_space_b, uint64_t free_inode_count, double active_service_count) const;

        uint64_t lustreComputeOstWeight(uint64_t free_space_b, uint64_t free_inode_count, uint64_t ost_penalty) const;

        uint64_t lustreComputeOssPenalty(uint64_t free_space_b, uint64_t free_inode_count, size_t ost_count, size_t oss_count) const;

        std::shared_ptr<fives::Config> config;

        std::vector<std::shared_ptr<wrench::StorageService>> static_rr_ordered_services;

        unsigned int start_count = 0;

        unsigned int start_ost_index = 0;

        uint64_t prio_wide;
    };

}; // namespace fives

#endif // ALLOCATION_STRATEGY_H