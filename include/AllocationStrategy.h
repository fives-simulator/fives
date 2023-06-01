#ifndef ALLOCATION_STRATEGY_H
#define ALLOCATION_STRATEGY_H

#include <memory>

#include <wrench-dev.h>

namespace storalloc {

    std::vector<std::shared_ptr<wrench::FileLocation>> genericRRStrategy(
        const std::shared_ptr<wrench::DataFile>& file, 
        const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>& resources,
        const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>>& mapping,
        const std::vector<std::shared_ptr<wrench::FileLocation>>& previous_allocations);

    /** @brief  Main entry point for the implementation of Lustre allocation strategy
     *          which redirects either to the Round-Robin allocator (lustreRRStrategy) 
     *          or the weighted allocator (lustreWeightedStrategy)
     *      
     *          The choice of the allocator is decided by the imbalance in terms of free space
     *          between "OST" (modeled by SimpleStorageServices with one disk in our case)
     */
    std::vector<std::shared_ptr<wrench::FileLocation>> lustreStrategy(
        const std::shared_ptr<wrench::DataFile>& file, 
        const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>& resources,
        const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>>& mapping,
        const std::vector<std::shared_ptr<wrench::FileLocation>>& previous_allocations);

    std::vector<std::shared_ptr<wrench::FileLocation>> lustreRRAllocator(
        const std::shared_ptr<wrench::DataFile>& file, 
        const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>& resources,
        const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>>& mapping,
        const std::vector<std::shared_ptr<wrench::FileLocation>>& previous_allocations);

    std::vector<std::shared_ptr<wrench::FileLocation>> lustreWeightedAllocator(
        const std::shared_ptr<wrench::DataFile>& file, 
        const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>& resources,
        const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>>& mapping,
        const std::vector<std::shared_ptr<wrench::FileLocation>>& previous_allocations);


    // "Magic" default Lustre settings, that equals to computing whether or not the free space
    // diff between OSTs is greater thant 17% or not
    const auto LUSTRE_lq_threshold_rr = 43;
    // In Lustre, this priority represents how important 
    // is free space compared to using a wide array of targets
    // Here we use the default priority, which balanced towards free space (91%)
    const auto LUSTRE_lq_prio_free = 232;
    const auto LUSTRE_prio_wide = 256;

    struct ba_min_max {
        uint64_t min;
        uint64_t max;
    };

    ba_min_max compute_min_max_utilization(const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>&); 

    bool lustre_use_rr(struct ba_min_max ba_min_max);

} // storalloc

#endif // ALLOCATION_STRATEGY_H