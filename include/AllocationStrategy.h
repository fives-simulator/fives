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

} // storalloc

#endif // ALLOCATION_STRATEGY_H