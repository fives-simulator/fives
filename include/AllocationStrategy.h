#ifndef ALLOCATION_STRATEGY_H
#define ALLOCATION_STRATEGY_H

#include <memory>

#include <wrench-dev.h>

namespace storalloc {

    std::vector<std::shared_ptr<wrench::FileLocation>> simpleRRStrategy(
        const std::shared_ptr<wrench::DataFile>& file, 
        const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>& resources,
        const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>>& mapping,
        const std::vector<std::shared_ptr<wrench::FileLocation>>& previous_allocations);

    std::vector<std::shared_ptr<wrench::FileLocation>> lustreStrategy(
        const std::shared_ptr<wrench::DataFile>& file, 
        const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>& resources,
        const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>>& mapping,
        const std::vector<std::shared_ptr<wrench::FileLocation>>& previous_allocations);

} // storalloc

#endif // ALLOCATION_STRATEGY_H