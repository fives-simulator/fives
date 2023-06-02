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


    /** 
     *  CONST FOR LUSTRE STRATEGIES
    */

    // "Magic" default Lustre settings, that equals to computing whether or not the free space
    // diff between OSTs is greater thant 17% or not
    const auto LUSTRE_lq_threshold_rr = 43;
    // In Lustre, this priority represents how important 
    // is free space compared to using a wide array of targets
    // Here we use the default priority, which balanced towards free space (91%)
    const auto LUSTRE_lq_prio_free = 232;
    const auto LUSTRE_prio_wide = 256 - LUSTRE_lq_prio_free;
    const auto LUSTRE_max_nb_ost = 2000;    // never stripe on more than 2000 OSTs, that's the Lustre limit when using ZFS.
         
    /** Currently, the stripe size is arbitrarily set to 512MB (recommended size is between 1-4MB, and max is 4GB, but anyway, our allocations
     *  do not necessarily represent files...)
     */
    const double LUSTRE_stripe_size = 512000000;    // how much data is written to a given OST before moving to the next one (512MB in this case)
    

    /** 
     *  HELPER FUNCTION FOR LUSTRE STRATEGIES
    */

    struct ba_min_max {
        uint64_t min;
        uint64_t max;
    };

    ba_min_max lustreComputeMinMaxUtilization(const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>&); 

    bool lustreUseRR(struct ba_min_max ba_min_max);

    std::vector<std::shared_ptr<wrench::StorageService>> lustreRROrderServices(const std::map<std::string, int> &hostname_to_service_count, 
                                                                           const std::vector<std::shared_ptr<wrench::StorageService>>& disk_level_services);

    unsigned int lustreComputeStripesPerOST(double file_size_b, double stripe_size, size_t number_of_OSTs, double stripe_count);

    bool lustreOstIsUsed(const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>>& mapping, const std::shared_ptr<wrench::StorageService> ost);

    std::vector<std::shared_ptr<wrench::FileLocation>> lustreCreateFileParts(const std::string &file_id, std::map<int, std::shared_ptr<wrench::StorageService>> temp_allocations);

    uint64_t lustreComputeOstPenalty(uint64_t free_space_b, uint64_t free_inode_count, double active_service_count);

    uint64_t lustreComputeOstWeight(uint64_t free_space_b, uint64_t free_inode_count, uint64_t ost_penalty);

    uint64_t lustreComputeOssPenalty(uint64_t free_space_b, uint64_t free_inode_count, size_t ost_count, size_t oss_count);

} // storalloc

#endif // ALLOCATION_STRATEGY_H