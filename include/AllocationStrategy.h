#ifndef ALLOCATION_STRATEGY_H
#define ALLOCATION_STRATEGY_H

#include <memory>
#include <cmath>

#include <wrench-dev.h>

namespace storalloc {

    std::vector<std::shared_ptr<wrench::FileLocation>> genericRRStrategy(
        const std::shared_ptr<wrench::DataFile>& file, 
        const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>& resources,
        const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>>& mapping,
        const std::vector<std::shared_ptr<wrench::FileLocation>>& previous_allocations);


    struct LustreAllocationStrategy {

        /** @brief  Main entry point for the implementation of Lustre allocation strategy
         *          which redirects either to the Round-Robin allocator (lustreRRStrategy) 
         *          or the weighted allocator (lustreWeightedStrategy)
         *      
         *          The choice of the allocator is decided by the imbalance in terms of free space
         *          between "OST" (modeled by SimpleStorageServices with one disk in our case)
         */
        static std::vector<std::shared_ptr<wrench::FileLocation>> lustreStrategy(
            const std::shared_ptr<wrench::DataFile>& file, 
            const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>& resources,
            const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>>& mapping,
            const std::vector<std::shared_ptr<wrench::FileLocation>>& previous_allocations);

        static std::vector<std::shared_ptr<wrench::FileLocation>> lustreRRAllocator(
            const std::shared_ptr<wrench::DataFile>& file, 
            const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>& resources,
            const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>>& mapping,
            const std::vector<std::shared_ptr<wrench::FileLocation>>& previous_allocations);

        static std::vector<std::shared_ptr<wrench::FileLocation>> lustreWeightedAllocator(
            const std::shared_ptr<wrench::DataFile>& file, 
            const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>& resources,
            const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>>& mapping,
            const std::vector<std::shared_ptr<wrench::FileLocation>>& previous_allocations);


        /** 
         *  CONST FOR LUSTRE STRATEGIES
        */


        /** 
         *  HELPER FUNCTION FOR LUSTRE STRATEGIES
        */

        struct ba_min_max {
            uint64_t min;
            uint64_t max;
        };

        struct striping {
            unsigned int stripe_size_b;             //  Size of a stripe in bytes
            unsigned int stripes_per_ost;            //  Number of stripes on each OST
            unsigned int stripes_count;              //  Total number of stripes
        };

        static ba_min_max lustreComputeMinMaxUtilization(const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>&); 

        static bool lustreUseRR(struct ba_min_max ba_min_max);

        static std::vector<std::shared_ptr<wrench::StorageService>> lustreRROrderServices(const std::map<std::string, int> &hostname_to_service_count, 
                                                                            const std::vector<std::shared_ptr<wrench::StorageService>>& disk_level_services);

        static striping lustreComputeStriping(double file_size_b, size_t number_of_OSTs);

        static bool lustreOstIsUsed(const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>>& mapping, const std::shared_ptr<wrench::StorageService> ost);

        static std::vector<std::shared_ptr<wrench::FileLocation>> lustreCreateFileParts(const std::string &file_id, std::map<int, std::shared_ptr<wrench::StorageService>> temp_allocations);

        static uint64_t lustreComputeOstPenalty(uint64_t free_space_b, uint64_t free_inode_count, double active_service_count);

        static uint64_t lustreComputeOstWeight(uint64_t free_space_b, uint64_t free_inode_count, uint64_t ost_penalty);

        static uint64_t lustreComputeOssPenalty(uint64_t free_space_b, uint64_t free_inode_count, size_t ost_count, size_t oss_count);

    };


    extern uint64_t LUSTRE_lq_threshold_rr;
    extern uint64_t LUSTRE_lq_prio_free;
    extern uint64_t LUSTRE_prio_wide;
    extern uint64_t LUSTRE_max_nb_ost;
    extern uint64_t LUSTRE_stripe_size;
    extern uint64_t LUSTRE_max_inodes;

} // storalloc

#endif // ALLOCATION_STRATEGY_H