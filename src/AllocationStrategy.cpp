#include <random>
#include <cstdint>

#include "AllocationStrategy.h"


std::vector<std::shared_ptr<wrench::FileLocation>> storalloc::genericRRStrategy(
    const std::shared_ptr<wrench::DataFile>& file, 
    const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>& resources,
    const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>>& mapping,
    const std::vector<std::shared_ptr<wrench::FileLocation>>& previous_allocations) {

    // Init round-robin
    static auto last_selected_server = resources.begin()->first;
    static auto internal_disk_selection = 0;
    auto capacity_req = file->getSize();
    std::shared_ptr<wrench::FileLocation> designated_location = nullptr;
    auto current = resources.find(last_selected_server);
    auto current_disk_selection = internal_disk_selection;

    auto continue_disk_loop = true;

    do {

        // std::cout << "Considering disk index " << std::to_string(current_disk_selection) << std::endl;
        auto nb_of_local_disks = current->second.size();
        auto storage_service = current->second[current_disk_selection % nb_of_local_disks];
        // std::cout << "- Looking at storage service " << storage_service->getName() << std::endl;

        auto free_space = storage_service->getTotalFreeSpace();
        // std::cout << "- It has " << free_space << "B of free space" << std::endl;

        if (free_space >= capacity_req) {
            designated_location = wrench::FileLocation::LOCATION(std::shared_ptr<wrench::StorageService>(storage_service), file);
            // std::cout << "Chose server " << current->first << storage_service->getBaseRootPath() << std::endl;
            // Update for next function call
            std::advance(current, 1);
            if (current == resources.end()) {
                current = resources.begin();
                current_disk_selection++;
            }
            last_selected_server = current->first;
            internal_disk_selection = current_disk_selection;
            // std::cout << "Next first server will be " << last_selected_server << std::endl;
            break;
        }

        std::advance(current, 1);
        if (current == resources.end()) {
            current = resources.begin();
            current_disk_selection++;
        }
        if (current_disk_selection > (internal_disk_selection + nb_of_local_disks + 1)) {
            // std::cout << "Stopping continue_disk_loop" << std::endl;
            continue_disk_loop = false;
        }
        // std::cout << "Next server will be " << current->first << std::endl;
    } while ((current->first != last_selected_server) or (continue_disk_loop));

    if (designated_location)
        // std::cout << "genericRRStrategy has completed" << std::endl;
        return std::vector<std::shared_ptr<wrench::FileLocation>>{designated_location};
    else
        // std::cout << "genericRRStrategy has completed with failure" << std::endl;
        return std::vector<std::shared_ptr<wrench::FileLocation>>();

}


/**
 * @brief Lustre-inspired allocation algorithm (round-robin with a few tweaks)
 *        meant to be used with the INTERNAL_STRIPING property of the CSS set to false
 *        (striping is done here).
 * 
*/
std::vector<std::shared_ptr<wrench::FileLocation>> storalloc::lustreStrategy(
        const std::shared_ptr<wrench::DataFile>& file, 
        const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>& resources,
        const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>>& mapping,
        const std::vector<std::shared_ptr<wrench::FileLocation>>& previous_allocations) {


    // Check for inbalance in the storage services utilization
    uint64_t ba_min = -1;       // overflowing on purpose to get max(uint64_t)
	uint64_t ba_max = 0;    

    for (const auto & resource : resources) {
        for (const auto & service : resource.second) {
            auto ss_service = dynamic_pointer_cast<wrench::SimpleStorageService>(service);
            uint64_t current_free_space = ss_service->traceTotalFreeSpace();
            current_free_space >>= 8;       // used in Lustre code to prevent overflows, we're blindly doing the same
            ba_min = min(current_free_space, ba_min);
            ba_max = max(current_free_space, ba_min);
        }
    }

    // "Magic" default Lustre settings, that equals to computing whether or not the free space
    // diff between OSTs is greater thant 17% or not
    const auto lq_threshold_rr = 43;

    if ((ba_max * (256 - lq_threshold_rr)) >> 8 < ba_min) {
        // Consider that every target has roughly the same free space, and use RR allocator
        std::cout << "[lustreStrategy] Using RR allocator" << std::endl;
        return storalloc::lustreRRAllocator(file, resources, mapping, previous_allocations);
	} else {
        // Consider that targets free space use is too much imbalanced and go for the weighted allocator
        std::cout << "[lustreStrategy] Using weighted allocator" << std::endl;
        return storalloc::lustreWeightedAllocator(file, resources, mapping, previous_allocations);
    }
   
}



std::vector<std::shared_ptr<wrench::FileLocation>> storalloc::lustreRRAllocator(
    const std::shared_ptr<wrench::DataFile>& file, 
    const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>& resources,
    const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>>& mapping,
    const std::vector<std::shared_ptr<wrench::FileLocation>>& previous_allocations) {

    // ## First build a list of services containing only up / non-full services and ordered for use with rr
    // This is roughly equivalent to lod_qos.c::lod_qos_calc_rr() function in Lustre sources.

    std::map<std::string, int> hostname_to_service_number;                          // OSS in Lustre
    std::vector<std::shared_ptr<wrench::StorageService>> disk_level_services;       // OST in Lustre
    for (const auto &host : resources) {
        disk_level_services.insert(disk_level_services.end(), host.second.begin(), host.second.end());
        hostname_to_service_number[host.first] = host.second.size();
    }

    std::vector<std::shared_ptr<wrench::StorageService>> rr_ordered_services(disk_level_services.size());
    fill(rr_ordered_services.begin(), rr_ordered_services.end(), nullptr);
    auto placed_services = 0;

    for (const auto &server : hostname_to_service_number) {

        auto j = 0;
        auto srv_hostname = server.first;
        auto srv_service_count = server.second;

        for (auto i = 0; i < disk_level_services.size(); i++) {
            
            int next;

            if (disk_level_services[i]->getHostname() != srv_hostname) {
                continue;
            }

            next = j * disk_level_services.size() / srv_service_count;
            while (rr_ordered_services[next] != nullptr) {
                next = (next +1) % disk_level_services.size();
            }

            rr_ordered_services[next] = disk_level_services[i];
            j++;
            placed_services++;
        }

    }

    if (placed_services != rr_ordered_services.size()) {
        std::cout << "LustreAlloc: couldn't place all services in the round-robin pre-processed list (missing " << std::to_string(rr_ordered_services.size() - placed_services) << " services)" << std::endl; 
        throw std::runtime_error("Number of placed services differs expected size of the RR-ordered services");
    }


    // ## 2. Use the rr ordered list of services to allocation stripes for the given file 
    // Roughly the equivalent of what happens in lod_qos.c::lod_ost_alloc_rr()

    /** 
     * In Lustre, the user is supposed to provide a striping pattern (stripe size and number of OSTs onto which the stripes will be placed)
     * Here we don't have (nor want) this kind of user input, so we need to derive the stripe size and number of used OSTs from the file 
     * size and the total number of OSTs, with some homemade rule.
     * 
     * Currently, the stripe size is arbitrarily set to 512MB (recommended size is between 1-4MB, and max is 4GB, but anyway, our allocations
     * do not necessarily represent files...)
    */ 
    int stripe_size = 512000000;    // how much data is written to a given OST before moving to the next one (512MB in this case)
    int stripe_count = 1;           // on how many OSTs to allocate stripes (eg: 1 means a single OST receives all, -1 means to stripe over all OSTs, 0 means to use Lustre's default)
    int stripe_per_ost = 1;
    auto file_size_b = file->getSize(); 
    if (file_size_b > stripe_size) {
        stripe_count = std::ceil(file_size_b / stripe_size);        // here we potentially ask for more storage than needed due to the rounding
        if (stripe_count > rr_ordered_services.size()) {
            while(stripe_count > rr_ordered_services.size() * stripe_per_ost) {
                stripe_per_ost++;   // here we don't ask for more storage than needed because the condition of the allocation loop
                                    // (see below) also checks whether all required stripes have been allocated or not.  
            }
        } 
    }
    int max_nb_ost = 2000;          // never stripe on more than 2000 OSTs, that's the Lustre limit when using ZFS.    

    // Index of first OST of the strip pattern
    // (from https://en.cppreference.com/w/cpp/numeric/random/uniform_int_distribution)
    std::random_device rd;
    std::mt19937 gen(rd());     // replace rd() by a static seed for reproducibility
    std::uniform_int_distribution<> distrib(0, rr_ordered_services.size() - 1);
    int start_ost_index = distrib(gen);     // similar to when Lustre chooses the first OST to be used.
    int temp_start_ost_index = start_ost_index;
    int stripe_idx = 0;

    std::cout << "LUSTRE ALLOC DEBUG" << std::endl;
    std::cout << "Start index = " << std::to_string(start_ost_index) << std::endl;
    std::cout << "Stripe per ost = " << std::to_string(stripe_per_ost) << std::endl;
    std::cout << "Stripe count = " << std::to_string(stripe_count) << std::endl;
    std::cout << "Current file_size = " << std::to_string(file_size_b) << std::endl;
    std::cout << "rr_ordered_services.size() = " << std::to_string(rr_ordered_services.size()) << std::endl;

      
    std::map<int, std::shared_ptr<wrench::StorageService>> temp_stripe_locations;
    std::vector<std::shared_ptr<wrench::FileLocation>> designated_locations;

    // Some sort of retry counter (same name as in Lustre but I don't know why they call it 'speed')
    for (auto speed = 0; speed < 2; speed++) {
        for (auto i = 0; i < rr_ordered_services.size() * stripe_per_ost && stripe_idx < stripe_count; i++) {
 
            auto array_idx = temp_start_ost_index % rr_ordered_services.size();
            ++temp_start_ost_index;
            auto current_ost = rr_ordered_services[array_idx];
            std::cout << "LustreAlloc : array_idx = " << std::to_string(array_idx) << std::endl;
            std::cout << "LustreAlloc : current_ost = " << current_ost->getHostname() << std::endl; 
            std::cout << "LustreAlloc : i = " << std::to_string(i) << std::endl;
            std::cout << "LustreAlloc : stripe_idx = " << std::to_string(stripe_idx) << std::endl;
            std::cout << "LustreAlloc : speed = " << std::to_string(speed) << std::endl;

            if (current_ost->traceTotalFreeSpace() < stripe_size or (current_ost->getState() != wrench::S4U_Daemon::State::UP)) {
                // Only keep running storage services associated with non-full disks
                std::cout << "LustreAlloc: skipping OST (total Free space == " << std::to_string(current_ost->traceTotalFreeSpace()) << " and current state == " << current_ost->getState() << ")" << std::endl;
                continue;
            }

            // For the first iteration, avoid services which already have an allocation on them.
            auto used = false;
            if (speed == 0) {
                for (const auto& alloc_mapping : mapping) {
                    auto location_vector = alloc_mapping.second;
                    for (const auto& location : location_vector) {
                        if (location->getStorageService()->getHostname() == current_ost->getHostname())
                            used = true;
                    }
                }
            }

            if (used) {
                std::cout << "LustreAlloc : Skipping OST because another allocation is already using it" << std::endl; 
                continue;
            } else {
                std::cout << "LustreAlloc : Using OST " << current_ost->getHostname() << std::endl;
                temp_stripe_locations[stripe_idx] = current_ost;
                
                stripe_idx++;
            }
        }

        if (stripe_idx == stripe_count) {
            std::cout << "LustreAlloc : Stopping because stripe_idx == stripe_count" << std::endl;
            break;
        } else {
            temp_stripe_locations.clear();
            temp_start_ost_index = start_ost_index;     // back to original index, but this time we'll allow 'slow' OSTs
            stripe_idx = 0;
            std::cout << "LustreAlloc : Starting over because stripe_idx != stripe_count and we reached the condition of this loop" << std::endl;
        }
    }
    
    // If we could allocate every stripe, return an empty vector that will be interpreted as an allocation failure
    if (temp_stripe_locations.size() != stripe_count) {
        std::cout << "LustreAlloc: Expected to allocate " << std::to_string(stripe_count) << " stripes, but could only allocate " << std::to_string(temp_stripe_locations.size()) << " instead" << std::endl;
    } 

    for (const auto& stripe_entry : temp_stripe_locations) {
        auto part = wrench::Simulation::addFile(file->getID() + "_part_" + std::to_string(stripe_entry.first), stripe_size);
        designated_locations.push_back(
            wrench::FileLocation::LOCATION(
                std::shared_ptr<wrench::StorageService>(stripe_entry.second), part
            )
        );
    }

    return designated_locations;
}



/**
 * @brief Lustre-inspired allocation algorithm (round-robin with a few tweaks)
 *        meant to be used with the INTERNAL_STRIPING property of the CSS set to false
 *        (striping is done here).
 * 
*/
std::vector<std::shared_ptr<wrench::FileLocation>> storalloc::lustreWeightedAllocator(
    const std::shared_ptr<wrench::DataFile>& file, 
    const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>& resources,
    const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>>& mapping,
    const std::vector<std::shared_ptr<wrench::FileLocation>>& previous_allocations) {

    // ## First for all services we need to calculate their weight.

    /* TODO: this is probably incorrect  : num_active_service should be the number of
     * ACTIVE targets (i.e. with a current allocation), not the entire number of targets
     * but so far we'll consider that every target is active at any time.
     * 
     * Also /!\ this variable is used throughout the function
     */
    auto num_active_services = 0;
    for (const auto& resource : resources) {
        num_active_services += resource.second.size(); 
    }
        
    // In Lustre, this priority represents how important 
    // is free space compared to using a wide array of targets
    // Here we use the default priority, which balanced towards free space (91%)
    const auto lq_prio_free = 232;
    const auto prio_wide = 256;

    uint64_t total_weight = 0;
    std::map<std::shared_ptr<wrench::StorageService>, uint64_t> weighted_services = {};

    for (const auto & resource : resources) {

        auto srv_free_space = 0;
        auto srv_free_inodes = UINT64_MAX;
        std::map<std::shared_ptr<wrench::StorageService>, uint64_t> weighted_oss_services = {};

        for (const auto & service : resource.second) {

            auto ss_service = dynamic_pointer_cast<wrench::SimpleStorageService>(service);
            uint64_t current_free_space = ss_service->traceTotalFreeSpace();
            srv_free_space += current_free_space;
            uint64_t current_files = ss_service->traceTotalFiles();
            srv_free_inodes -= current_files;

            // Pseudo weight computation, for actual code, see:
            // https://github.com/whamcloud/lustre/blob/a336d7c7c1cd62a5a5213835aa85b8eaa87b076a/lustre/obdclass/lu_tgt_descs.c#L232
            // https://github.com/whamcloud/lustre/blob/a336d7c7c1cd62a5a5213835aa85b8eaa87b076a/lustre/obdclass/lu_tgt_descs.c#L532
            uint64_t weight = (current_free_space >> 16) * ((UINT64_MAX - current_files) >> 8);
            
            // Compute a penalty per OST
            current_free_space >>= 16;     // Bitshift for overflow
            current_files >>= 8;
            uint64_t penalty = ( (prio_wide * current_free_space * current_files) >> 8 ) / num_active_services; 
            penalty >>= 1;

            if (weight < penalty)
                weight = 0;
            else
                weight = weight - penalty;

            // At this point we should also take into account for how long the OST has been IDLE.
            // So far we don't because... it's very complex to get it right in the simulation.

            weighted_oss_services[service] = weight;
        }

        /** Per server penalty: 
         *  ~ prio * current_free_space * current_free_inodes / number of targets in server / (number of server - 1) / 2
         */
        auto server_penalty = ( prio_wide * srv_free_space * srv_free_inodes ) >> 8;
        server_penalty = server_penalty / weighted_oss_services.size() * resources.size();
        server_penalty >>= 1;

        // At this point we should also take into account for how long the OSS has been IDLE.
        // So far we don't because... it's very complex to get it right in the simulation.
        
        // Update all weights with server penalty
        for (auto & oss_service : weighted_oss_services) {
            if (oss_service.second < server_penalty) 
                oss_service.second = 0;
            else
                oss_service.second -= server_penalty;
            
            total_weight += oss_service.second;
        }

        // Update global weight map
        weighted_services.insert(weighted_oss_services.begin(), weighted_oss_services.end());
    }


    // ## Secondly, pick OST at random, but favor larger weights
    // https://github.com/whamcloud/lustre/blob/a336d7c7c1cd62a5a5213835aa85b8eaa87b076a/lustre/lod/lod_qos.c#L1503

    /** 
     * In Lustre, the user is supposed to provide a striping pattern (stripe size and number of OSTs onto which the stripes will be placed)
     * Here we don't have (nor want) this kind of user input, so we need to derive the stripe size and number of used OSTs from the file 
     * size and the total number of OSTs, with some homemade rule.
     * 
     * Currently, the stripe size is arbitrarily set to 512MB (recommended size is between 1-4MB, and max is 4GB, but anyway, our allocations
     * do not necessarily represent files...)
    */ 
    int stripe_size = 512000000;    // how much data is written to a given OST before moving to the next one (512MB in this case)
    int stripe_count = 1;           // on how many OSTs to allocate stripes (eg: 1 means a single OST receives all, -1 means to stripe over all OSTs, 0 means to use Lustre's default)
    int stripe_per_ost = 1;
    auto file_size_b = file->getSize(); 
    if (file_size_b > stripe_size) {
        stripe_count = std::ceil(file_size_b / stripe_size);        // here we potentially ask for more storage than needed due to the rounding
        if (stripe_count > num_active_services) {
            while(stripe_count > num_active_services * stripe_per_ost) {
                stripe_per_ost++;   // here we don't ask for more storage than needed because the condition of the allocation loop
                                    // (see below) also checks whether all required stripes have been allocated or not.  
            }
        } 
    }
    int max_nb_ost = 2000;          // never stripe on more than 2000 OSTs, that's the Lustre limit when using ZFS.    

    std::cout << "LUSTRE WEIGHT ALLOC DEBUG" << std::endl;
    std::cout << "Stripe per ost = " << std::to_string(stripe_per_ost) << std::endl;
    std::cout << "Stripe count = " << std::to_string(stripe_count) << std::endl;
    std::cout << "Current file_size = " << std::to_string(file_size_b) << std::endl;
    std::cout << "Number of OST / services = " << std::to_string(num_active_services) << std::endl;

    // from https://en.cppreference.com/w/cpp/numeric/random/uniform_int_distribution
    std::random_device rd;
    std::mt19937 gen(rd());     // replace rd() by a static seed for reproducibility
    std::uniform_int_distribution<> distrib(0, total_weight);

    std::map<int, std::shared_ptr<wrench::StorageService>> temp_stripe_locations;
    std::vector<std::shared_ptr<wrench::FileLocation>> designated_locations;

    // Let's find enough OSTs for our allocation
    auto nfound = 0;
    while (nfound < stripe_count) {
        uint64_t rand, cur_weight;

        cur_weight = 0;
        rand = distrib(gen);

        auto stripe_idx = 0;
        for (const auto& service : weighted_services) {
           
            cur_weight += service.second;
            if (cur_weight < rand) {
                continue;
            }

            temp_stripe_locations[nfound] = service.first;
            nfound++;
        }

    }

    // If we could allocate every stripe, return an empty vector that will be interpreted as an allocation failure
    if (temp_stripe_locations.size() != stripe_count) {
        std::cout << "LustreAlloc: Expected to allocate " << std::to_string(stripe_count) << " stripes, but could only allocate " << std::to_string(temp_stripe_locations.size()) << " instead" << std::endl;
    } 

    for (const auto & stripe_entry : temp_stripe_locations) {
        auto part = wrench::Simulation::addFile(file->getID() + "_part_" + std::to_string(stripe_entry.first), stripe_size);
        designated_locations.push_back(
            wrench::FileLocation::LOCATION(
                std::shared_ptr<wrench::StorageService>(stripe_entry.second), part
            )
        );
    }

    return designated_locations;
}