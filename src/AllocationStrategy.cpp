#include <random>

#include "AllocationStrategy.h"


std::vector<std::shared_ptr<wrench::FileLocation>> storalloc::simpleRRStrategy(
    const std::shared_ptr<wrench::DataFile>& file, 
    const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>& resources,
    const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>>& mapping,
    const std::vector<std::shared_ptr<wrench::FileLocation>>& previous_allocations) {

    // Init round-robin
    static auto last_selected_server = resources.begin()->first;
    static auto internal_disk_selection = 0;
    // static auto call_count = 0;
    // std::cout << "# Call count 1: "<< std::to_string(call_count) << std::endl;
    auto capacity_req = file->getSize();
    std::shared_ptr<wrench::FileLocation> designated_location = nullptr;
    // std::cout << "Calling on the rrStorageSelectionStrategy for file " << file->getID() << " (" << std::to_string(file->getSize()) << "B)" << std::endl;
    auto current = resources.find(last_selected_server);
    auto current_disk_selection = internal_disk_selection;
    // std::cout << "Last selected server " << last_selected_server << std::endl;
    // std::cout << "Starting from server " << current->first << std::endl;
    // std::cout << "Internal disk selection " << std::to_string(internal_disk_selection) << std::endl;

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

    // call_count++;
    // std::cout << "# Call count 2: "<< std::to_string(call_count) << std::endl;

    // std::cout << "smartStorageSelectionStrategy has done its work." << std::endl;
    if (designated_location)
        return std::vector<std::shared_ptr<wrench::FileLocation>>{designated_location};
    else
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
     * Currently, the stripe size is arbitrarily set to 640MB (recommended size is between 1-4MB, and max is 4GB, but anyway, our allocations
     * do not necessarily represent files...)
     * 
    */ 
    int stripe_size = 640000000;    // how much data is written to a given OST before moving to the next one (640MB in this case)
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
            designated_locations.clear();
            temp_start_ost_index = start_ost_index;     // back to original index, but this time we'll allow 'slow' OSTs
            stripe_idx = 0;
            std::cout << "LustreAlloc : Starting over because stripe_idx != stripe_count and we reached the condition of this loop" << std::endl;
        }
    }
    
    for (const auto& stripe_entry : temp_stripe_locations) {
        auto part = wrench::Simulation::addFile(file->getID() + "_part_" + std::to_string(stripe_entry.first), stripe_size);
        designated_locations.push_back(
            wrench::FileLocation::LOCATION(
                std::shared_ptr<wrench::StorageService>(stripe_entry.second), part
            )
        );
    }

    // If we could allocate every stripe, return an empty vector that will be interpreted as an allocation failure
    if (designated_locations.size() != stripe_count) {
        std::cout << "LustreAlloc: Expected to allocate " << std::to_string(stripe_count) << " stripes, but could only allocate " << std::to_string(designated_locations.size()) << " instead" << std::endl;
        designated_locations.clear();
    } 
    
    return designated_locations;
}