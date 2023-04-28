#include "AllocationStrategy.h"




std::shared_ptr<wrench::FileLocation> storalloc::simpleRRStrategy(
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
    return designated_location;

}



std::shared_ptr<wrench::FileLocation> storalloc::lustreStrategy(
    const std::shared_ptr<wrench::DataFile>& file, 
    const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>>& resources,
    const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>>& mapping,
    const std::vector<std::shared_ptr<wrench::FileLocation>>& previous_allocations) {

    // ## First build a list of services containing only up / non-full services and ordered for use with rr
    // This is roughly equivalent to lod_qos.c::lod_qos_calc_rr() function in Lustre sources.

    std::map<std::string, int> hostname_to_service_number;      // OSS in Lustre
    std::vector<std::shared_ptr<wrench::StorageService>> disk_level_services;    // OST in Lustre
        
    for (const auto host_it = resources.begin(); host_it != resources.end(); ++host_it) {

        auto service_count = 0;

        for (const auto srv : host_it->second) {
            if (srv->traceTotalFreeSpace() != 0) {
                disk_level_services.push_back(srv);
                service_count++;
            }
        }

        if (!disk_level_services.empty()) {
            hostname_to_service_number[host_it->first] = service_count; 
        }
            
    }

    std::vector<std::shared_ptr<wrench::StorageService>> rr_ordered_services(disk_level_services.size());
    fill(rr_ordered_services.begin(), rr_ordered_services.end(), nullptr);
    auto placed_services = 0;

    for (const auto server : hostname_to_service_number) {

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
        // Error throw or return ? 
    }

    // ## 2. Use the rr ordered list of services to allocation stripes for the given file 
    // Roughly the equivalent of what happens in lod_qos.c::lod_ost_alloc_rr()




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
    return designated_location;

}