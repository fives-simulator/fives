#include "Utils.h"

#include <simgrid/kernel/routing/NetPoint.hpp>

#include "yaml-cpp/yaml.h"


/**
 * @brief Describe topology of zones, hosts and links.
 * (should be used to create a diagram..)
 * 
*/
void storalloc::describe_platform() {
    
    std::set<simgrid::kernel::routing::NetZoneImpl*> zones = {};

    // Dragonfly zone for controllers is actually seen as "clusters"
    for (auto const & hostcluster : wrench::S4U_Simulation::getAllHostnamesByCluster()) {
        for (auto const & host : hostcluster.second) {
            std::cout << host << "@" << hostcluster.first << std::endl;
            auto netpt = wrench::S4U_Simulation::get_host_or_vm_by_name(host)->get_netpoint();
            auto zone = netpt->get_englobing_zone();
            zones.insert(zone);
        }
    }

    // Storage and control zone is considered as an actual zone (its created as a "floyd_zone")
    for (auto const & hostzone : wrench::S4U_Simulation::getAllHostnamesByZone()) {
        for (auto const & host : hostzone.second) {
            std::cout << host << "@" << hostzone.first << std::endl;
            auto netpt = wrench::S4U_Simulation::get_host_or_vm_by_name(host)->get_netpoint();
            auto zone = netpt->get_englobing_zone();
            zones.insert(zone);
        }
    }

    // Zone info recap
    for (const auto& zone : zones) {
        std::cout << "Zone: " << zone->get_name() << std::endl;
        // std::cout << "  - Network model: " << zone->get_network_model() << std::endl;
        std::cout << "  - Host count: " << zone->get_host_count() << std::endl;
        std::cout << "  - Parent zone: " << zone->get_parent()->get_name() << std::endl;
        std::cout << "  - Links:" << std::endl;
        for (const auto& link : zone->get_all_links()) {
            std::cout << "     - " << link->get_name() << std::endl;
        }
    }

    // Showing a route between two hosts
    /*
    auto storage0 = wrench::S4U_Simulation::get_host_or_vm_by_name("storage0");
    auto compute14 = wrench::S4U_Simulation::get_host_or_vm_by_name("compute14");
    auto user0 = wrench::S4U_Simulation::get_host_or_vm_by_name("user0");
    std::vector<simgrid::s4u::Link*> linksInRoute;
    double latency = 0;
    //std::unordered_set<simgrid::kernel::routing::NetZoneImpl*> netzonesInRoute;
    storage0->route_to(compute14, linksInRoute, &latency);

    for (const auto & link : linksInRoute) {
        std::cout << link->get_name() << std::endl;
    }

    linksInRoute.clear();
    user0->route_to(storage0, linksInRoute, &latency);
    for (const auto & link : linksInRoute) {
        std::cout << link->get_name() << std::endl;
    }
    */

}

storalloc::Config storalloc::loadConfig(const std::string& yaml_file_name) {

    YAML::Node config = YAML::LoadFile(yaml_file_name);

    if (!(config["general"]) or !(config["dragonfly"]) or !(config["storage"])) {
        std::cout << "# Invalid config file, missing one or many sections." << std::endl;
        throw std::invalid_argument("Invalid config file, missing one or many sections.");
    }

    std::cout << "# Loading configuration : " << config["general"]["config_name"] << " (v" << config["general"]["config_version"] << ")" << std::endl;

    return config.as<storalloc::Config>();
}


std::vector<storalloc::YamlJob> storalloc::loadYamlJobs(const std::string& yaml_file_name) {

    YAML::Node jobs = YAML::LoadFile(yaml_file_name);
    if (!(jobs["jobs"]) or !(jobs["jobs"].IsSequence())) {
        std::cout << "# Invalid job file" << std::endl;
        throw std::invalid_argument("Invalid job file as input data");
    }

    std::vector<storalloc::YamlJob> job_list;
    for (const auto& job : jobs["jobs"]) {
        job_list.push_back(job.as<storalloc::YamlJob>());
    }

    std::cout << "# Loading " << std::to_string(job_list.size()) << " jobs" << std::endl;

    return job_list;
}