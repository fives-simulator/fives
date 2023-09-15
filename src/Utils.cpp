#include "Utils.h"
#include "AllocationStrategy.h"

#include "yaml-cpp/yaml.h"
#include <simgrid/kernel/routing/NetPoint.hpp>
#include <wrench-dev.h>

WRENCH_LOG_CATEGORY(storalloc_utils, "Log category for StorAlloc util functions");

namespace storalloc {

    /**
     * @brief [WIP] Describe topology of zones, hosts and links, printed to stdout.
     *        Some elements are missing, and not all routes can be properly described so far.
     */
    void describe_platform() {

        std::set<simgrid::kernel::routing::NetZoneImpl *> zones = {};

        // Dragonfly zone for controllers is actually seen as "clusters"
        for (auto const &hostcluster : wrench::S4U_Simulation::getAllHostnamesByCluster()) {
            for (auto const &host : hostcluster.second) {
                std::cout << host << "@" << hostcluster.first << std::endl;
                auto netpt = wrench::S4U_Simulation::get_host_or_vm_by_name(host)->get_netpoint();
                auto zone = netpt->get_englobing_zone();
                zones.insert(zone);
            }
        }

        // Storage and control zone is considered as an actual zone (its created as a "floyd_zone")
        for (auto const &hostzone : wrench::S4U_Simulation::getAllHostnamesByZone()) {
            for (auto const &host : hostzone.second) {
                std::cout << host << "@" << hostzone.first << std::endl;
                auto netpt = wrench::S4U_Simulation::get_host_or_vm_by_name(host)->get_netpoint();
                auto zone = netpt->get_englobing_zone();
                zones.insert(zone);
            }
        }

        // Zone info recap
        for (const auto &zone : zones) {
            std::cout << "Zone: " << zone->get_name() << std::endl;
            // std::cout << "  - Network model: " << zone->get_network_model() << std::endl;
            std::cout << "  - Host count: " << zone->get_host_count() << std::endl;
            std::cout << "  - Parent zone: " << zone->get_parent()->get_name() << std::endl;
            std::cout << "  - Links:" << std::endl;
            for (const auto &link : zone->get_all_links()) {
                std::cout << "     - " << link->get_name() << std::endl;
            }
        }

        /*
        // Showing a route between two hosts
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

    /**
     * @brief Load simulation configuration from yaml file.
     * @param yaml_file_path Path to the configuration file
     * @return Config structure, parsed according to the rules defined in ConfigDefinition.cpp
     */
    Config loadConfig(const std::string &yaml_file_path) {

        YAML::Node config = YAML::LoadFile(yaml_file_path);
        WRENCH_INFO("Opening config file %s", yaml_file_path.c_str());

        if (!(config["general"]) or !(config["dragonfly"]) or !(config["storage"])) {
            std::cout << "# Invalid config file, missing one or many sections." << std::endl;
            throw std::invalid_argument("Invalid config file, missing one or many sections.");
        }

        auto parsed_config = config.as<Config>();
        WRENCH_INFO("Configuration loaded : %s - v%s", parsed_config.config_name.c_str(), parsed_config.config_version.c_str());

        return parsed_config;
    }

    /**
     * @brief Load a dataset of jobs
     * @param yaml_file_path Path to the data file
     * @return Vector of parsed jobs
     */
    std::vector<YamlJob> loadYamlJobs(const std::string &yaml_file_path) {

        YAML::Node dataset = YAML::LoadFile(yaml_file_path);
        if (!(dataset["jobs"]) or !(dataset["jobs"].IsSequence())) {
            WRENCH_WARN("Invalid job file %s", yaml_file_path.c_str());
            throw std::invalid_argument("Invalid job file as input data");
        }

        std::vector<YamlJob> job_list;
        for (const auto &job : dataset["jobs"]) {
            try {
                job_list.push_back(job.as<storalloc::YamlJob>());
            } catch (std::exception &e) {
                std::string id = job["id"].as<std::string>();
                WRENCH_WARN("Job %s in file %s has invalid caracteristics", id.c_str(), yaml_file_path.c_str());
                WRENCH_WARN(e.what());
                throw runtime_error("Job " + id + " in file " + yaml_file_path + " has invalid caracteristics");
            }
        }

        WRENCH_INFO("Dataset loaded : %s -- %ld jobs", yaml_file_path.c_str(), job_list.size());
        return job_list;
    }

    /**
     * @brief Load header from job dataset (containing general statistics about the dataset)
     * @param yaml_file_path Path to the data file
     * @return Parsed header inside JobStats structure
     */
    JobsStats loadYamlHeader(const std::string &yaml_file_path) {

        YAML::Node dataset = YAML::LoadFile(yaml_file_path);
        if (!dataset["preload"]) {
            WRENCH_WARN("# Missing preload header in job file");
            throw std::invalid_argument("Missing preload header in job file");
        }

        auto parsed_header = dataset["preload"].as<JobsStats>();
        WRENCH_INFO("Dataset header loaded from file %s", yaml_file_path.c_str());
        return parsed_header;
    }

} // namespace storalloc