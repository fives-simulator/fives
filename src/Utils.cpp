#include "Utils.h"
#include "AllocationStrategy.h"

#include "yaml-cpp/yaml.h"
#include <iomanip>
#include <simgrid/kernel/routing/NetPoint.hpp>
#include <wrench-dev.h>

WRENCH_LOG_CATEGORY(fives_utils, "Log category for Fives Utils");

namespace fives {

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
    std::map<std::string, YamlJob> loadYamlJobs(const std::string &yaml_file_path) {

        YAML::Node dataset = YAML::LoadFile(yaml_file_path);
        if (!(dataset["jobs"]) or !(dataset["jobs"].IsSequence())) {
            WRENCH_WARN("Invalid job file %s", yaml_file_path.c_str());
            throw std::invalid_argument("Invalid job file as input data");
        }

        const char *timedate_fm = "";
        std::map<std::string, YamlJob> job_map;
        uint64_t previous_ts = 0;
        std::tm previous_date{};

        for (const auto &job : dataset["jobs"]) {
            try {
                auto parsed_job = job.as<fives::YamlJob>();

                // Here we have a dirty way of checking that jobs in our dataset are ordered by submission date.
                std::istringstream ss(parsed_job.submissionTime);
                std::tm t = {};
                ss >> std::get_time(&t, "%Y-%m-%d %H:%M:%S"); // eg. '2022-10-01 03:42:52'
                uint64_t ts = (t.tm_year - 2000) * 31536000 + t.tm_yday * 86400 + t.tm_hour * 3600 + t.tm_min * 60 + t.tm_sec;
                if (ts >= previous_ts) {
                    job_map[parsed_job.id] = parsed_job;
                    previous_ts = ts;
                    previous_date = t;
                } else {
                    WRENCH_WARN("Current job's submission time : %s", std::asctime(&t));
                    WRENCH_WARN("Previous job's submission time : %s", std::asctime(&previous_date));
                    WRENCH_WARN("Job %s submission time is inferior to the previous parsed job (job ordering is incorrect in dataset)", parsed_job.id.c_str());
                    throw runtime_error("Job " + parsed_job.id + " submission time is inferior to the previous parsed job");
                }
            } catch (std::exception &e) {
                std::string id = job["id"].as<std::string>();
                WRENCH_WARN("Job %s in file %s has invalid caracteristics", id.c_str(), yaml_file_path.c_str());
                WRENCH_WARN(e.what());
                throw runtime_error("Job " + id + " in file " + yaml_file_path + " has invalid caracteristics : " + e.what());
            }
        }

        WRENCH_INFO("Dataset loaded : %s -- %ld jobs", yaml_file_path.c_str(), job_map.size());
        return job_map;
    }

} // namespace fives