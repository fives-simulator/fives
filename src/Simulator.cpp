
/**
 * Copyright (c) 2017-2021. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

/**
 ** This is the main function for a WRENCH simulator. The simulator takes
 ** a input an XML platform description file. It generates a workflow with
 ** a simple diamond structure, instantiates a few services on the platform, and
 ** starts an execution controller to execute the workflow using these services
 ** using a simple greedy algorithm.
 **/

#include <iostream>

#include <wrench-dev.h>
#include <simgrid/plugins/energy.h>
#include <simgrid/kernel/routing/NetPoint.hpp>

#include "yaml-cpp/yaml.h"

#include "JobDefinition.h"
#include "ConfigDefinition.h"
#include "Controller.h"
#include "Platform.h"


/**
 * @brief Describe topology of zones, hosts and links.
 * (should be used to create a diagram..)
 * 
*/
void describe_platform() {
    
    std::set<simgrid::kernel::routing::NetZoneImpl*> zones = {};

    // Dragonfly zonefor controllers is actually seen as "clusters"
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


auto loadConfig(const std::string& yaml_file_name) {

    YAML::Node config = YAML::LoadFile(yaml_file_name);

    if (!(config["general"]) or !(config["dragonfly"]) or !(config["storage"])) {
        std::cout << "# Invalid config file, missing one or many sections." << std::endl;
        throw std::invalid_argument("Invalid config file, missing one or many sections.");
    }

    std::cout << "# Loading configuration : " << config["general"]["config_name"] << "::" << config["general"]["config_version"] << std::endl;

    return config.as<storalloc::Config>();
}


auto loadYamlJobs(const std::string& yaml_file_name) {

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

/**
 * @brief The Simulator's main function
 *
 * @param argc: argument count
 * @param argv: argument array
 * @return 0 on success, non-zero otherwise
 */
int main(int argc, char **argv) {

    if (argc < 3) {
        std::cout << "###############################################################" << std::endl;
        std::cout << "# USAGE: " << argv[0] << " <config file> <job file>" << std::endl;
        std::cout << "#          [Both files are expected to be YAML]" << std::endl;
        std::cout << "# This program starts a WRENCH simulation of a batch scheduler" << std::endl;
        std::cout << "###############################################################" << std::endl;
        return 1;
    }

    auto config = std::make_shared<storalloc::Config>(loadConfig(argv[1]));
    auto jobs = loadYamlJobs(argv[2]);



    std::cout << "In use node templates are : " << std:: endl;
    for (const auto& node_tpl : config->node_templates) {
        std::cout << "- " << node_tpl.first << " nodes with id " << node_tpl.second.id << std::endl; 
        std::cout << "   - " << std::to_string(node_tpl.second.disks.size()) << " disks" << std::endl;
        std::cout << "   - to_string: " << node_tpl.second.to_string() << std::endl;
        std::cout << "  Disks : " << std::endl;
        for (const auto& disk : node_tpl.second.disks) {
            std::cout << "  - " << disk.to_string() << std::endl;
            std::cout << "  - R/W bw " << disk.tpl.read_bw << " / " << disk.tpl.write_bw << std::endl;
        }
    }

    /* Create a WRENCH simulation object */
    auto simulation = wrench::Simulation::createSimulation();

    sg_host_energy_plugin_init();

    /* Initialize the simulation */
    simulation->init(&argc, argv);

    /* Instantiating the simulated platform with user-provided config*/
    auto platform_factory = storalloc::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);
    simulation->getOutput().enableDiskTimestamps(true);

    // Just to make sure the platform looks about correct.
    describe_platform();

    // Simple storage services that will be accessed through CompoundStorageService
    std::set<std::shared_ptr<wrench::StorageService>> sstorageservices;
    for (const auto& node : config->nodes) {    // node types

        // mount points list is the same for all nodes of a given type
        std::set<std::string> mount_points;
        for (const auto& disk: node.tpl.disks) {
            for (auto j = 0; j < disk.qtt; j++) {
                mount_points.insert(disk.tpl.mount_prefix + std::to_string(j));
            } 
        }

        for(auto i = 0; i < node.qtt; i++) {    // qtt of each type

            std::cout << "Inserting a new SimpleStorageService" << std::endl;
            sstorageservices.insert(
                simulation->add(
                    wrench::SimpleStorageService::createSimpleStorageService(
                        node.tpl.id + std::to_string(i),                      // ID based on node template and global index
                        mount_points, {}, {}
                    )
                )
            );
        }
    }

    // CompoundStorageService, on first storage node
    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "compound_storage", sstorageservices, {}, {}
        )
    );

    // Additionnal Simple Storage service, to be used as-is.
    auto permanent_storage = simulation->add(
        wrench::SimpleStorageService::createSimpleStorageService(
            "permanent_storage", {"/dev/disk0"}, {}, {}
        )
    );

    /* Instantiate a batch compute service on the platform, with a special algorithm 
       able to handle the CompoundStorageService
     */
    std::vector<std::string> compute_nodes;
    auto nb_compute_nodes = config->d_nodes * config->d_routers * config->d_chassis * config->d_groups;
    for(unsigned int i = 0; i < nb_compute_nodes; i++){
        compute_nodes.push_back("compute" + std::to_string(i));
    }
    auto batch_service = simulation->add(new wrench::BatchComputeService(
            "batch0", compute_nodes, "", 
            {{wrench::BatchComputeServiceProperty::BATCH_SCHEDULING_ALGORITHM, "conservative_bf_storage"}}, {})
    );

    /* Instantiate an execution controller */
    auto wms = simulation->add(
            new wrench::Controller(batch_service, permanent_storage, compound_storage_service, "user0", jobs));

    std::cout << "Launching simulation..." << std::endl;

    /* Launch the simulation */
    simulation->launch();

    // Playing around with energy plugin, not useful
    auto storage_host = sg4::Host::by_name("compound_storage");
    auto consummed = sg_host_get_consumed_energy(storage_host);
    std::cout << "Energy consumed : " << consummed << std::endl;


    simulation->getOutput().dumpDiskOperationsJSON("./wrench_disk_ops.json", true);
    simulation->getOutput().dumpHostEnergyConsumptionJSON("./wrench_energy_consumption", true);

    auto trace = simulation->getOutput().getTrace<wrench::SimulationTimestampTaskCompletion>();
    for (auto const &item: trace) {
        std::cerr << "Task " << item->getContent()->getTask()->getID() << " completed at time " << item->getDate() << std::endl;
    }

    return 0;
}
