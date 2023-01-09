
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

#include "Controller.h"
#include "Platform.h"


/**
 * @brief The Simulator's main function
 *
 * @param argc: argument count
 * @param argv: argument array
 * @return 0 on success, non-zero otherwise
 */
int main(int argc, char **argv) {

    /* Create a WRENCH simulation object */
    auto simulation = wrench::Simulation::createSimulation();

    sg_host_energy_plugin_init();

    /* Initialize the simulation */
    simulation->init(&argc, argv);

    /* Instantiating the simulated platform */
    auto platform_factory = storalloc::PlatformFactory(10000 * storalloc::MBPS);
    simulation->instantiatePlatform(platform_factory);
    simulation->getOutput().enableDiskTimestamps(true);


    /*  Describe topology of zones, hosts and links.
     *  (should be used to create a diagram..)
     *
     */
    std::set<simgrid::kernel::routing::NetZoneImpl*> zones = {};
    
    for (const auto& hostname : wrench::S4U_Simulation::getAllHostnames()) {
        std::cout << " - " << hostname << std::endl;
        auto netpt = wrench::S4U_Simulation::get_host_or_vm_by_name(hostname)->get_netpoint();
        auto zone = netpt->get_englobing_zone();
        zones.insert(zone);
        std::cout << "    - NetPoint is :" << netpt->get_name() << "@" << zone->get_name() << std::endl;
    }

    // getting info on NetZoneImpl instances
    for (const auto& zone : zones) {
        std::cout << "Zone: " << zone->get_name() << std::endl;
        // std::cout << "  - Network model: " << zone->get_network_model() << std::endl;
        std::cout << "  - Host count: " << zone->get_host_count() << std::endl;
        std::cout << "  - Parent zone: " << zone->get_parent()->get_name() << std::endl;
        std::cout << "With links:" << std::endl;
        for (const auto& link : zone->get_all_links()) {
            std::cout << "   - " << link->get_name() << std::endl;
        }
        auto hostsInZone = zone->get_all_hosts();
        std::cout << "Hosts:" << std::endl;
        for (const auto & host : hostsInZone) {
            std::cout << " - " << host->get_name() << std::endl;
        }
    }

    auto storage0 = wrench::S4U_Simulation::get_host_or_vm_by_name("storage0");
    auto compute14 = wrench::S4U_Simulation::get_host_or_vm_by_name("compute14");
    std::vector<simgrid::s4u::Link*> linksInRoute;
    double latency = 0;
    //std::unordered_set<simgrid::kernel::routing::NetZoneImpl*> netzonesInRoute;
    storage0->route_to(compute14, linksInRoute, &latency);

    for (const auto & link : linksInRoute) {
        std::cout << link->get_name() << std::endl;
    }




    /*
    for (auto const & [tmpzone, tmphostnames] : wrench::S4U_Simulation::getAllHostnamesByZone()) {
        for (auto const & host : tmphostnames) {
            std::cout << tmpzone << " : " << host << std::endl;
        }
    }
    */

    /* Instantiate a storage service on the platform */
    auto storage_service0 = simulation->add(
        wrench::SimpleStorageService::createSimpleStorageService(
            "storage0", {"/dev/hdd0", "/dev/ssd0"}, {}, {}
        )
    );

        /* Instantiate a storage service on the platform */
    auto storage_service1 = simulation->add(
        wrench::SimpleStorageService::createSimpleStorageService(
            "storage1", {"/dev/hdd0", "/dev/ssd0"}, {}, {}
        )
    );

        /* Instantiate a storage service on the platform */
    auto storage_service2 = simulation->add(
        wrench::SimpleStorageService::createSimpleStorageService(
            "storage2", {"/dev/hdd0", "/dev/ssd0"}, {}, {}
        )
    );

        /* Instantiate a storage service on the platform */
    auto storage_service3 = simulation->add(
        wrench::SimpleStorageService::createSimpleStorageService(
            "storage3", {"/dev/hdd0", "/dev/ssd0"}, {}, {}
        )
    );

    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "storage1", {storage_service2, storage_service3}, {}, {}
        )
    );

    /* Instantiate a bare-metal compute service on the platform */
    auto batch_service = simulation->add(new wrench::BatchComputeService(
            "batch0", {"compute0", "compute1"}, "", 
            {{wrench::BatchComputeServiceProperty::BATCH_SCHEDULING_ALGORITHM, "conservative_bf_storage"}}, {})
    );

    /* Instantiate an execution controller */
    auto wms = simulation->add(
            new wrench::Controller(batch_service, storage_service0, compound_storage_service, "user0"));

    std::cout << "Launching simulation..." << std::endl;

    /* Launch the simulation */
    simulation->launch();

    auto storage_host = sg4::Host::by_name("storage0");
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
