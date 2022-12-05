
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

#include "simgrid/plugins/energy.h"

#include "Controller.h"


constexpr unsigned int MBPS (1000 * 1000);

namespace sg4 = simgrid::s4u;


/*  Contention and variability models.
 *  (just placeholders so far)
 */

static double non_linear_disk_bw_read(double capacities, int n_activities) {
    // std::cout << "Read capacities " << capacities << std::endl;
    // std::cout << "n_activities " << n_activities << std::endl;

    // Slowdown disk in regard to current number of activities
    return capacities * (1 / n_activities) * 0.9;
}

static double non_linear_disk_bw_write(double capacities, int n_activities) {
    // std::cout << "Write capacities " << capacities << std::endl;
    // std::cout << "n_activities " << n_activities << std::endl;

    // Slowdown disk in regard to current number of activities
    return capacities * (1 / n_activities) * 0.8;
}

static double hdd_variability(sg_size_t size, sg4::Io::OpType op)
{
    if ( op == sg4::Io::OpType::READ ) {
        return 1.2;     // accelerate read operation (always the same value in this case)
    } else {
        return 0.8;     // slowdown writes
    }

}


/**
 * @brief Function to instantiate a simulated platform, instead of
 * loading it from an XML file. This function directly uses SimGrid's s4u API
 * (see the SimGrid documentation). This function creates a platform that's
 * identical to that described in the file two_hosts.xml located in this directory.
 */

class PlatformFactory {

public:

    PlatformFactory(double link_bw) : link_bw(link_bw) {}

    void operator()() const {
        create_platform(this->link_bw);
    }

private:
    double link_bw;

    void create_platform(double link_bw) const {
        // Create the top-level zone
        auto zone = sg4::create_full_zone("AS0");
        // zone->set_property("routing", "Full");
        
        // Create a User host
        auto user_host = zone->create_host("user0", {"100.0Mf","50.0Mf","20.0Mf"});
        user_host->set_core_count(16);
        user_host->set_property("ram", "128GB");
        user_host->set_property("wattage_per_state", "95.0:120.0:200.0, 93.0:115.0:170.0, 90.0:110.0:150.0");
        user_host->set_property("wattage_off", "10");
        

        // Create a compute host
        auto compute_host = zone->create_host("compute0", {"100.0Mf","50.0Mf","20.0Mf"});
        compute_host->set_core_count(16);
        compute_host->set_property("ram", "128GB");
        compute_host->set_property("wattage_per_state", "95.0:120.0:200.0, 93.0:115.0:170.0, 90.0:110.0:150.0");
        compute_host->set_property("wattage_off", "10");
        
        // Create a storage host
        auto storage_host = zone->create_host("storage0", {"100.0Mf","50.0Mf","20.0Mf"});
        storage_host->set_core_count(16);
        storage_host->set_property("wattage_per_state", "95.0:120.0:200.0, 93.0:115.0:170.0, 90.0:110.0:150.0");
        storage_host->set_property("wattage_off", "10");
        storage_host->set_property("latency", "10");
        auto disk0 = storage_host->create_disk("hdd0", "50MBps", "50MBps");
        disk0->set_property("size", "600GiB");
        disk0->set_property("mount", "/dev/disk0");

        // Input for contention and variability on HDD
        disk0->set_sharing_policy(sg4::Disk::Operation::READ, sg4::Disk::SharingPolicy::NONLINEAR, non_linear_disk_bw_read);
        disk0->set_sharing_policy(sg4::Disk::Operation::WRITE, sg4::Disk::SharingPolicy::NONLINEAR, non_linear_disk_bw_write);
        disk0->set_factor_cb(hdd_variability);

        auto disk5 = storage_host->create_disk("ssd0", "1000MBps", "1000MBps");
        disk5->set_property("size", "200GiB");
        disk5->set_property("mount", "/dev/disk5");

        // Create three network links
        auto network_link = zone->create_link("network_link", link_bw)->set_latency("20us");
        auto loopback_UserHost = zone->create_link("loopback_UserHost", "1000EBps")->set_latency("0us");
        auto loopback_ComputeHost = zone->create_link("loopback_ComputeHost", "1000EBps")->set_latency("0us");
        auto loopback_StorageHost = zone->create_link("loopback_StorageHost", "1000EBps")->set_latency("0us");

        // Add routes
        {
            sg4::LinkInRoute network_link_in_route{network_link};
            zone->add_route(compute_host->get_netpoint(),
                            storage_host->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
        }
        {
            sg4::LinkInRoute network_link_in_route{network_link};
            zone->add_route(user_host->get_netpoint(),
                            storage_host->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
        }
        {
            sg4::LinkInRoute network_link_in_route{network_link};
            zone->add_route(user_host->get_netpoint(),
                            compute_host->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
        }
        {
            sg4::LinkInRoute network_link_in_route{loopback_StorageHost};
            zone->add_route(storage_host->get_netpoint(),
                            storage_host->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
        }
        {
            sg4::LinkInRoute network_link_in_route{loopback_ComputeHost};
            zone->add_route(compute_host->get_netpoint(),
                            compute_host->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
        }
        {
            sg4::LinkInRoute network_link_in_route{network_link};
            zone->add_route(user_host->get_netpoint(),
                            user_host->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
        }

        zone->seal();

    }
};




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
    PlatformFactory platform_factory(10000 * MBPS);
    simulation->instantiatePlatform(platform_factory);

    /* Instantiate a storage service on the platform */
    auto storage_service = simulation->add(
        wrench::SimpleStorageService::createSimpleStorageService(
            "storage0", {"/dev/disk0", "/dev/disk5"}, {}, {}
        )
    );

    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "storage0", {storage_service}, {}, {}
        )
    );

    /* Instantiate a bare-metal compute service on the platform */
    auto baremetal_service = simulation->add(new wrench::BareMetalComputeService(
            "compute0", {"compute0"}, "", {}, {}));

    /* Instantiate an execution controller */
    auto wms = simulation->add(
            new wrench::Controller(baremetal_service, storage_service, "user0"));

    std::cout << "Launching simulation..." << std::endl;

    /* Launch the simulation */
    simulation->launch();

    auto storage_host = sg4::Host::by_name("storage0");
    auto consummed = sg_host_get_consumed_energy(storage_host);
    std::cout << "Energy consumed : " << consummed << std::endl;

    return 0;
}
