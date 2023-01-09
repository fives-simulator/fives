#include "Platform.h"

namespace storalloc {

    /*  Contention and variability models.
    *  (just placeholders so far)
    */
    static double non_linear_disk_bw_read(double capacities, int n_activities) {
        // Slowdown disk in regard to current number of activities
        return capacities * (1 / n_activities) * 0.9;
    }

    static double non_linear_disk_bw_write(double capacities, int n_activities) {
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


    /*************************************************************************************************/
    /**
     * @brief Callback to create limiter link (1Gbs) for each netpoint
     *
     * The coord parameter depends on the cluster being created:
     * - Torus: Direct translation of the Torus' dimensions, e.g. (0, 0, 0) for a 3-D Torus
     * - Fat-Tree: A pair (level in the tree, id), e.g. (0, 0) for first leaf in the tree and (1,0) for the first switch at
     * level 1.
     * - Dragonfly: a tuple (group, chassis, blades/routers, nodes), e.g. (0, 0, 0, 0) for first node in the cluster. To
     * identify the router inside a (group, chassis, blade), we use MAX_UINT in the last parameter (e.g. 0, 0, 0,
     * 4294967295).
     *
     * @param zone Torus netzone being created (usefull to create the hosts/links inside it)
     * @param coord Coordinates in the cluster
     * @param id Internal identifier in the torus (for information)
     * @return Limiter link
     */
    static sg4::Link* create_limiter(sg4::NetZone* zone, const std::vector<unsigned long>& /*coord*/, unsigned long id)
    {
        return zone->create_link("limiter-" + std::to_string(id), 1e9)->seal();
    }


    /**
    * @brief Callback to set a cluster leaf/element
    * 
    * @param zone Cluster netzone being created (usefull to create the hosts/links inside it)
    * @param coord Coordinates in the cluster
    * @param id Internal identifier in the torus (for information)
    * @return netpoint, gateway: the netpoint to the StarZone and CPU0 as gateway
    */
    static std::pair<simgrid::kernel::routing::NetPoint*, simgrid::kernel::routing::NetPoint*>
    create_hostzone(sg4::NetZone* zone, const std::vector<unsigned long>& /*coord*/, unsigned long id)
    {

        std::string hostname = "compute" + std::to_string(id);
        auto compute_host = zone->create_host(hostname, {"100.0Mf","50.0Mf","20.0Mf"});
        compute_host->set_core_count(16);
        compute_host->set_property("ram", "128GB");
        compute_host->set_property("wattage_per_state", "95.0:120.0:200.0, 93.0:115.0:170.0, 90.0:110.0:150.0");
        compute_host->set_property("wattage_off", "10");
        compute_host->seal();

        return std::make_pair(zone->get_netpoint(), compute_host->get_netpoint());
    
    }


    void PlatformFactory::create_platform(double link_bw) const {

        // Create the top-level zone and backbone "link"
        auto main_zone = sg4::create_star_zone("AS0");
        auto main_link_bb = main_zone->create_link("backbone", link_bw)->set_latency("20us");
        sg4::LinkInRoute backbone{main_link_bb};
        auto main_zone_router = main_zone->create_router("main_zone_router");

        // Create a network zone for control hosts
        auto control_zone = sg4::create_star_zone("AS_Ctrl");
        control_zone->set_parent(main_zone);
        //control_zone->set_property("routing", "floyd");
        auto backbone_link_ctrl = control_zone->create_link("backbone_ctrl", "1.25GBps")->seal();
        sg4::LinkInRoute backbone_ctrl(backbone_link_ctrl);
        
        // Create a user host & batch head node
        std::vector<s4u_Host*> control_hosts = {};
        auto user_host = control_zone->create_host("user0", {"25.0Mf","10.0Mf","5.0Mf"});
        control_hosts.push_back(user_host);
        auto batch_head = control_zone->create_host("batch0", {"25.0Mf","10.0Mf","5.0Mf"});
        control_hosts.push_back(batch_head);
        auto control_router = control_zone->create_router("control_zone_router_0");

        for (auto &host: control_hosts) {
            host->set_core_count(4);
            host->set_property("ram", "32GB");
            host->set_property("wattage_per_state", "95.0:120.0:200.0, 93.0:115.0:170.0, 90.0:110.0:150.0");
            host->set_property("wattage_off", "10");

            auto link = control_zone->create_split_duplex_link(host->get_name(), "125MBps")->set_latency("24us")->seal();
            /* add link and backbone for communications from the host */
            control_zone->add_route(host->get_netpoint(), nullptr, nullptr, nullptr,
                       {{link, sg4::LinkInRoute::Direction::UP}, backbone_ctrl}, true);
        }

        
        control_zone->seal();

        // Create compute zone
        auto compute_zone = sg4::create_dragonfly_zone("cluster", main_zone, {{2, 2}, {2, 1}, {2, 2}, 2}, {create_hostzone, {}, create_limiter},
                             10e9, 10e-6, sg4::Link::SharingPolicy::SPLITDUPLEX);
        auto compute_router = compute_zone-> create_router("compute_router_0");
        compute_zone->seal();

        // Create storage zone
        auto storage_zone = sg4::create_full_zone("AS_Storage");
        storage_zone->set_parent(main_zone);
        auto backbone_link_storage = storage_zone->create_link("backbone_storage", "1.25GBps");
        sg4::LinkInRoute backbone_storage(backbone_link_storage);
        auto storage_router = storage_zone->create_router("storage_zone_router_0");

        std::vector<s4u_Host*> storage_hosts = {};
        for (auto i=0; i<4; i++){

            auto hostname = "storage"+std::to_string(i);
            auto storage_host = storage_zone->create_host(
                hostname,
                {"100.0Mf","50.0Mf","20.0Mf"}
            );
            storage_host->set_core_count(16);
            storage_host->set_property(
                "wattage_per_state", "95.0:120.0:200.0, 93.0:115.0:170.0, 90.0:110.0:150.0"
            );
            storage_host->set_property("wattage_off", "10");
            storage_host->set_property("latency", "10");
            
            auto link = storage_zone->create_split_duplex_link(hostname, "125MBps")->set_latency("24us")->seal();

            /* add link and backbone for communications from the host */
            storage_zone->add_route(storage_host->get_netpoint(), storage_router, nullptr, nullptr,
                       {{link, sg4::LinkInRoute::Direction::UP}, backbone_storage}, true);

            for (auto j=0; j<2; j++) {
                auto hdd = storage_host->create_disk("hdd"+std::to_string(j), "50MBps", "50MBps");
                hdd->set_property("size", "600GiB");
                hdd->set_property("mount", "/dev/hdd"+std::to_string(j));

                // Input for contention and variability on HDD
                hdd->set_sharing_policy(sg4::Disk::Operation::READ, sg4::Disk::SharingPolicy::NONLINEAR, non_linear_disk_bw_read);
                hdd->set_sharing_policy(sg4::Disk::Operation::WRITE, sg4::Disk::SharingPolicy::NONLINEAR, non_linear_disk_bw_write);
                hdd->set_factor_cb(hdd_variability);

                auto ssd = storage_host->create_disk("ssd"+std::to_string(j), "1000MBps", "1000MBps");
                ssd->set_property("size", "200GiB");
                ssd->set_property("mount", "/dev/ssd"+std::to_string(j));
            }
            storage_hosts.push_back(storage_host);
        }

        
        storage_zone->seal();

        main_zone->add_route(storage_zone->get_netpoint(), nullptr, storage_router, nullptr, {backbone});
        main_zone->add_route(control_zone->get_netpoint(), nullptr, control_router, nullptr, {backbone});
        main_zone->add_route(compute_zone->get_netpoint(), nullptr, compute_router, nullptr, {backbone});
        

        /*
        // Create links for hosts loopback
        auto loopback_UserHost = control_zone->create_link("loopback_UserHost", "1000EBps")->set_latency("0us");
        auto loopback_StorageHost = storage_zone->create_link("loopback_StorageHost", "1000EBps")->set_latency("0us");

        // Add routes (so many of them, and we're still missing a few)
        {
            for(const auto& storage_host: storage_hosts) {
                {
                    sg4::LinkInRoute network_link_in_route{network_link};
                    zone->add_route(compute_host_0->get_netpoint(),
                            storage_host->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
                }
                {
                    sg4::LinkInRoute network_link_in_route{network_link};
                    zone->add_route(compute_host_1->get_netpoint(),
                            storage_host->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
                }
                {
                    sg4::LinkInRoute network_link_in_route{network_link};
                    zone->add_route(batch_head->get_netpoint(),
                            storage_host->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
                }
            }
        }
        {
            for(const auto& storage_host: storage_hosts) {
                sg4::LinkInRoute network_link_in_route{network_link};
                zone->add_route(user_host->get_netpoint(),
                            storage_host->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
            }
        }
        {
            sg4::LinkInRoute network_link_in_route{network_link};
            zone->add_route(user_host->get_netpoint(),
                            compute_host_0->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
            zone->add_route(batch_head->get_netpoint(),
                            compute_host_0->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
        }
        {
            sg4::LinkInRoute network_link_in_route{network_link};
            zone->add_route(user_host->get_netpoint(),
                            compute_host_1->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
            zone->add_route(batch_head->get_netpoint(),
                            compute_host_1->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
        }
        {
            sg4::LinkInRoute network_link_in_route{network_link};
            zone->add_route(user_host->get_netpoint(),
                            batch_head->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
        }
        */
        {/*
            for(auto it = storage_hosts.begin(); it!=storage_hosts.end(); it++) {
                sg4::LinkInRoute network_link_in_route{loopback_StorageHost};
                auto current_ss = *it;
                s4u_Host* next_ss = nullptr;
                if (it++ == storage_hosts.end()) {
                    next_ss = *(storage_hosts.begin());
                } else {
                    next_ss = *(it++);
                }
                zone->add_route(current_ss->get_netpoint(),
                            next_ss->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
            }
        */}
        /*
        {
            for(const auto& storage_host: storage_hosts) {
                sg4::LinkInRoute network_link_in_route{loopback_StorageHost};
                zone->add_route(storage_host->get_netpoint(),
                            storage_host->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
            }
        }
        {
            sg4::LinkInRoute network_link_in_route{loopback_ComputeHost};
            zone->add_route(compute_host_0->get_netpoint(),
                            compute_host_0->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
        }
        {
            sg4::LinkInRoute network_link_in_route{loopback_ComputeHost};
            zone->add_route(compute_host_1->get_netpoint(),
                            compute_host_1->get_netpoint(),
                            nullptr,
                            nullptr,
                            {network_link_in_route});
        }
        {
            sg4::LinkInRoute network_link_in_route{loopback_ComputeHost};
            zone->add_route(batch_head->get_netpoint(),
                            batch_head->get_netpoint(),
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
*/
        main_zone->seal();

    }

} // namespace storalloc