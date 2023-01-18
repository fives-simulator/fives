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
    * @brief Callback to set a compute cluster leaf/element. Creates nodes called "comptute[X]"
    *        (compute0, compute1, ...)
    * 
    * @param zone Cluster netzone being created (usefull to create the hosts/links inside it)
    * @param coord Coordinates in the cluster
    * @param id Internal identifier in the torus (for information)
    * @return netpoint, gateway: the netpoint to the Dragonfly zone
    */
    static std::pair<simgrid::kernel::routing::NetPoint*, simgrid::kernel::routing::NetPoint*>
    create_hostzone(sg4::NetZone* zone, const std::vector<unsigned long>& /*coord*/, unsigned long id)
    {

        std::string hostname = "compute" + std::to_string(id);
        auto compute_host = zone->create_host(hostname, {"100.0Mf","50.0Mf","20.0Mf"});
        compute_host->set_core_count(24);
        compute_host->set_property("ram", "128GB");
        compute_host->set_property("wattage_per_state", "95.0:120.0:200.0, 93.0:115.0:170.0, 90.0:110.0:150.0");
        compute_host->set_property("wattage_off", "10");
        compute_host->seal();

        return std::make_pair(zone->get_netpoint(), compute_host->get_netpoint());
    
    }


    void PlatformFactory::create_platform(const std::shared_ptr<storalloc::Config> cfg) const {

        // Create the top-level zone and backbone "link"
        auto main_zone = sg4::create_star_zone("AS_Root");
        auto main_link_bb = main_zone->create_link("backbone", cfg->bw)->set_latency("20us");
        sg4::LinkInRoute backbone{main_link_bb};
        auto main_zone_router = main_zone->create_router("main_zone_router");

        // Create a network zone for control hosts
        auto control_zone = sg4::create_floyd_zone("AS_Ctrl");
        control_zone->set_parent(main_zone);
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
            control_zone->add_route(host->get_netpoint(), control_router, nullptr, nullptr,
                       {{link, sg4::LinkInRoute::Direction::UP}, backbone_ctrl}, true);
        }

        control_zone->seal();

        // Create a Dragonfly compute zone
        auto groups = 2;        // for entire dragonfly zone
        auto group_links = 2;
        auto chassis = 2;       // per group
        auto chassis_links = 2;
        auto routers = 2;       // per chassis
        auto routers_links = 2;
        auto nodes = 2;         // per router
        auto compute_zone = sg4::create_dragonfly_zone(
            "AS_DragonflyCompute", 
            main_zone, 
            {{cfg->d_groups, cfg->d_group_links}, {cfg->d_chassis, cfg->d_chassis_links}, {cfg->d_routers, cfg->d_router_links}, cfg->d_nodes}, {create_hostzone, {}, create_limiter},
            10e9, 10e-6, sg4::Link::SharingPolicy::SPLITDUPLEX
        );
        // Add a global router for zone-zone routes
        auto compute_router = compute_zone-> create_router("compute_router_0");
        compute_zone->seal();

        // Create a storage zone
        auto storage_zone = sg4::create_floyd_zone("AS_Storage");
        storage_zone->set_parent(main_zone);
        auto backbone_link_storage = storage_zone->create_link("backbone_storage", "1.25GBps");
        sg4::LinkInRoute backbone_storage(backbone_link_storage);
        auto storage_router = storage_zone->create_router("storage_zone_router_0");

        // Simple storage services that will be accessed through CompoundStorageService
        auto node_id = 0;
        for (const auto& node : config->nodes) {    // node types in use

            for(auto i = 0; i < node.qtt; i++) {    // qtt of each type

                // Base node characteristics
                auto hostname = node.tpl.id + std::to_string(i);
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

                // Link to storage backbone
                auto link = storage_zone->create_split_duplex_link(hostname, "125MBps")->set_latency("24us")->seal();
                storage_zone->add_route(storage_host->get_netpoint(), storage_router, nullptr, nullptr,
                        {{link, sg4::LinkInRoute::Direction::UP}, backbone_storage}, true);


                for (const auto& disk: node.tpl.disks) {

                    for (auto j = 0; j < disk.qtt; j++) {
                        auto new_disk = storage_host->create_disk(
                            disk.tpl.id + std::to_string(j), 
                            std::to_string(disk.tpl.read_bw) + "MBps", 
                            std::to_string(disk.tpl.write_bw) + "MBps"
                        );
                        new_disk->set_property("size", std::to_string(disk.tpl.capacity)+"GiB");
                        new_disk->set_property("mount", disk.tpl.mount_prefix + std::to_string(j));

                        // Input for contention and variability on HDD
                        new_disk->set_sharing_policy(sg4::Disk::Operation::READ, sg4::Disk::SharingPolicy::NONLINEAR, non_linear_disk_bw_read);
                        new_disk->set_sharing_policy(sg4::Disk::Operation::WRITE, sg4::Disk::SharingPolicy::NONLINEAR, non_linear_disk_bw_write);
                        new_disk->set_factor_cb(hdd_variability);           // TODO: Add config parameter to select which variability function should be used
                    }
                }
            }
        }

        // Also add one 'special' storage service for permanent storage (external to the supercomputer, or at
        // least not directly among the usual storage nodes used by jobs)
        auto permanent_storage = storage_zone->create_host(
            "permanent_storage",
            {"100.0Mf","50.0Mf","20.0Mf"}
        )->set_core_count(16)->set_property("ram", "32GB")->set_property(
            "wattage_per_state", "95.0:120.0:200.0, 93.0:115.0:170.0, 90.0:110.0:150.0"
        )->set_property("wattage_off", "10")->set_property("latency", "10");
        permanent_storage->create_disk("disk0", "1000MBps", "1000MBps")->set_property("size", "1000TiB")->set_property("mount", "/dev/disk0");
        // Link to storage backbone
        auto link_perm = storage_zone->create_split_duplex_link("permanent_storage", "125MBps")->set_latency("24us")->seal();
        storage_zone->add_route(permanent_storage->get_netpoint(), storage_router, nullptr, nullptr,
                       {{link_perm, sg4::LinkInRoute::Direction::UP}, backbone_storage}, true);



        // And finally a node for the compound storage service itself
        auto cmpd_storage = storage_zone->create_host(
            "compound_storage",
            {"100.0Mf","50.0Mf","20.0Mf"}
        )->set_core_count(4)->set_property("ram", "16GB")->set_property(
            "wattage_per_state", "95.0:120.0:200.0, 93.0:115.0:170.0, 90.0:110.0:150.0"
        )->set_property("wattage_off", "10")->set_property("latency", "10");
        auto link_cpd = storage_zone->create_split_duplex_link("compound_storage", "125MBps")->set_latency("24us")->seal();
        storage_zone->add_route(cmpd_storage->get_netpoint(), storage_router, nullptr, nullptr,
                       {{link_cpd, sg4::LinkInRoute::Direction::UP}, backbone_storage}, true);

        storage_zone->seal();

        // Route from main zone to sub zones.
        main_zone->add_route(storage_zone->get_netpoint(), nullptr, storage_router, nullptr, {backbone});
        main_zone->add_route(control_zone->get_netpoint(), nullptr, control_router, nullptr, {backbone});
        main_zone->add_route(compute_zone->get_netpoint(), nullptr, compute_router, nullptr, {backbone});
        
        main_zone->seal();
    }

} // namespace storalloc