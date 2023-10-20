#include "Platform.h"

#include <random>

WRENCH_LOG_CATEGORY(storalloc_platform, "Log category for StorAlloc platform factory");

namespace storalloc {

    /**
     * @brief Factory for the disk_dynamic_sharing callback used on the Simgrid Disk objects
     *        of the simulated storage system.
     */
    static auto non_linear_disk_bw_factory(float non_linear_coef) {
        /* capacity = Current Simgrid capacity of this disk
         * n_activities = Number of Simgrid activities sharing this resource (~ Wrench actions)
         */
        return [non_linear_coef](double capacity, int n_activities) {
            if (n_activities < 1) {
                n_activities = 1;
            }
            return capacity * (1 / n_activities) * non_linear_coef;
        };
    }

    // REMOVE
    static auto hdd_variability_factory(double read_variability, double write_variability) {
        auto variability = [=](sg_size_t size, sg4::Io::OpType op) {
            std::random_device rd{};
            std::mt19937 gen{rd()};
            std::normal_distribution<double> read_distrib{read_variability, 0.1};
            std::normal_distribution<double> write_distrib{write_variability, 0.1};

            if (op == sg4::Io::OpType::READ) {
                return read_distrib(gen);
            } else {
                return write_distrib(gen);
            }
        };
        return variability;
    }

    /*************************************************************************************************/
    /**
     * @brief Callback to create limiter link (1Gbs) for each netpoint
     *
     * The coord parameter depends on the cluster being created:
     * - Torus: Direct translation of the Torus' dimensions, e.g. (0, 0, 0) for a
     * 3-D Torus
     * - Fat-Tree: A pair (level in the tree, id), e.g. (0, 0) for first leaf in the
     * tree and (1,0) for the first switch at level 1.
     * - Dragonfly: a tuple (group, chassis, blades/routers, nodes), e.g. (0, 0, 0,
     * 0) for first node in the cluster. To identify the router inside a (group,
     * chassis, blade), we use MAX_UINT in the last parameter (e.g. 0, 0, 0,
     * 4294967295).
     *
     * @param zone Torus netzone being created (usefull to create the hosts/links
     * inside it)
     * @param coord Coordinates in the cluster
     * @param id Internal identifier in the torus (for information)
     * @return Limiter link
     */
    static sg4::Link *create_limiter(sg4::NetZone *zone,
                                     const std::vector<unsigned long> & /*coord*/,
                                     unsigned long id) {
        return zone->create_link("limiter-" + std::to_string(id), 1e9)->seal();
    }

    /**
     * @brief Callback to set a compute cluster leaf/element. Creates nodes called
     * "comptute[X]" (compute0, compute1, ...)
     *
     * @param zone Cluster netzone being created (usefull to create the hosts/links
     * inside it)
     * @param coord Coordinates in the cluster
     * @param id Internal identifier in the torus (for information)
     * @return netpoint, gateway: the netpoint to the Dragonfly zone
     */
    static std::pair<simgrid::kernel::routing::NetPoint *,
                     simgrid::kernel::routing::NetPoint *>
    create_hostzone(sg4::NetZone *zone,
                    const std::vector<unsigned long> & /*coord*/,
                    unsigned long id) {

        std::string hostname = "compute" + std::to_string(id);
        auto *host_zone = sg4::create_star_zone(hostname);
        host_zone->set_parent(zone);

        auto compute_host =
            host_zone->create_host(hostname, "2.2Tf"); // Computed value from Theta spec is 2.6TF, adjusted to 2.2TF to take into account some variability
        compute_host->set_core_count(64);              // Theta specs
        compute_host->set_property("ram", "192GB");    // Theta specs
        compute_host->set_property("speed", "2.2Tf");  // Computed value from Theta spec is 2.6TF, adjusted to 2.2TF to take into account some variability

        auto link = host_zone->create_link("loopback_compute" + std::to_string(id), "10000Gbps")->set_latency("0");
        host_zone->add_route(compute_host->get_netpoint(), compute_host->get_netpoint(), nullptr, nullptr, {{link, sg4::LinkInRoute::Direction::UP}, {link, sg4::LinkInRoute::Direction::DOWN}});

        host_zone->seal();

        return std::make_pair(host_zone->get_netpoint(), compute_host->get_netpoint());
    }

    void PlatformFactory::create_platform(const std::shared_ptr<storalloc::Config> cfg) const {

        // Create the top-level zone and backbone "link"
        auto main_zone = sg4::create_star_zone("AS_Root");
        auto main_link_bb =
            main_zone->create_link("backbone", cfg->bkbone_bw)->set_latency("20us");
        sg4::LinkInRoute backbone{main_link_bb};
        auto main_zone_router = main_zone->create_router("main_zone_router");

        // Create a network zone for control hosts
        auto control_zone = sg4::create_floyd_zone("AS_Ctrl");
        control_zone->set_parent(main_zone);
        auto backbone_link_ctrl =
            control_zone->create_link("backbone_ctrl", "1.25GBps")->seal();
        sg4::LinkInRoute backbone_ctrl(backbone_link_ctrl);

        // Create a user host & batch head node
        std::vector<s4u_Host *> control_hosts = {};
        auto user_host =
            control_zone->create_host("user0", {"25.0Mf", "10.0Mf", "5.0Mf"});
        control_hosts.push_back(user_host);
        auto batch_head =
            control_zone->create_host("batch0", {"25.0Mf", "10.0Mf", "5.0Mf"});
        control_hosts.push_back(batch_head);
        auto control_router = control_zone->create_router("control_zone_router_0");

        for (auto &host : control_hosts) {
            host->set_core_count(64);
            host->set_property("ram", "64GB");
            host->set_property("wattage_per_state",
                               "95.0:120.0:200.0, 93.0:115.0:170.0, 90.0:110.0:150.0");
            host->set_property("wattage_off", "10");

            auto link =
                control_zone->create_split_duplex_link(host->get_name(), "1.25GBps")
                    ->set_latency("24us")
                    ->seal();
            /* add link and backbone for communications from the host */
            control_zone->add_route(
                host->get_netpoint(), control_router, nullptr, nullptr,
                {{link, sg4::LinkInRoute::Direction::UP}, backbone_ctrl}, true);
        }

        control_zone->seal();

        // Create a Dragonfly compute zone
        auto compute_zone = sg4::create_dragonfly_zone("AS_DragonflyCompute", main_zone,
                                                       {{cfg->d_groups, cfg->d_group_links},
                                                        {cfg->d_chassis, cfg->d_chassis_links},
                                                        {cfg->d_routers, cfg->d_router_links},
                                                        cfg->d_nodes},
                                                       {create_hostzone, {}, create_limiter}, 10e9,
                                                       10e-6, sg4::Link::SharingPolicy::SPLITDUPLEX);
        // Add a global router for zone-zone routes
        auto compute_router = compute_zone->create_router("compute_router_0");
        compute_zone->seal();

        // Create a storage zone
        auto storage_zone = sg4::create_floyd_zone("AS_Storage");
        storage_zone->set_parent(main_zone);
        auto backbone_link_storage =
            storage_zone->create_link("backbone_storage", cfg->bkbone_bw);
        sg4::LinkInRoute backbone_storage(backbone_link_storage);
        auto storage_router = storage_zone->create_router("storage_zone_router_0");

        // Simple storage services that will be accessed through
        // CompoundStorageService
        for (const auto &node : config->nodes) { // node types in use

            for (auto i = 0; i < node.qtt; i++) { // qtt of each type

                // Base node characteristics
                auto hostname = node.tpl.id + std::to_string(i);
                auto storage_host =
                    storage_zone->create_host(hostname, "2.6Tf");
                storage_host->set_core_count(16);
                // storage_host->set_property(
                //    "wattage_per_state",
                //    "95.0:120.0:200.0, 93.0:115.0:170.0, 90.0:110.0:150.0");
                // storage_host->set_property("wattage_off", "10");
                // storage_host->set_property("latency", "10");

                // Link to storage backbone
                auto link = storage_zone->create_split_duplex_link(hostname, "100GBps")
                                ->set_latency("24us")
                                ->seal();
                storage_zone->add_route(
                    storage_host->get_netpoint(), storage_router, nullptr, nullptr,
                    {{link, sg4::LinkInRoute::Direction::UP}, backbone_storage}, true);

                for (const auto &disk : node.tpl.disks) {

                    for (auto j = 0; j < disk.qtt; j++) {
                        auto new_disk = storage_host->create_disk(
                            disk.tpl.id + std::to_string(j),
                            std::to_string(disk.tpl.read_bw) + "MBps",
                            std::to_string(disk.tpl.write_bw) + "MBps");
                        new_disk->set_property("size",
                                               std::to_string(disk.tpl.capacity) + "GB");
                        new_disk->set_property("mount",
                                               disk.tpl.mount_prefix + std::to_string(j));

                        /*if (disk.tpl.id == "hdd_capa") {
                            new_disk->set_concurrency_limit(500);       // Not working, but I
                        don't why so far
                        }*/

                        // Input for contention and variability on HDDs
                        if ((config->non_linear_coef_read != 1) or (config->non_linear_coef_write != 1)) {
                            new_disk->set_sharing_policy(sg4::Disk::Operation::READ,
                                                         sg4::Disk::SharingPolicy::NONLINEAR,
                                                         non_linear_disk_bw_factory(config->non_linear_coef_read));
                            new_disk->set_sharing_policy(sg4::Disk::Operation::WRITE,
                                                         sg4::Disk::SharingPolicy::NONLINEAR,
                                                         non_linear_disk_bw_factory(config->non_linear_coef_write));
                        }
                        if ((config->read_variability != 1) or (config->write_variability != 1)) {
                            WRENCH_WARN("[PlatformFactory:create_platform] Using read and write variability factor on disk");
                            new_disk->set_factor_cb(
                                hdd_variability_factory(config->read_variability, config->write_variability));
                        }
                    }
                }
            }
        }

        // Also add one 'special' storage service for permanent storage (external to
        // the supercomputer, or at least not directly among the usual storage nodes
        // used by jobs)
        auto permanent_storage = storage_zone->create_host("permanent_storage", "2.6Tf")
                                     ->set_core_count(16)
                                     ->set_property("ram", "32GB")
                                     ->set_property("latency", "10");
        permanent_storage->create_disk("disk0", cfg->perm_storage_r_bw, cfg->perm_storage_w_bw)
            ->set_property("size", cfg->perm_storage_capa)
            ->set_property("mount", "/dev/disk0");
        // Link to storage backbone
        auto link_perm =
            storage_zone->create_split_duplex_link("permanent_storage", "100GBps")
                ->set_latency("24us")
                ->seal();
        storage_zone->add_route(
            permanent_storage->get_netpoint(), storage_router, nullptr, nullptr,
            {{link_perm, sg4::LinkInRoute::Direction::UP}, backbone_storage}, true);

        // And finally a node for the compound storage service itself
        auto cmpd_storage =
            storage_zone
                ->create_host("compound_storage", "2.6Tf")
                ->set_core_count(64)
                ->set_property("ram", "192GB")
                ->set_property("latency", "10");
        auto link_cpd =
            storage_zone->create_split_duplex_link("compound_storage", "25GBps")
                ->set_latency("24us")
                ->seal();
        storage_zone->add_route(
            cmpd_storage->get_netpoint(), storage_router, nullptr, nullptr,
            {{link_cpd, sg4::LinkInRoute::Direction::UP}, backbone_storage}, true);

        storage_zone->seal();

        // Route from main zone to sub zones.
        main_zone->add_route(storage_zone->get_netpoint(), nullptr, storage_router,
                             nullptr, {backbone});
        main_zone->add_route(control_zone->get_netpoint(), nullptr, control_router,
                             nullptr, {backbone});
        main_zone->add_route(compute_zone->get_netpoint(), nullptr, compute_router,
                             nullptr, {backbone});

        main_zone->seal();
    }

} // namespace storalloc