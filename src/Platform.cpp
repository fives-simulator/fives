#include "Platform.h"
#include "Constants.h"

#include <simgrid/forward.h>

#include <cmath>
#include <random>

WRENCH_LOG_CATEGORY(fives_platform, "Log category for Fives Platform factory");

namespace fives {

    /**
     * @brief Factory for the disk_dynamic_sharing callback used on the Simgrid Disk objects
     *        of the simulated storage system.
     */
    static auto non_linear_disk_bw_factory(const sg4::Disk *disk, float non_linear_coef) {
        /* capacity = Current Simgrid capacity of this disk
         * n_activities = Number of Simgrid activities sharing this resource (~ Wrench actions)
         */
        return [non_linear_coef, disk](double capacity, int n_activities) {
            // auto clock = wrench::S4U_Simulation::getClock();
            // std::cout << "[NL Cb at " << std::to_string(clock) << "] Capacity  " << std::to_string(capacity) << " / Activities : " << std::to_string(n_activities) << std::endl;
            // std::cout << "[NL disk] " << disk->get_name() << " on " << disk->get_host()->get_name() << std::endl;

            if (n_activities < 1) {
                n_activities = 1;
            }
            // std::cout << "[DEBUG NON LINEAR] OUTPUT : " << std::to_string(capacity * (non_linear_coef / std::sqrt(n_activities))) << std::endl;
            return capacity * (1 / std::log(n_activities * non_linear_coef));
        };
    }

    // REMOVE OR REPLACE WITH DETERMINISTIC VERSION ?
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
    static auto create_hostzone_factory(const std::string &flops, unsigned int core_count, unsigned int ram) {

        return [flops, core_count, ram](sg4::NetZone *zone,
                                        const std::vector<unsigned long> & /*coord*/,
                                        unsigned long id) {
            std::string hostname = "compute" + std::to_string(id);

            auto compute_host =
                zone->create_host(hostname, flops);
            compute_host->set_core_count(core_count);
            compute_host->set_property("ram", std::to_string(ram) + "GB");

            return compute_host;
        };
    }

    void PlatformFactory::create_platform(const std::shared_ptr<fives::Config> cfg) const {

        // --- TOP LEVEL ZONE AND MAIN BACKBONE ---
        auto main_zone = sg4::create_star_zone("AS_Root");
        auto main_link_bb =
            main_zone->create_link("backbone", cfg->net.bw_backbone)->set_latency(cfg->net.link_latency);
        // main_link_bb->set_sharing_policy(sg4::Link::SharingPolicy::NONLINEAR, non_linear_link_bw); // Last time used, simgrid crashed pretty hard
        sg4::LinkInRoute main_backbone{main_link_bb};
        auto main_zone_router = main_zone->create_router("main_zone_router");

        // --- CONTROL ZONE ---
        auto ctrl_zone = sg4::create_star_zone("AS_Ctrl");
        ctrl_zone->set_parent(main_zone);
        auto backbone_link_ctrl = ctrl_zone->create_link("backbone_ctrl", cfg->net.bw_backbone_ctrl)->set_latency(cfg->net.link_latency)->seal();

        // Create a user host & batch head node
        std::vector<s4u_Host *> control_hosts = {};
        control_hosts.push_back(ctrl_zone->create_host(USER, cfg->compute.flops)); // cfg->compute.flops));
        control_hosts.push_back(ctrl_zone->create_host(BATCH, cfg->compute.flops));
        auto control_router = ctrl_zone->create_router("ctrl_zone_router");
        sg4::LinkInRoute ctrl_backbone(backbone_link_ctrl);

        for (auto &ctrl_host : control_hosts) {
            ctrl_host->set_core_count(64);
            ctrl_host->set_property("ram", "64GB");

            auto uplink = ctrl_zone->create_link(ctrl_host->get_name() + "_up", SLOWLINK)
                              ->set_latency(cfg->net.link_latency)
                              ->seal();
            auto downlink = ctrl_zone->create_link(ctrl_host->get_name() + "_down", SLOWLINK)
                                ->set_latency(cfg->net.link_latency)
                                ->seal();

            sg4::LinkInRoute link_up{uplink};
            sg4::LinkInRoute link_down{downlink};
            ctrl_zone->add_route(ctrl_host, nullptr, {{link_up, ctrl_backbone}}, false);
            ctrl_zone->add_route(
                nullptr, ctrl_host,
                {{ctrl_backbone, link_down}}, false);
        }
        ctrl_zone->set_gateway(control_router);
        ctrl_zone->seal();

        // DRAGONFLY ZONE (COMPUTE)
        auto create_hostzone = create_hostzone_factory(cfg->compute.flops, cfg->compute.core_count, cfg->compute.ram);
        auto compute_zone = sg4::create_dragonfly_zone("AS_DragonflyCompute", main_zone,
                                                       {{cfg->compute.d_groups, cfg->compute.d_group_links},
                                                        {cfg->compute.d_chassis, cfg->compute.d_chassis_links},
                                                        {cfg->compute.d_routers, cfg->compute.d_router_links},
                                                        cfg->compute.d_nodes},
                                                       {create_hostzone, {}, create_limiter}, 10e9,
                                                       10e-6, sg4::Link::SharingPolicy::SPLITDUPLEX);
        // Add a global router for zone-zone routes
        auto compute_router = compute_zone->create_router("compute_router");
        compute_zone->set_gateway(compute_router);
        compute_zone->seal();

        // --- STORAGE ZONE (PFS) ---
        auto storage_zone = sg4::create_star_zone("AS_Storage");
        storage_zone->set_parent(main_zone);
        auto backbone_link_storage = storage_zone->create_link("backbone_storage",
                                                               cfg->net.bw_backbone_storage)
                                         ->set_latency(cfg->net.link_latency)
                                         ->seal();
        auto storage_router = storage_zone->create_router("storage_zone_router");
        sg4::LinkInRoute storage_backbone{backbone_link_storage};

        // Nodes for Simple storage services that will be accessed through the CSS
        for (const auto &node : config->stor.nodes) {
            for (unsigned int i = 0; i < node.qtt; i++) {

                auto hostname = node.tpl.id + std::to_string(i);
                auto storage_host = storage_zone->create_host(hostname, cfg->compute.flops);
                storage_host->set_core_count(16);

                // Link to storage backbone
                auto uplink = storage_zone->create_link(hostname + "_up", FASTLINK)
                                  ->set_latency(cfg->net.link_latency)
                                  ->seal();
                auto downlink = storage_zone->create_link(hostname + "_down", FASTLINK)
                                    ->set_latency(cfg->net.link_latency)
                                    ->seal();

                sg4::LinkInRoute link_up{uplink};
                sg4::LinkInRoute link_down{downlink};
                storage_zone->add_route(
                    storage_host, nullptr,
                    {{link_up, storage_backbone}}, false);
                storage_zone->add_route(
                    nullptr, storage_host,
                    {{storage_backbone, link_down}}, false);

                // Adding disk(s) to the current storage node
                for (const auto &disk : node.tpl.disks) {

                    for (unsigned int j = 0; j < disk.qtt; j++) {

                        auto new_disk = storage_host->create_disk(
                            disk.tpl.id + std::to_string(j),
                            std::to_string(disk.tpl.read_bw) + "MBps",
                            std::to_string(disk.tpl.write_bw) + "MBps");
                        new_disk->set_property("size", std::to_string(disk.tpl.capacity) + "GB");
                        new_disk->set_property("mount", disk.tpl.mount_prefix + std::to_string(j));

                        // Input for contention and variability on HDDs
                        if ((config->stor.non_linear_coef_read != 1) or (config->stor.non_linear_coef_write != 1)) {
                            WRENCH_DEBUG("[PlatformFactory:create_platform] Adding non linear sharing policy to disk %s%i on %s", disk.tpl.id.c_str(), j, hostname.c_str());
                            new_disk->set_sharing_policy(sg4::Disk::Operation::READ,
                                                         sg4::Disk::SharingPolicy::NONLINEAR,
                                                         non_linear_disk_bw_factory(new_disk, cfg->stor.non_linear_coef_read));
                            new_disk->set_sharing_policy(sg4::Disk::Operation::WRITE,
                                                         sg4::Disk::SharingPolicy::NONLINEAR,
                                                         non_linear_disk_bw_factory(new_disk, cfg->stor.non_linear_coef_write));
                        }
                        if ((config->stor.read_variability != 1) or (config->stor.write_variability != 1)) {
                            WRENCH_WARN("[PlatformFactory:create_platform] Using read and write variability factor on disk");
                            new_disk->set_factor_cb(
                                hdd_variability_factory(config->stor.read_variability, config->stor.write_variability));
                        }
                        new_disk->seal();
                    }
                }
            }
        }

        // Add a node for the compound storage service itself
        auto cmpd_storage =
            storage_zone
                ->create_host(COMPOUND_STORAGE, cfg->compute.flops)
                ->set_core_count(cfg->compute.core_count)
                ->set_property("ram", std::to_string(cfg->compute.ram) + "GB");

        auto css_uplink = storage_zone->create_link("compound_storage_up", FASTLINK)
                              ->set_latency(cfg->net.link_latency)
                              ->seal();
        auto css_downlink = storage_zone->create_link("compound_storage_down", FASTLINK)
                                ->set_latency(cfg->net.link_latency)
                                ->seal();

        sg4::LinkInRoute css_link_up{css_uplink};
        sg4::LinkInRoute css_link_down{css_downlink};
        storage_zone->add_route(
            cmpd_storage, nullptr,
            {{css_link_up, storage_backbone}}, false);
        storage_zone->add_route(
            nullptr, cmpd_storage,
            {{storage_backbone, css_link_down}}, false);
        storage_zone->set_gateway(storage_router);

        storage_zone->seal();

        // --- STORAGE ZONE (PERMANENT) ---
        auto pstorage_zone = sg4::create_star_zone("AS_StoragePermanent");
        pstorage_zone->set_parent(main_zone);
        // TODO: backbone bandwidth value could be a separate config field for this zone
        auto backbone_link_pstorage = pstorage_zone->create_link("backbone_permanent_storage",
                                                                 cfg->net.bw_backbone_perm_storage)
                                          ->set_latency(cfg->net.link_latency)
                                          ->seal();
        auto pstorage_router = pstorage_zone->create_router("permanent_storage_zone_router");

        auto perm_uplink = pstorage_zone->create_link("permanent_storage_up", FASTLINK)
                               ->set_latency(cfg->net.link_latency)
                               ->seal();
        auto perm_downlink = pstorage_zone->create_link("permanent_storage_down", FASTLINK)
                                 ->set_latency(cfg->net.link_latency)
                                 ->seal();

        sg4::LinkInRoute pstorage_backbone{backbone_link_pstorage};
        sg4::LinkInRoute link_up{perm_uplink};
        sg4::LinkInRoute link_down{perm_downlink};

        auto permanent_storage = pstorage_zone->create_host(PERMANENT_STORAGE, cfg->compute.flops)
                                     ->set_core_count(16)
                                     ->set_property("ram", "32GB");
        permanent_storage->create_disk(cfg->pstor.disk_id, cfg->pstor.r_bw, cfg->pstor.w_bw)
            ->set_property("size", cfg->pstor.capa)
            ->set_property("mount", cfg->pstor.mount_prefix);

        pstorage_zone->add_route(
            permanent_storage, nullptr,
            {{link_up, pstorage_backbone}}, false);
        pstorage_zone->add_route(
            nullptr, permanent_storage,
            {{pstorage_backbone, link_down}}, false);
        pstorage_zone->set_gateway(pstorage_router);
        pstorage_zone->seal();

        // ROUTES FROM MAIN BACKBONE TO SUB ZONES
        main_zone->add_route(storage_zone, nullptr, {main_backbone}, true);
        main_zone->add_route(ctrl_zone, nullptr, {main_backbone}, true);
        main_zone->add_route(compute_zone, nullptr, {main_backbone}, true);
        main_zone->add_route(pstorage_zone, nullptr, {main_backbone}, true);

        main_zone->seal();
    }

} // namespace fives