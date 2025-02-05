/**
 ** This is the main file for the simulator.
 ** It handles configuration and dataset loading, services instantiation
 ** and starting the simulation.
 */

#include "Simulator.h"

#include <chrono>
#include <iostream>

#include <yaml-cpp/yaml.h>

#include "AllocationStrategy.h"
#include "BuildInfo.h"
#include "Constants.h"
#include "Controller.h"
#include "Platform.h"
#include "Utils.h"

WRENCH_LOG_CATEGORY(fives_main,
                    "Log category for Fives main simulation process");

namespace fives {

    std::set<std::shared_ptr<wrench::StorageService>>
    instantiateStorageServices(std::shared_ptr<wrench::Simulation> simulation,
                               std::shared_ptr<Config> config) {

        // Simple storage services that will be accessed through
        // CompoundStorageService
        std::set<std::shared_ptr<wrench::StorageService>> sstorageservices;

        for (const auto &node : config->stor.nodes) { // node types

            // mount points list is the same for all nodes of a given type
            std::set<std::string> mount_points;
            for (const auto &disk : node.tpl.disks) {
                for (unsigned int j = 0; j < disk.qtt; j++) {
                    mount_points.insert(disk.tpl.mount_prefix + std::to_string(j));
                }
            }

            wrench::WRENCH_PROPERTY_COLLECTION_TYPE ss_params = {};
            if (config->stor.io_buffer_size != "0GB") {
                ss_params[wrench::SimpleStorageServiceProperty::BUFFER_SIZE] =
                    config->stor.io_buffer_size;
            }

            for (unsigned int i = 0; i < node.qtt; i++) { // qtt of each type
                auto disk_count = 0;
                for (const auto &mnt_pt : mount_points) {
                    sstorageservices.insert(simulation->add(
                        wrench::SimpleStorageService::createSimpleStorageService(
                            node.tpl.id + std::to_string(i), {mnt_pt}, ss_params, {})));
                }
            }
        }

        WRENCH_INFO("Using %ld storage services", sstorageservices.size());

        return sstorageservices;
    }

    std::shared_ptr<wrench::BatchComputeService>
    instantiateComputeServices(std::shared_ptr<wrench::Simulation> simulation,
                               std::shared_ptr<Config> config) {
        std::vector<std::string> compute_nodes;
        auto nb_compute_nodes = config->compute.d_nodes * config->compute.d_routers *
                                config->compute.d_chassis * config->compute.d_groups;
        WRENCH_INFO(
            "Dragonfly has %d available compute nodes, and config set limit is %u",
            nb_compute_nodes, config->compute.max_compute_nodes);
        for (unsigned int i = 0;
             i < nb_compute_nodes && i < config->compute.max_compute_nodes; i++) {
            compute_nodes.push_back("compute" + std::to_string(i));
        }
        auto batch_service = simulation->add(new wrench::BatchComputeService(
            BATCH, compute_nodes, "",
            {{wrench::BatchComputeServiceProperty::BATCH_SCHEDULING_ALGORITHM,
              "conservative_bf"}},
            {}));

        WRENCH_INFO("  -> %ld compute services instantiated", compute_nodes.size());

        return batch_service;
    }

    /**
     * Setup log level for xbt components (WRENCH/Simgrid logging)
     */
    void setLogLevel() {
        // Default log settings for a few components
        xbt_log_control_set("fives_jobs.thres:warning");
        xbt_log_control_set("fives_config.thres:info");
    }

    /**
     * Print Fives usage
     * @param binary: Name of this binary file
     */
    void usage(char *binary) {
        std::cout << "##########################################################################" << std::endl;
        std::cout << "# USAGE: " << binary << " <config file> <job file> <experiment_tag>" << std::endl;
        std::cout << "#          [Both files are expected to be YAML]" << std::endl;
        std::cout << "# This program starts a WRENCH simulation of a batch scheduler, with" << std::endl;
        std::cout << "# emphasis on storage resources model and collecting storage-related" << std::endl;
        std::cout << "# metrics" << std::endl;
        std::cout << "# Version : " << fives_version << std::endl;
        std::cout << "# WRENCH build : " << wrench_version << std::endl;
        std::cout << "# SimGrid build : " << simgrid_version << std::endl;
        std::cout << "# FSMod build version : " << fsmod_version << std::endl;
        std::cout << "# CMake config timestamp : " << config_ts << std::endl;
        std::cout << "##########################################################################" << std::endl;
    }

    /**
     * Update a string containing a path in order to keep only the stem
     * (part between the last '/' and the extension, excluded)
     */
    void reduceToStem(string &path) {
        auto start = path.find_last_of("/") + 1;
        path = path.substr(start, path.find_last_of(".") - start);
    }

    /**
     * Validate experiment tag format (should not contain '-' or space)
     *
     * @param tag : Tag to validate
     * @return : Validation result
     */
    bool isTagFormatCorrect(const std::string &tag) {
        if ((tag.find("-") != std::string::npos) or
            (tag.find(' ') != std::string::npos)) {
            WRENCH_WARN("ERROR: Experiment tag cannot contain '-' or space");
            return false;
        }
        return true;
    }

    std::shared_ptr<wrench::Simulation> createAndInitSimulation(int argc,
                                                                char **argv,
                                                                std::shared_ptr<Config> config) {

        /* Create the WRENCH simulation and platform */
        auto simulation = wrench::Simulation::createSimulation();
        simulation->init(&argc, argv);
        auto platform_factory = PlatformFactory(config);
        simulation->instantiatePlatform(platform_factory);
        simulation->getOutput().enableDiskTimestamps(true);

        return simulation;
    }

    std::shared_ptr<wrench::SimpleStorageService> createAndInitPermanentStorage(std::shared_ptr<wrench::Simulation> simulation,
                                                                                std::shared_ptr<Config> config) {

        /* Permanent storage */
        wrench::WRENCH_PROPERTY_COLLECTION_TYPE ss_params = {};
        if (config->pstor.io_buffer_size != "0GB") {
            ss_params[wrench::SimpleStorageServiceProperty::BUFFER_SIZE] =
                config->pstor.io_buffer_size;
        }
        auto permanent_storage =
            simulation->add(wrench::SimpleStorageService::createSimpleStorageService(
                PERMANENT_STORAGE, {config->pstor.mount_prefix}, ss_params, {}));

        return permanent_storage;
    }

    /**
     * @brief The Simulator's main function
     *
     * @param argc: argument count
     * @param argv: argument array
     * @return 0 on success, non-zero otherwise
     */
    int run_simulation(int argc, char **argv) {

        if (argc < 4) {
            usage(argv[0]);
            return 1;
        }

        std::string tag = argv[3];
        if (not isTagFormatCorrect(tag))
            return 1;

        // Record simulation time
        const std::chrono::time_point<std::chrono::steady_clock> chrono_start = std::chrono::steady_clock::now();

        setLogLevel();
        WRENCH_INFO("Starting StorAlloc simulator");

        /* Loading config and job files */
        std::shared_ptr<Config> config;
        std::string configFilename = argv[1];
        try {
            config = std::make_shared<Config>(fives::loadConfig(configFilename));
            reduceToStem(configFilename);
        } catch (const std::exception &e) {
            WRENCH_WARN("ERROR while loading config : %s", e.what());
            return 1;
        }

        std::map<std::string, YamlJob> jobs;
        std::string jobFilename = argv[2];
        try {
            jobs = fives::loadYamlJobs(jobFilename);
            reduceToStem(jobFilename);
        } catch (const std::exception &e) {
            WRENCH_WARN("ERROR while loading jobs : %s", e.what());
            return 1;
        }

        /* Instanciation / init of various services */
        auto simulation = createAndInitSimulation(argc, argv, config);
        auto sstorageservices = fives::instantiateStorageServices(simulation, config);

        LustreAllocator allocator(config);
        wrench::StorageSelectionStrategyCallback allocatorCallback =
            [&allocator](
                const std::shared_ptr<wrench::DataFile> &file,
                const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
                const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
                const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations,
                unsigned int stripe_count = 0) {
                return allocator(file, resources, mapping, previous_allocations,
                                 stripe_count);
            };

        /* Compound storage service*/
        auto css =
            simulation->add(new wrench::CompoundStorageService(
                COMPOUND_STORAGE, sstorageservices, allocatorCallback,
                {{wrench::CompoundStorageServiceProperty::MAX_ALLOCATION_CHUNK_SIZE,
                  std::to_string(config->max_stripe_size)},
                 {wrench::CompoundStorageServiceProperty::INTERNAL_STRIPING,
                  "false"}}, // because the Lustre allocator takes charge of striping
                {}));

        auto permanent_storage = createAndInitPermanentStorage(simulation, config);
        auto batch_service = fives::instantiateComputeServices(simulation, config);
        auto ctrl = simulation->add(new fives::Controller(batch_service, permanent_storage, css, USER, jobs, config));

        /* Running Wrench simulation */
        WRENCH_INFO("Starting simulation...");
        const auto sim_start = std::chrono::steady_clock::now();
        simulation->launch();
        const auto sim_end = std::chrono::steady_clock::now();

        /* Get task completion traces (debug only) */
        auto trace = simulation->getOutput()
                         .getTrace<wrench::SimulationTimestampTaskCompletion>();
        for (auto const &item : trace) {
            WRENCH_DEBUG("Task %s completed at time %f",
                         item->getContent()->getTask()->getID().c_str(),
                         item->getDate());
        }

        /* Job failure assessment (it might be a good idea to condition the return
            code of the simulation to a max. number of failed jobs when running
            calibration) */
        int return_code = 0;

        auto failed_cnt = ctrl->getFailedJobCount();
        if (failed_cnt > 0) {
            WRENCH_WARN("%lu jobs have failed", failed_cnt);
            std::cerr << "FAILED:" << failed_cnt << std::endl;
        }
        if (failed_cnt > jobs.size() * config->allowed_failure_percent) {
            WRENCH_WARN("More than %f of jobs have failed - Simulation fail",
                        config->allowed_failure_percent * 100);
            std::cerr << "More than " << failed_cnt
                      << " of jobs have failed - Simulation fail" << std::endl;
            return failed_cnt;
        }

        /* Extract traces into files tagged with dataset and config version. */
        try {
            /* Storage system traces (currently unavailable): */
            // ctrl->extractSSSIO(jobFilename, config->config_name + "_" +
            // config->config_version, tag);
            /* Job execution traces: */
            ctrl->processCompletedJobs(
                jobFilename, config->config_name + "_" + config->config_version, tag);
        } catch (const std::exception &e) {
            std::cout << "## ERROR in trace analysis : " << e.what() << std::endl;
            return 2;
        }

        const auto chrono_end = std::chrono::steady_clock::now();

        std::cout << "##########################################" << std::endl;
        std::cout << "# Program duration : " << (chrono_end - chrono_start) / 1ms << "ms" << std::endl;
        std::cout << "# Sim duration : " << (sim_end - sim_start) / 1ms << "ms" << std::endl;
        std::cout << "# Trace processing duration : " << (chrono_end - sim_end) / 1ms << "ms" << std::endl;
        std::cout << "##########################################" << std::endl;

        return 0;
    }

} // namespace fives