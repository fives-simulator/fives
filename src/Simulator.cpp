/**
 ** This is the main file for the simulator.
 ** It handles configuration and dataset loading, services instantiation
 ** and starting the simulation.
 */

#include "Simulator.h"

#include <chrono>
#include <iostream>

#include "yaml-cpp/yaml.h"
#include <simgrid/kernel/routing/NetPoint.hpp>

#include "AllocationStrategy.h"
#include "Constants.h"
#include "Controller.h"
#include "Platform.h"
#include "Utils.h"

WRENCH_LOG_CATEGORY(fives_main, "Log category for Fives main simulation process");

namespace fives {

    std::set<std::shared_ptr<wrench::StorageService>> instantiateStorageServices(std::shared_ptr<wrench::Simulation> simulation,
                                                                                 std::shared_ptr<Config> config) {

        // Simple storage services that will be accessed through CompoundStorageService
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
                ss_params[wrench::SimpleStorageServiceProperty::BUFFER_SIZE] = config->stor.io_buffer_size;
            }

            for (unsigned int i = 0; i < node.qtt; i++) { // qtt of each type
                auto disk_count = 0;
                for (const auto &mnt_pt : mount_points) {
                    sstorageservices.insert(
                        simulation->add(
                            wrench::SimpleStorageService::createSimpleStorageService(
                                node.tpl.id + std::to_string(i),
                                {mnt_pt},
                                ss_params,
                                {})));
                }
            }
        }

        WRENCH_INFO("Using %ld storage services", sstorageservices.size());

        return sstorageservices;
    }

    std::shared_ptr<wrench::BatchComputeService> instantiateComputeServices(std::shared_ptr<wrench::Simulation> simulation,
                                                                            std::shared_ptr<Config> config) {
        std::vector<std::string> compute_nodes;
        for (unsigned int i = 0; i < config->compute.max_compute_nodes; i++) {
            compute_nodes.push_back("compute" + std::to_string(i));
            // compute_nodes.push_back("compute" + std::to_string(i) + "_1");
        }
        auto batch_service = simulation->add(new wrench::BatchComputeService(
            BATCH, compute_nodes, "",
            {{wrench::BatchComputeServiceProperty::BATCH_SCHEDULING_ALGORITHM, "conservative_bf"}}, {}));

        WRENCH_INFO("  -> %ld compute services instantiated", compute_nodes.size());

        return batch_service;
    }

    /**
     * @brief The Simulator's main function
     *
     * @param argc: argument count
     * @param argv: argument array
     * @return 0 on success, non-zero otherwise
     */
    int run_simulation(int argc, char **argv) {

        const std::chrono::time_point<std::chrono::steady_clock> chrono_start = std::chrono::steady_clock::now();

        // Default log settings for a few components
        xbt_log_control_set("fives_jobs.thres:warning");
        xbt_log_control_set("fives_config.thres:info");
        xbt_log_control_set("fives_main.thres:info");

        if (argc < 4) {
            std::cout << "##########################################################################" << std::endl;
            std::cout << "# USAGE: " << argv[0] << " <config file> <job file> <experiment_suffix>" << std::endl;
            std::cout << "#          [Both files are expected to be YAML]" << std::endl;
            std::cout << "# This program starts a WRENCH simulation of a batch scheduler, with      " << std::endl;
            std::cout << "# emphasis on storage resources model and collecting storage-related      " << std::endl;
            std::cout << "# metrics.                                                                " << std::endl;
            std::cout << "##########################################################################" << std::endl;
            return 1;
        }

        // wrench::TerminalOutput::setThisProcessLoggingColor(wrench::TerminalOutput::COLOR_GREEN);
        WRENCH_INFO("Starting StorAlloc simulator");

        std::string tag = argv[3];
        if ((tag.find("-") != std::string::npos) or (tag.find(' ') != std::string::npos)) {
            WRENCH_WARN("Experiment tag cannot contain '-' or ' ' (space)");
            return 1;
        }

        // Load Compute and Storage configuration
        std::string configFilename = argv[1];
        std::shared_ptr<Config> config;
        try {
            config = std::make_shared<Config>(loadConfig(configFilename));
        } catch (const YAML::TypedBadConversion<fives::Config> &e) {
            WRENCH_WARN("ERROR : Unable to load config due to bad type conversion : %s", e.what());
            return 1;
        } catch (const YAML::InvalidNode &e) {
            WRENCH_WARN("ERROR : Unable to load config due to invalid config format : %s", e.what());
            return 1;
        }
        auto start = configFilename.find_last_of("/") + 1;
        configFilename = configFilename.substr(start, configFilename.find_last_of(".") - start);

        std::string jobFilename = argv[2];
        auto header = std::make_shared<JobsStats>(loadYamlHeader(jobFilename));
        auto jobs = fives::loadYamlJobs(jobFilename);
        start = jobFilename.find_last_of("/") + 1;
        jobFilename = jobFilename.substr(start, jobFilename.find_last_of(".") - start);

        /* Create a WRENCH simulation object */
        auto simulation = wrench::Simulation::createSimulation();
        simulation->init(&argc, argv);

        /* Instantiating the simulated platform with user-provided config */
        WRENCH_INFO("Instantiating platform...");
        auto platform_factory = PlatformFactory(config);
        simulation->instantiatePlatform(platform_factory);
        simulation->getOutput().enableDiskTimestamps(true);

        /* Simple storage services and compound storage service */
        WRENCH_INFO(" - Instantiating individual storage services...");
        auto sstorageservices = fives::instantiateStorageServices(simulation, config);

        /**
         * What is this useless sorcery you say? Well it was either that or template + type traits + shared_ptr --'
         * The issue: if you pass to the CompoundStorageService constructor a functor that has the same signature as a
         * StorageSelectionStrategyCallback, but isn't one, it will be implicitely converted to a StorageSelectionStrategyCallback.
         * Because we want to work with references, what seems to happen is that the CSS ends up not with a reference to the actual
         * functor, but to the (temporary?) StorageSelectionStrategyCallback created from converting our functor.
         * This has no consequences... except if the allocator (eg. LustreAllocator here), has member variables which were initialized
         * before the conversion/copy (maybe we could go around the issue by specifying a user defined conversion function but...)
         */
        LustreAllocator allocator(config);
        wrench::StorageSelectionStrategyCallback allocatorCallback = [&allocator](
                                                                         const std::shared_ptr<wrench::DataFile> &file,
                                                                         const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
                                                                         const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
                                                                         const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations,
                                                                         unsigned int stripe_count = 0) {
            return allocator(file, resources, mapping, previous_allocations, stripe_count);
        };

        WRENCH_INFO(" - Instantiating CSS...");
        /* Compound storage service*/
        auto compound_storage_service = simulation->add(
            new wrench::CompoundStorageService(
                COMPOUND_STORAGE,
                sstorageservices,
                allocatorCallback,
                {{wrench::CompoundStorageServiceProperty::MAX_ALLOCATION_CHUNK_SIZE, std::to_string(config->max_stripe_size)},
                 {wrench::CompoundStorageServiceProperty::INTERNAL_STRIPING, "false"}}, // because the Lustre allocator takes charge of striping
                {}));

        /* Permanent storage */
        wrench::WRENCH_PROPERTY_COLLECTION_TYPE ss_params = {};
        if (config->pstor.io_buffer_size != "0GB") {
            ss_params[wrench::SimpleStorageServiceProperty::BUFFER_SIZE] = config->pstor.io_buffer_size;
        }
        auto permanent_storage = simulation->add(
            wrench::SimpleStorageService::createSimpleStorageService(
                PERMANENT_STORAGE, {config->pstor.mount_prefix}, ss_params, {}));

        /* Batch compute service */
        WRENCH_INFO(" - Instantiating individual compute services and BatchComputeService...");
        auto batch_service = fives::instantiateComputeServices(simulation, config);

        /* Execution controller */
        auto ctrl = simulation->add(
            new fives::Controller(batch_service, permanent_storage, compound_storage_service, USER, header, jobs, config));

        /* Start Wrench simulation */
        WRENCH_INFO("Starting simulation...");
        const std::chrono::time_point<std::chrono::steady_clock> sim_start = std::chrono::steady_clock::now();
        simulation->launch();
        const std::chrono::time_point<std::chrono::steady_clock> sim_end = std::chrono::steady_clock::now();

        /* Get task completion traces (debug only) */
        auto trace = simulation->getOutput().getTrace<wrench::SimulationTimestampTaskCompletion>();
        for (auto const &item : trace) {
            WRENCH_DEBUG("Task %s completed at time %f", item->getContent()->getTask()->getID().c_str(), item->getDate());
        }

        /* Job failure assessment (it might be a good idea to condition the return code of
           the simulation to a max. number of failed jobs when running calibration) */
        int return_code = 0;
        auto failed_cnt = ctrl->getFailedJobCount();
        if (failed_cnt > 0) {
            WRENCH_WARN("%lu jobs have failed", failed_cnt);
            std::cerr << "FAILED:" << failed_cnt << std::endl;
        }
        if (failed_cnt > jobs.size() * config->allowed_failure_percent) {
            WRENCH_WARN("More than %f of jobs have failed - Simulation fail", config->allowed_failure_percent * 100);
            std::cerr << "More than " << failed_cnt << " of jobs have failed - Simulation fail" << std::endl;
            return failed_cnt;
        }

        /* Extract traces into files tagged with dataset and config version. */
        try {
            /* Storage system traces (currently unavailable): */
            // ctrl->extractSSSIO(jobFilename, config->config_name + "_" + config->config_version, tag);
            /* Job execution traces: */
            ctrl->processCompletedJobs(jobFilename, config->config_name + "_" + config->config_version, tag);
        } catch (const std::exception &e) {
            std::cout << "## ERROR in trace analysis : " << e.what() << std::endl;
            return 2;
        }

        const std::chrono::time_point<std::chrono::steady_clock> chrono_end = std::chrono::steady_clock::now();

        std::cout << "##########################################" << std::endl;
        std::cout << "# Program duration : " << (chrono_end - chrono_start) / 1ms << "ms" << std::endl;
        std::cout << "# Sim duration : " << (sim_end - sim_start) / 1ms << "ms" << std::endl;
        std::cout << "# Trace processing duration : " << (chrono_end - sim_end) / 1ms << "ms" << std::endl;
        std::cout << "##########################################" << std::endl;

        return 0;
    }

} // namespace fives