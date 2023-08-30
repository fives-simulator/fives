/**
 ** This is the main function for a WRENCH simulator. The simulator takes
 ** a input an XML platform description file. It generates a workflow with
 ** a simple diamond structure, instantiates a few services on the platform, and
 ** starts an execution controller to execute the workflow using these services
 ** using a simple greedy algorithm.
 **/

#include "Simulator.h"

#include <iostream>

#include "yaml-cpp/yaml.h"
#include <simgrid/plugins/energy.h>

#include "AllocationStrategy.h"
#include "Controller.h"
#include "Platform.h"
#include "Utils.h"

std::set<std::shared_ptr<wrench::StorageService>> storalloc::instantiateStorageServices(std::shared_ptr<wrench::Simulation> simulation,
                                                                                        std::shared_ptr<storalloc::Config> config) {

    // Simple storage services that will be accessed through CompoundStorageService
    std::set<std::shared_ptr<wrench::StorageService>> sstorageservices;

    for (const auto &node : config->nodes) { // node types

        // mount points list is the same for all nodes of a given type
        std::set<std::string> mount_points;
        for (const auto &disk : node.tpl.disks) {
            for (auto j = 0; j < disk.qtt; j++) {
                mount_points.insert(disk.tpl.mount_prefix + std::to_string(j));
            }
        }

        for (auto i = 0; i < node.qtt; i++) { // qtt of each type
            auto disk_count = 0;
            for (const auto &mnt_pt : mount_points) {
                // std::cout << "Inserting a new SimpleStorageService on node " << node.tpl.id << std::to_string(i) << " for disk " << mnt_pt << std::endl;
                sstorageservices.insert(
                    simulation->add(
                        wrench::SimpleStorageService::createSimpleStorageService(
                            node.tpl.id + std::to_string(i),
                            {mnt_pt}, {}, {})));
            }
        }
    }

    std::cout << "# Using " << sstorageservices.size() << " storage services" << std::endl;

    return sstorageservices;
}

std::shared_ptr<wrench::BatchComputeService> storalloc::instantiateComputeServices(std::shared_ptr<wrench::Simulation> simulation,
                                                                                   std::shared_ptr<storalloc::Config> config) {
    std::vector<std::string> compute_nodes;
    auto nb_compute_nodes = config->d_nodes * config->d_routers * config->d_chassis * config->d_groups;
    std::cout << "# Using " << nb_compute_nodes << " compute nodes" << std::endl;
    for (unsigned int i = 0; i < nb_compute_nodes; i++) {
        compute_nodes.push_back("compute" + std::to_string(i));
    }
    auto batch_service = simulation->add(new wrench::BatchComputeService(
        "batch0", compute_nodes, "",
        {{wrench::BatchComputeServiceProperty::BATCH_SCHEDULING_ALGORITHM, "conservative_bf"}}, {}));

    return batch_service;
}

/**
 * @brief The Simulator's main function
 *
 * @param argc: argument count
 * @param argv: argument array
 * @return 0 on success, non-zero otherwise
 */
int storalloc::run_simulation(int argc, char **argv) {

    if (argc < 3) {
        std::cout << "###############################################################" << std::endl;
        std::cout << "# USAGE: " << argv[0] << " <config file> <job file>" << std::endl;
        std::cout << "#          [Both files are expected to be YAML]" << std::endl;
        std::cout << "# This program starts a WRENCH simulation of a batch scheduler" << std::endl;
        std::cout << "###############################################################" << std::endl;
        return 1;
    }

    std::string tag = "i";
    if (argc == 4) {
        tag = argv[3];
    }

    // Load Compute and Storage configuration
    std::string configFilename = argv[1];
    std::shared_ptr<storalloc::Config> config;
    try {
        config = std::make_shared<storalloc::Config>(storalloc::loadConfig(configFilename));
    } catch (const YAML::TypedBadConversion<storalloc::Config> &e) {
        cout << "ERROR : Unable to load config due to bad type conversion : " << e.what() << std::endl;
        return 1;
    } catch (const YAML::InvalidNode &e) {
        cout << "ERROR : Unable to load config due to invalid config format : " << e.what() << std::endl;
        return 1;
    }

    auto start = configFilename.find_last_of("/") + 1;
    configFilename = configFilename.substr(start, configFilename.find_last_of(".") - start);
    std::cout << "Config filename (no ext) : " << configFilename << std::endl;

    /* Loading jobs */
    std::string jobFilename = argv[2];
    auto header = std::make_shared<storalloc::JobsStats>(storalloc::loadYamlHeader(jobFilename));

    auto jobs = storalloc::loadYamlJobs(jobFilename);
    start = jobFilename.find_last_of("/") + 1;
    jobFilename = jobFilename.substr(start, jobFilename.find_last_of(".") - start);
    std::cout << "Jobs filename (no ext) : " << jobFilename << std::endl;

    /* Create a WRENCH simulation object */
    auto simulation = wrench::Simulation::createSimulation();

    // sg_host_energy_plugin_init();

    /* Initialize the simulation */
    simulation->init(&argc, argv);

    /* Instantiating the simulated platform with user-provided config*/
    auto platform_factory = storalloc::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);
    simulation->getOutput().enableDiskTimestamps(true);

    /* Simple storage services */
    auto sstorageservices = storalloc::instantiateStorageServices(simulation, config);

    auto config_ref = *(config);
    auto allocator = LustreAllocator(config_ref);

    /* Compound storage service*/
    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "compound_storage",
            sstorageservices,
            allocator,
            {{wrench::CompoundStorageServiceProperty::MAX_ALLOCATION_CHUNK_SIZE, std::to_string(config->max_stripe_size)},
             {wrench::CompoundStorageServiceProperty::INTERNAL_STRIPING, "false"}},
            {}));
    std::cout << "Right after CSS creation" << std::endl;

    /* Permanent storage */
    auto permanent_storage = simulation->add(
        wrench::SimpleStorageService::createSimpleStorageService(
            "permanent_storage", {"/dev/disk0"}, {}, {}));

    /* Batch compute service */
    auto batch_service = storalloc::instantiateComputeServices(simulation, config);

    /* Execution controller */
    auto ctrl = simulation->add(
        new storalloc::Controller(batch_service, permanent_storage, compound_storage_service, "user0", header, jobs, config));

    /* Launch the simulation */
    std::cout << "Launching simulation..." << std::endl;
    std::cout << "Right before launching simulation, use_count on config is " << config.use_count() << std::endl;
    simulation->launch();

    // Playing around with energy plugin, not useful so far
    // auto storage_host = sg4::Host::by_name("compound_storage");
    // auto consummed = sg_host_get_consumed_energy(storage_host);
    // std::cout << "Energy consumed : " << consummed << std::endl;
    // simulation->getOutput().dumpDiskOperationsJSON("./wrench_disk_ops.json", true);
    // simulation->getOutput().dumpHostEnergyConsumptionJSON("./wrench_energy_consumption", true);

    auto trace = simulation->getOutput().getTrace<wrench::SimulationTimestampTaskCompletion>();
    for (auto const &item : trace) {
        std::cout << "Task " << item->getContent()->getTask()->getID() << " completed at time " << item->getDate() << std::endl;
    }

    auto action_results = ctrl->actionsAllCompleted();
    if (not action_results) {
        std::cout << "Some actions have failed" << std::endl;
    }

    // Extract traces into files tagged with dataset and config version.
    ctrl->extractSSSIO(jobFilename, config->config_name + "_" + config->config_version, tag);
    ctrl->processCompletedJobs(jobFilename, config->config_name + "_" + config->config_version, tag);

    return 0;
}