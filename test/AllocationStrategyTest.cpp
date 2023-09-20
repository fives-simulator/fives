#include <cstdint>
#include <regex>

#include <gtest/gtest.h>

#include "./include/TestConstants.h"
#include "./include/TestWithFork.h"

#include "../include/AllocationStrategy.h"
#include "../include/Controller.h"
#include "../include/Platform.h"
#include "../include/Simulator.h"
#include "../include/Utils.h"

using namespace storalloc;

class BasicAllocTest : public ::testing::Test {

public:
    void lustreUseRR_test();
    void lustreOstPenalty_test();
    void lustreOssPenalty_test();
    void lustreComputeOstWeight_test();
    void lustreComputeStriping_test();

    std::shared_ptr<Config> cfg;

protected:
    ~BasicAllocTest() {
    }

    BasicAllocTest() {
        cfg = std::make_shared<Config>();
        this->cfg->lustre = LustreConfig(); // default lustre config
    }
};

/**********************************************************************/
/**  Testing allocator selection (rr or weighted)                    **/
/**********************************************************************/

TEST_F(BasicAllocTest, lustreUseRR_test) {
    DO_TEST_WITH_FORK(lustreUseRR_test);
}

/**
 *  @brief  Testing lustreUseRR(), responsible for deciding whether to use
 *          the RR allocator or the Weighted allocator.
 */
void BasicAllocTest::lustreUseRR_test() {

    const uint64_t GB = 1000 * 1000 * 1000;

    // min and max free space in bytes
    struct storalloc::ba_min_max test_min_max;

    auto allocator = LustreAllocator(this->cfg);

    // Right above the 17% threshold
    test_min_max.min = 833 * GB;
    test_min_max.max = 1000 * GB;
    ASSERT_TRUE(allocator.lustreUseRR(test_min_max));

    // Slightly below the 17% threshold
    test_min_max.min = 830 * GB;
    test_min_max.max = 1000 * GB;
    ASSERT_FALSE(allocator.lustreUseRR(test_min_max));

    // Same free space
    test_min_max.min = 1000 * GB;
    test_min_max.max = 1000 * GB;
    ASSERT_TRUE(allocator.lustreUseRR(test_min_max));

    // High values (overflow testing)
    test_min_max.min = 1000 * 1000 * GB;
    test_min_max.max = 1000 * 1000 * GB;
    ASSERT_TRUE(allocator.lustreUseRR(test_min_max));
}

/**********************************************************************/
/**  Testing OST penalty computation                                 **/
/**********************************************************************/

TEST_F(BasicAllocTest, lustreOstPenalty_test) {
    DO_TEST_WITH_FORK(lustreOstPenalty_test);
}

/**
 *  @brief Testing lustreComputeOstPenalty() (disk/raid level)
 */
void BasicAllocTest::lustreOstPenalty_test() {

    auto allocator = LustreAllocator(this->cfg);
    const uint64_t GB = 1000 * 1000 * 1000;

    uint64_t free_space_b = 250 * GB; // 250 GB free
    uint64_t used_inode_count = 1'200'000;
    size_t active_service_count = 200; // ~ number of active targets, aka disks/raid in the whole system

    ASSERT_EQ(allocator.lustreComputeOstPenalty(free_space_b,
                                                used_inode_count,
                                                active_service_count),
              14995807557);

    free_space_b = 0;
    ASSERT_EQ(allocator.lustreComputeOstPenalty(free_space_b,
                                                used_inode_count,
                                                active_service_count),
              0);

    free_space_b = 250 * GB;
    used_inode_count = this->cfg->lustre.max_inodes;
    ASSERT_EQ(allocator.lustreComputeOstPenalty(free_space_b,
                                                used_inode_count,
                                                active_service_count),
              0);
    used_inode_count = 1'200'000;

    ASSERT_THROW(allocator.lustreComputeOstPenalty(free_space_b,
                                                   used_inode_count,
                                                   0),
                 std::runtime_error);

    // The more services / active targets, the smaller the penalty per target
    ASSERT_GT(allocator.lustreComputeOstPenalty(free_space_b,
                                                used_inode_count,
                                                active_service_count),
              allocator.lustreComputeOstPenalty(free_space_b,
                                                used_inode_count,
                                                300));
    // The more bytes available for allocation, the larger the penalty
    ASSERT_GT(allocator.lustreComputeOstPenalty(400 * GB,
                                                used_inode_count,
                                                active_service_count),
              allocator.lustreComputeOstPenalty(free_space_b,
                                                used_inode_count,
                                                active_service_count));

    // The more inodes available for allocation, the larger the penalty
    ASSERT_GT(allocator.lustreComputeOstPenalty(free_space_b,
                                                used_inode_count - 300000,
                                                active_service_count),
              allocator.lustreComputeOstPenalty(free_space_b,
                                                used_inode_count,
                                                active_service_count));
}

TEST_F(BasicAllocTest, lustreOssPenalty_test) {
    DO_TEST_WITH_FORK(lustreOssPenalty_test);
}

/**
 *  @brief Testing lustreComputeOssPenalty() (storage server level)
 */
void BasicAllocTest::lustreOssPenalty_test() {

    auto allocator = LustreAllocator(this->cfg);

    const uint64_t GB = 1000 * 1000 * 1000;

    // Let's suppose we have 8 OSTs in this OSS, and a total of 10 OSS
    uint64_t srv_free_space_b = (250 * GB >> 16) * 8;
    uint64_t srv_free_inode_count = ((this->cfg->lustre.max_inodes - 1'200'000) >> 8) * 8;
    size_t service_count = 200; // Targets / OSTs
    size_t oss_count = 10;      // Storage nodes /  OSS

    ASSERT_EQ(allocator.lustreComputeOssPenalty(srv_free_space_b,
                                                srv_free_inode_count,
                                                service_count,
                                                oss_count),
              95973168366);

    ASSERT_EQ(allocator.lustreComputeOssPenalty(0,
                                                srv_free_inode_count,
                                                service_count,
                                                oss_count),
              0);

    ASSERT_EQ(allocator.lustreComputeOssPenalty(srv_free_space_b,
                                                0,
                                                service_count,
                                                oss_count),
              0);

    // Divide / 0
    ASSERT_THROW(allocator.lustreComputeOssPenalty(srv_free_space_b,
                                                   srv_free_inode_count,
                                                   0,
                                                   oss_count),
                 std::runtime_error);

    // Divide / 0
    ASSERT_THROW(allocator.lustreComputeOssPenalty(srv_free_space_b,
                                                   srv_free_inode_count,
                                                   service_count,
                                                   0),
                 std::runtime_error);

    // The more OSS in the system, the lower the per-OSS penalty
    ASSERT_GT(allocator.lustreComputeOssPenalty(srv_free_space_b,
                                                srv_free_inode_count,
                                                service_count,
                                                oss_count),
              allocator.lustreComputeOssPenalty(srv_free_space_b,
                                                srv_free_inode_count,
                                                service_count,
                                                oss_count + 40));

    // The more OSTs in the whole system, the lower the per-OSS penalty
    ASSERT_GT(allocator.lustreComputeOssPenalty(srv_free_space_b,
                                                srv_free_inode_count,
                                                service_count,
                                                oss_count),
              allocator.lustreComputeOssPenalty(srv_free_space_b,
                                                srv_free_inode_count,
                                                service_count + 60,
                                                oss_count));

    // The more space avail on OSS the larger the penalty
    ASSERT_GT(allocator.lustreComputeOssPenalty(srv_free_space_b + 400 * GB,
                                                srv_free_inode_count,
                                                service_count,
                                                oss_count),
              allocator.lustreComputeOssPenalty(srv_free_space_b,
                                                srv_free_inode_count,
                                                service_count,
                                                oss_count));

    // The more inodes avail on OSS the larger the penalty
    ASSERT_GT(allocator.lustreComputeOssPenalty(srv_free_space_b,
                                                srv_free_inode_count + 200'000,
                                                service_count,
                                                oss_count),
              allocator.lustreComputeOssPenalty(srv_free_space_b,
                                                srv_free_inode_count,
                                                service_count,
                                                oss_count));
}

TEST_F(BasicAllocTest, lustreComputeOstWeight_test) {
    DO_TEST_WITH_FORK(lustreComputeOstWeight_test);
}

/**
 *  @brief Testing lustreComputeOstWeight() (disk/raid level)
 */
void BasicAllocTest::lustreComputeOstWeight_test() {

    auto allocator = LustreAllocator(this->cfg);

    const uint64_t GB = 1000 * 1000 * 1000;

    uint64_t free_space_b = 250 * GB;
    uint64_t used_inode_count = 1'200'000;
    size_t service_count = 12;

    auto ost_penalty = allocator.lustreComputeOstPenalty(free_space_b,
                                                         this->cfg->lustre.max_inodes - used_inode_count,
                                                         service_count);

    ASSERT_EQ(allocator.lustreComputeOstWeight(free_space_b,
                                               used_inode_count,
                                               ost_penalty),
              63982042402279);

    // The more bytes available, the larger the weight
    ost_penalty = allocator.lustreComputeOstPenalty(free_space_b + (100 * GB),
                                                    this->cfg->lustre.max_inodes - used_inode_count,
                                                    service_count);
    ASSERT_GT(allocator.lustreComputeOstWeight(free_space_b + (100 * GB),
                                               used_inode_count,
                                               ost_penalty),
              allocator.lustreComputeOstWeight(free_space_b,
                                               used_inode_count,
                                               ost_penalty));

    // The more inodes available, the larger the weight
    ost_penalty = allocator.lustreComputeOstPenalty(free_space_b,
                                                    this->cfg->lustre.max_inodes - used_inode_count - 300'000,
                                                    service_count);
    ASSERT_GT(allocator.lustreComputeOstWeight(free_space_b,
                                               used_inode_count - 300'000,
                                               ost_penalty),
              allocator.lustreComputeOstWeight(free_space_b,
                                               used_inode_count,
                                               ost_penalty));

    // If OST penalty is larger than weight, weight becomes 0
    ASSERT_EQ(allocator.lustreComputeOstWeight(free_space_b,
                                               used_inode_count,
                                               (free_space_b >> 16) * ((this->cfg->lustre.max_inodes - used_inode_count) >> 8) + 1),
              0);
}

TEST_F(BasicAllocTest, lustreComputeStriping_test) {
    DO_TEST_WITH_FORK(lustreComputeStriping_test);
}

/**
 *  @brief Testing lustreComputeStripesPerOST() (disk/raid level)
 */
void BasicAllocTest::lustreComputeStriping_test() {

    auto allocator = LustreAllocator(this->cfg);
    auto striping = allocator.lustreComputeStriping(3000000000, 85);
    ASSERT_EQ(striping.stripe_size_b, 300000000);
    ASSERT_EQ(striping.stripes_count, 1);
    ASSERT_EQ(striping.stripes_per_ost, 1);
    ASSERT_THROW(allocator.lustreComputeStriping(3000000000, 0), std::runtime_error);

    auto cfg_2 = std::make_shared<Config>();
    cfg_2->lustre = LustreConfig();  // default lustre config
    cfg_2->lustre.stripe_count = 50; // load-balancing on 50 OSTs
    auto allocator2 = LustreAllocator(cfg_2);

    striping = {};
    striping = allocator2.lustreComputeStriping(30000000000, 85);
    ASSERT_EQ(striping.stripe_size_b, 60000000);
    ASSERT_EQ(striping.stripes_count, 50);
    ASSERT_EQ(striping.stripes_per_ost, 1);

    auto cfg_3 = std::make_shared<Config>();
    cfg_3->lustre = LustreConfig();        // default lustre config
    cfg_3->lustre.stripe_count = 50;       // load-balancing on 50 OSTs
    cfg_3->lustre.max_chunks_per_ost = 45; // allowing for more chunks on each OST
    auto allocator3 = LustreAllocator(cfg_3);

    striping = {};
    striping = allocator3.lustreComputeStriping(3000000, 85);
    ASSERT_EQ(striping.stripe_size_b, 2097152);
    ASSERT_EQ(striping.stripes_count, 50);
    ASSERT_EQ(striping.stripes_per_ost, 1);
}

// ###################################################################################################

class FunctionalAllocTest : public ::testing::Test {

public:
    void lustreComputeMinMaxUtilization_test();
    void lustreOstIsUsed_test();
    void lustreRROrderServices_test();
    void lustreRROrderServices2_test(); // != config file in simulation
    void lustreRROrderServices3_test(); // != config file in simulation
    void lustreCreateFileParts_test();
    void lustreFullSim_test();

protected:
    ~FunctionalAllocTest() {}
    FunctionalAllocTest() {}
};

/**
 * @brief Basic custom controller for tests on Lustre allocation
 */
class LustreTestController : public wrench::ExecutionController {
public:
    LustreTestController(const std::shared_ptr<wrench::ComputeService> &compute_service,
                         const std::set<std::shared_ptr<wrench::StorageService>> &storage_services,
                         const std::shared_ptr<wrench::CompoundStorageService> compound_storage_svc,
                         const std::string &hostname,
                         std::shared_ptr<LustreAllocator> alloc) : wrench::ExecutionController(hostname, "controller"),
                                                                   storage_services(storage_services),
                                                                   compound(compound_storage_svc),
                                                                   compute_svc(compute_service),
                                                                   alloc(alloc) {

        this->file_10GB = wrench::Simulation::addFile("file_10GB", 10000000000); // 10GB file
        this->file_20GB = wrench::Simulation::addFile("file_20GB", 20000000000); // 20GB file
        this->file_30GB = wrench::Simulation::addFile("file_30GB", 30000000000); // 30GB file
        this->file_40GB = wrench::Simulation::addFile("file_40GB", 40000000000); // 40GB file
        this->file_50GB = wrench::Simulation::addFile("file_50GB", 50000000000); // 50GB file
    }

    std::shared_ptr<wrench::DataFile> file_10GB;
    std::shared_ptr<wrench::DataFile> file_20GB;
    std::shared_ptr<wrench::DataFile> file_30GB;
    std::shared_ptr<wrench::DataFile> file_40GB;
    std::shared_ptr<wrench::DataFile> file_50GB;

    std::set<std::shared_ptr<wrench::StorageService>> storage_services;
    std::shared_ptr<wrench::CompoundStorageService> compound;
    std::shared_ptr<wrench::ComputeService> compute_svc;
    std::shared_ptr<storalloc::LustreAllocator> alloc;
};

/**
 * @brief Custom controller for testing lustreComputeMinMaxUtilization
 */
class LustreTestControllerMinMax : public LustreTestController {
public:
    LustreTestControllerMinMax(const std::shared_ptr<wrench::ComputeService> &compute_service,
                               const std::set<std::shared_ptr<wrench::StorageService>> &storage_services,
                               const std::shared_ptr<wrench::CompoundStorageService> compound_storage_svc,
                               const std::string &hostname,
                               std::shared_ptr<LustreAllocator> alloc) : LustreTestController(compute_service, storage_services, compound_storage_svc, hostname, alloc) {}

    int main() override {

        // Prepare a map of services for lustreComputeMinMaxUtilization
        std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> storage_map;
        for (const auto &service : this->storage_services) {
            if (storage_map.find(service->getHostname()) == storage_map.end()) {
                storage_map[service->getHostname()] = std::vector<std::shared_ptr<wrench::StorageService>>();
            }
            storage_map[service->getHostname()].push_back(service);
        }

        // Create a dummy job writing data
        auto job_manager = this->createJobManager();
        auto job = job_manager->createCompoundJob("Job1");

        auto simple = std::dynamic_pointer_cast<wrench::SimpleStorageService>(*(this->storage_services.begin()));
        uint64_t free_space_in_service = simple->getTotalFreeSpaceZeroTime();

        job->addFileWriteAction(
            "write1", wrench::FileLocation::LOCATION(*(this->storage_services.begin()), this->file_50GB));
        job->addFileWriteAction(
            "write2", wrench::FileLocation::LOCATION(*(this->storage_services.begin()), this->file_50GB));
        job->addFileWriteAction(
            "write3", wrench::FileLocation::LOCATION(*(this->storage_services.begin()), this->file_50GB));

        std::map<std::string, std::string> service_specific_args =
            {{"-N", "2"},     // nb of nodes
             {"-c", "16"},    // core per node
             {"-t", "6000"}}; // seconds
        job_manager->submitJob(job, this->compute_svc, service_specific_args);

        // Sleep until we're sure the writes have begun (and thus, space is reserved on the service)
        wrench::Simulation::sleep(300);

        // Test lustreComputeMinMaxUtilization
        auto ba_min_max = this->alloc->lustreComputeMinMaxUtilization(storage_map);

        // Max free space is 200 GB (>>8)
        uint64_t expected_max = 20000000000000 >> 8; // bitshift for overflow, as computed in Lustre
        // Min free space should be 200GB - 150GB (files being written) (>>8)
        uint64_t expected_min = (free_space_in_service - 150000000000) >> 8;

        if (ba_min_max.max != expected_max) {
            throw std::runtime_error("Max free space != from expected max");
        }
        if (ba_min_max.min != expected_min) {
            std::cout << "Free space in service (before job) " << std::to_string(free_space_in_service) << std::endl;
            std::cout << "Expected min " << std::to_string(expected_min) << std::endl;
            throw std::runtime_error("Min free space != from expected min");
        }

        // While we're at it, also test whether or not we should use the weighted allocator in this case
        if (!this->alloc->lustreUseRR(ba_min_max)) {
            throw std::runtime_error("We should be using the RR allocator");
        }

        // Wait for job completion (making sure nothing stalled)
        auto event = this->waitForNextEvent();
        if (std::dynamic_pointer_cast<wrench::CompoundJobCompletedEvent>(event) == nullptr) {
            throw std::runtime_error("Test failed because job did not complete");
        }

        return 0;
    }
};

TEST_F(FunctionalAllocTest, lustreComputeMinMaxUtilization_test) {
    DO_TEST_WITH_FORK(lustreComputeMinMaxUtilization_test);
}

/**
 *  @brief Testing lustreComputeMinMaxUtilization()
 */
void FunctionalAllocTest::lustreComputeMinMaxUtilization_test() {

    auto config = std::make_shared<storalloc::Config>(storalloc::loadConfig(test::CONFIG_PATH + "lustre_config_base.yml"));

    // Check that the default value for max_inodes was not overriden during config load
    ASSERT_EQ(config->lustre.max_inodes, (1ULL << 32));
    // Check that the default value for stripe size was correctly overrriden
    ASSERT_EQ(config->lustre.stripe_size, 40000000);

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    ASSERT_NO_THROW(simulation->init(&argc, argv));

    auto platform_factory = storalloc::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);

    auto batch_service = storalloc::instantiateComputeServices(simulation, config);
    auto sstorageservices = storalloc::instantiateStorageServices(simulation, config);
    LustreAllocator allocator(config);
    wrench::StorageSelectionStrategyCallback allocatorCallback = [&allocator](
                                                                     const std::shared_ptr<wrench::DataFile> &file,
                                                                     const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
                                                                     const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
                                                                     const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations) {
        return allocator(file, resources, mapping, previous_allocations);
    };

    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "compound_storage",
            sstorageservices,
            allocatorCallback,
            {{wrench::CompoundStorageServiceProperty::MAX_ALLOCATION_CHUNK_SIZE, std::to_string(config->lustre.stripe_size)},
             {wrench::CompoundStorageServiceProperty::INTERNAL_STRIPING, "false"}},
            // {{wrench::CompoundStorageServiceProperty::MAX_ALLOCATION_CHUNK_SIZE, "30000000000"}},
            {}));

    auto wms = simulation->add(
        new LustreTestControllerMinMax(batch_service, sstorageservices, compound_storage_service, "user0", std::make_shared<LustreAllocator>(allocator)));

    simulation->launch();
}

/**
 * @brief Custom controller for testing lustreComputeMinMaxUtilization
 */
class LustreTestControllerUsage : public LustreTestController {
public:
    LustreTestControllerUsage(const std::shared_ptr<wrench::ComputeService> &compute_service,
                              const std::set<std::shared_ptr<wrench::StorageService>> &storage_services,
                              const std::shared_ptr<wrench::CompoundStorageService> compound_storage_svc,
                              const std::string &hostname,
                              std::shared_ptr<LustreAllocator> alloc) : LustreTestController(compute_service, storage_services, compound_storage_svc, hostname, alloc) {}

    int main() override {

        // Prepare a map of services for lustreComputeMinMaxUtilization
        std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> storage_map;
        for (const auto &service : this->storage_services) {
            if (storage_map.find(service->getHostname()) == storage_map.end()) {
                storage_map[service->getHostname()] = std::vector<std::shared_ptr<wrench::StorageService>>();
            }
            storage_map[service->getHostname()].push_back(service);
        }

        // Create a dummy job writing data
        auto job_manager = this->createJobManager();
        auto job = job_manager->createCompoundJob("Job1");
        auto simple_1 = storage_map["lustre_OSS2"][0];
        auto simple_2 = storage_map["lustre_OSS5"][0];

        auto file_loc = wrench::FileLocation::LOCATION(simple_1, this->file_50GB);
        job->addFileWriteAction("write1", file_loc);
        std::map<std::string, std::string> service_specific_args =
            {{"-N", "2"},     // nb of nodes
             {"-c", "16"},    // core per node
             {"-t", "6000"}}; // seconds
        job_manager->submitJob(job, this->compute_svc, service_specific_args);
        wrench::Simulation::sleep(300);

        // ACTUAL TEST
        std::vector<std::shared_ptr<wrench::FileLocation>> locations = {file_loc}; // Vector of locations for parts of a file
        std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> usage_map = {{this->file_50GB, locations}};
        bool used = this->alloc->lustreOstIsUsed(usage_map, simple_1);
        if (not used) {
            throw std::runtime_error("OSS 2 should be seen as used");
        }
        used = this->alloc->lustreOstIsUsed(usage_map, simple_2);
        if (used) {
            throw std::runtime_error("OSS 5 should not be used");
        }
        // --------------

        // Wait for job completion (making sure nothing stalled)
        auto event = this->waitForNextEvent();
        if (std::dynamic_pointer_cast<wrench::CompoundJobCompletedEvent>(event) == nullptr) {
            throw std::runtime_error("Test failed because job did not complete");
        }

        return 0;
    }
};

TEST_F(FunctionalAllocTest, lustreOstIsUsed_test) {
    DO_TEST_WITH_FORK(lustreOstIsUsed_test);
}

/**
 *  @brief Testing lustreComputeMinMaxUtilization()
 */
void FunctionalAllocTest::lustreOstIsUsed_test() {

    auto config = std::make_shared<storalloc::Config>(storalloc::loadConfig(test::CONFIG_PATH + "lustre_config_base.yml"));

    // Check that the default value for max_inodes was not overriden during config load
    ASSERT_EQ(config->lustre.max_inodes, (1ULL << 32));
    // Check that the default value for stripe size was correctly overrriden
    ASSERT_EQ(config->lustre.stripe_size, 40000000);

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    ASSERT_NO_THROW(simulation->init(&argc, argv));

    auto platform_factory = storalloc::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);

    /* Batch compute Service*/
    auto batch_service = storalloc::instantiateComputeServices(simulation, config);

    /* Simple storage services */
    auto sstorageservices = storalloc::instantiateStorageServices(simulation, config);

    LustreAllocator allocator(config);
    wrench::StorageSelectionStrategyCallback allocatorCallback = [&allocator](
                                                                     const std::shared_ptr<wrench::DataFile> &file,
                                                                     const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
                                                                     const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
                                                                     const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations) {
        return allocator(file, resources, mapping, previous_allocations);
    };

    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "compound_storage",
            sstorageservices,
            allocatorCallback,
            {{wrench::CompoundStorageServiceProperty::MAX_ALLOCATION_CHUNK_SIZE, std::to_string(config->lustre.stripe_size)},
             {wrench::CompoundStorageServiceProperty::INTERNAL_STRIPING, "false"}},
            {}));

    auto wms = simulation->add(
        new LustreTestControllerUsage(batch_service, sstorageservices, compound_storage_service, "user0", std::make_shared<LustreAllocator>(allocator)));

    simulation->launch();
}

/**
 * @brief Custom controller for testing lustreComputeMinMaxUtilization
 */
class LustreTestControllerOrderRR : public LustreTestController {
public:
    LustreTestControllerOrderRR(const std::shared_ptr<wrench::ComputeService> &compute_service,
                                const std::set<std::shared_ptr<wrench::StorageService>> &storage_services,
                                const std::shared_ptr<wrench::CompoundStorageService> compound_storage_svc,
                                const std::string &hostname,
                                std::shared_ptr<LustreAllocator> alloc,
                                const std::vector<std::pair<std::string, std::string>> &ordered_alloc) : LustreTestController(compute_service, storage_services, compound_storage_svc, hostname, alloc), ordered_alloc(ordered_alloc) {}

    int main() override {

        std::map<std::string, int> hostname_to_service_count;
        std::vector<std::shared_ptr<wrench::StorageService>> disk_level_services;
        for (const auto &service : this->storage_services) {
            if (hostname_to_service_count.find(service->getHostname()) == hostname_to_service_count.end()) {
                hostname_to_service_count[service->getHostname()] = 1;
            } else {
                hostname_to_service_count[service->getHostname()] += 1;
            }
            disk_level_services.push_back(service);
        }

        if (disk_level_services.size() != this->ordered_alloc.size()) {
            throw std::runtime_error("Incorrect 'hostname_to_service_count' length (" + std::to_string(disk_level_services.size()) + ")");
        }

        auto ordered = this->alloc->lustreRROrderServices(hostname_to_service_count, disk_level_services);

        int index = 0;
        for (const auto &service : ordered) {
            // std::cout << this->ordered_alloc[index].first << " -- " << service->getHostname() << std::endl;
            // std::cout << " [ " << this->ordered_alloc[index].second << " -- " << service->getName() << " ] " << std::endl;
            if ((service->getHostname() != this->ordered_alloc[index].first) or (service->getName() != this->ordered_alloc[index].second)) {
                throw std::runtime_error("Mismatch between computed list of ordered services and expected result");
            }
            index++;
        }

        hostname_to_service_count.clear();
        disk_level_services.clear();
        try {
            this->alloc->lustreRROrderServices(hostname_to_service_count, disk_level_services);
        } catch (std::runtime_error &e) {
            return 0; // OK, exception raised as expected;
        }
        throw std::runtime_error("Error with lustreRROrderServices when given empty vector/map (no exception raised)");
    }

    std::vector<std::pair<std::string, std::string>> ordered_alloc = {};
};

TEST_F(FunctionalAllocTest, lustreRROrderServices_test) {
    DO_TEST_WITH_FORK(lustreRROrderServices_test);
}

/**
 *  @brief Testing lustreRROrderServices() with a config file presenting 1 OSS Type A with 3 OSTs and 1 OSS Type B with 5 OSTs
 *         Expected result is a vector of OSTs from OSSs "ABABBABB" (more OSTs in type B OSS, that need to be distributed)
 */
void FunctionalAllocTest::lustreRROrderServices_test() {

    auto config = std::make_shared<storalloc::Config>(storalloc::loadConfig(test::CONFIG_PATH + "lustre_config_het_oss.yml"));

    // Check that the default value for max_inodes was not overriden during config load
    ASSERT_EQ(config->lustre.max_inodes, (1ULL << 32));
    // Check that the default value for stripe size was correctly overrriden
    ASSERT_EQ(config->lustre.stripe_size, 40000000);

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    ASSERT_NO_THROW(simulation->init(&argc, argv));

    auto platform_factory = storalloc::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);

    auto batch_service = storalloc::instantiateComputeServices(simulation, config);
    auto sstorageservices = storalloc::instantiateStorageServices(simulation, config);
    LustreAllocator allocator(config);
    wrench::StorageSelectionStrategyCallback allocatorCallback = [&allocator](
                                                                     const std::shared_ptr<wrench::DataFile> &file,
                                                                     const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
                                                                     const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
                                                                     const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations) {
        return allocator(file, resources, mapping, previous_allocations);
    };

    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "compound_storage",
            sstorageservices,
            allocatorCallback,
            {{wrench::CompoundStorageServiceProperty::MAX_ALLOCATION_CHUNK_SIZE, std::to_string(config->lustre.stripe_size)},
             {wrench::CompoundStorageServiceProperty::INTERNAL_STRIPING, "false"}},
            {}));

    std::vector<std::pair<std::string, std::string>> result = {
        {"lustre_OSS_A0", "simple_storage_0_5003"},
        {"lustre_OSS_B0", "simple_storage_3_5012"},
        {"lustre_OSS_A0", "simple_storage_1_5006"},
        {"lustre_OSS_B0", "simple_storage_4_5015"},
        {"lustre_OSS_B0", "simple_storage_5_5018"},
        {"lustre_OSS_A0", "simple_storage_2_5009"},
        {"lustre_OSS_B0", "simple_storage_6_5021"},
        {"lustre_OSS_B0", "simple_storage_7_5024"},
    };

    auto wms = simulation->add(
        new LustreTestControllerOrderRR(batch_service, sstorageservices, compound_storage_service, "user0", std::make_shared<LustreAllocator>(allocator), result));

    ASSERT_NO_THROW(simulation->launch());
}

TEST_F(FunctionalAllocTest, lustreRROrderServices2_test) {
    DO_TEST_WITH_FORK(lustreRROrderServices2_test);
}

/**
 *  @brief Testing lustreRROrderServices() with a config file presenting 2 OSS types (A and B) with 3 OSTs each
 *         Expected result is a vector of OSTs from OSSs in the form "ABABAB"
 */
void FunctionalAllocTest::lustreRROrderServices2_test() {

    auto config = std::make_shared<storalloc::Config>(storalloc::loadConfig(test::CONFIG_PATH + "lustre_config_2OSS_3OST.yml"));

    // Check that the default value for max_inodes was not overriden during config load
    ASSERT_EQ(config->lustre.max_inodes, (1ULL << 32));
    // Check that the default value for stripe size was correctly overrriden
    ASSERT_EQ(config->lustre.stripe_size, 40000000);

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    ASSERT_NO_THROW(simulation->init(&argc, argv));

    auto platform_factory = storalloc::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);

    /* Batch compute Service*/
    auto batch_service = storalloc::instantiateComputeServices(simulation, config);
    auto sstorageservices = storalloc::instantiateStorageServices(simulation, config);
    LustreAllocator allocator(config);
    wrench::StorageSelectionStrategyCallback allocatorCallback = [&allocator](
                                                                     const std::shared_ptr<wrench::DataFile> &file,
                                                                     const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
                                                                     const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
                                                                     const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations) {
        return allocator(file, resources, mapping, previous_allocations);
    };

    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "compound_storage",
            sstorageservices,
            allocatorCallback,
            {{wrench::CompoundStorageServiceProperty::MAX_ALLOCATION_CHUNK_SIZE, std::to_string(config->lustre.stripe_size)},
             {wrench::CompoundStorageServiceProperty::INTERNAL_STRIPING, "false"}},
            {}));

    std::vector<std::pair<std::string, std::string>> result = {
        {"lustre_OSS_A0", "simple_storage_0_5003"},
        {"lustre_OSS_B0", "simple_storage_3_5012"},
        {"lustre_OSS_A0", "simple_storage_1_5006"},
        {"lustre_OSS_B0", "simple_storage_4_5015"},
        {"lustre_OSS_A0", "simple_storage_2_5009"},
        {"lustre_OSS_B0", "simple_storage_5_5018"},
    };

    auto wms = simulation->add(
        new LustreTestControllerOrderRR(batch_service, sstorageservices, compound_storage_service, "user0", std::make_shared<LustreAllocator>(allocator), result));

    ASSERT_NO_THROW(simulation->launch());
}

TEST_F(FunctionalAllocTest, lustreRROrderServices3_test) {
    DO_TEST_WITH_FORK(lustreRROrderServices3_test);
}

/**
 *  @brief Testing lustreRROrderServices() with a config file presenting 1 OSS type "A", 1 OSS type "B", and 1 OSS type "C", each with 3 OSTs
 *         Expected result is a vector of OSTs from OSSs "ABCABCABC"
 */
void FunctionalAllocTest::lustreRROrderServices3_test() {

    auto config = std::make_shared<storalloc::Config>(storalloc::loadConfig(test::CONFIG_PATH + "lustre_config_3OSS_3OST.yml"));

    // Check that the default value for max_inodes was not overriden during config load
    ASSERT_EQ(config->lustre.max_inodes, (1ULL << 32));
    // Check that the default value for stripe size was correctly overrriden
    ASSERT_EQ(config->lustre.stripe_size, 40000000);

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    ASSERT_NO_THROW(simulation->init(&argc, argv));

    auto platform_factory = storalloc::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);

    /* Batch compute Service*/
    auto batch_service = storalloc::instantiateComputeServices(simulation, config);
    auto sstorageservices = storalloc::instantiateStorageServices(simulation, config);
    LustreAllocator allocator(config);
    wrench::StorageSelectionStrategyCallback allocatorCallback = [&allocator](
                                                                     const std::shared_ptr<wrench::DataFile> &file,
                                                                     const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
                                                                     const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
                                                                     const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations) {
        return allocator(file, resources, mapping, previous_allocations);
    };
    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "compound_storage",
            sstorageservices,
            allocatorCallback,
            {{wrench::CompoundStorageServiceProperty::MAX_ALLOCATION_CHUNK_SIZE, std::to_string(config->lustre.stripe_size)},
             {wrench::CompoundStorageServiceProperty::INTERNAL_STRIPING, "false"}},
            {}));

    std::vector<std::pair<std::string, std::string>> result = {
        {"lustre_OSS_A0", "simple_storage_0_5003"},
        {"lustre_OSS_B0", "simple_storage_3_5012"},
        {"lustre_OSS_C0", "simple_storage_6_5021"},
        {"lustre_OSS_A0", "simple_storage_1_5006"},
        {"lustre_OSS_B0", "simple_storage_4_5015"},
        {"lustre_OSS_C0", "simple_storage_7_5024"},
        {"lustre_OSS_A0", "simple_storage_2_5009"},
        {"lustre_OSS_B0", "simple_storage_5_5018"},
        {"lustre_OSS_C0", "simple_storage_8_5027"},
    };

    auto wms = simulation->add(
        new LustreTestControllerOrderRR(batch_service, sstorageservices, compound_storage_service, "user0", std::make_shared<LustreAllocator>(allocator), result));

    ASSERT_NO_THROW(simulation->launch());
}

TEST_F(FunctionalAllocTest, lustreCreateFileParts_test) {
    DO_TEST_WITH_FORK(lustreCreateFileParts_test);
}

/**
 *  @brief Testing lustreRROrderServices() with a config file presenting 2 OSS (type A) with 3 OST and 2 OSS (type B) with 3 OST
 *         Expected result is a vector of OSTs from OSSs "ABABABABABAB"
 */
void FunctionalAllocTest::lustreCreateFileParts_test() {

    auto config = std::make_shared<storalloc::Config>(storalloc::loadConfig(test::CONFIG_PATH + "lustre_config_base.yml"));

    // Check that the default value for max_inodes was not overriden during config load
    ASSERT_EQ(config->lustre.max_inodes, (1ULL << 32));
    // Check that the default value for stripe size was correctly overrriden
    ASSERT_EQ(config->lustre.stripe_size, 40000000);

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    ASSERT_NO_THROW(simulation->init(&argc, argv));

    auto platform_factory = storalloc::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);

    /* Batch compute Service*/
    auto batch_service = storalloc::instantiateComputeServices(simulation, config);
    auto sstorageservices = storalloc::instantiateStorageServices(simulation, config);
    auto allocator = std::make_shared<LustreAllocator>(config);

    std::vector<std::shared_ptr<wrench::StorageService>> alloc_map;
    for (const auto &svc : sstorageservices) {
        alloc_map.push_back(svc);
    }

    auto file = simulation->addFile("Test", 2000000000);
    auto locations = allocator->lustreCreateFileParts(file, alloc_map, config->lustre.stripe_size);
    ASSERT_EQ(locations.size(), 50);

    auto file_map = simulation->getFileMap();
    ASSERT_EQ(file_map.size(), 51); // 50 parts + original file still present in map

    for (const auto &loc : locations) {
        // Make sure every file parts from designated locations has correctly been added to the simulation
        auto file_part = *(file_map.find(loc->getFile()->getID()));
        ASSERT_EQ(file_part.second->getSize(), loc->getFile()->getSize());
    }
}

TEST_F(FunctionalAllocTest, lustreFullSim_test) {
    DO_TEST_WITH_FORK(lustreFullSim_test);
}

/**
 *  @brief  Functional test with a complete simulation on a small dataset (6 jobs), and a small platform (Lustre-oriented)
 *          The simulation uses the LustreAllocator (should only use the RR one in this case), and checks the correct execution
 *          of the simulation and the validity of collected metrics.
 */
void FunctionalAllocTest::lustreFullSim_test() {
    // # Start a simulation with all components as they would be in a real case
    auto config = std::make_shared<storalloc::Config>(storalloc::loadConfig(test::CONFIG_PATH + "lustre_config_hdd.yml"));
    auto header = std::make_shared<storalloc::JobsStats>(storalloc::loadYamlHeader(test::DATA_PATH + "IOJobsTest_6_LustreSim.yml"));
    auto jobs = storalloc::loadYamlJobs(test::DATA_PATH + "IOJobsTest_6_LustreSim.yml");

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test_full_sim_lustre");
    ASSERT_NO_THROW(simulation->init(&argc, argv));

    auto platform_factory = storalloc::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);

    // Services
    auto batch_service = storalloc::instantiateComputeServices(simulation, config);
    auto sstorageservices = storalloc::instantiateStorageServices(simulation, config);
    ASSERT_EQ(sstorageservices.size(), 16);
    LustreAllocator allocator(config);
    wrench::StorageSelectionStrategyCallback allocatorCallback = [&allocator](
                                                                     const std::shared_ptr<wrench::DataFile> &file,
                                                                     const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
                                                                     const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
                                                                     const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations) {
        return allocator(file, resources, mapping, previous_allocations);
    };
    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "compound_storage",
            sstorageservices,
            allocatorCallback,
            {{wrench::CompoundStorageServiceProperty::MAX_ALLOCATION_CHUNK_SIZE, std::to_string(config->lustre.stripe_size)},
             {wrench::CompoundStorageServiceProperty::INTERNAL_STRIPING, "false"}},
            {}));
    auto permanent_storage = simulation->add(
        wrench::SimpleStorageService::createSimpleStorageService(
            "permanent_storage", {"/dev/disk0"}, {}, {}));

    // Controler
    auto ctrl = simulation->add(new storalloc::Controller(batch_service, permanent_storage, compound_storage_service, "user0", header, jobs, config));

    ASSERT_NO_THROW(simulation->launch());

    // Make sure that the controler finished and that all actions from all jobs completed
    ASSERT_TRUE(ctrl->hasReturnedFromMain());
    ASSERT_TRUE(ctrl->actionsAllCompleted());

    ///  END OF SIMULATION - STARTING TESTS

    // Run a few checks at test level ('randomly' using the first test)
    auto job_1 = ctrl->getCompletedJobById("1");
    ASSERT_EQ(job_1->getState(), wrench::CompoundJob::State::COMPLETED);
    ASSERT_TRUE(job_1->hasSuccessfullyCompleted());
    ASSERT_NEAR(job_1->getSubmitDate(), 0, 2);
    ASSERT_EQ(job_1->getActions().size(), 11);
    ASSERT_EQ(job_1->getName(), "1");
    ASSERT_EQ(job_1->getMinimumRequiredNumCores(), jobs[0].coresUsed / jobs[0].nodesUsed);

    // This is the list of actions, in correct order, that every job should complete (due to the job type used in this test)
    std::vector<std::string> action_types = {
        "FILECOPY-", "FILEREAD-",               // Copy from permanent storage to local, read from local
        "COMPUTE-",                             // Compute something
        "FILEWRITE-", "FILECOPY-",              // Write results to local, archive it to permanent
        "SLEEP-", "FILEDELETE-", "FILEDELETE-", // Clean up input data
        "SLEEP-", "FILEDELETE-", "FILEDELETE-", // Clean up output data
    };

    /*
    for (const auto &action : ctrl->getCompletedJobById("2")->getActions()) {
        std::cout << "Action : " << action->getName() << " (" << wrench::Action::getActionTypeAsString(action) << ")" << std::endl;
        std::cout << action->getStartDate() << " :: " << action->getEndDate() << std::endl;
    }
    */

    // Check actions from one job in details
    auto job_1_actions = job_1->getActions();
    ASSERT_EQ(job_1_actions.size(), 11);
    auto set_cmp = [](const std::shared_ptr<wrench::Action> &a, const std::shared_ptr<wrench::Action> &b) {
        return (a->getStartDate() < b->getStartDate());
    };
    auto sorted_actions = std::set<std::shared_ptr<wrench::Action>, decltype(set_cmp)>(set_cmp);
    sorted_actions.insert(job_1_actions.begin(), job_1_actions.end());
    int index = 0;
    for (const auto &action : sorted_actions) {

        ASSERT_EQ(wrench::Action::getActionTypeAsString(action), action_types[index]);

        if (auto r_action = std::dynamic_pointer_cast<wrench::FileReadAction>(action)) {
            auto file = r_action->getFile();
            auto file_locations = r_action->getFileLocations();

            ASSERT_EQ(file->getID(), "input_data_file_1");
            ASSERT_EQ(file->getSize(), 2000000000);
            ASSERT_EQ(file_locations.size(), 1);
            ASSERT_EQ(file_locations[0]->getPath(), "/");
            ASSERT_EQ(file_locations[0]->getStorageService()->getName(), "compound_storage_0_5051");
        }

        if (auto w_action = std::dynamic_pointer_cast<wrench::FileWriteAction>(action)) {
            auto file = w_action->getFile();
            auto file_location = w_action->getFileLocation();

            ASSERT_EQ(file->getID(), "output_data_file_1");
            ASSERT_EQ(file->getSize(), 2500000000);
            ASSERT_EQ(file_location->getPath(), "/");
            ASSERT_EQ(file_location->getStorageService()->getName(), "compound_storage_0_5051");
        }

        if (auto c_action = std::dynamic_pointer_cast<wrench::ComputeAction>(action)) {
            ASSERT_EQ(c_action->getFlops(), 5940000000000000);
        }

        if (auto d_action = std::dynamic_pointer_cast<wrench::FileDeleteAction>(action)) {
            auto file = d_action->getFile();
            if (file->getID() == "output_data_file_1") {
                ASSERT_EQ(file->getSize(), 2500000000);
            } else if (file->getID() == "input_data_file_1") {
                ASSERT_EQ(file->getSize(), 2000000000);
            } else {
                GTEST_FAIL();
            }
        }

        index++;
    }

    // Test results from CompoundStorageService internal metrics
    auto first_ts = compound_storage_service->internal_storage_use.front().first;
    auto last_ts = compound_storage_service->internal_storage_use.back().first;
    ASSERT_EQ(first_ts, 0);                                               // Simulation starts at 0s here (no waiting time for first job)
    ASSERT_NEAR(last_ts, 92500, 1000);                                    // Simulation should end close to 94700s, due to sleep times + last job runtime
    ASSERT_EQ(compound_storage_service->internal_storage_use.size(), 49); // 8 traces * 6 jobs + initial trace

    // All actions from all 6 jobs in the order in which they should execute (only two jobs slightly overlap)
    std::vector<std::pair<int, wrench::IOAction>> action_list = {
        {0, wrench::IOAction::None},
        {1, wrench::IOAction::CopyToStart},
        {1, wrench::IOAction::CopyToEnd},
        {1, wrench::IOAction::WriteStart},
        {1, wrench::IOAction::WriteEnd},
        {1, wrench::IOAction::DeleteStart},
        {1, wrench::IOAction::DeleteEnd},
        {1, wrench::IOAction::DeleteStart},
        {1, wrench::IOAction::DeleteEnd},
        {2, wrench::IOAction::CopyToStart},
        {2, wrench::IOAction::CopyToEnd},
        {2, wrench::IOAction::WriteStart},
        {2, wrench::IOAction::WriteEnd},
        {2, wrench::IOAction::DeleteStart},
        {2, wrench::IOAction::DeleteEnd},
        {2, wrench::IOAction::DeleteStart},
        {2, wrench::IOAction::DeleteEnd},
        {3, wrench::IOAction::CopyToStart},
        {3, wrench::IOAction::CopyToEnd},
        {3, wrench::IOAction::WriteStart},
        {3, wrench::IOAction::WriteEnd},
        {3, wrench::IOAction::DeleteStart},
        {3, wrench::IOAction::DeleteEnd},
        {3, wrench::IOAction::DeleteStart},
        {3, wrench::IOAction::DeleteEnd},
        {4, wrench::IOAction::CopyToStart},
        {4, wrench::IOAction::CopyToEnd},
        {4, wrench::IOAction::WriteStart},
        {4, wrench::IOAction::WriteEnd},
        {4, wrench::IOAction::DeleteStart},
        {4, wrench::IOAction::DeleteEnd},
        {4, wrench::IOAction::DeleteStart},
        {4, wrench::IOAction::DeleteEnd},
        {5, wrench::IOAction::CopyToStart},
        {5, wrench::IOAction::CopyToEnd},
        {6, wrench::IOAction::CopyToStart},
        {6, wrench::IOAction::CopyToEnd},
        {5, wrench::IOAction::WriteStart},
        {5, wrench::IOAction::WriteEnd},
        {5, wrench::IOAction::DeleteStart},
        {5, wrench::IOAction::DeleteEnd},
        {5, wrench::IOAction::DeleteStart},
        {5, wrench::IOAction::DeleteEnd},
        {6, wrench::IOAction::WriteStart},
        {6, wrench::IOAction::WriteEnd},
        {6, wrench::IOAction::DeleteStart},
        {6, wrench::IOAction::DeleteEnd},
        {6, wrench::IOAction::DeleteStart},
        {6, wrench::IOAction::DeleteEnd},
    };

    std::vector<int> disk_usage_sizes = {
        16, // Init

        16, // Job 1
        16,
        16,
        16,
        16,
        16,
        16,
        16,

        15, /// Job 2
        15,
        8,
        8,
        15,
        15,
        8,
        8,

        16, // Job 3
        16,
        7,
        7,
        16,
        16,
        7,
        7,

        16, // Job 4
        16,
        7,
        7,
        16,
        16,
        7,
        7,

        10, // Copy To - Start / End - Job 5
        10,

        16, // Copy To - Start / End - Job6
        16,

        7, // Job 5
        7,
        10,
        10,
        7,
        7,

        7, // Job 6
        7,
        16,
        16,
        7,
        7,
    };

    // Check first trace (before any job action takes place), it's a special case before any IO happens
    for (const auto &first_disk_usage : compound_storage_service->internal_storage_use[0].second.disk_usage) {
        // std::cout << first_disk_usage.service->getHostname() << " / " << first_disk_usage.service->getName() << " : " << first_disk_usage.free_space << std::endl;
        ASSERT_EQ(first_disk_usage.file_count, 0);
        ASSERT_EQ(first_disk_usage.free_space, 20000000000);
    }

    // Checking disk_usage for all traces
    index = 0;
    auto previous_ts = first_ts;
    std::regex file_name_re("(?:(input)|(output))_data_file_([-\\w]+)_part_(\\d+)", std::regex_constants::ECMAScript | std::regex_constants::icase);
    for (const auto &entry : compound_storage_service->internal_storage_use) {

        auto ts = entry.first;     // ts
        auto alloc = entry.second; // AllocationTrace structure

        // Order of entries in the trace and coherence between the two timestamp for each entry
        ASSERT_EQ(ts, alloc.ts);
        ASSERT_GE(ts, previous_ts);
        previous_ts = ts;

        /*
        std::cout << "INDEX " << index << std::endl;
        std::cout << " - " << alloc.file_name << std::endl;
        if ((entry.second.act == wrench::IOAction::CopyFromStart) or (entry.second.act == wrench::IOAction::CopyFromEnd)) {
            std::cout << " - CopyFrom" << std::endl;
        }
        if ((entry.second.act == wrench::IOAction::CopyToStart) or (entry.second.act == wrench::IOAction::CopyToEnd)) {
            std::cout << " - CopyTo" << std::endl;
        }
        if ((entry.second.act == wrench::IOAction::WriteStart) or (entry.second.act == wrench::IOAction::WriteEnd)) {
            std::cout << " - Write" << std::endl;
        }
        if ((entry.second.act == wrench::IOAction::DeleteStart) or (entry.second.act == wrench::IOAction::DeleteEnd)) {
            std::cout << " - Delete" << std::endl;
        }
        */

        ASSERT_EQ(entry.second.act, action_list[index].second);
        ASSERT_EQ(alloc.disk_usage.size(), disk_usage_sizes[index]);

        // Check correct initial values for all storage services at step 0
        if (index == 0) {
            for (const auto &usage : alloc.disk_usage) {
                std::cout << usage.service->getHostname() << " / " << usage.service->getName() << " : " << usage.free_space << std::endl;
                ASSERT_EQ(usage.file_count, 0);
                ASSERT_EQ(usage.free_space, 20000000000);
            }
        }

        // std::cout << " -- Number of files on " << alloc.disk_usage[3].service->getName() << " : " << alloc.disk_usage[3].file_count << std::endl;
        // std::cout << " -- Free space on " << alloc.disk_usage[3].service->getName() << " : " << alloc.disk_usage[3].free_space << std::endl;

        // Look into file names from all disk_usage structures (this gives for instance the job ID associated with each trace)

        auto file_name = alloc.file_name;
        std::smatch base_match;
        std::string input, output, file_part;
        int job_id;
        std::regex_match(file_name, base_match, file_name_re);

        if (!file_name.empty()) {
            if (base_match.size() == 5) {
                input = base_match[1].str();
                output = base_match[2].str();
                job_id = stoi(base_match[3].str());
                file_part = base_match[4].str();
                ASSERT_EQ(job_id, action_list[index].first);
            } else {
                if (file_name == "nofile") {
                    index++;
                    continue;
                } else {
                    std::cout << "Unable to parse file name " << file_name << std::endl;
                    GTEST_FAIL();
                }
            }

            // Find associated job
            auto current_job = jobs[job_id - 1];
            /* TODO: MORE TESTS HERE */
        }

        index++;
    }

    // Look at some of the traces and assess whether the storage system is the expected state or not, in terms of file count and free space (per disk)

    // Check a random trace, where we know what should be the file count and free_space on each OSS
    ASSERT_EQ(compound_storage_service->internal_storage_use[20].second.act, wrench::IOAction::WriteEnd);
    for (const auto &first_disk_usage : compound_storage_service->internal_storage_use[20].second.disk_usage) {
        // std::cout << "File count on server " << first_disk_usage.service->getHostname() << " : " << first_disk_usage.file_count << std::endl;
        // assertions with more or less one file part as error, to account for small irregularities in rounding
        ASSERT_NEAR(first_disk_usage.file_count, 4, 1);                  // 4/5 files parts/server after copying input parts (~4 per OST) and writing output data (1 for some OSTs)
        ASSERT_NEAR(first_disk_usage.free_space, 19840000000, 40000000); // each file part is 40 MB
    }

    auto alloc_filename = compound_storage_service->internal_storage_use[20].second.file_name;
    std::smatch base_match1;
    std::regex_match(alloc_filename, base_match1, file_name_re);
    ASSERT_EQ(base_match1[2].str(), "output");
    ASSERT_EQ(base_match1[3].str(), "3");
    ASSERT_LT(stoi(base_match1[4].str()), 100);

    // Check a random trace, where we know what should be the file count and free_space on each OSS (On a DeleteEnd)
    ASSERT_EQ(compound_storage_service->internal_storage_use[8].second.act, wrench::IOAction::DeleteEnd);
    for (const auto &first_disk_usage : compound_storage_service->internal_storage_use[8].second.disk_usage) {
        // std::cout << "File count on server " << first_disk_usage.service->getHostname() << " : " << first_disk_usage.file_count << std::endl;

        ASSERT_EQ(first_disk_usage.file_count, 0);
        ASSERT_EQ(first_disk_usage.free_space, 20000000000);
    }

    alloc_filename = compound_storage_service->internal_storage_use[8].second.file_name;
    std::smatch base_match2;
    std::regex_match(alloc_filename, base_match2, file_name_re);
    ASSERT_EQ(base_match2[2].str(), "output");
    ASSERT_EQ(base_match2[3].str(), "1");
    ASSERT_LT(stoi(base_match2[4].str()), 625);

    // Check a random trace, where we know what should be the file count and free_space on each OSS (On a DeleteEnd, )
    ASSERT_EQ(compound_storage_service->internal_storage_use[34].second.act, wrench::IOAction::CopyToEnd);
    for (const auto &first_disk_usage : compound_storage_service->internal_storage_use[34].second.disk_usage) {

        // std::cout << "File count on server " << first_disk_usage.service->getHostname() << " : " << first_disk_usage.file_count << std::endl;

        ASSERT_NEAR(first_disk_usage.file_count, 1, 1);
        ASSERT_NEAR(first_disk_usage.free_space, 19960000000, 40000000);
    }

    alloc_filename = compound_storage_service->internal_storage_use[34].second.file_name;
    std::smatch base_match3;
    std::regex_match(alloc_filename, base_match3, file_name_re);
    ASSERT_EQ(base_match3[1].str(), "input");
    ASSERT_EQ(base_match3[3].str(), "5");
    ASSERT_LT(stoi(base_match3[4].str()), 100);
}
