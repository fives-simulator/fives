#include <cstdint>
#include <regex>

#include <gtest/gtest.h>

#include "./include/TestConstants.h"
#include "./include/TestWithFork.h"

#include "../include/AllocationStrategy.h"
#include "../include/Constants.h"
#include "../include/Controller.h"
#include "../include/Platform.h"
#include "../include/Simulator.h"
#include "../include/Utils.h"

using namespace fives;

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
    struct fives::ba_min_max test_min_max;

    LustreAllocator allocator(this->cfg);

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

    LustreAllocator allocator(this->cfg);
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

    LustreAllocator allocator(this->cfg);

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

    LustreAllocator allocator(this->cfg);

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
 *  @brief Testing lustreComputeStripesPerOST()
 */
void BasicAllocTest::lustreComputeStriping_test() {

    LustreAllocator allocator(this->cfg);
    auto striping = allocator.lustreComputeStriping(15'000'000, 12, 1);
    ASSERT_EQ(striping.stripe_size_b, 2'097'152); // stripe_size unmodified from config default
    ASSERT_EQ(striping.stripes_count, 1);         // unchanged
    ASSERT_EQ(striping.max_stripes_per_ost, 8);   // max stripes per OST adjusted to fit the actual number of stripes used
    ASSERT_THROW(allocator.lustreComputeStriping(3'000'000, 0, 1), std::runtime_error);
    ASSERT_THROW(allocator.lustreComputeStriping(0, 12, 1), std::runtime_error);

    striping = {};
    striping = allocator.lustreComputeStriping(30'000'000, 12, 1);
    ASSERT_EQ(striping.stripe_size_b, 3'000'000); // recomputed based on max_chunks_per_ost=10 in config
    ASSERT_EQ(striping.stripes_count, 1);         // unchanged
    ASSERT_EQ(striping.max_stripes_per_ost, 10);  // unchanged

    LustreAllocator allocator2(cfg);
    striping = {};
    striping = allocator2.lustreComputeStriping(300'000'000, 85, 13);                     // Using stripe_count = 13
    ASSERT_EQ(striping.stripe_size_b, 2'307'693);                                         // stripe size slightly increased
    ASSERT_EQ(striping.stripes_count, 13);                                                // unchanged
    ASSERT_EQ(striping.max_stripes_per_ost, 10);                                          // unchanged
    ASSERT_THROW(allocator2.lustreComputeStriping(3'000'000, 3, 13), std::runtime_error); // stripe_count > nb of ost

    auto cfg_3 = std::make_shared<Config>();
    cfg_3->lustre = LustreConfig();        // default lustre config
    cfg_3->lustre.max_chunks_per_ost = 12; // allowing for more chunks on each OST
    LustreAllocator allocator3(cfg_3);

    striping = {};
    striping = allocator3.lustreComputeStriping(370'000'000, 85, 18); // stripe_count = 18
    ASSERT_EQ(striping.stripe_size_b, 2'097'152);
    ASSERT_EQ(striping.stripes_count, 18);
    ASSERT_EQ(striping.max_stripes_per_ost, 10);

    auto cfg_4 = std::make_shared<Config>();
    cfg_4->lustre = LustreConfig();          // default lustre config
    cfg_4->lustre.max_chunks_per_ost = 12;   // allowing for more chunks on each OST (useless here)
    cfg_4->lustre.stripe_size = 100'000'000; // stripe size at 100 MB
    LustreAllocator allocator4(cfg_4);

    striping = {};
    striping = allocator4.lustreComputeStriping(50'000'000, 4, 2); // stripe_count = 2
    ASSERT_EQ(striping.stripe_size_b, 100'000'000);
    ASSERT_EQ(striping.stripes_count, 1);
    ASSERT_EQ(striping.max_stripes_per_ost, 1);
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
    std::shared_ptr<fives::LustreAllocator> alloc;
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
        if (free_space_in_service != 200000000000) {
            throw std::runtime_error("Unexpected initial value for service free space");
        }

        // Write the 50GB file onto the first storage service (without even going through a CSS)
        job->addFileWriteAction(
            "write1", wrench::FileLocation::LOCATION(*(this->storage_services.begin()), this->file_50GB));

        std::map<std::string, std::string> service_specific_args =
            {{"-N", "1"},     // nb of nodes
             {"-c", "16"},    // core per node
             {"-t", "6000"}}; // seconds
        job_manager->submitJob(job, this->compute_svc, service_specific_args);

        // Sleep until we're sure the writes have begun (and thus, space is reserved on the service)
        wrench::Simulation::sleep(300);

        // Test lustreComputeMinMaxUtilization
        auto ba_min_max = this->alloc->lustreComputeMinMaxUtilization(storage_map);

        // Max free space is 200 GB (>>8)
        uint64_t expected_max = 200000000000 >> 8; // bitshift for overflow, as computed in Lustre
        // Min free space should be 200GB - 150GB (files being written) (>>8)
        uint64_t expected_min = (free_space_in_service - 50000000000) >> 8;

        if (ba_min_max.max != expected_max) {
            std::cout << "Free space in service (before job) " << std::to_string(free_space_in_service) << std::endl;
            std::cout << "Expected max : " << std::to_string(expected_max) << std::endl;
            std::cout << "Computed max : " << std::to_string(ba_min_max.max) << std::endl;
            throw std::runtime_error("Max free space != from expected max");
        }
        if (ba_min_max.min != expected_min) {
            std::cout << "Free space in service (before job) " << std::to_string(free_space_in_service) << std::endl;
            std::cout << "Expected min : " << std::to_string(expected_min) << std::endl;
            std::cout << "Computed min : " << std::to_string(ba_min_max.min) << std::endl;
            throw std::runtime_error("Min free space != from expected min");
        }

        // While we're at it, also test whether or not we should use the weighted allocator in this case
        if (this->alloc->lustreUseRR(ba_min_max)) {
            throw std::runtime_error("We should NOT be using the RR allocator (diff between min and max free space is 50GB, with a max capacity per storage service of 200GB)");
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

    auto config = std::make_shared<fives::Config>(fives::loadConfig(test::CONFIG_PATH + "lustre_config_base.yml"));

    // Check that the default value for max_inodes was not overriden during config load
    ASSERT_EQ(config->lustre.max_inodes, (1ULL << 32));
    // Check that the default value for stripe size was correctly overrriden
    ASSERT_EQ(config->lustre.stripe_size, 40000000);

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    ASSERT_NO_THROW(simulation->init(&argc, argv));

    auto platform_factory = fives::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);

    auto batch_service = fives::instantiateComputeServices(simulation, config);
    auto sstorageservices = fives::instantiateStorageServices(simulation, config);
    LustreAllocator allocator(config);
    wrench::StorageSelectionStrategyCallback allocatorCallback = [&allocator](
                                                                     const std::shared_ptr<wrench::DataFile> &file,
                                                                     const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
                                                                     const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
                                                                     const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations,
                                                                     unsigned int stripe_count) {
        return allocator(file, resources, mapping, previous_allocations, stripe_count);
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
        new LustreTestControllerMinMax(batch_service, sstorageservices, compound_storage_service, "user42", std::make_shared<LustreAllocator>(allocator)));

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

    auto config = std::make_shared<fives::Config>(fives::loadConfig(test::CONFIG_PATH + "lustre_config_base.yml"));

    // Check that the default value for max_inodes was not overriden during config load
    ASSERT_EQ(config->lustre.max_inodes, (1ULL << 32));
    // Check that the default value for stripe size was correctly overrriden
    ASSERT_EQ(config->lustre.stripe_size, 40000000);

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    ASSERT_NO_THROW(simulation->init(&argc, argv));

    auto platform_factory = fives::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);

    /* Batch compute Service*/
    auto batch_service = fives::instantiateComputeServices(simulation, config);

    /* Simple storage services */
    auto sstorageservices = fives::instantiateStorageServices(simulation, config);

    LustreAllocator allocator(config);
    wrench::StorageSelectionStrategyCallback allocatorCallback = [&allocator](
                                                                     const std::shared_ptr<wrench::DataFile> &file,
                                                                     const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
                                                                     const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
                                                                     const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations,
                                                                     unsigned int stripe_count) {
        return allocator(file, resources, mapping, previous_allocations, stripe_count);
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
        new LustreTestControllerUsage(batch_service, sstorageservices, compound_storage_service, "user42", std::make_shared<LustreAllocator>(allocator)));

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

        std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> resources;
        for (const auto &service : this->storage_services) {
            if (resources.find(service->getHostname()) == resources.end()) {
                resources[service->getHostname()] = std::vector<std::shared_ptr<wrench::StorageService>>{service};
            } else {
                resources[service->getHostname()].push_back(service);
            }
        }

        auto ordered = this->alloc->lustreRROrderServices(resources);

        int index = 0;
        for (const auto &service : ordered) {
            // std::cout << this->ordered_alloc[index].first << " -- " << service->getHostname() << std::endl;
            // std::cout << " [ " << this->ordered_alloc[index].second << " -- " << service->getName() << " ] " << std::endl;
            if ((service->getHostname() != this->ordered_alloc[index].first) or (service->getName() != this->ordered_alloc[index].second)) {
                throw std::runtime_error("Mismatch between computed list of ordered services and expected result");
            }
            index++;
        }

        resources.clear();
        try {
            this->alloc->lustreRROrderServices(resources);
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

    auto config = std::make_shared<fives::Config>(fives::loadConfig(test::CONFIG_PATH + "lustre_config_het_oss.yml"));

    // Check that the default value for max_inodes was not overriden during config load
    ASSERT_EQ(config->lustre.max_inodes, (1ULL << 32));
    // Check that the default value for stripe size was correctly overrriden
    ASSERT_EQ(config->lustre.stripe_size, 40000000);

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    ASSERT_NO_THROW(simulation->init(&argc, argv));

    auto platform_factory = fives::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);

    auto batch_service = fives::instantiateComputeServices(simulation, config);
    auto sstorageservices = fives::instantiateStorageServices(simulation, config);
    LustreAllocator allocator(config);
    wrench::StorageSelectionStrategyCallback allocatorCallback = [&allocator](
                                                                     const std::shared_ptr<wrench::DataFile> &file,
                                                                     const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
                                                                     const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
                                                                     const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations,
                                                                     unsigned int stripe_count) {
        return allocator(file, resources, mapping, previous_allocations, stripe_count);
    };

    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "compound_storage",
            sstorageservices,
            allocatorCallback,
            {{wrench::CompoundStorageServiceProperty::MAX_ALLOCATION_CHUNK_SIZE, std::to_string(config->lustre.stripe_size)},
             {wrench::CompoundStorageServiceProperty::INTERNAL_STRIPING, "false"}},
            {}));

    /* NOTE: the second part (simple storage service HOSTname) may change (for instance when the platform is updated)
       BUT the first part (storage service NAME) should not */
    std::vector<std::pair<std::string, std::string>> result = {
        {"lustre_OSS_A0", "simple_storage_0_5002"},
        {"lustre_OSS_B0", "simple_storage_3_5005"},
        {"lustre_OSS_A0", "simple_storage_1_5003"},
        {"lustre_OSS_B0", "simple_storage_4_5006"},
        {"lustre_OSS_B0", "simple_storage_5_5007"},
        {"lustre_OSS_A0", "simple_storage_2_5004"},
        {"lustre_OSS_B0", "simple_storage_6_5008"},
        {"lustre_OSS_B0", "simple_storage_7_5009"},
    };

    auto wms = simulation->add(
        new LustreTestControllerOrderRR(batch_service, sstorageservices, compound_storage_service, "user42", std::make_shared<LustreAllocator>(allocator), result));

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

    auto config = std::make_shared<fives::Config>(fives::loadConfig(test::CONFIG_PATH + "lustre_config_2OSS_3OST.yml"));

    // Check that the default value for max_inodes was not overriden during config load
    ASSERT_EQ(config->lustre.max_inodes, (1ULL << 32));
    // Check that the default value for stripe size was correctly overrriden
    ASSERT_EQ(config->lustre.stripe_size, 40000000);

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    ASSERT_NO_THROW(simulation->init(&argc, argv));

    auto platform_factory = fives::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);

    /* Batch compute Service*/
    auto batch_service = fives::instantiateComputeServices(simulation, config);
    auto sstorageservices = fives::instantiateStorageServices(simulation, config);
    LustreAllocator allocator(config);
    wrench::StorageSelectionStrategyCallback allocatorCallback = [&allocator](
                                                                     const std::shared_ptr<wrench::DataFile> &file,
                                                                     const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
                                                                     const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
                                                                     const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations,
                                                                     unsigned int stripe_count) {
        return allocator(file, resources, mapping, previous_allocations, stripe_count);
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
        {"lustre_OSS_A0", "simple_storage_0_5002"},
        {"lustre_OSS_B0", "simple_storage_3_5005"},
        {"lustre_OSS_A0", "simple_storage_1_5003"},
        {"lustre_OSS_B0", "simple_storage_4_5006"},
        {"lustre_OSS_A0", "simple_storage_2_5004"},
        {"lustre_OSS_B0", "simple_storage_5_5007"},
    };

    auto wms = simulation->add(
        new LustreTestControllerOrderRR(batch_service, sstorageservices, compound_storage_service, "user42", std::make_shared<LustreAllocator>(allocator), result));

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

    auto config = std::make_shared<fives::Config>(fives::loadConfig(test::CONFIG_PATH + "lustre_config_3OSS_3OST.yml"));

    // Check that the default value for max_inodes was not overriden during config load
    ASSERT_EQ(config->lustre.max_inodes, (1ULL << 32));
    // Check that the default value for stripe size was correctly overrriden
    ASSERT_EQ(config->lustre.stripe_size, 40000000);

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    ASSERT_NO_THROW(simulation->init(&argc, argv));

    auto platform_factory = fives::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);

    /* Batch compute Service*/
    auto batch_service = fives::instantiateComputeServices(simulation, config);
    auto sstorageservices = fives::instantiateStorageServices(simulation, config);
    LustreAllocator allocator(config);
    wrench::StorageSelectionStrategyCallback allocatorCallback = [&allocator](
                                                                     const std::shared_ptr<wrench::DataFile> &file,
                                                                     const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
                                                                     const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
                                                                     const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations,
                                                                     unsigned int stripe_count) {
        return allocator(file, resources, mapping, previous_allocations, stripe_count);
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
        {"lustre_OSS_A0", "simple_storage_0_5002"},
        {"lustre_OSS_B0", "simple_storage_3_5005"},
        {"lustre_OSS_C0", "simple_storage_6_5008"},
        {"lustre_OSS_A0", "simple_storage_1_5003"},
        {"lustre_OSS_B0", "simple_storage_4_5006"},
        {"lustre_OSS_C0", "simple_storage_7_5009"},
        {"lustre_OSS_A0", "simple_storage_2_5004"},
        {"lustre_OSS_B0", "simple_storage_5_5007"},
        {"lustre_OSS_C0", "simple_storage_8_5010"},
    };

    auto wms = simulation->add(
        new LustreTestControllerOrderRR(batch_service, sstorageservices, compound_storage_service, "user42", std::make_shared<LustreAllocator>(allocator), result));

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

    auto config = std::make_shared<fives::Config>(fives::loadConfig(test::CONFIG_PATH + "lustre_config_base.yml"));

    // Check that the default value for max_inodes was not overriden during config load
    ASSERT_EQ(config->lustre.max_inodes, (1ULL << 32));
    // Check that the default value for stripe size was correctly overrriden
    ASSERT_EQ(config->lustre.stripe_size, 40000000);

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    ASSERT_NO_THROW(simulation->init(&argc, argv));

    auto platform_factory = fives::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);

    /* Batch compute Service*/
    auto batch_service = fives::instantiateComputeServices(simulation, config);
    auto sstorageservices = fives::instantiateStorageServices(simulation, config);
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
    auto config = std::make_shared<fives::Config>(fives::loadConfig(test::CONFIG_PATH + "lustre_config_hdd.yml"));
    auto jobs = fives::loadYamlJobs(test::DATA_PATH + "IOJobsTest_LustreSim3jobs.yml");

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 2;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test_full_sim_lustre");
    argv[1] = strdup("--wrench-commport-pool-size=200000");
    ASSERT_NO_THROW(simulation->init(&argc, argv));

    auto platform_factory = fives::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);

    // Services
    auto batch_service = fives::instantiateComputeServices(simulation, config);
    auto sstorageservices = fives::instantiateStorageServices(simulation, config);
    ASSERT_EQ(sstorageservices.size(), 16);
    LustreAllocator allocator(config);
    wrench::StorageSelectionStrategyCallback allocatorCallback = [&allocator](
                                                                     const std::shared_ptr<wrench::DataFile> &file,
                                                                     const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
                                                                     const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
                                                                     const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations,
                                                                     unsigned int stripe_count) {
        return allocator(file, resources, mapping, previous_allocations, stripe_count);
    };
    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "compound_storage",
            sstorageservices,
            allocatorCallback,
            {{wrench::CompoundStorageServiceProperty::MAX_ALLOCATION_CHUNK_SIZE, std::to_string(config->lustre.stripe_size)},
             {wrench::CompoundStorageServiceProperty::INTERNAL_STRIPING, "false"}},
            {}));

    wrench::WRENCH_PROPERTY_COLLECTION_TYPE ss_params = {};
    if (config->pstor.io_buffer_size != "0GB") {
        ss_params[wrench::SimpleStorageServiceProperty::BUFFER_SIZE] = config->pstor.io_buffer_size;
    }
    auto permanent_storage = simulation->add(
        wrench::SimpleStorageService::createSimpleStorageService(
            PERMANENT_STORAGE, {config->pstor.mount_prefix}, ss_params, {}));

    // Controler
    auto ctrl = simulation->add(new fives::Controller(batch_service, permanent_storage, compound_storage_service, "user42", jobs, config));

    ASSERT_NO_THROW(simulation->launch());

    // Make sure that the controler finished and that all actions from all jobs completed
    ASSERT_TRUE(ctrl->hasReturnedFromMain());
    // ASSERT_TRUE(ctrl->actionsAllCompleted());

    ///  END OF SIMULATION - STARTING TESTS

    ASSERT_EQ(ctrl->getFailedJobCount(), 0);

    auto job_1 = ctrl->getCompletedJobsById("job1");
    auto job_2 = ctrl->getCompletedJobsById("job2");
    auto job_3 = ctrl->getCompletedJobsById("job3");

    /*     for (const auto &subjob : job_3) {
            std::cout << subjob->getName() << std::endl;
        } */

    ASSERT_EQ(job_1.size(), 7);  // parent job + (Copy / Read / Write jobs) + (Read / Write / Archive) (+ 2 sleep jobs, ignored)
    ASSERT_EQ(job_2.size(), 7);  // parent job + 3 * (write + archive)  (no sleep jobs)
    ASSERT_EQ(job_3.size(), 13); // parent job + 6 * (copy / read) (and 6 ignored sleep actions)

    ASSERT_EQ(job_1[0]->getName(), "job1");
    ASSERT_EQ(job_2[0]->getName(), "job2");
    ASSERT_EQ(job_3[0]->getName(), "job3");

    ASSERT_EQ(job_1[2]->getName(), "writeFiles_idjob1_exec1");
    ASSERT_EQ(job_1[3]->getName(), "archiveCopy_idjob1_exec1");
    ASSERT_EQ(job_1[4]->getName(), "stagingCopy_idjob1_exec2");
    ASSERT_EQ(job_1[6]->getName(), "writeFiles_idjob1_exec2");

    ASSERT_EQ(job_2[2]->getName(), "archiveCopy_idjob2_exec1");
    ASSERT_EQ(job_2[3]->getName(), "writeFiles_idjob2_exec2");

    ASSERT_EQ(job_3[1]->getName(), "stagingCopy_idjob3_exec1");
    ASSERT_EQ(job_3[2]->getName(), "readFiles_idjob3_exec1");
    ASSERT_EQ(job_3[4]->getName(), "readFiles_idjob3_exec2");

    // JOB 1 (easy, all sequential)
    for (unsigned int i = 0; i < 6; i++) {
        ASSERT_TRUE(job_1[i]->getSubmitDate() <= job_1[i + 1]->getSubmitDate());
    }

    // JOB 2
    for (unsigned int i = 1; i < 2; i++) {
        ASSERT_TRUE(job_2[i]->getSubmitDate() <= job_2[i + 1]->getSubmitDate());
    }
    for (unsigned int i = 3; i < 4; i++) {
        ASSERT_TRUE(job_2[i]->getSubmitDate() <= job_2[i + 1]->getSubmitDate());
    }
    for (unsigned int i = 5; i < 6; i++) {
        ASSERT_TRUE(job_2[i]->getSubmitDate() <= job_2[i + 1]->getSubmitDate());
    }

    ASSERT_NEAR(job_2[1]->getSubmitDate(), job_2[3]->getSubmitDate(), 1);
    ASSERT_NEAR(job_2[1]->getSubmitDate(), job_2[5]->getSubmitDate(), 1);

    // JOB 3
    for (unsigned int i = 1; i < 2; i++) {
        ASSERT_TRUE(job_3[i]->getSubmitDate() <= job_3[i + 1]->getSubmitDate());
    }
    for (unsigned int i = 3; i < 4; i++) {
        ASSERT_TRUE(job_3[i]->getSubmitDate() <= job_3[i + 1]->getSubmitDate());
    }
    for (unsigned int i = 5; i < 6; i++) {
        ASSERT_TRUE(job_3[i]->getSubmitDate() <= job_3[i + 1]->getSubmitDate());
    }
    for (unsigned int i = 7; i < 8; i++) {
        ASSERT_TRUE(job_3[i]->getSubmitDate() <= job_3[i + 1]->getSubmitDate());
    }
    for (unsigned int i = 9; i < 10; i++) {
        ASSERT_TRUE(job_3[i]->getSubmitDate() <= job_3[i + 1]->getSubmitDate());
    }
    for (unsigned int i = 11; i < 12; i++) {
        ASSERT_TRUE(job_3[i]->getSubmitDate() <= job_3[i + 1]->getSubmitDate());
    }

    ASSERT_TRUE(job_3[5]->getSubmitDate() <= job_3[4]->getSubmitDate());
    ASSERT_TRUE(job_3[11]->getSubmitDate() <= job_3[10]->getSubmitDate());

    // Introspection in parent job for job 1
    auto parent1 = job_1[0];
    ASSERT_EQ(parent1->getState(), wrench::CompoundJob::State::COMPLETED);
    ASSERT_TRUE(parent1->hasSuccessfullyCompleted());
    ASSERT_NEAR(parent1->getSubmitDate(), 60, 1);
    ASSERT_EQ(parent1->getActions().size(), 2); // one custom action
    ASSERT_EQ(wrench::Action::getActionTypeAsString(*(parent1->getActions().begin())), "CUSTOM-");
    ASSERT_EQ(parent1->getName(), "job1");
    ASSERT_EQ(parent1->getMinimumRequiredNumCores(), 0);

    // Parent job 2
    auto parent2 = job_2[0];
    ASSERT_NEAR(parent2->getSubmitDate(), 3860, 1);

    // Parent job 3
    auto parent3 = job_3[0];
    ASSERT_NEAR(parent3->getSubmitDate(), 3860, 1);

    // Introspection in one of the sub jobs
    auto write2 = job_2[1];
    ASSERT_EQ(write2->getName(), "writeFiles_idjob2_exec1");
    ASSERT_EQ(write2->getState(), wrench::CompoundJob::State::COMPLETED);
    ASSERT_TRUE(write2->hasSuccessfullyCompleted());
    auto write2Actions = write2->getActions();
    ASSERT_EQ(write2Actions.size(), 8); // Configuration using 2 nodes / 4 files = 8 writes actions in the job
    std::vector<std::string> fileNames{
        "outputFile_writeFiles_idjob2_exec1_part0",
        "outputFile_writeFiles_idjob2_exec1_part1",
        "outputFile_writeFiles_idjob2_exec1_part2",
        "outputFile_writeFiles_idjob2_exec1_part3"};
    regex reg("FW_outputFile_writeFiles_idjob2_exec1_part[0-7]_compute\\d+_act[0-9]+");

    /*     for (const auto &act : write2Actions) {
            std::cout << "- Action : " << act->getName() << std::endl;
        } */

    for (const auto &act : write2Actions) {
        auto action_name = act->getName();
        if (wrench::Action::getActionTypeAsString(act) == "SLEEP-") {
            continue; // ignoring read/write overhead sleep action at the moment.
        }
        std::smatch base_match;
        ASSERT_TRUE(std::regex_match(action_name, base_match, reg));
        ASSERT_EQ(act->getState(), wrench::Action::COMPLETED);
        auto customWriteAcion = std::dynamic_pointer_cast<fives::PartialWriteCustomAction>(act);
        auto file = customWriteAcion->getFile();
        ASSERT_NE(std::find(fileNames.begin(), fileNames.end(), file->getID()), fileNames.end());
        ASSERT_EQ(file->getSize(), 3750000000);

        // Each for each action, each 2 nodes involved writes to half of each part (in this case it works
        // neatly, but sometimes the last host will write a few more stripes)
        auto expectedWrittenSize = static_cast<double>(file->getSize());
        ASSERT_NEAR(customWriteAcion->getWrittenSize(), expectedWrittenSize, expectedWrittenSize / 2);
    }

    // Check actions order inside multiple sub jobs and make sure they match the job's boundary
    for (const auto &parent : {job_1, job_2, job_3}) {
        auto parent_job_start_date = parent[0]->getActions().begin()->get()->getStartDate();
        auto parent_job_end_date = parent[0]->getActions().begin()->get()->getEndDate();
        for (const auto &job : parent) {
            if ((job->getName() == "job1") or (job->getName() == "job2") or (job->getName() == "job3")) {
                continue;
            }
            auto last_job_submit_date = job->getSubmitDate();

            auto actions = job->getActions();
            auto action_count = actions.size();
            for (const auto &action : actions) {
                auto action_start_date = action->getStartDate();
                auto action_end_date = action->getEndDate();

                ASSERT_TRUE(action_start_date >= last_job_submit_date);
                ASSERT_TRUE(action_start_date <= action_end_date);
                ASSERT_TRUE(action_start_date >= parent_job_start_date);
                ASSERT_TRUE(action_end_date <= parent_job_end_date);
            }
        }
    }

    /* CURRENTLY COMMENTED OUT IN THE CSS FOR PERFORMANCE REASONS */
    /* std::map<wrench::IOAction, std::string> IOActionStrings = {
        {wrench::IOAction::ReadStart, "ReadStart"},
        {wrench::IOAction::ReadEnd, "ReadEnd"},
        {wrench::IOAction::WriteStart, "WriteStart"},
        {wrench::IOAction::WriteEnd, "WriteEnd"},
        {wrench::IOAction::CopyToStart, "CopyToStart"},
        {wrench::IOAction::CopyToEnd, "CopyToEnd"},
        {wrench::IOAction::CopyFromStart, "CopyFromStart"},
        {wrench::IOAction::CopyFromEnd, "CopyFromEnd"},
        {wrench::IOAction::DeleteStart, "DeleteStart"},
        {wrench::IOAction::DeleteEnd, "DeleteEnd"},
        {wrench::IOAction::None, "None"},
    };

    const uint64_t initialFreeSpace = 200000000000;
    // only the first file stripe name is included in the trace, and the
    // parts_count field actually informs on the number of stripes for the file
    std::map<std::string, uint64_t> fileSizes{
        // Job 1
        {"input_data_file_job1_copyFromPermanent_sub0_stripe_0", 80000000},
        {"input_data_file_job1_copyFromPermanent_sub1_stripe_0", 80000000},
        {"output_data_file_job1_writeFiles_sub0_stripe_0", 78125000},
        {"output_data_file_job1_writeFiles_sub1_stripe_0", 78125000},
        // Job 2
        {"output_data_file_job2_writeFiles_sub0_stripe_0", 78947369},
        {"output_data_file_job2_writeFiles_sub1_stripe_0", 78947369},
        // Job 3
        {"input_data_file_job3_copyFromPermanent_sub0_stripe_0", 80000000.0},
        {"input_data_file_job3_copyFromPermanent_sub1_stripe_0", 80000000.0},
    };

    double last_ts = 0;
    std::map<std::string, uint64_t> currentOSTUsage{};

    ASSERT_EQ(compound_storage_service->internal_storage_use.size(), 213);
    for (const auto &trace : compound_storage_service->internal_storage_use) {
        ASSERT_TRUE(trace.first >= last_ts);
        ASSERT_EQ(trace.first, trace.second.ts);
        last_ts = trace.first;

        if (trace.second.act == wrench::IOAction::None) {
            for (const auto &du : trace.second.disk_usage) {
                ASSERT_EQ(du.file_count, 0);                            // no file copied/written yet
                ASSERT_EQ(du.free_space, initialFreeSpace);             // all disks empty
                currentOSTUsage[du.service->getName()] = du.free_space; // initiate usage map for future checks
            }
            continue;
        }

        if (trace.second.act == wrench::IOAction::WriteEnd) {
            std::cout << "WRITE END : " << trace.second.file_name << std::endl;
            std::cout << " ** TS : " << trace.first << std::endl;
            for (const auto &du : trace.second.disk_usage) {
                std::cout << " -  on " << du.service->getName() << " / " << du.service->getHostname() << " du.free_space = " << std::to_string(du.free_space) << " and du.file_count = " << std::to_string(du.file_count) << std::endl;
            }
        }

        if (trace.second.act == wrench::IOAction::CopyToEnd) {
            std::cout << "COPY END : " << trace.second.file_name << std::endl;
            std::cout << " ** TS : " << trace.first << std::endl;
            for (const auto &du : trace.second.disk_usage) {
                std::cout << " - on " << du.service->getName() << " / " << du.service->getHostname() << " du.free_space = " << std::to_string(du.free_space) << " and du.file_count = " << std::to_string(du.file_count) << std::endl;
            }
        }

        if (trace.second.act == wrench::IOAction::DeleteEnd) {
            std::cout << "DELETE END : " << trace.second.file_name << std::endl;
            std::cout << " ** TS : " << trace.first << std::endl;
            for (const auto &du : trace.second.disk_usage) {
                std::cout << " - on " << du.service->getName() << " / " << du.service->getHostname() << " du.free_space = " << std::to_string(du.free_space) << " and du.file_count = " << std::to_string(du.file_count) << std::endl;
            }
        }

        if ((trace.second.act == wrench::IOAction::CopyToEnd) or (trace.second.act == wrench::IOAction::WriteEnd)) {
            // Note : we used a stripe_count == 16 and files are big enough so every OST receives at least 2 stripes
            for (const auto &du : trace.second.disk_usage) {
                ASSERT_NE(du.file_count, 0);
                uint64_t allocated = initialFreeSpace - du.free_space;
                ASSERT_EQ(allocated % config->lustre.stripe_size, 0); // initialFreeSpace - config->lustre.stripe_size * du.file_count);
                ASSERT_LE(du.free_space, initialFreeSpace - fileSizes[trace.second.file_name]);
            }
        } else if ((trace.second.act == wrench::IOAction::DeleteEnd) and
                   (trace.second.file_name != "input_data_file_job1_copyFromPermanent_sub0_stripe_0") and
                   (trace.second.file_name != "input_data_file_job1_copyFromPermanent_sub1_stripe_0")) {
            // This is a case where jobs don't overlap on each other
            for (const auto &du : trace.second.disk_usage) {
                // ASSERT_EQ(du.file_count, 0);             // TODO : there(s a bug to investigate here : for a single OST at (last one of the list), the file_count remains at 1 when the trace is saved)
                // ASSERT_EQ(du.free_space, initialFreeSpace);
            }
        }
    } */
}
