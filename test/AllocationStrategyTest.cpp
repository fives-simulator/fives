#include <cstdint>

#include <gtest/gtest.h>

#include "./include/TestWithFork.h"

#include "../include/Simulator.h"
#include "../include/AllocationStrategy.h"
#include "../include/Utils.h"
#include "../include/Platform.h"
#include "../include/Controller.h"

using namespace storalloc;

class BasicAllocTest : public ::testing::Test
{

public:
    void lustreUseRR_test();
    void lustreOstPenalty_test();
    void lustreOssPenalty_test();
    void lustreComputeOstWeight_test();
    void lustreComputeStripesPerOST_test();

    std::shared_ptr<Config> cfg;
    

protected:
    ~BasicAllocTest()
    {
    }

    BasicAllocTest()
    {
        cfg = std::make_shared<Config>();
        this->cfg->lustre = LustreConfig();  // default lustre config
    }
};

/**********************************************************************/
/**  Testing allocator selection (rr or weighted)                    **/
/**********************************************************************/

TEST_F(BasicAllocTest, lustreUseRR_test)
{
    DO_TEST_WITH_FORK(lustreUseRR_test);
}

/**
 *  @brief  Testing lustreUseRR(), responsible for deciding whether to use
 *          the RR allocator or the Weighted allocator.
 */
void BasicAllocTest::lustreUseRR_test()
{

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

TEST_F(BasicAllocTest, lustreOstPenalty_test)
{
    DO_TEST_WITH_FORK(lustreOstPenalty_test);
}

/**
 *  @brief Testing lustreComputeOstPenalty() (disk/raid level)
 */
void BasicAllocTest::lustreOstPenalty_test()
{

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

TEST_F(BasicAllocTest, lustreOssPenalty_test)
{
    DO_TEST_WITH_FORK(lustreOssPenalty_test);
}

/**
 *  @brief Testing lustreComputeOssPenalty() (storage server level)
 */
void BasicAllocTest::lustreOssPenalty_test()
{

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

TEST_F(BasicAllocTest, lustreComputeOstWeight_test)
{
    DO_TEST_WITH_FORK(lustreComputeOstWeight_test);
}

/**
 *  @brief Testing lustreComputeOstWeight() (disk/raid level)
 */
void BasicAllocTest::lustreComputeOstWeight_test()
{

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


TEST_F(BasicAllocTest, lustreComputeStripesPerOST_test)
{
    DO_TEST_WITH_FORK(lustreComputeStripesPerOST_test);
}

/**
 *  @brief Testing lustreComputeStripesPerOST() (disk/raid level)
 */
void BasicAllocTest::lustreComputeStripesPerOST_test() {


    auto allocator = LustreAllocator(this->cfg);

    auto striping = allocator.lustreComputeStriping(3000000000, 85);

    ASSERT_EQ(striping.stripe_size_b, this->cfg->lustre.stripe_size);
    ASSERT_EQ(striping.stripes_count, 60);
    ASSERT_EQ(striping.stripes_per_ost, 1);

    striping = {};
    striping = allocator.lustreComputeStriping(30000000000, 85);
    ASSERT_EQ(striping.stripe_size_b, this->cfg->lustre.stripe_size);
    ASSERT_EQ(striping.stripes_count, 600);
    ASSERT_EQ(striping.stripes_per_ost, 8);


    striping = {};
    striping = allocator.lustreComputeStriping(3000000, 85);
    ASSERT_EQ(striping.stripe_size_b, 3000000);
    ASSERT_EQ(striping.stripes_count, 1);
    ASSERT_EQ(striping.stripes_per_ost, 1);


}


// ###################################################################################################

class FunctionalAllocTest : public ::testing::Test
{

public:
    void lustreComputeMinMaxUtilization_test();
    void lustreOstIsUsed_test();
    void lustreRROrderServices_test();
    void lustreRROrderServices2_test();     // != config file in simulation
    void lustreRROrderServices3_test();     // != config file in simulation
    void lustreCreateFileParts_test();
    void lustreFullSim_test();

protected:
    ~FunctionalAllocTest() {}
    FunctionalAllocTest() {}
};

/**
 * @brief Basic custom controller for tests on Lustre allocation
 */
class LustreTestController : public wrench::ExecutionController
{
public:
    LustreTestController(const std::shared_ptr<wrench::ComputeService> &compute_service,
                         const std::set<std::shared_ptr<wrench::StorageService>> &storage_services,
                         const std::shared_ptr<wrench::CompoundStorageService> compound_storage_svc,
                         const std::string &hostname,
                         std::shared_ptr<LustreAllocator> alloc) : wrench::ExecutionController(hostname, "controller"),
                                                        storage_services(storage_services), 
                                                        compound(compound_storage_svc), 
                                                        compute_svc(compute_service), 
                                                        alloc(alloc)
    {

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
class LustreTestControllerMinMax : public LustreTestController
{
public:
    LustreTestControllerMinMax(const std::shared_ptr<wrench::ComputeService> &compute_service,
                               const std::set<std::shared_ptr<wrench::StorageService>> &storage_services,
                               const std::shared_ptr<wrench::CompoundStorageService> compound_storage_svc,
                               const std::string &hostname,
                               std::shared_ptr<LustreAllocator> alloc) : LustreTestController(compute_service, storage_services, compound_storage_svc, hostname, alloc) {}

    int main() override
    {
        
        // Prepare a map of services for lustreComputeMinMaxUtilization
        std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> storage_map;
        for (const auto &service: this->storage_services) {
            if (storage_map.find(service->getHostname()) == storage_map.end() ) {
                storage_map[service->getHostname()] = std::vector<std::shared_ptr<wrench::StorageService>>();
            } 
            storage_map[service->getHostname()].push_back(service);
        }

        // Create a dummy job writing data
        auto job_manager = this->createJobManager();
        auto job = job_manager->createCompoundJob("Job1");

        auto simple = std::dynamic_pointer_cast<wrench::SimpleStorageService>(*(this->storage_services.begin()));
        uint64_t free_space_in_service = simple->traceTotalFreeSpace();

        job->addFileWriteAction(
            "write1", wrench::FileLocation::LOCATION(*(this->storage_services.begin()), this->file_50GB)
        );
        job->addFileWriteAction(
            "write2", wrench::FileLocation::LOCATION(*(this->storage_services.begin()), this->file_50GB)
        );
        job->addFileWriteAction(
            "write3", wrench::FileLocation::LOCATION(*(this->storage_services.begin()), this->file_50GB)
        );

        std::map<std::string, std::string> service_specific_args =
                    {{"-N", "2"},                           // nb of nodes
                     {"-c", "16"},                          // core per node
                     {"-t", "6000"}};                        // seconds
        job_manager->submitJob(job, this->compute_svc, service_specific_args);

        // Sleep until we're sure the writes have begun (and thus, space is reserved on the service)
        wrench::Simulation::sleep(300);


        // Test lustreComputeMinMaxUtilization
        auto ba_min_max = this->alloc->lustreComputeMinMaxUtilization(storage_map);

        // Max free space is 200 GB (>>8)
        uint64_t expected_max = 20000000000000 >> 8;  // bitshift for overflow, as computed in Lustre
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

TEST_F(FunctionalAllocTest, lustreComputeMinMaxUtilization_test)
{
    DO_TEST_WITH_FORK(lustreComputeMinMaxUtilization_test);
}

/**
 *  @brief Testing lustreComputeMinMaxUtilization()
 */
void FunctionalAllocTest::lustreComputeMinMaxUtilization_test()
{

    auto config = std::make_shared<storalloc::Config>(storalloc::loadConfig("../configs/lustre_config.yml"));

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
    auto allocator = std::make_shared<LustreAllocator>(config);

    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "compound_storage",
            sstorageservices,
            allocator,
            {{wrench::CompoundStorageServiceProperty::MAX_ALLOCATION_CHUNK_SIZE, std::to_string(config->lustre.stripe_size)},
             {wrench::CompoundStorageServiceProperty::INTERNAL_STRIPING, "false"}},
            // {{wrench::CompoundStorageServiceProperty::MAX_ALLOCATION_CHUNK_SIZE, "30000000000"}},
            {}));

    auto wms = simulation->add(
        new LustreTestControllerMinMax(batch_service, sstorageservices, compound_storage_service, "user0", allocator));

    simulation->launch();
}


/**
 * @brief Custom controller for testing lustreComputeMinMaxUtilization
 */
class LustreTestControllerUsage : public LustreTestController
{
public:
    LustreTestControllerUsage(const std::shared_ptr<wrench::ComputeService> &compute_service,
                               const std::set<std::shared_ptr<wrench::StorageService>> &storage_services,
                               const std::shared_ptr<wrench::CompoundStorageService> compound_storage_svc,
                               const std::string &hostname,
                               std::shared_ptr<LustreAllocator> alloc) : LustreTestController(compute_service, storage_services, compound_storage_svc, hostname, alloc) {}

    int main() override
    {
        
        // Prepare a map of services for lustreComputeMinMaxUtilization
        std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> storage_map;
        for (const auto &service: this->storage_services) {
            if (storage_map.find(service->getHostname()) == storage_map.end() ) {
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
                    {{"-N", "2"},                           // nb of nodes
                     {"-c", "16"},                          // core per node
                     {"-t", "6000"}};                        // seconds
        job_manager->submitJob(job, this->compute_svc, service_specific_args);
        wrench::Simulation::sleep(300);

        // ACTUAL TEST
        std::vector<std::shared_ptr<wrench::FileLocation>> locations = {file_loc};   // Vector of locations for parts of a file
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


TEST_F(FunctionalAllocTest, lustreOstIsUsed_test)
{
    DO_TEST_WITH_FORK(lustreOstIsUsed_test);
}

/**
 *  @brief Testing lustreComputeMinMaxUtilization()
 */
void FunctionalAllocTest::lustreOstIsUsed_test()
{

    auto config = std::make_shared<storalloc::Config>(storalloc::loadConfig("../configs/lustre_config.yml"));

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

    auto allocator = std::make_shared<LustreAllocator>(config);

    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "compound_storage",
            sstorageservices,
            allocator,
            {{wrench::CompoundStorageServiceProperty::MAX_ALLOCATION_CHUNK_SIZE, std::to_string(config->lustre.stripe_size)},
             {wrench::CompoundStorageServiceProperty::INTERNAL_STRIPING, "false"}},
            {}));

    auto wms = simulation->add(
        new LustreTestControllerUsage(batch_service, sstorageservices, compound_storage_service, "user0", allocator));

    simulation->launch();
}


/**
 * @brief Custom controller for testing lustreComputeMinMaxUtilization
 */
class LustreTestControllerOrderRR : public LustreTestController
{
public:
    LustreTestControllerOrderRR(const std::shared_ptr<wrench::ComputeService> &compute_service,
                               const std::set<std::shared_ptr<wrench::StorageService>> &storage_services,
                               const std::shared_ptr<wrench::CompoundStorageService> compound_storage_svc,
                               const std::string &hostname,
                               std::shared_ptr<LustreAllocator> alloc, 
                               const std::vector<std::pair<std::string, std::string>> &ordered_alloc) : LustreTestController(compute_service, storage_services, compound_storage_svc, hostname, alloc), ordered_alloc(ordered_alloc) {}

    int main() override
    {
        
        std::map<std::string, int> hostname_to_service_count;
        std::vector<std::shared_ptr<wrench::StorageService>> disk_level_services;
        for (const auto &service: this->storage_services) {
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
            if ((service->getHostname() != this->ordered_alloc[index].first) or (service->getName() != this->ordered_alloc[index].second)) {
                throw std::runtime_error("Mismatch between computed list of ordered services and expected result");
            }
            index++;
        }

        hostname_to_service_count.clear();
        disk_level_services.clear();
        try {
            this->alloc->lustreRROrderServices(hostname_to_service_count, disk_level_services);
        } catch(std::runtime_error& e) {
            // OK, exception raised as expected;
            return 0;
        } 
        throw std::runtime_error("Error with lustreRROrderServices when given empty vector/map (no exception raised)");
    }

    std::vector<std::pair<std::string, std::string>> ordered_alloc = {};
};


TEST_F(FunctionalAllocTest, lustreRROrderServices_test)
{
    DO_TEST_WITH_FORK(lustreRROrderServices_test);
}

/**
 *  @brief Testing lustreRROrderServices() with a config file presenting 1 OSS A with 3 OST and 1 OSS B with 5 OST
 *         Expected result is a vector of OSTs from OSSs "ABABBABB"
 */
void FunctionalAllocTest::lustreRROrderServices_test()
{

    auto config = std::make_shared<storalloc::Config>(storalloc::loadConfig("../configs/lustre_config_het_oss.yml"));

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
    auto allocator = std::make_shared<LustreAllocator>(config);
    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "compound_storage",
            sstorageservices,
            allocator,
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
        new LustreTestControllerOrderRR(batch_service, sstorageservices, compound_storage_service, "user0", allocator, result));

    ASSERT_NO_THROW(simulation->launch());
}


TEST_F(FunctionalAllocTest, lustreRROrderServices2_test)
{
    DO_TEST_WITH_FORK(lustreRROrderServices2_test);
}

/**
 *  @brief Testing lustreRROrderServices() with a config file presenting 2 OSS (type A) with 3 OST and 2 OSS (type B) with 3 OST
 *         Expected result is a vector of OSTs from OSSs "ABABABABABAB"
 */
void FunctionalAllocTest::lustreRROrderServices2_test()
{

    auto config = std::make_shared<storalloc::Config>(storalloc::loadConfig("../configs/lustre_config_3OST1OSS.yml"));

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
    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "compound_storage",
            sstorageservices,
            allocator,
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
        new LustreTestControllerOrderRR(batch_service, sstorageservices, compound_storage_service, "user0", allocator, result));

    ASSERT_NO_THROW(simulation->launch());
}


TEST_F(FunctionalAllocTest, lustreRROrderServices3_test)
{
    DO_TEST_WITH_FORK(lustreRROrderServices3_test);
}

/**
 *  @brief Testing lustreRROrderServices() with a config file presenting 2 OSS (type A) with 3 OST and 2 OSS (type B) with 3 OST
 *         Expected result is a vector of OSTs from OSSs "ABABABABABAB"
 */
void FunctionalAllocTest::lustreRROrderServices3_test()
{

    auto config = std::make_shared<storalloc::Config>(storalloc::loadConfig("../configs/lustre_config_3OST3OSS.yml"));

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
    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "compound_storage",
            sstorageservices,
            allocator,
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
        new LustreTestControllerOrderRR(batch_service, sstorageservices, compound_storage_service, "user0", allocator, result));

    ASSERT_NO_THROW(simulation->launch());
}

TEST_F(FunctionalAllocTest, lustreCreateFileParts_test)
{
    DO_TEST_WITH_FORK(lustreCreateFileParts_test);
}

/**
 *  @brief Testing lustreRROrderServices() with a config file presenting 2 OSS (type A) with 3 OST and 2 OSS (type B) with 3 OST
 *         Expected result is a vector of OSTs from OSSs "ABABABABABAB"
 */
void FunctionalAllocTest::lustreCreateFileParts_test()
{

    auto config = std::make_shared<storalloc::Config>(storalloc::loadConfig("../configs/lustre_config.yml"));

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

    std::map<int, std::shared_ptr<wrench::StorageService>> alloc_map;
    int index = 0;
    for(const auto &svc : sstorageservices) {
        alloc_map[index] = svc;
        index++;
    }

    auto locations = allocator->lustreCreateFileParts("Test", alloc_map);
    ASSERT_EQ(locations.size(), 16);

    auto file_map = simulation->getFileMap();
    ASSERT_EQ(file_map.size(), 16);

    for(const auto& loc : locations) {
        // Make sure every file parts from designated locations has correctly been added to the simulation
        auto file_part = *(file_map.find(loc->getFile()->getID()));
        ASSERT_EQ(file_part.second->getSize(), loc->getFile()->getSize());
    }
}


TEST_F(FunctionalAllocTest, lustreFullSim_test)
{
    DO_TEST_WITH_FORK(lustreFullSim_test);
}

/**
 *  @brief Testing lustreRROrderServices() with a config file presenting 1 OSS A with 3 OST and 1 OSS B with 5 OST
 *         Expected result is a vector of OSTs from OSSs "ABABBABB"
 */
void FunctionalAllocTest::lustreFullSim_test()
{
    auto config = std::make_shared<storalloc::Config>(storalloc::loadConfig("../configs/lustre_config_het_oss.yml"));
    auto jobs = storalloc::loadYamlJobs("../data/IOJobsTest_6_small_IO.yml");

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
    auto allocator = std::make_shared<LustreAllocator>(config);
    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "compound_storage",
            sstorageservices,
            allocator,
            {{wrench::CompoundStorageServiceProperty::MAX_ALLOCATION_CHUNK_SIZE, std::to_string(config->lustre.stripe_size)},
             {wrench::CompoundStorageServiceProperty::INTERNAL_STRIPING, "false"}},
            {}));
     auto permanent_storage = simulation->add(
        wrench::SimpleStorageService::createSimpleStorageService(
            "permanent_storage", {"/dev/disk0"}, {}, {}));
    

    // Controler
    auto ctrl = simulation->add(new storalloc::Controller(batch_service, permanent_storage, compound_storage_service, "user0", jobs));
    
    ASSERT_NO_THROW(simulation->launch());    

    ASSERT_TRUE(ctrl->hasReturnedFromMain());

    ASSERT_TRUE(ctrl->jobsCompleted());

}
