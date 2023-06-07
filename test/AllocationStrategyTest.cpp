#include <cstdint>

#include <gtest/gtest.h>

#include "./include/TestWithFork.h"

#include "../include/Simulator.h"
#include "../include/AllocationStrategy.h"
#include "../include/Utils.h"
#include "../include/Platform.h"
#include "../include/Controller.h"

class BasicAllocTest : public ::testing::Test
{

public:
    void lustreUseRR_test();
    void lustreOstPenalty_test();
    void lustreOssPenalty_test();
    void lustreComputeOstWeight_test();

protected:
    ~BasicAllocTest()
    {
    }

    BasicAllocTest()
    {
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

    // Right above the 17% threshold
    test_min_max.min = 833 * GB;
    test_min_max.max = 1000 * GB;
    ASSERT_TRUE(storalloc::lustreUseRR(test_min_max));

    // Slightly below the 17% threshold
    test_min_max.min = 830 * GB;
    test_min_max.max = 1000 * GB;
    ASSERT_FALSE(storalloc::lustreUseRR(test_min_max));

    // Same free space
    test_min_max.min = 1000 * GB;
    test_min_max.max = 1000 * GB;
    ASSERT_TRUE(storalloc::lustreUseRR(test_min_max));

    // High values (overflow testing)
    test_min_max.min = 1000 * 1000 * GB;
    test_min_max.max = 1000 * 1000 * GB;
    ASSERT_TRUE(storalloc::lustreUseRR(test_min_max));
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

    const uint64_t GB = 1000 * 1000 * 1000;

    uint64_t free_space_b = 250 * GB; // 250 GB free
    uint64_t used_inode_count = 1'200'000;
    size_t active_service_count = 200; // ~ number of active targets, aka disks/raid in the whole system

    ASSERT_EQ(storalloc::lustreComputeOstPenalty(free_space_b,
                                                 used_inode_count,
                                                 active_service_count),
              14995807557);

    free_space_b = 0;
    ASSERT_EQ(storalloc::lustreComputeOstPenalty(free_space_b,
                                                 used_inode_count,
                                                 active_service_count),
              0);

    free_space_b = 250 * GB;
    used_inode_count = storalloc::LUSTRE_max_inodes;
    ASSERT_EQ(storalloc::lustreComputeOstPenalty(free_space_b,
                                                 used_inode_count,
                                                 active_service_count),
              0);
    used_inode_count = 1'200'000;

    ASSERT_THROW(storalloc::lustreComputeOstPenalty(free_space_b,
                                                    used_inode_count,
                                                    0),
                 std::runtime_error);

    // The more services / active targets, the smaller the penalty per target
    ASSERT_GT(storalloc::lustreComputeOstPenalty(free_space_b,
                                                 used_inode_count,
                                                 active_service_count),
              storalloc::lustreComputeOstPenalty(free_space_b,
                                                 used_inode_count,
                                                 300));
    // The more bytes available for allocation, the larger the penalty
    ASSERT_GT(storalloc::lustreComputeOstPenalty(400 * GB,
                                                 used_inode_count,
                                                 active_service_count),
              storalloc::lustreComputeOstPenalty(free_space_b,
                                                 used_inode_count,
                                                 active_service_count));

    // The more inodes available for allocation, the larger the penalty
    ASSERT_GT(storalloc::lustreComputeOstPenalty(free_space_b,
                                                 used_inode_count - 300000,
                                                 active_service_count),
              storalloc::lustreComputeOstPenalty(free_space_b,
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

    const uint64_t GB = 1000 * 1000 * 1000;

    // Let's suppose we have 8 OSTs in this OSS, and a total of 10 OSS
    uint64_t srv_free_space_b = (250 * GB >> 16) * 8;
    uint64_t srv_free_inode_count = ((storalloc::LUSTRE_max_inodes - 1'200'000) >> 8) * 8;
    size_t service_count = 200; // Targets / OSTs
    size_t oss_count = 10;      // Storage nodes /  OSS

    ASSERT_EQ(storalloc::lustreComputeOssPenalty(srv_free_space_b,
                                                 srv_free_inode_count,
                                                 service_count,
                                                 oss_count),
              95973168366);

    ASSERT_EQ(storalloc::lustreComputeOssPenalty(0,
                                                 srv_free_inode_count,
                                                 service_count,
                                                 oss_count),
              0);

    ASSERT_EQ(storalloc::lustreComputeOssPenalty(srv_free_space_b,
                                                 0,
                                                 service_count,
                                                 oss_count),
              0);

    // Divide / 0
    ASSERT_THROW(storalloc::lustreComputeOssPenalty(srv_free_space_b,
                                                    srv_free_inode_count,
                                                    0,
                                                    oss_count),
                 std::runtime_error);

    // Divide / 0
    ASSERT_THROW(storalloc::lustreComputeOssPenalty(srv_free_space_b,
                                                    srv_free_inode_count,
                                                    service_count,
                                                    0),
                 std::runtime_error);

    // The more OSS in the system, the lower the per-OSS penalty
    ASSERT_GT(storalloc::lustreComputeOssPenalty(srv_free_space_b,
                                                 srv_free_inode_count,
                                                 service_count,
                                                 oss_count),
              storalloc::lustreComputeOssPenalty(srv_free_space_b,
                                                 srv_free_inode_count,
                                                 service_count,
                                                 oss_count + 40));

    // The more OSTs in the whole system, the lower the per-OSS penalty
    ASSERT_GT(storalloc::lustreComputeOssPenalty(srv_free_space_b,
                                                 srv_free_inode_count,
                                                 service_count,
                                                 oss_count),
              storalloc::lustreComputeOssPenalty(srv_free_space_b,
                                                 srv_free_inode_count,
                                                 service_count + 60,
                                                 oss_count));

    // The more space avail on OSS the larger the penalty
    ASSERT_GT(storalloc::lustreComputeOssPenalty(srv_free_space_b + 400 * GB,
                                                 srv_free_inode_count,
                                                 service_count,
                                                 oss_count),
              storalloc::lustreComputeOssPenalty(srv_free_space_b,
                                                 srv_free_inode_count,
                                                 service_count,
                                                 oss_count));

    // The more inodes avail on OSS the larger the penalty
    ASSERT_GT(storalloc::lustreComputeOssPenalty(srv_free_space_b,
                                                 srv_free_inode_count + 200'000,
                                                 service_count,
                                                 oss_count),
              storalloc::lustreComputeOssPenalty(srv_free_space_b,
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

    const uint64_t GB = 1000 * 1000 * 1000;

    uint64_t free_space_b = 250 * GB;
    uint64_t used_inode_count = 1'200'000;
    size_t service_count = 12;

    auto ost_penalty = storalloc::lustreComputeOstPenalty(free_space_b,
                                                          storalloc::LUSTRE_max_inodes - used_inode_count,
                                                          service_count);

    ASSERT_EQ(storalloc::lustreComputeOstWeight(free_space_b,
                                                used_inode_count,
                                                ost_penalty),
              63982042402279);

    // The more bytes available, the larger the weight
    ost_penalty = storalloc::lustreComputeOstPenalty(free_space_b + (100 * GB),
                                                     storalloc::LUSTRE_max_inodes - used_inode_count,
                                                     service_count);
    ASSERT_GT(storalloc::lustreComputeOstWeight(free_space_b + (100 * GB),
                                                used_inode_count,
                                                ost_penalty),
              storalloc::lustreComputeOstWeight(free_space_b,
                                                used_inode_count,
                                                ost_penalty));

    // The more inodes available, the larger the weight
    ost_penalty = storalloc::lustreComputeOstPenalty(free_space_b,
                                                     storalloc::LUSTRE_max_inodes - used_inode_count - 300'000,
                                                     service_count);
    ASSERT_GT(storalloc::lustreComputeOstWeight(free_space_b,
                                                used_inode_count - 300'000,
                                                ost_penalty),
              storalloc::lustreComputeOstWeight(free_space_b,
                                                used_inode_count,
                                                ost_penalty));

    // If OST penalty is larger than weight, weight becomes 0
    ASSERT_EQ(storalloc::lustreComputeOstWeight(free_space_b,
                                                used_inode_count,
                                                (free_space_b >> 16) * ((storalloc::LUSTRE_max_inodes - used_inode_count) >> 8) + 1),
              0);
}

// ###################################################################################################

class FunctionalAllocTest : public ::testing::Test
{

public:
    void lustreComputeMinMaxUtilization_test();

protected:
    ~FunctionalAllocTest()
    {
    }

    FunctionalAllocTest()
    {
    }
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
                         const std::string &hostname) : wrench::ExecutionController(hostname, "controller"),
                                                        storage_services(storage_services), compound(compound_storage_svc), compute_svc(compute_service)
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
                               const std::string &hostname) : LustreTestController(compute_service, storage_services, compound_storage_svc, hostname) {}

    int main() override
    {

        auto job_manager = this->createJobManager();
        auto job = job_manager->createCompoundJob("Job1");

        for (const auto &service : this->storage_services) {
            std::cout << "service space " << std::to_string(service->getTotalSpace()) << std::endl;
        }

        auto write1 = job->addFileWriteAction(
            "write1", wrench::FileLocation::LOCATION(*(this->storage_services.begin()), this->file_10GB)
        );


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

    auto config = std::make_shared<storalloc::Config>(storalloc::loadConfig("../configs/test_config.yml"));

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    ASSERT_NO_THROW(simulation->init(&argc, argv));

    auto platform_factory = storalloc::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);

    auto batch_service = storalloc::instantiateComputeServices(simulation, config);

    /* Simple storage services */
    auto sstorageservices = storalloc::instantiateStorageServices(simulation, config);

    auto compound_storage_service = simulation->add(
        new wrench::CompoundStorageService(
            "compound_storage",
            sstorageservices,
            storalloc::lustreStrategy,
            {{wrench::CompoundStorageServiceProperty::MAX_ALLOCATION_CHUNK_SIZE, config->max_stripe_size},
             {wrench::CompoundStorageServiceProperty::INTERNAL_STRIPING, "false"}},
            // {{wrench::CompoundStorageServiceProperty::MAX_ALLOCATION_CHUNK_SIZE, "30000000000"}},
            {}));

    auto wms = simulation->add(
        new LustreTestControllerMinMax(batch_service, sstorageservices, compound_storage_service, "user0"));

    simulation->launch();
}
