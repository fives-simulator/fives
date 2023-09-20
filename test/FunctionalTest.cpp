#include <gtest/gtest.h>

#include "./include/TestConstants.h"
#include "./include/TestWithFork.h"

#include "../include/Platform.h"
#include "../include/Simulator.h"
#include "../include/Utils.h"

using namespace storalloc;

class BasicFunctionalTest : public ::testing::Test {

public:
    void create_platform_test();
    void simulation_run_rr();
    void instantiate_storage_services_test();
    void instantiate_compute_services_test();

protected:
    ~BasicFunctionalTest() {}

    BasicFunctionalTest() {}
};

/**********************************************************************/
/**  Functionnal Test 1                                              **/
/**********************************************************************/

TEST_F(BasicFunctionalTest, BasicSimulationRun) {
    DO_TEST_WITH_FORK(simulation_run_rr);
}

/**
 *  @brief Basic functional test (just running a simple configuration and job file scenario),
 *         not checking anything except the execution without exceptions.
 */
void BasicFunctionalTest::simulation_run_rr() {

    int argc = 3;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    argv[1] = strdup((test::CONFIG_PATH + "rr_config.yml").c_str());
    argv[2] = strdup((test::DATA_PATH + "test_1_job.yml").c_str());

    ASSERT_NO_THROW(auto ret = run_simulation(argc, argv););

    for (int i = 0; i < argc; i++)
        free(argv[i]);
    free(argv);
}

TEST_F(BasicFunctionalTest, create_platform_test) {
    DO_TEST_WITH_FORK(create_platform_test);
}

/**
 *  @brief Testing programmatic platform creation
 */
void BasicFunctionalTest::create_platform_test() {

    auto config = std::make_shared<Config>(loadConfig(test::CONFIG_PATH + "rr_config.yml"));

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    ASSERT_NO_THROW(simulation->init(&argc, argv));

    auto platform_factory = PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);

    // Check compute nodes topo
    auto all_hosts_by_cluster = simulation->getHostnameListByCluster();
    ASSERT_NE(all_hosts_by_cluster.find("AS_DragonflyCompute"), all_hosts_by_cluster.end());
    ASSERT_EQ(all_hosts_by_cluster["AS_DragonflyCompute"].size(), 384);
    ASSERT_TRUE(simulation->doesHostExist("compute0"));   // first
    ASSERT_TRUE(simulation->doesHostExist("compute383")); // last compute node

    ASSERT_EQ(wrench::Simulation::getHostNumCores("compute0"), 64);
    ASSERT_EQ(wrench::S4U_Simulation::getHostMemoryCapacity("compute78"), 192000000000); // In bytes

    // Check other zones topo
    auto all_hosts_by_zone = wrench::S4U_Simulation::getAllHostnamesByZone();
    ASSERT_NE(all_hosts_by_zone.find("AS_Ctrl"), all_hosts_by_zone.end());
    ASSERT_TRUE(simulation->doesHostExist("user0"));
    ASSERT_TRUE(simulation->doesHostExist("batch0"));
    ASSERT_EQ(all_hosts_by_zone["AS_Ctrl"][0], "batch0");
    ASSERT_EQ(all_hosts_by_zone["AS_Ctrl"][1], "user0");

    ASSERT_NE(all_hosts_by_zone.find("AS_Storage"), all_hosts_by_zone.end());
    ASSERT_TRUE(simulation->doesHostExist("capacity0"));
    ASSERT_TRUE(simulation->doesHostExist("capacity1"));
    ASSERT_TRUE(simulation->doesHostExist("capacity2"));
    ASSERT_TRUE(simulation->doesHostExist("capacity3"));
    ASSERT_TRUE(simulation->doesHostExist("permanent_storage"));
    ASSERT_TRUE(simulation->doesHostExist("compound_storage"));
    ASSERT_EQ(all_hosts_by_zone["AS_Storage"][0], "capacity0");
    ASSERT_EQ(all_hosts_by_zone["AS_Storage"][4], "compound_storage");

    // Check disks for at least one of the storage nodes
    std::vector<std::string> capacity1_disks = wrench::S4U_Simulation::getDisks("capacity1");
    ASSERT_EQ(capacity1_disks[0], "/dev/hdd0");
    ASSERT_EQ(capacity1_disks[1], "/dev/hdd1");
    ASSERT_EQ(capacity1_disks[2], "/dev/ssd0");

    ASSERT_EQ(wrench::S4U_Simulation::getDiskCapacity("capacity1", "/dev/hdd0"), 200000000000); // in bytes
    ASSERT_EQ(wrench::S4U_Simulation::getDiskCapacity("capacity1", "/dev/hdd1"), 200000000000);
    ASSERT_EQ(wrench::S4U_Simulation::getDiskCapacity("capacity1", "/dev/ssd0"), 96000000000);

    auto capacity2 = wrench::S4U_Simulation::get_host_or_vm_by_name("capacity2");
    ASSERT_EQ(capacity2->get_disks()[0]->get_sharing_policy(sg4::Disk::Operation::READ), sg4::Disk::SharingPolicy::NONLINEAR);
    ASSERT_EQ(capacity2->get_disks()[0]->get_sharing_policy(sg4::Disk::Operation::WRITE), sg4::Disk::SharingPolicy::NONLINEAR);

    for (const auto disk : capacity2->get_disks()) {
        if ((disk->get_name() == "hdd_capa0") || (disk->get_name() == "hdd_capa1")) {
            ASSERT_EQ(disk->get_read_bandwidth(), 100000000);
            ASSERT_EQ(disk->get_write_bandwidth(), 50000000);
        } else if (disk->get_name() == "ssd_perf0") {
            ASSERT_EQ(disk->get_read_bandwidth(), 1000000000);
            ASSERT_EQ(disk->get_write_bandwidth(), 500000000);
        } else {
            GTEST_FAIL();
        }
    }

    // Static "permanent storage" node
    auto permanent = wrench::S4U_Simulation::get_host_or_vm_by_name("permanent_storage");
    ASSERT_EQ(permanent->get_disks()[0]->get_read_bandwidth(), 10000000000);
    ASSERT_EQ(permanent->get_disks()[0]->get_write_bandwidth(), 10000000000);
    auto perm_disk_properties = permanent->get_disks()[0]->get_properties();
    ASSERT_EQ(perm_disk_properties->at("size"), "10000TB");
    ASSERT_EQ(wrench::S4U_Simulation::getDiskCapacity("permanent_storage", "/dev/disk0"), 10000000000000000);
    ASSERT_EQ(perm_disk_properties->at("mount"), "/dev/disk0");

    // Check links
    auto link_names = wrench::S4U_Simulation::getAllLinknames();
    ASSERT_EQ(link_names.size(), 1606);

    ASSERT_TRUE(simulation->doesLinkExist("backbone_ctrl"));
    ASSERT_TRUE(simulation->doesLinkExist("backbone"));
    ASSERT_TRUE(simulation->doesLinkExist("batch0_DOWN"));
    ASSERT_TRUE(simulation->doesLinkExist("batch0_UP"));
    ASSERT_TRUE(simulation->doesLinkExist("user0_DOWN"));
    ASSERT_TRUE(simulation->doesLinkExist("user0_UP"));
    ASSERT_TRUE(simulation->doesLinkExist("backbone_storage"));
    ASSERT_TRUE(simulation->doesLinkExist("capacity0_DOWN"));
    ASSERT_TRUE(simulation->doesLinkExist("capacity0_UP"));
    ASSERT_TRUE(simulation->doesLinkExist("capacity3_DOWN"));
    ASSERT_TRUE(simulation->doesLinkExist("capacity3_UP"));
    ASSERT_TRUE(simulation->doesLinkExist("permanent_storage_DOWN"));
    ASSERT_TRUE(simulation->doesLinkExist("permanent_storage_UP"));
    ASSERT_TRUE(simulation->doesLinkExist("compound_storage_DOWN"));
    ASSERT_TRUE(simulation->doesLinkExist("compound_storage_UP"));

    // auto s1 = wrench::SimpleStorageService::createSimpleStorageService("capacity1", {"/dev/hdd1"});
}

TEST_F(BasicFunctionalTest, instantiate_storage_services_test) {
    DO_TEST_WITH_FORK(instantiate_storage_services_test);
}

/**
 *  @brief Testing programmatic platform creation
 */
void BasicFunctionalTest::instantiate_storage_services_test() {

    auto config = std::make_shared<Config>(loadConfig(test::CONFIG_PATH + "rr_config.yml"));

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    ASSERT_NO_THROW(simulation->init(&argc, argv));

    auto platform_factory = storalloc::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);

    auto ss_storages = storalloc::instantiateStorageServices(simulation, config);

    ASSERT_EQ(ss_storages.size(), 12); // Four storage nodes * 3 disks = 12 storage services

    for (const auto &service : ss_storages) {
        auto simple = std::dynamic_pointer_cast<wrench::SimpleStorageService>(service);
        auto mount_point = simple->getMountPoints();
        ASSERT_EQ(mount_point.size(), 1);
        if ((mount_point.find("/dev/hdd0") != mount_point.end()) || (mount_point.find("/dev/hdd1") != mount_point.end())) {
            ASSERT_EQ(simple->getTotalFreeSpaceZeroTime(), 200000000000);
            ASSERT_EQ(simple->getTotalFilesZeroTime(), 0);
        } else if (mount_point.find("/dev/ssd0") != mount_point.end()) {
            ASSERT_EQ(simple->getTotalFreeSpaceZeroTime(), 96000000000);
            ASSERT_EQ(simple->getTotalFilesZeroTime(), 0);
        } else {
            GTEST_FAIL();
        }
    }
}

class TestController : public wrench::ExecutionController {
public:
    TestController(const std::shared_ptr<wrench::ComputeService> &compute_service,
                   const std::string &hostname) : wrench::ExecutionController(hostname, "controller"), compute_s(compute_service) {}

    int main() override {

        if (this->compute_s->getNumHosts() != 384) {
            throw std::runtime_error("Invalid number of compute hosts");
        }

        if (this->compute_s->getTotalNumCores() != 384 * 64) {
            throw std::runtime_error("Invalid core number");
        }

        for (const auto &mem : this->compute_s->getMemoryCapacity()) {
            if (mem.second != 192000000000) {
                throw std::runtime_error("Invalid RAM capcity " + std::to_string(mem.second));
            }
        }

        return 0;
    }

    std::shared_ptr<wrench::ComputeService> compute_s;
};

TEST_F(BasicFunctionalTest, instantiate_compute_services_test) {
    DO_TEST_WITH_FORK(instantiate_compute_services_test);
}

/**
 *  @brief Testing programmatic platform creation
 */
void BasicFunctionalTest::instantiate_compute_services_test() {

    auto config = std::make_shared<Config>(loadConfig(test::CONFIG_PATH + "rr_config.yml"));

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    char **argv = (char **)calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    ASSERT_NO_THROW(simulation->init(&argc, argv));

    auto platform_factory = PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);

    auto batch_service = storalloc::instantiateComputeServices(simulation, config);

    auto wms = simulation->add(
        new TestController(batch_service, "user0"));

    ASSERT_NO_THROW(simulation->launch());
}