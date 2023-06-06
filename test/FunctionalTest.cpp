#include <gtest/gtest.h>

#include "./include/TestWithFork.h"

#include "../include/Simulator.h"
#include "../include/Utils.h"
#include "../include/Platform.h"


class BasicFunctionalTest : public ::testing::Test {

public:

    void create_platform_test();
    void do_Functional_test();

protected:
    ~BasicFunctionalTest() {}

    BasicFunctionalTest() {}

};


/**********************************************************************/
/**  Functionnal Test 1                                              **/
/**********************************************************************/

TEST_F(BasicFunctionalTest, BasicFunctionalTest1) {
    // DO_TEST_WITH_FORK(do_Functional_test);
}


/**
 *  @brief Basic functional test (just running a simple configuration and job file scenario), 
 *         not checking anything except the execution without exceptions.
 */
void BasicFunctionalTest::do_Functional_test() {

    int argc = 3;
    char **argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    argv[1] = strdup("../configs/test_config.yml");
    argv[2] = strdup("../data/IOJobsTest_1.yml");

    ASSERT_NO_THROW(auto ret = storalloc::run_simulation(argc, argv););

    for (int i = 0; i < argc; i++)
        free(argv[i]);
    free(argv);
}


TEST_F(BasicFunctionalTest, create_platform_test)
{
    DO_TEST_WITH_FORK(create_platform_test);
}

/**
 *  @brief Testing programmatic platform creation
 */
void BasicFunctionalTest::create_platform_test() {

    auto config = std::make_shared<storalloc::Config>(storalloc::loadConfig("../configs/test_config.yml"));

    auto simulation = wrench::Simulation::createSimulation();
    int argc = 1;
    char **argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    ASSERT_NO_THROW(simulation->init(&argc, argv));
    
    auto platform_factory = storalloc::PlatformFactory(config);
    simulation->instantiatePlatform(platform_factory);
    
    // Check compute nodes topo
    auto all_hosts_by_cluster = simulation->getHostnameListByCluster();
    ASSERT_NE(all_hosts_by_cluster.find("AS_DragonflyCompute"), all_hosts_by_cluster.end());
    ASSERT_EQ(all_hosts_by_cluster["AS_DragonflyCompute"].size(), 384);
    ASSERT_TRUE(simulation->doesHostExist("compute0")); // first
    ASSERT_TRUE(simulation->doesHostExist("compute383")); // last compute node

    ASSERT_EQ(wrench::Simulation::getHostNumCores("compute0"), 64);
    ASSERT_EQ(wrench::S4U_Simulation::getHostMemoryCapacity("compute78"), 192000000000);    // In bytes

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
            //ASSERT_EQ(disk->get_concurrency_limit(), 500);
        } else if (disk->get_name() == "ssd_perf0") {
            ASSERT_EQ(disk->get_read_bandwidth(), 1000000000);
            ASSERT_EQ(disk->get_write_bandwidth(), 500000000);
            // ASSERT_EQ(disk->get_concurrency_limit(), -1);
        } else {
            GTEST_FAIL_AT(__FILE__, __LINE__);
        }
    }

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


    /*
    std::string start, end;
    start = "compute34";
    end = "capacity2";
    auto route = wrench::Simulation::getRoute(start, end);
    for (const auto &hop : route) {
        std::cout << " - " << hop << std::endl;
    }

    start = "user0";
    end = "capacity2";
    auto route2 = wrench::Simulation::getRoute(start, end);
    for (const auto &hop : route2) {
        std::cout << " - " << hop << std::endl;
    }

    
    auto compute14 = wrench::S4U_Simulation::get_host_or_vm_by_name("compute14");
        auto compute101 = wrench::S4U_Simulation::get_host_or_vm_by_name("compute101");
    auto user0 = wrench::S4U_Simulation::get_host_or_vm_by_name("user0");
    std::vector<simgrid::s4u::Link*> linksInRoute;
    double latency = 0;
    //std::unordered_set<simgrid::kernel::routing::NetZoneImpl*> netzonesInRoute;
    user0->route_to(compute14, linksInRoute, &latency);

    auto compute_zone = compute101->get_englobing_zone();
    auto properties = capacity2->get_properties();
    */

    // auto s1 = wrench::SimpleStorageService::createSimpleStorageService("capacity1", {"/dev/hdd1"});

}