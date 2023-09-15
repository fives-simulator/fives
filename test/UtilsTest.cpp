#include <cstdint>

#include <gtest/gtest.h>

#include "./include/TestConstants.h"
#include "./include/TestWithFork.h"

#include "../include/AllocationStrategy.h"
#include "../include/ConfigDefinition.h"
#include "../include/Simulator.h"
#include "../include/Utils.h"

using namespace storalloc;

class BasicUtilsTest : public ::testing::Test {

public:
    void loadConfig_test();

    void loadYamlJobs_test();

protected:
    ~BasicUtilsTest() {}

    BasicUtilsTest() {}
};

TEST_F(BasicUtilsTest, loadYamlJobs_test) {
    DO_TEST_WITH_FORK(loadYamlJobs_test);
}

void BasicUtilsTest::loadYamlJobs_test() {

    auto jobs = loadYamlJobs(test::DATA_PATH + "IOJobsTest_6_small_IO.yml");

    ASSERT_EQ(jobs.size(), 6);

    auto job_2 = jobs[1];

    ASSERT_EQ(job_2.id, "2");
    ASSERT_EQ(job_2.coresUsed, 32);
    ASSERT_EQ(job_2.endTime, "2023-01-01 00:11:00");
    ASSERT_EQ(job_2.nodesUsed, 2);
    ASSERT_EQ(job_2.readBytes, 6000000000);
    ASSERT_EQ(job_2.runtimeSeconds, 3300);
    ASSERT_EQ(job_2.sleepSimulationSeconds, 7500);
    ASSERT_EQ(job_2.startTime, "2023-01-01 00:10:05");
    ASSERT_EQ(job_2.submissionTime, "2023-01-01 00:10:00");
    ASSERT_EQ(job_2.writtenBytes, 3000000000);

    ASSERT_THROW(loadYamlJobs(test::DATA_PATH + "IOJobsTest__invalid_coresUsed.yml"), std::runtime_error);
    ASSERT_THROW(loadYamlJobs(test::DATA_PATH + "IOJobsTest__invalid_computeTime.yml"), std::runtime_error);
    ASSERT_THROW(loadYamlJobs(test::DATA_PATH + "IOJobsTest__invalid_nodesUsed.yml"), std::runtime_error);
    ASSERT_THROW(loadYamlJobs(test::DATA_PATH + "IOJobsTest__invalid_runTime.yml"), std::runtime_error);
}

/**********************************************************************/
/**  Testing configuration loading                                   **/
/**********************************************************************/

TEST_F(BasicUtilsTest, loadConfig_test) {
    DO_TEST_WITH_FORK(loadConfig_test);
}

/**
 *  @brief  Testing loadConfig() with a test config file
 */
void BasicUtilsTest::loadConfig_test() {

    auto config = loadConfig(test::CONFIG_PATH + "rr_config.yml");

    ASSERT_EQ(config.bkbone_bw, "25000MBps");
    ASSERT_EQ(config.config_version, "0.0.1");
    ASSERT_EQ(config.config_name, "StorAlloc_Test_RR");
    ASSERT_EQ(config.max_stripe_size, 100000000);
    ASSERT_EQ(config.d_groups, 2);
    ASSERT_EQ(config.d_group_links, 3);
    ASSERT_EQ(config.d_chassis, 4);
    ASSERT_EQ(config.d_chassis_links, 5);
    ASSERT_EQ(config.d_routers, 6);
    ASSERT_EQ(config.d_router_links, 7);

    // nodes
    auto nodes = config.nodes;

    auto capa0 = nodes[0];
    ASSERT_EQ(capa0.qtt, 4); // number of nodes of type capacity

    auto capa0_tpl = capa0.tpl;
    ASSERT_EQ(capa0_tpl.id, "capacity");

    auto capa0_tpl_disk0 = capa0_tpl.disks[0];
    ASSERT_EQ(capa0_tpl_disk0.qtt, 1);

    auto capa0_tpl_disk0_tpl = capa0_tpl_disk0.tpl;
    ASSERT_EQ(capa0_tpl_disk0_tpl.capacity, 96);
    ASSERT_EQ(capa0_tpl_disk0_tpl.read_bw, 1000);
    ASSERT_EQ(capa0_tpl_disk0_tpl.write_bw, 500);
    ASSERT_EQ(capa0_tpl_disk0_tpl.mount_prefix, "/dev/ssd");
    ASSERT_EQ(capa0_tpl_disk0_tpl.id, "ssd_perf");

    auto capa0_tpl_disk1 = capa0_tpl.disks[1];
    ASSERT_EQ(capa0_tpl_disk1.qtt, 2);
    auto capa0_tpl_disk1_tpl = capa0_tpl_disk1.tpl;
    ASSERT_EQ(capa0_tpl_disk1_tpl.capacity, 200);
    ASSERT_EQ(capa0_tpl_disk1_tpl.read_bw, 100);
    ASSERT_EQ(capa0_tpl_disk1_tpl.write_bw, 50);
    ASSERT_EQ(capa0_tpl_disk1_tpl.mount_prefix, "/dev/hdd");
    ASSERT_EQ(capa0_tpl_disk1_tpl.id, "hdd_capa");

    auto nodes_tpl = config.node_templates;
    ASSERT_EQ(nodes_tpl.size(), 1);
    auto node_capa = nodes_tpl["capacity"];
    ASSERT_EQ(node_capa.disks.size(), 2);
    ASSERT_EQ(node_capa.id, "capacity");

    auto disks_tpl = config.disk_templates;
    ASSERT_EQ(disks_tpl.size(), 2);

    auto disk_perf = disks_tpl["ssd_perf"];
    ASSERT_EQ(disk_perf.capacity, 96);

    auto disk_capa = disks_tpl["hdd_capa"];
    ASSERT_EQ(disk_capa.capacity, 200);
}
