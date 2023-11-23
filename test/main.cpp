#include <gtest/gtest.h>
#include <wrench/logging/TerminalOutput.h>
#include <xbt.h>

int main(int argc, char **argv) {

    // disable log
    xbt_log_control_set("root.thresh:critical");

    // Example selective log enabling
    // xbt_log_control_set("simulation_timestamps.thresh:debug");
    // xbt_log_control_set("mailbox.thresh:debug");
    // xbt_log_control_set("comprehensive_failure_integration_test.thresh:info");
    // xbt_log_control_set("s4u_daemon.thresh:info");
    // xbt_log_control_set("host_random_repeat_switcher.thresh:info");
    // xbt_log_control_set("simple_storage_service.thresh:info");
    // xbt_log_control_set("multicore_compute_service.thresh:info");
    // xbt_log_control_set("wrench_core_compound_storage_system.thresh:debug");
    // xbt_log_control_set("storalloc_allocator.thresh=debug");
    // xbt_log_control_set("storalloc_controller.thresh=debug");

    //   Disable color for test logs
    //   wrench::TerminalOutput::disableColor();

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}