#include <gtest/gtest.h>

#include "./include/TestWithFork.h"

#include "Simulator.h"


class BasicFunctionalTest : public ::testing::Test {

public:

    void do_Functional_test();

protected:
    ~BasicFunctionalTest() {
    }

    BasicFunctionalTest() {

    }

};


/**********************************************************************/
/**  Functionnal Test 1                                              **/
/**********************************************************************/

TEST_F(BasicFunctionalTest, BasicFunctionalTest1) {
    DO_TEST_WITH_FORK(do_Functional_test);
}


/**
 *  @brief Basic functional test (just running a simple configuration and job file scenario), 
 *         not checking anything except the execution without exceptions.
 */
void BasicFunctionalTest::do_Functional_test() {

    int argc = 3;
    char **argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    argv[1] = strdup("../config.yml");
    argv[2] = strdup("../data/IOJobsTest_10.yml");

    ASSERT_NO_THROW(auto ret = storalloc::run_simulation(argc, argv););

    for (int i = 0; i < argc; i++)
        free(argv[i]);
    free(argv);
}