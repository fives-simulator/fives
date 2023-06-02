#include <cstdint>

#include <gtest/gtest.h>

#include "./include/TestWithFork.h"

#include "../include/Simulator.h"
#include "../include/AllocationStrategy.h"

class BasicAllocTest : public ::testing::Test
{

public:
    void lustreUseRR_test();
    void lustreOstPenalty_test();

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
 *  @brief Basic functional test (just running a simple configuration and job file scenario),
 *         not checking anything except the execution without exceptions.
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
 *  @brief Basic functional test (just running a simple configuration and job file scenario),
 *         not checking anything except the execution without exceptions.
 */
void BasicAllocTest::lustreOstPenalty_test()
{

    const uint64_t GB = 1000 * 1000 * 1000;

    uint64_t free_space_b = 250 * GB;
    uint64_t free_inode_count = UINT64_MAX - 1'200'000;
    size_t service_count = 45;

    ASSERT_EQ(storalloc::lustreComputeOstPenalty(free_space_b,
                                                 free_inode_count, 
                                                 service_count),
              675539925477137);

}
