#ifndef SIMULATOR_H
#define SIMULATOR_H

#include <memory>
#include <set>
#include <wrench-dev.h>

#include "ConfigDefinition.h"

namespace fives {

    /**
     * @brief Helper function to instatiate all required StorageServices from the main config
     * @param simulation An initialized Wrench simulation object
     * @param config The configuration loaded from YAML
     * @return Set of instantiated storage services
     */
    std::set<std::shared_ptr<wrench::StorageService>> instantiateStorageServices(std::shared_ptr<wrench::Simulation> simulation,
                                                                                 std::shared_ptr<fives::Config> config);

    /**
     * @brief Helper function to instatiate all required ComputeServices from the main config
     * @param simulation An initialized Wrench simulation object
     * @param config The configuration loaded from YAML
     * @return Resulting wrench BatchComputeService
     */
    std::shared_ptr<wrench::BatchComputeService> instantiateComputeServices(std::shared_ptr<wrench::Simulation> simulation,
                                                                            std::shared_ptr<fives::Config> config);

    /**
     * @brief The Simulator's main function
     *
     * @param argc: argument count
     * @param argv: argument array
     * @return 0 on success, non-zero otherwise
     */
    int run_simulation(int argc, char **argv);

} // namespace fives

#endif // SIMULATOR_H