#ifndef PLATFORM_H
#define PLATFORM_H

#include "ConfigDefinition.h"
#include <wrench-dev.h>

namespace sg4 = simgrid::s4u;

namespace fives {

    constexpr unsigned int MBPS(1000 * 1000);

    /**
     * @brief Function to instantiate a simulated platform, instead of
     * loading it from an XML file. This function directly uses SimGrid's s4u API
     * (see the SimGrid documentation). This function creates a platform that's
     * identical to that described in the file two_hosts.xml located in this directory.
     */

    class PlatformFactory {

    public:
        PlatformFactory(const std::shared_ptr<fives::Config> cfg) : config(cfg) {}

        void operator()() const {
            create_platform(this->config);
        }

    private:
        std::shared_ptr<fives::Config> config;

        void create_platform(const std::shared_ptr<fives::Config>) const;
    };

}; // namespace fives

#endif // PLATFORM_H