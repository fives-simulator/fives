#ifndef PLATFORM_H
#define PLATFORM_H

#include <wrench-dev.h>
#include "ConfigDefinition.h"

namespace sg4 = simgrid::s4u;

namespace storalloc {

    constexpr unsigned int MBPS (1000 * 1000);

    /**
     * @brief Function to instantiate a simulated platform, instead of
     * loading it from an XML file. This function directly uses SimGrid's s4u API
     * (see the SimGrid documentation). This function creates a platform that's
     * identical to that described in the file two_hosts.xml located in this directory.
     */

    class PlatformFactory {

    public:

        PlatformFactory(const std::shared_ptr<storalloc::Config> cfg) : config(cfg){}

        void operator()() const {
            create_platform(this->config);
        }

    private:

        std::shared_ptr<storalloc::Config> config;

        void create_platform(const std::shared_ptr<storalloc::Config>) const;

    };

}; // namespace storalloc

#endif // PLATFORM_H