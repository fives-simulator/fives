#ifndef PLATFORM_H
#define PLATFORM_H

#include <wrench-dev.h>

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

        PlatformFactory(double link_bw) : link_bw(link_bw) {}

        void operator()() const {
            create_platform(this->link_bw);
        }

    private:

        double link_bw;

        void create_platform(double link_bw) const;

    };

}; // namespace storalloc

#endif // PLATFORM_H