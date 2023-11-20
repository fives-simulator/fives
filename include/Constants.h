/**
 *  Global constants used throughout the simulator
 *  (Limit usage as much as possible and prefer config fields)
 *
 */
#ifndef CONSTANTS_H
#define CONSTANTS_H

#include <string>

namespace storalloc {

    static const std::string PERMANENT_STORAGE = "permanent_storage";
    static const std::string COMPOUND_STORAGE = "compound_storage";
    static const std::string BATCH = "batch0";
    static const std::string USER = "user0";
    static const std::string FASTLINK = "12.5GBps";
    static const std::string SLOWLINK = "1.25GBps";
    static const std::string LOOPBACK = "1000GBps";

}; // namespace storalloc

#endif // CONSTANTS_H