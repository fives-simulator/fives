#ifndef UTILS_H
#define UTILS_H

#include <wrench-dev.h>

#include "ConfigDefinition.h"
#include "JobDefinition.h"


namespace storalloc {

    void describe_platform();

    storalloc::Config loadConfig(const std::string& yaml_file_name);

    std::map<std::string, storalloc::YamlJob> loadYamlJobs(const std::string& yaml_file_name);

} // storalloc

#endif // Utils.h