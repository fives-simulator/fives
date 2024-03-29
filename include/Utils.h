#ifndef UTILS_H
#define UTILS_H

#include <wrench-dev.h>

#include "ConfigDefinition.h"
#include "JobDefinition.h"

namespace fives {

    void describe_platform();

    Config loadConfig(const std::string &yaml_file_name);

    std::vector<YamlJob> loadYamlJobs(const std::string &yaml_file_name);

    JobsStats loadYamlHeader(const std::string &yaml_file_name);

} // namespace fives

#endif // Utils.h