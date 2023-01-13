
/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#ifndef CONTROLLER_H
#define CONTROLLER_H

#include <wrench-dev.h>
#include "JobDefinition.h"

namespace wrench {

    /**
     *  @brief A Workflow Management System (WMS) implementation
     */
    class Controller : public ExecutionController {

    public:
        // Constructor
        Controller(
                  const std::shared_ptr<ComputeService> &compute_service,
                  const std::shared_ptr<SimpleStorageService> &storage_service,
                  const std::shared_ptr<CompoundStorageService> &compound_storage_service,
                  const std::string &hostname,
                  const std::vector<storalloc::YamlJob>& jobs);

    protected:

        // Overridden method
        void processEventCompoundJobCompletion(std::shared_ptr<CompoundJobCompletedEvent>) override;

    private:
        // main() method of the WMS
        int main() override;

        const std::shared_ptr<ComputeService> compute_service;
        const std::shared_ptr<SimpleStorageService> storage_service;
        const std::shared_ptr<CompoundStorageService> compound_storage_service;
        const std::vector<storalloc::YamlJob>& jobs;

    };
}
#endif //CONTROLLER_H
