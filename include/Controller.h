
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

#include "ConfigDefinition.h"
#include "JobDefinition.h"

#include <wrench-dev.h>

namespace storalloc {

    /**
     *  @brief A Workflow Management System (WMS) implementation
     */
    class Controller : public wrench::ExecutionController {

    public:
        // Constructor
        Controller(
            const std::shared_ptr<wrench::ComputeService> &compute_service,
            const std::shared_ptr<wrench::SimpleStorageService> &storage_service,
            const std::shared_ptr<wrench::CompoundStorageService> &compound_storage_service,
            const std::string &hostname,
            const std::shared_ptr<storalloc::JobsStats> &header,
            const std::vector<YamlJob> &jobs,
            const std::shared_ptr<storalloc::Config> &storalloc_config);

        std::shared_ptr<wrench::CompoundJob> getCompletedJobById(std::string id);

        virtual void processCompletedJobs(const std::string &jobsFilename, const std::string &config_version, const std::string &tag);

        virtual bool actionsAllCompleted();

        virtual void extractSSSIO(const std::string &jobsFilename, const std::string &configVersion, const std::string &tag);

    protected:
        virtual int main() override;

        virtual void processEventTimer(std::shared_ptr<wrench::TimerEvent> timerEvent) override;

        virtual void processEventCompoundJobCompletion(std::shared_ptr<wrench::CompoundJobCompletedEvent>) override;

        virtual void processEventCompoundJobFailure(std::shared_ptr<wrench::CompoundJobFailedEvent>) override;

        virtual std::vector<storalloc::YamlJob> createPreloadJobs() const;

        virtual void submitJob();

        virtual std::vector<std::shared_ptr<wrench::DataFile>> copyFromPermanent(unsigned int nb_hosts = 1);

        virtual void readFromTemporary(std::vector<std::shared_ptr<wrench::DataFile>> inputs);

        virtual void compute();

        virtual std::vector<std::shared_ptr<wrench::DataFile>> writeToTemporary(unsigned int nb_hosts = 1);

        virtual void copyToPermanent(std::vector<std::shared_ptr<wrench::DataFile>> outputs);

        virtual void cleanupInput(std::vector<std::shared_ptr<wrench::DataFile>> inputs);

        virtual void cleanupOutput(std::vector<std::shared_ptr<wrench::DataFile>> outputs);

        // Temporary placeholder for the yaml data of the job being configured
        YamlJob current_yaml_job;
        // Temporary placeholder for job being configured
        std::shared_ptr<wrench::CompoundJob> current_job;

        /* Tree map of dependencies : each inner vector holds 1..n actions which all share the same dependencies.
         * Action(s) from one of the inner vectors depends on action(s) from the previous inner vector.
         */
        std::vector<std::vector<std::shared_ptr<wrench::Action>>> actions = {};

        std::map<std::string, std::pair<YamlJob, std::shared_ptr<wrench::CompoundJob>>> compound_jobs = {};

        const std::shared_ptr<wrench::ComputeService> compute_service;

        const std::shared_ptr<wrench::SimpleStorageService> storage_service;

        const std::shared_ptr<wrench::CompoundStorageService> compound_storage_service;

        std::shared_ptr<storalloc::JobsStats> preload_header;

        const std::vector<storalloc::YamlJob> &jobs;

        std::shared_ptr<wrench::JobManager> job_manager;

        double flopRate;

        std::shared_ptr<storalloc::Config> config;
    };

} // namespace storalloc

#endif // CONTROLLER_H
