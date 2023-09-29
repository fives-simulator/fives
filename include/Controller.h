
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

    struct DiskIOCounters {
        double total_capacity;
        double total_capacity_used;
        int total_allocation_count;
    };

    struct StorageServiceIOCounters {
        std::string service_name;
        double total_capacity_used;
        int total_allocation_count;
        std::map<std::string, DiskIOCounters> disks;
    };

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

        std::vector<std::shared_ptr<wrench::CompoundJob>> getCompletedJobsById(std::string id);

        virtual void processCompletedJobs(const std::string &jobsFilename, const std::string &config_version, const std::string &tag);

        virtual bool actionsAllCompleted();

        virtual void extractSSSIO(const std::string &jobsFilename, const std::string &configVersion, const std::string &tag);

    protected:
        virtual int main() override;

        virtual void processEventTimer(std::shared_ptr<wrench::TimerEvent> timerEvent) override;

        virtual void processEventCompoundJobCompletion(std::shared_ptr<wrench::CompoundJobCompletedEvent>) override;

        virtual void processEventCompoundJobFailure(std::shared_ptr<wrench::CompoundJobFailedEvent>) override;

        virtual std::vector<storalloc::YamlJob> createPreloadJobs() const;

        virtual void submitJob(std::string jobID);

        virtual std::vector<std::shared_ptr<wrench::DataFile>> copyFromPermanent(std::shared_ptr<wrench::ActionExecutor> action_executor,
                                                                                 std::shared_ptr<wrench::JobManager> internalJobManager,
                                                                                 std::pair<YamlJob, std::vector<std::shared_ptr<wrench::CompoundJob>>> &jobPair,
                                                                                 unsigned int nb_hosts = 1);

        virtual void readFromTemporary(const std::shared_ptr<wrench::ActionExecutor> &action_executor,
                                       const std::shared_ptr<wrench::JobManager> &internalJobManager,
                                       std::pair<YamlJob, std::vector<std::shared_ptr<wrench::CompoundJob>>> &jobPair,
                                       std::vector<std::shared_ptr<wrench::DataFile>> inputs);

        virtual void compute(const std::shared_ptr<wrench::ActionExecutor> &action_executor,
                             const std::shared_ptr<wrench::JobManager> &internalJobManager,
                             std::pair<YamlJob, std::vector<std::shared_ptr<wrench::CompoundJob>>> &jobPair);

        virtual std::vector<std::shared_ptr<wrench::DataFile>> writeToTemporary(const std::shared_ptr<wrench::ActionExecutor> &action_executor,
                                                                                const std::shared_ptr<wrench::JobManager> &internalJobManager,
                                                                                std::pair<YamlJob, std::vector<std::shared_ptr<wrench::CompoundJob>>> &jobPair,
                                                                                unsigned int nb_hosts = 1);

        virtual void copyToPermanent(const std::shared_ptr<wrench::ActionExecutor> &action_executor,
                                     const std::shared_ptr<wrench::JobManager> &internalJobManager,
                                     std::pair<YamlJob, std::vector<std::shared_ptr<wrench::CompoundJob>>> &jobPair,
                                     std::vector<std::shared_ptr<wrench::DataFile>> outputs);

        virtual void cleanupInput(const std::shared_ptr<wrench::ActionExecutor> &action_executor,
                                  const std::shared_ptr<wrench::JobManager> &internalJobManager,
                                  std::pair<YamlJob, std::vector<std::shared_ptr<wrench::CompoundJob>>> &jobPair,
                                  std::vector<std::shared_ptr<wrench::DataFile>> inputs);

        virtual void cleanupOutput(const std::shared_ptr<wrench::ActionExecutor> &action_executor,
                                   const std::shared_ptr<wrench::JobManager> &internalJobManager,
                                   std::pair<YamlJob, std::vector<std::shared_ptr<wrench::CompoundJob>>> &jobPair,
                                   std::vector<std::shared_ptr<wrench::DataFile>> outputs);

        void processActions(YAML::Emitter &out_jobs, YAML::Emitter &out_actions,
                            const std::set<std::shared_ptr<wrench::Action>> &actions,
                            double &job_start_time);

        std::map<std::string, std::pair<YamlJob, std::vector<std::shared_ptr<wrench::CompoundJob>>>> compound_jobs = {};

        const std::shared_ptr<wrench::ComputeService> compute_service;

        const std::shared_ptr<wrench::SimpleStorageService> storage_service;

        const std::shared_ptr<wrench::CompoundStorageService> compound_storage_service;

        std::shared_ptr<storalloc::JobsStats> preload_header;

        const std::vector<storalloc::YamlJob> &jobs;

        std::map<std::string, storalloc::YamlJob> jobsWithPreload{}; // jobId, YamlJob

        std::shared_ptr<wrench::JobManager> job_manager;

        double flopRate;

        std::shared_ptr<storalloc::Config> config;

        std::map<std::string, StorageServiceIOCounters> volume_per_storage_service_disk = {};
    };

} // namespace storalloc

#endif // CONTROLLER_H
