
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

    class PartialWriteCustomAction : public wrench::CustomAction {
    public:
        std::shared_ptr<wrench::DataFile> writtenFile;
        double writtenSize;

        double getWrittenSize() const {
            return this->writtenSize;
        }

        std::shared_ptr<wrench::DataFile> getFile() const {
            return this->writtenFile;
        };

        /**
         * @brief Constructor that takes it one extra parameter
         */
        PartialWriteCustomAction(const std::string &name,
                                 double ram,
                                 unsigned long num_cores,
                                 std::function<void(std::shared_ptr<wrench::ActionExecutor> action_executor)> lambda_execute,
                                 std::function<void(std::shared_ptr<wrench::ActionExecutor> action_executor)> lambda_terminate,
                                 std::shared_ptr<wrench::DataFile> writtenFile,
                                 uint64_t writtenSize) : CustomAction(name, ram, num_cores, std::move(lambda_execute), std::move(lambda_terminate)),
                                                         writtenFile(writtenFile), writtenSize(writtenSize) {}
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

        uint64_t getFailedJobCount() const { return this->failed_jobs_count; };

        virtual void extractSSSIO(const std::string &jobsFilename, const std::string &configVersion, const std::string &tag);

    protected:
        virtual int main() override;

        virtual void processEventTimer(std::shared_ptr<wrench::TimerEvent> timerEvent) override;

        virtual void processEventCompoundJobCompletion(std::shared_ptr<wrench::CompoundJobCompletedEvent>) override;

        virtual void processEventCompoundJobFailure(std::shared_ptr<wrench::CompoundJobFailedEvent>) override;

        virtual std::vector<storalloc::YamlJob> createPreloadJobs() const;

        virtual void preloadData(const std::map<std::string, storalloc::YamlJob> &job_map);

        virtual void submitJob(std::string jobID);

        virtual std::vector<std::shared_ptr<wrench::DataFile>> copyFromPermanent(std::shared_ptr<wrench::BareMetalComputeService> bare_metal,
                                                                                 std::shared_ptr<wrench::CompoundJob> copyJob,
                                                                                 std::map<std::string, std::map<std::string, std::string>> &service_specific_args,
                                                                                 uint64_t readBytes,
                                                                                 unsigned int nb_files, unsigned int max_nb_hosts);

        virtual void readFromTemporary(std::shared_ptr<wrench::BareMetalComputeService> bare_metal,
                                       std::shared_ptr<wrench::CompoundJob> readJob,
                                       std::string jobID,
                                       unsigned runID,
                                       std::map<std::string, std::map<std::string, std::string>> &service_specific_args,
                                       uint64_t readBytes,
                                       std::vector<std::shared_ptr<wrench::DataFile>> inputs,
                                       unsigned int max_nb_hosts = 1);

        virtual std::vector<std::shared_ptr<wrench::DataFile>> writeToTemporary(std::shared_ptr<wrench::BareMetalComputeService> bare_metal,
                                                                                std::shared_ptr<wrench::CompoundJob> writeJob,
                                                                                std::string jobID,
                                                                                unsigned runID,
                                                                                std::map<std::string, std::map<std::string, std::string>> &service_specific_args,
                                                                                uint64_t writtenBytes,
                                                                                unsigned int nb_files = 1, unsigned int max_nb_hosts = 1);

        virtual void copyToPermanent(std::shared_ptr<wrench::BareMetalComputeService> bare_metal,
                                     std::shared_ptr<wrench::CompoundJob> copyJob,
                                     std::map<std::string, std::map<std::string, std::string>> &service_specific_args,
                                     uint64_t writtenBytes,
                                     std::vector<std::shared_ptr<wrench::DataFile>> outputs,
                                     unsigned int max_nb_hosts = 1);

        virtual void cleanupInput(std::shared_ptr<wrench::BareMetalComputeService> bare_metal,
                                  std::shared_ptr<wrench::CompoundJob> cleanupJob,
                                  std::map<std::string, std::map<std::string, std::string>> &service_specific_args,
                                  std::vector<std::shared_ptr<wrench::DataFile>> inputs,
                                  bool cleanup_external = true);

        virtual void cleanupOutput(std::shared_ptr<wrench::BareMetalComputeService> bare_metal,
                                   std::shared_ptr<wrench::CompoundJob> cleanupJob,
                                   std::map<std::string, std::map<std::string, std::string>> &service_specific_args,
                                   std::vector<std::shared_ptr<wrench::DataFile>> outputs,
                                   bool cleanup_external = true);

        void processActions(YAML::Emitter &out_jobs,
                            const std::set<std::shared_ptr<wrench::Action>> &actions,
                            double &job_start_time,
                            const std::string &job_id);

        void processCompletedJob(const std::string &job_id);

        void pruneIONodes(std::map<std::string, unsigned long> &resources, unsigned int max_nb_hosts) const;

        std::vector<std::shared_ptr<wrench::DataFile>> createFileParts(uint64_t total_bytes, uint64_t nb_files, const std::string &prefix_name) const;

        unsigned int determineReadFileCount(double io_volume) const;

        unsigned int determineWriteFileCount(double io_volume) const;

        std::map<std::string, std::pair<YamlJob, std::vector<std::shared_ptr<wrench::CompoundJob>>>> compound_jobs = {};

        std::map<std::string, std::map<unsigned int, std::map<std::string, unsigned int>>> stripes_per_action; // map for to-level jobs, then runs inside jobs, then actions

        const std::shared_ptr<wrench::ComputeService> compute_service;

        const std::shared_ptr<wrench::SimpleStorageService> storage_service;

        const std::shared_ptr<wrench::CompoundStorageService> compound_storage_service;

        std::shared_ptr<storalloc::JobsStats> preload_header;

        const std::vector<storalloc::YamlJob> &jobs;

        std::map<std::string, storalloc::YamlJob> jobsWithPreload{}; // jobId, YamlJob

        std::map<std::string, std::vector<std::shared_ptr<wrench::DataFile>>> preloadedData;

        std::random_device rd;

        std::mt19937 gen; // (rd());

        std::uniform_real_distribution<float> uni_dis; //(0, 1.0);

        std::shared_ptr<wrench::JobManager> job_manager;

        double flopRate;

        std::shared_ptr<storalloc::Config> config;

        std::map<std::string, StorageServiceIOCounters> volume_per_storage_service_disk = {};

        YAML::Emitter completed_jobs;

        uint64_t failed_jobs_count = 0;
    };

} // namespace storalloc

#endif // CONTROLLER_H
