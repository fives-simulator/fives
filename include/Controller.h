
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

namespace fives {

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

    struct JobManagementStruct {
        std::shared_ptr<wrench::JobManager> jobManager;
        std::shared_ptr<wrench::ActionExecutionService> executionService;
        std::shared_ptr<wrench::BareMetalComputeService> bareMetalCS;
        std::map<std::string, std::map<std::string, std::string>> serviceSpecificArgs;
    };

    typedef std::map<uint32_t, std::vector<std::shared_ptr<wrench::CompoundJob>>> subjobsPerRunMap;

    typedef std::map<std::string, std::map<unsigned int, std::map<std::string, unsigned int>>> uglyTripleMap;

    struct SimulationJobTrace {
        const YamlJob *yamlJob;
        std::shared_ptr<wrench::CompoundJob> reservationJob;
        subjobsPerRunMap subJobs;
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
        Controller(
            std::shared_ptr<wrench::ComputeService> compute_service,
            std::shared_ptr<wrench::SimpleStorageService> storage_service,
            std::shared_ptr<wrench::CompoundStorageService> compound_storage_service,
            const std::string &hostname,
            std::map<std::string, YamlJob> jobs,
            std::shared_ptr<fives::Config> fives_config);

        subjobsPerRunMap getCompletedJobsById(const std::string &id);

        std::shared_ptr<wrench::CompoundJob> getReservationJobById(const std::string &id);

        virtual void processCompletedJobs(const std::string &jobsFilename, const std::string &config_version, const std::string &tag);

        uint64_t getFailedJobCount() const { return this->failed_jobs_count; };

        virtual void extractSSSIO(const std::string &jobsFilename, const std::string &configVersion, const std::string &tag);

    protected:
        virtual int main() override;

        virtual void processEventTimer(const std::shared_ptr<wrench::TimerEvent> &timerEvent) override;

        virtual void processEventCompoundJobCompletion(const std::shared_ptr<wrench::CompoundJobCompletedEvent> &event) override;

        virtual void processEventCompoundJobFailure(const std::shared_ptr<wrench::CompoundJobFailedEvent> &event) override;

        virtual void preloadData();

        virtual void registerJob(const std::string &jobId,
                                 uint32_t runId,
                                 std::shared_ptr<wrench::CompoundJob> job,
                                 bool child);

        virtual void addSleepJob(JobManagementStruct &jms,
                                 const std::string &jobID,
                                 const DarshanRecord &run);

        virtual void addReadJob(JobManagementStruct &jms,
                                const std::string &jobID,
                                const DarshanRecord &run);

        virtual void addWriteJob(JobManagementStruct &jms,
                                 const std::string &jobID,
                                 const DarshanRecord &run);

        virtual void submitJob(const std::string &jobID);

        virtual void setReadStripesPerHost(const std::vector<std::shared_ptr<wrench::DataFile>> &inputs,
                                           unsigned int participating_hosts_count,
                                           std::map<std::shared_ptr<wrench::DataFile>, unsigned int> &stripes_per_file,
                                           std::map<std::shared_ptr<wrench::DataFile>, std::vector<unsigned int>> &stripes_per_host_per_file,
                                           const std::string &jobName = "unknown");

        virtual void setWriteStripesPerHost(const std::vector<std::shared_ptr<wrench::DataFile>> &inputs,
                                            unsigned int participating_hosts_count,
                                            std::map<std::shared_ptr<wrench::DataFile>, unsigned int> &stripes_per_file,
                                            std::map<std::shared_ptr<wrench::DataFile>, std::vector<unsigned int>> &stripes_per_host_per_file,
                                            unsigned int stripe_count, // stripe count computed for each Darshan run
                                            const std::string &jobName = "unknown");

        virtual std::vector<std::shared_ptr<wrench::DataFile>> copyFromPermanent(JobManagementStruct &jms,
                                                                                 std::shared_ptr<wrench::CompoundJob> copyJob,
                                                                                 const DarshanRecord &run,
                                                                                 unsigned int max_nb_hosts = 1);

        virtual void readFromTemporary(JobManagementStruct &jms,
                                       std::shared_ptr<wrench::CompoundJob> readJob,
                                       const DarshanRecord &run,
                                       std::map<std::string, unsigned int> &current_stripes_per_action,
                                       std::vector<std::shared_ptr<wrench::DataFile>> inputs,
                                       unsigned int max_nb_hosts = 1);

        virtual std::vector<std::shared_ptr<wrench::DataFile>> writeToTemporary(JobManagementStruct &jms,
                                                                                std::shared_ptr<wrench::CompoundJob> writeJob,
                                                                                const DarshanRecord &run,
                                                                                std::map<std::string, unsigned int> &current_stripes_per_action,
                                                                                unsigned int max_nb_hosts = 1);

        virtual void copyToPermanent(JobManagementStruct &jms,
                                     std::shared_ptr<wrench::CompoundJob> copyJob,
                                     const DarshanRecord &run,
                                     std::vector<std::shared_ptr<wrench::DataFile>> outputs,
                                     unsigned int max_nb_hosts = 1);

        void processActions(YAML::Emitter &out_jobs,
                            const std::set<std::shared_ptr<wrench::Action>> &actions,
                            double &job_start_time,
                            const std::string &job_id,
                            uint32_t run_id);

        void processCompletedJob(const std::string &job_id);

        void pruneIONodes(std::map<std::string, unsigned long> &resources, unsigned int max_nb_hosts) const;

        std::vector<std::shared_ptr<wrench::DataFile>> createFileParts(uint64_t total_bytes, uint64_t nb_files, const std::string &prefix_name) const;

        unsigned int getReadNodeCount(unsigned int max_nodes, double cumul_read_bw, unsigned int stripe_count) const;

        unsigned int getWriteNodeCount(unsigned int max_nodes, double cumul_read_bw, unsigned int stripe_count) const;

        unsigned int getReadStripeCount(double cumul_read_bw) const;

        unsigned int getWriteStripeCount(double cumul_write_bw) const;

        unsigned int getReadFileCount(unsigned int stripe_count) const;

        unsigned int getWriteFileCount(unsigned int stripe_count) const;

        // Map to accumulate every simulation job created
        // Jobs are removed from the map once processed (after they completed) or in case of failure.
        std::map<std::string, SimulationJobTrace> sim_jobs = {};

        uglyTripleMap stripes_per_action; // map for to-level jobs, then runs inside jobs, then actions

        const std::shared_ptr<wrench::ComputeService> compute_service;

        const std::shared_ptr<wrench::SimpleStorageService> storage_service;

        const std::shared_ptr<wrench::CompoundStorageService> compound_storage_service;

        const std::map<std::string, fives::YamlJob> jobs;

        std::map<std::string, std::vector<std::shared_ptr<wrench::DataFile>>> preloadedData;

        std::shared_ptr<wrench::JobManager> job_manager;

        double flopRate;

        std::shared_ptr<fives::Config> config;

        std::map<std::string, StorageServiceIOCounters> volume_per_storage_service_disk = {};

        // YAML object that accumulates processed informations about (successfully) completed jobs
        YAML::Emitter completed_jobs;

        uint64_t failed_jobs_count = 0;

        unsigned int ost_count;
    };

} // namespace fives

#endif // CONTROLLER_H
