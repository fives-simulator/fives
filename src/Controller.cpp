/**
 * Copyright (c) 2017-2021. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

/**
 ** An execution controller to execute a workflow
 **/

#define GFLOP (1000.0 * 1000.0 * 1000.0)
#define MBYTE (1000.0 * 1000.0)
#define GBYTE (1000.0 * 1000.0 * 1000.0)

#include "Controller.h"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <random>
#include <utility>
#include <wrench/services/helper_services/action_execution_service/ActionExecutionService.h>
#include <wrench/util/UnitParser.h>

#include "yaml-cpp/yaml.h"

WRENCH_LOG_CATEGORY(storalloc_controller, "Log category for storalloc controller");

namespace storalloc {

    struct StripeParams {
        uint64_t partial_io_size;
        uint64_t nb_stripes;
    };

    /**
     * @brief Constructor
     *
     * @param compute_service: a compute services available to run actions
     * @param storage_services: a set of storage services available to store files
     * @param hostname: the name of the host on which to start the WMS
     */
    Controller::Controller(const std::shared_ptr<wrench::ComputeService> &compute_service,
                           const std::shared_ptr<wrench::SimpleStorageService> &storage_service,
                           const std::shared_ptr<wrench::CompoundStorageService> &compound_storage_service,
                           const std::string &hostname,
                           const std::shared_ptr<storalloc::JobsStats> &header,
                           const std::vector<storalloc::YamlJob> &jobs,
                           const std::shared_ptr<storalloc::Config> &storalloc_config) : ExecutionController(hostname, "controller"),
                                                                                         compute_service(compute_service),
                                                                                         storage_service(storage_service),
                                                                                         compound_storage_service(compound_storage_service),
                                                                                         preload_header(header),
                                                                                         jobs(jobs),
                                                                                         config(storalloc_config) {}

    /**
     * @brief main method of the Controller
     *
     * @return 0 on completion
     *
     * @throw std::runtime_error
     */
    int Controller::main() {

        wrench::TerminalOutput::setThisProcessLoggingColor(wrench::TerminalOutput::COLOR_GREEN);
        WRENCH_INFO("Controller : %s jobs to create and submit", std::to_string(jobs.size()).c_str());

        this->completed_jobs << YAML::BeginSeq;

        // Forced seed for rand (used later on to decide whether or not to cleanup files after write / copy operations of jobs)
        std::srand(1);

        this->flopRate = this->compute_service->getCoreFlopRate().begin()->second; // flop rate from first compute node

        this->job_manager = this->createJobManager();

        // Simulate 'fake' load before the actual dataset is used
        auto preload_jobs = this->createPreloadJobs();

        // Concat fake preload jobs with dataset
        for (const auto &job : preload_jobs) {
            this->jobsWithPreload[job.id] = job;
        }
        for (const auto &job : this->jobs) {
            this->jobsWithPreload[job.id] = job;
        }
        preload_jobs.clear(); // no need for the extra memory footprint

        this->preloadData(this->jobsWithPreload);

        auto total_events = 0;
        auto processed_events = 0;
        for (const auto &yaml_entry : this->jobsWithPreload) {

            WRENCH_INFO("[%s] Setting %ds timer", yaml_entry.second.id.c_str(), yaml_entry.second.sleepSimulationSeconds);
            double timer_off_date = wrench::Simulation::getCurrentSimulatedDate() + yaml_entry.second.sleepSimulationSeconds;
            this->setTimer(timer_off_date, yaml_entry.first);
            total_events += 1;

            auto nextSubmission = false;
            while (!nextSubmission) {
                WRENCH_INFO("[%s] Now waiting for next event...", yaml_entry.second.id.c_str());
                auto event = this->waitForNextEvent();
                processed_events += 1;

                if (auto timer_event = std::dynamic_pointer_cast<wrench::TimerEvent>(event)) {
                    WRENCH_INFO("[During loop for %s] Timer event received", yaml_entry.second.id.c_str());
                    this->processEventTimer(timer_event);
                    total_events += 1; // job that was just submitted
                    nextSubmission = true;
                    WRENCH_INFO("[During loop for %s] Timer event processed", yaml_entry.second.id.c_str());
                } else if (auto completion_event = std::dynamic_pointer_cast<wrench::CompoundJobCompletedEvent>(event)) {
                    WRENCH_INFO("[During loop for %s] Job completed event received ()", yaml_entry.second.id.c_str());
                    this->processEventCompoundJobCompletion(completion_event);
                } else if (auto failure_event = std::dynamic_pointer_cast<wrench::CompoundJobFailedEvent>(event)) {
                    WRENCH_INFO("[During loop for %s] Job failure event received ()", yaml_entry.second.id.c_str());
                    this->processEventCompoundJobFailure(failure_event);
                } else {
                    throw std::runtime_error("Unexpected Controller Event : " + event->toString());
                }
            }
        }

        // Process any remaining events (number unknown, depends on jobs runtime vs simulation sleep between jobs)
        while (processed_events < total_events) {
            this->waitForAndProcessNextEvent();
            WRENCH_INFO("[Outside of submission loop] Processing new event...");
            processed_events += 1;
        }

        this->completed_jobs << YAML::EndSeq;
        WRENCH_INFO("/// Storalloc Controller execution complete");
        return 0;
    }

    /**
     * @brief Find compound jobs by id in the internal job list.
     *        Note that every compound job in this map is actually a list of jobs, always starting
     *        with the parent job and then complete by sub-jobs started from inside the customAction of the
     *        parent job.
     * @param id String id of the parent job to look for
     * @return Vector of shared_ptr on Compound jobs, including both parent job and all sub-jobs
     */
    std::vector<std::shared_ptr<wrench::CompoundJob>> Controller::getCompletedJobsById(std::string id) {

        auto job_pair = this->compound_jobs.find(id);
        if (job_pair == this->compound_jobs.end()) {
            return {};
        }

        return job_pair->second.second;
    }

    /**
     * @brief Check all actions from all jobs for any failed action
     * @return True if no action has failed, false otherwise
     */
    bool Controller::actionsAllCompleted() {

        bool success = true;

        for (const auto &job_list : this->compound_jobs) {
            for (const auto &job : job_list.second.second) {
                for (const auto &a : job->getActions()) {
                    if (a->getState() != wrench::Action::State::COMPLETED) {
                        wrench::TerminalOutput::setThisProcessLoggingColor(wrench::TerminalOutput::COLOR_RED);
                        WRENCH_WARN("Error for action %s: %.2fs - %.2fs", a->getName().c_str(), a->getStartDate(), a->getEndDate());
                        std::cout << "Failed action for job : " << job_list.first << " : " << a->getName() << std::endl;
                        if (a->getFailureCause()) {
                            WRENCH_WARN("-> Failure cause: %s", a->getFailureCause()->toString().c_str());
                        }
                        wrench::TerminalOutput::setThisProcessLoggingColor(wrench::TerminalOutput::COLOR_GREEN);
                        success = false;
                    }
                }
            }
        }

        return success;
    }

    void Controller::preloadData(const std::map<std::string, storalloc::YamlJob> &job_map) {

        for (const auto &map_entry : job_map) {
            auto job = map_entry.second;
            for (const auto &run : job.runs) {
                if ((run.readBytes > 0) and (run.readBytes >= this->config->stor.read_bytes_preload_thres)) {

                    auto nb_read_files = determineReadFileCount(job.cumulReadBW, run.nprocs);

                    auto prefix = "pInputFile_id" + job.id + "_exec" + std::to_string(run.id);
                    WRENCH_DEBUG("preloading file prefix %s on external storage system", prefix.c_str());
                    auto read_files = this->createFileParts(run.readBytes, nb_read_files, prefix);

                    for (const auto &read_file : read_files) {
                        unsigned int local_stripe_count = this->determineReadStripeCount(job.cumulReadBW);
                        auto file_stripes = this->compound_storage_service->lookupOrDesignateStorageService(wrench::FileLocation::LOCATION(this->compound_storage_service, read_file), local_stripe_count);
                        for (const auto &stripe : file_stripes) {
                            wrench::StorageService::createFileAtLocation(stripe);
                        }
                    }

                    this->preloadedData[job.id + "_" + std::to_string(run.id)] = read_files;
                }
            }
        }
    }

    std::vector<storalloc::YamlJob> Controller::createPreloadJobs() const {

        // How many preload jobs to create (20% of the total number of jobs):
        unsigned int preloadJobsCount = std::ceil(this->preload_header->job_count * this->config->preload_percent);
        if (preloadJobsCount == 0) {
            WRENCH_INFO("No preloads jobs created (see configuration 'general.preload_percent' to adjust)");
            return {};
        }
        WRENCH_INFO("Preparing %u preload jobs (see configuration 'general.preload_percent' to adjust)", preloadJobsCount);

        std::random_device rd;
        std::mt19937 gen(rd());

        // Runtimes :
        std::vector<int> rand_runtimes_s;
        std::lognormal_distribution<> dr(
            std::log(this->preload_header->mean_runtime_s / std::sqrt(this->preload_header->var_runtime_s / std::pow(this->preload_header->mean_runtime_s, 2) + 1)),
            std::sqrt(std::log((this->preload_header->var_runtime_s / std::pow(this->preload_header->mean_runtime_s, 2)) + 1)));
        while (rand_runtimes_s.size() != preloadJobsCount) {
            auto val = std::floor(dr(gen));
            if (val <= this->preload_header->max_runtime_s) {
                rand_runtimes_s.push_back(val);
            }
        }

        // Interval between jobs :
        std::vector<int> rand_intervals_s;
        std::lognormal_distribution<> di(
            std::log(this->preload_header->mean_interval_s / std::sqrt(this->preload_header->var_interval_s / std::pow(this->preload_header->mean_interval_s, 2) + 1)),
            std::sqrt(std::log((this->preload_header->var_interval_s / std::pow(this->preload_header->mean_interval_s, 2)) + 1)));
        while (rand_intervals_s.size() != preloadJobsCount) {
            auto val = std::floor(di(gen));
            if (val <= this->preload_header->max_interval_s) {
                rand_intervals_s.push_back(val);
            }
        }

        // Nodes used:
        std::vector<int> rand_nodes_count;
        std::lognormal_distribution<> dn(
            std::log(this->preload_header->mean_nodes_used / std::sqrt(this->preload_header->var_nodes_used / std::pow(this->preload_header->mean_nodes_used, 2) + 1)),
            std::sqrt(std::log((this->preload_header->var_nodes_used / std::pow(this->preload_header->mean_nodes_used, 2)) + 1)));
        while (rand_nodes_count.size() != preloadJobsCount) {
            auto val = std::floor(dn(gen));
            if (val <= this->preload_header->max_nodes_used) {
                rand_nodes_count.push_back(val);
            }
        }

        // Bytes READ
        std::vector<double> rand_read_tbytes;
        std::lognormal_distribution<> drb(
            std::log(this->preload_header->mean_read_tbytes / std::sqrt(this->preload_header->var_read_tbytes / std::pow(this->preload_header->mean_read_tbytes, 2) + 1)),
            std::sqrt(std::log((this->preload_header->var_read_tbytes / std::pow(this->preload_header->mean_read_tbytes, 2)) + 1)));
        while (rand_read_tbytes.size() != preloadJobsCount) {
            auto val = drb(gen);
            if (val <= this->preload_header->max_read_tbytes) {
                rand_read_tbytes.push_back(val);
            }
        }

        // Bytes WRITTEN
        std::vector<double> rand_written_tbytes;
        std::lognormal_distribution<> dwb(
            std::log(this->preload_header->mean_written_tbytes / std::sqrt(this->preload_header->var_written_tbytes / std::pow(this->preload_header->mean_written_tbytes, 2) + 1)),
            std::sqrt(std::log((this->preload_header->var_written_tbytes / std::pow(this->preload_header->mean_written_tbytes, 2)) + 1)));
        while (rand_written_tbytes.size() != preloadJobsCount) {
            auto val = dwb(gen);
            if (val <= this->preload_header->max_written_tbytes) {
                rand_written_tbytes.push_back(val);
            }
        }

        auto cores_per_node = this->compute_service->getPerHostNumCores().begin()->second;
        std::vector<storalloc::YamlJob> preload_jobs;
        unsigned int i = 0;
        while (i < preloadJobsCount) {
            storalloc::YamlJob job = {};

            uint64_t read_bytes = rand_read_tbytes[i] * 1'000'000'000'000;
            uint64_t written_bytes = rand_written_tbytes[i] * 1'000'000'000'000;

            job.coreHoursReq = 0;                    // not used
            job.coreHoursUsed = 0;                   // not used
            job.endTime = "NA";                      // not used
            job.id = "preload_" + std::to_string(i); // used to filter out jobs in the end.
            job.metaTimeSeconds = 0;                 // not used
            job.nodesUsed = rand_nodes_count[i];
            job.readBytes = read_bytes; // converting back to bytes from terabytes
            job.readTimeSeconds = 0;    // not used
            job.runtimeSeconds = rand_runtimes_s[i] + written_bytes / 100000 + read_bytes / 200000;
            job.sleepSimulationSeconds = rand_intervals_s[i]; // not used
            job.startTime = "NA";                             // not used
            job.submissionTime = "NA";                        // not used
            job.waitingTimeSeconds = 0;                       // not used
            job.walltimeSeconds = rand_runtimes_s[i] * 2 + written_bytes / 100000 + read_bytes / 200000;
            job.writeTimeSeconds = 0;         // not used
            job.writtenBytes = written_bytes; // converting back to bytes from terabytes
            job.coresUsed = rand_nodes_count[i] * cores_per_node;

            job.runs = std::vector<storalloc::DarshanRecord>();
            DarshanRecord rec = {
                .id = 1,
                .nprocs = job.coresUsed,
                .readBytes = read_bytes,
                .writtenBytes = written_bytes,
                .runtime = job.runtimeSeconds - 5,
                .dStartTime = 0,
                .dEndTime = 10,
                .sleepDelay = 1,
            };
            job.runs.push_back(rec);

            preload_jobs.push_back(job);
            i++;
        }

        WRENCH_INFO("%u preload jobs created", preloadJobsCount);

        return preload_jobs;
    }

    unsigned int Controller::determineReadStripeCount(double cumul_read_bw) const {

        unsigned int local_stripe_count = this->config->lustre.stripe_count;
        if (this->config->lustre.stripe_count_high_thresh_read && cumul_read_bw >= this->config->lustre.stripe_count_high_thresh_read) {
            auto ratio = cumul_read_bw / this->config->lustre.stripe_count_high_thresh_read;
            local_stripe_count = std::ceil(this->config->lustre.stripe_count_high_read_add * ratio);
            // local_stripe_count = this->config->lustre.stripe_count + this->config->lustre.stripe_count_high_read_add;
        }

        return local_stripe_count;
    }

    unsigned int Controller::determineWriteStripeCount(double cumul_write_bw) const {

        unsigned int local_stripe_count = this->config->lustre.stripe_count;
        if (this->config->lustre.stripe_count_high_thresh_write && cumul_write_bw >= this->config->lustre.stripe_count_high_thresh_write) {
            auto ratio = cumul_write_bw / this->config->lustre.stripe_count_high_thresh_write;
            local_stripe_count = std::ceil(this->config->lustre.stripe_count_high_write_add * ratio);
            // local_stripe_count = this->config->lustre.stripe_count + this->config->lustre.stripe_count_high_write_add;
        }

        return local_stripe_count;
    }

    unsigned int Controller::determineReadFileCount(double cumul_read_bw, unsigned int run_nprocs) const {

        // unsigned int nb_files = std::max(static_cast<unsigned int>((cumul_read_bw * run_nprocs) / this->config->stor.nb_files_per_read), 1u);

        unsigned int nb_files = 1;

        unsigned int local_stripe_count = this->determineReadStripeCount(cumul_read_bw);
        nb_files = this->config->stor.nb_files_per_read * local_stripe_count;
        nb_files = std::max(nb_files, 1u);

        return nb_files;
    }

    unsigned int Controller::determineWriteFileCount(double cumul_write_bw, unsigned int run_nprocs) const {

        // unsigned int nb_files = std::max(static_cast<unsigned int>((cumul_write_bw * run_nprocs) / this->config->stor.nb_files_per_write), 1u);

        unsigned int nb_files = 1;

        unsigned int local_stripe_count = this->determineWriteStripeCount(cumul_write_bw);
        nb_files = this->config->stor.nb_files_per_write * local_stripe_count;
        nb_files = std::max(nb_files, 1u);

        return nb_files;
    }

    void Controller::submitJob(std::string jobID) {

        auto yJob = this->jobsWithPreload[jobID];
        auto parentJob = this->job_manager->createCompoundJob(yJob.id);
        // Save job for future analysis (note : we'll need to find sub jobs one by one by ID)
        this->compound_jobs[yJob.id] = std::make_pair(yJob, std::vector<std::shared_ptr<wrench::CompoundJob>>());
        this->compound_jobs[yJob.id].second.push_back(parentJob);
        WRENCH_INFO("[%s] Preparing parent job for submission", yJob.id.c_str());

        this->gen = std::mt19937(this->rd());
        this->uni_dis = std::uniform_real_distribution<float>(0.0, 1.0);

        /* Make sure the reservation lasts for as long as the real one, no matter what happens inside.
         * This is because some jobs have internal scripts or interactive behaviours that make the
         * reservation stay alive even though nothing much might be happening on the nodes
         */
        parentJob->addSleepAction("fullRuntimeSleep", static_cast<double>(std::min(yJob.runtimeSeconds, yJob.walltimeSeconds - 1)));

        // In // add the actual workload taken from Darshan traces (there might not be any)
        if (yJob.runs.size() != 0) {

            parentJob->addCustomAction(
                "parentJob" + yJob.id,
                0, 0, // RAM & num cores
                [this, jobID](const std::shared_ptr<wrench::ActionExecutor> &action_executor) {
                    // Internal job manager used to create jobs for all Darshan records

                    // BareMetalComputeService gives access to the list of resources for this reservation, used by actions which require
                    // some service_specific_arguments.
                    auto internalJobManager = action_executor->createJobManager();
                    auto actionExecutorService = action_executor->getActionExecutionService();
                    auto bare_metal = std::dynamic_pointer_cast<wrench::BareMetalComputeService>(actionExecutorService->getParentService());

                    std::map<std::string, std::map<std::string, std::string>> service_specific_args;

                    // Create and keep trace of all exec_jobs for each of the Darshan records / monitored runs of an application
                    // inside the reservation  -- DO NOT SUBMIT JOBS IN THIS LOOP
                    std::map<unsigned int, std::vector<std::shared_ptr<wrench::CompoundJob>>> exec_jobs;
                    this->stripes_per_action[jobID] = std::map<unsigned int, std::map<std::string, unsigned int>>(); // empty map for new job

                    for (const auto &run : this->compound_jobs[jobID].first.runs) {

                        WRENCH_INFO("submitJob : bare_metal compute service for job %s :: %s", jobID.c_str(), bare_metal->getName().c_str());
                        this->stripes_per_action[jobID][run.id] = std::map<std::string, unsigned int>();

                        exec_jobs[run.id] = std::vector<std::shared_ptr<wrench::CompoundJob>>();

                        // NUMBER OF READ FILES and WRITE FILES
                        auto nb_read_files = this->determineReadFileCount(this->compound_jobs[jobID].first.cumulReadBW, run.nprocs);
                        auto nb_write_files = this->determineWriteFileCount(this->compound_jobs[jobID].first.cumulWriteBW, run.nprocs);

                        // 1. Determine how many nodes (at most) may be used for the IO operations of this exec job
                        // (depending on the size of the current MPI Communicator given by nprocs)
                        float nprocs_nodes = std::ceil(static_cast<double>(run.nprocs) / this->config->compute.core_count); // How many nodes, at most, may be used? (and at least one)
                        nprocs_nodes = max(nprocs_nodes, 1.0F);

                        if (run.sleepDelay != 0) {
                            auto sleepJob = internalJobManager->createCompoundJob("sleep_id" + jobID + "_exec" + std::to_string(run.id));
                            sleepJob->addSleepAction("sleep", run.sleepDelay);
                            exec_jobs[run.id].push_back(sleepJob);
                            service_specific_args[sleepJob->getName()] = std::map<std::string, std::string>();
                            service_specific_args[sleepJob->getName()]["sleep"] = {};
                        }

                        // 2. Create copy / read / write / copy jobs
                        // bool cleanup_external_read = true;
                        // bool cleanup_external_write = true;

                        // 2.1 READ
                        std::vector<std::shared_ptr<wrench::DataFile>> input_files;
                        if (run.readBytes != 0) {

                            float nb_nodes_read = std::ceil((this->determineReadStripeCount(this->compound_jobs[jobID].first.cumulReadBW) * this->config->stor.node_templates.begin()->second.disks[0].tpl.read_bw) / (this->compound_jobs[jobID].first.cumulReadBW / 1e6));
                            nb_nodes_read = max(nb_nodes_read, 1.0F);         // Ensure nb_nodes_read >= 1
                            nb_nodes_read = min(nb_nodes_read, nprocs_nodes); // Ensure nb_node_read <= nprocs_nodes
                            WRENCH_DEBUG(" - [%s-exec%u] : %f nodes will be doing copy/read IOs", jobID.c_str(), run.id, nb_nodes_read);

                            if (this->preloadedData.find(jobID + "_" + std::to_string(run.id)) == this->preloadedData.end()) {
                                // Copy job before the read
                                auto copyJob = internalJobManager->createCompoundJob("stagingCopy_id" + jobID + "_exec" + std::to_string(run.id));
                                input_files = this->copyFromPermanent(bare_metal, copyJob, service_specific_args, run.readBytes, nb_read_files, nb_nodes_read);
                                if (exec_jobs[run.id].size() != 0) {
                                    exec_jobs[run.id].back()->addChildJob(copyJob);
                                }
                                exec_jobs[run.id].push_back(copyJob);
                            } else {
                                // No need for copy jobs, files have been created already
                                input_files = this->preloadedData[jobID + "_" + std::to_string(run.id)];
                                // cleanup_external_read = false;
                            }

                            // Read job
                            auto readJob = internalJobManager->createCompoundJob("readFiles_id" + jobID + "_exec" + std::to_string(run.id));
                            this->readFromTemporary(bare_metal, readJob, jobID, run.id, service_specific_args, run.readBytes, input_files, nb_nodes_read);
                            if (exec_jobs[run.id].size() != 0) {
                                exec_jobs[run.id].back()->addChildJob(readJob); // Add dependencies between jobs (but inside a job, actions are //)
                            }
                            exec_jobs[run.id].push_back(readJob);
                        }

                        // 2.2 WRITE
                        std::vector<std::shared_ptr<wrench::DataFile>> output_data;
                        if (run.writtenBytes != 0) {

                            float nb_nodes_write = std::ceil((this->determineWriteStripeCount(this->compound_jobs[jobID].first.cumulWriteBW) * this->config->stor.node_templates.begin()->second.disks[0].tpl.write_bw) / (this->compound_jobs[jobID].first.cumulWriteBW / 1e6));
                            nb_nodes_write = max(nb_nodes_write, 1.0F);
                            nb_nodes_write = min(nb_nodes_write, nprocs_nodes);
                            WRENCH_DEBUG(" - [%s-exec%u] : %f nodes will be doing write/copy IOs", jobID.c_str(), run.id, nb_nodes_write);

                            auto writeJob = internalJobManager->createCompoundJob("writeFiles_id" + jobID + "_exec" + std::to_string(run.id));
                            output_data = this->writeToTemporary(bare_metal, writeJob, jobID, run.id, service_specific_args, run.writtenBytes, nb_write_files, nb_nodes_write);

                            if (exec_jobs[run.id].size() != 0) {
                                exec_jobs[run.id].back()->addChildJob(writeJob); // Add dependencies between jobs (but inside a job, actions are //)
                            }
                            exec_jobs[run.id].push_back(writeJob);

                            if (run.writtenBytes <= this->config->stor.write_bytes_copy_thres) {
                                auto copyJob = internalJobManager->createCompoundJob("archiveCopy_id" + jobID + "_exec" + std::to_string(run.id));
                                this->copyToPermanent(bare_metal, copyJob, service_specific_args, run.writtenBytes, output_data, nb_nodes_write);
                                exec_jobs[run.id].back()->addChildJob(copyJob);
                                exec_jobs[run.id].push_back(copyJob);
                                // } else {
                                //    cleanup_external_write = false;
                            }
                        }

                        // 2.3 CLEANUP
                        /*
                        if (run.readBytes != 0) {
                            if (this->config->stor.cleanup_threshold <= this->uni_dis(this->gen)) {
                                auto cleanupJob = internalJobManager->createCompoundJob("cleanupInput_id" + jobID + "_exec" + std::to_string(run.id));
                                this->cleanupInput(bare_metal, cleanupJob, service_specific_args, input_files, cleanup_external_read);
                                exec_jobs[run.id].back()->addChildJob(cleanupJob);
                                exec_jobs[run.id].push_back(cleanupJob);
                            }
                        }
                        if (run.writtenBytes != 0) {
                            if (this->config->stor.cleanup_threshold <= this->uni_dis(this->gen)) {
                                auto cleanupJob = internalJobManager->createCompoundJob("cleanupOutput_id" + jobID + "_exec" + std::to_string(run.id));
                                this->cleanupOutput(bare_metal, cleanupJob, service_specific_args, output_data, cleanup_external_write);
                                exec_jobs[run.id].back()->addChildJob(cleanupJob);
                                exec_jobs[run.id].push_back(cleanupJob);
                            }
                        }
                        */

                        // Keep trace of all jobs for later analysis

                        for (const auto &job : exec_jobs[run.id]) {
                            if (job->getName().substr(0, 5) == "sleep") {
                                continue;
                            }
                            this->compound_jobs[jobID].second.push_back(job);
                        }
                    }

                    for (const auto &run : this->compound_jobs[jobID].first.runs) {
                        // wrench::S4U_Simulation::sleep(run.sleepDelay);
                        for (const auto &subJob : exec_jobs[run.id]) {
                            WRENCH_DEBUG("Submitting job %s with %lu actions", subJob->getName().c_str(), subJob->getActions().size());
                            if (bare_metal->hasReturnedFromMain()) {
                                WRENCH_DEBUG("Bare metal service has already returned from main");
                            }
                            if (bare_metal->getState() != wrench::S4U_Daemon::State::UP) {
                                WRENCH_DEBUG("Bare metal service is not up anymore");
                            }
                            WRENCH_DEBUG("Using internal job manager %s and bare_metal %s", internalJobManager->getName().c_str(), bare_metal->getName().c_str());
                            internalJobManager->submitJob(subJob, bare_metal, service_specific_args[subJob->getName()]);
                        }
                    }

                    for (const auto &run : exec_jobs) {
                        for (const auto &subJob : run.second) {
                            auto event = action_executor->waitForNextEvent();
                            if (not std::dynamic_pointer_cast<wrench::CompoundJobCompletedEvent>(event)) {
                                auto failure = std::dynamic_pointer_cast<wrench::CompoundJobFailedEvent>(event);
                                throw std::runtime_error("One of the subjobs failed -> " + failure->toString() + " // " + failure->failure_cause->toString());
                            }
                        }
                    }
                },
                [jobID](std::shared_ptr<wrench::ActionExecutor> action_executor) {
                    WRENCH_INFO(" >> [parent customAction_%s] terminating", jobID.c_str());
                });
        }

        // Submit job
        std::map<std::string, std::string> service_specific_args =
            {{"-N", std::to_string(yJob.nodesUsed)},                                                             // nb of nodes
             {"-c", std::to_string(yJob.coresUsed / yJob.nodesUsed)},                                            // cores per node
             {"-t", std::to_string(static_cast<int>(yJob.walltimeSeconds * this->config->walltime_extension))}}; // seconds
        WRENCH_INFO("[%s] Submitting parent job (%d nodes, %d cores per node, %ds of walltime)",
                    parentJob->getName().c_str(),
                    yJob.nodesUsed, yJob.coresUsed / yJob.nodesUsed, yJob.walltimeSeconds);
        job_manager->submitJob(parentJob, this->compute_service, service_specific_args);
        WRENCH_INFO("[%s] Job successfully submitted", parentJob->getName().c_str());
    }

    /**
     * @brief In-place random pruning of compute nodes from a given resource map. The goal is to be left with a smaller set
     *        of compute nodes that will be used for the IO actions of a sub-job.
     */
    void Controller::pruneIONodes(std::map<std::string, unsigned long> &resources, unsigned int max_nb_hosts) const {

        auto resourceNodeCount = resources.size();
        if (max_nb_hosts < resourceNodeCount and max_nb_hosts > 0) {

            auto nb_nodes_to_delete = resourceNodeCount - max_nb_hosts;
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<unsigned long> distrib(0, nb_nodes_to_delete - 1);

            WRENCH_DEBUG("filterOutIONodes: %lu nodes in total, max_nb_hosts=%u, %lu to be deleted", resourceNodeCount, max_nb_hosts, nb_nodes_to_delete);

            for (unsigned int i = 0; i < nb_nodes_to_delete; i++) {
                auto it = resources.begin();
                auto adv_indx = distrib(gen);
                while (adv_indx >= resources.size()) {
                    adv_indx = distrib(gen);
                }
                std::advance(it, adv_indx);
                resources.erase(it);
            }
        }
        WRENCH_DEBUG("filterOutIONodes: now left with : %lu compute nodes for the copy/read op", resources.size());
    }

    std::vector<std::shared_ptr<wrench::DataFile>> Controller::createFileParts(uint64_t total_bytes, uint64_t nb_files, const std::string &prefix_name) const {

        uint64_t bytes_per_file = static_cast<uint64_t>(std::floor(total_bytes / nb_files));
        int64_t remainder = total_bytes % nb_files;

        WRENCH_INFO("createFileParts: For an initial size of %lu bytes, creating %lu files with base size %lu and remainder %lu",
                    total_bytes, nb_files, bytes_per_file, remainder);

        std::vector<std::shared_ptr<wrench::DataFile>> files{};
        for (uint32_t i = 0; i < nb_files; i++) {
            uint64_t fileSize = bytes_per_file;
            if (remainder-- > 0) {
                fileSize += 1;
            }
            auto file_name = prefix_name + "_part" + std::to_string(i);
            files.push_back(wrench::Simulation::addFile(file_name, fileSize));
            if (fileSize < 1) {
                throw std::runtime_error("FILE SIZE is < 1 B for prefix :" + prefix_name);
            }
            WRENCH_DEBUG("createFileParts: file %s created with size %lu and added to simulation", file_name.c_str(), fileSize);
        }

        WRENCH_INFO("createFileParts: file prefix '%s' : %lu subfiles created, each one of size %lu bytes", prefix_name.c_str(), files.size(), bytes_per_file);
        return files;
    }

    /** @brief Create a job with copy actions going from the 'external' storage service to the local distributed
     *         filesystem (CompoundStorageService).
     *  @param action_executor Action executor from the customAction lambda calling this method
     *  @param internalJobManager Job manager created inside the calling lambda, and shared amond all sub jobs
     *  @param nb_files Number of files to create for the different copy actions (one file per copy action). Each file
     *                  accounts for 1 / nb_files of the total readBytes of the job.
     *  @param max_nb_hosts Maximum number of compute hosts involved in the copy actions : i) using 0 means all compute hosts
     *                      can be used (to the extent of the number of files) ii) using 1 or more compute nodes as io nodes, if
     *                      max_nb_hosts < nb_files, some hosts will be used more than once
     *  @return
     */
    std::vector<std::shared_ptr<wrench::DataFile>> Controller::copyFromPermanent(std::shared_ptr<wrench::BareMetalComputeService> bare_metal,
                                                                                 std::shared_ptr<wrench::CompoundJob> copyJob,
                                                                                 std::map<std::string, std::map<std::string, std::string>> &service_specific_args,
                                                                                 uint64_t readBytes,
                                                                                 unsigned int nb_files, unsigned int max_nb_hosts) {

        WRENCH_INFO("[%s] Creating copy sub-job (in) for a total read size of %ld bytes, on %u files and at most %u IO nodes (0 means all avail)",
                    copyJob->getName().c_str(), readBytes, nb_files, max_nb_hosts);

        if (nb_files < 1)
            throw std::runtime_error("Copy should happen on 1 file at least");

        // Subdivide the amount of copied bytes between as many files as requested (one allocation per file)
        auto prefix = "inputFile_" + copyJob->getName();
        auto read_files = this->createFileParts(readBytes, nb_files, prefix);

        // Randomly remove nodes from resources in order to never use more than 'max_nb_hosts'
        auto computeResources = bare_metal->getPerHostNumCores();
        this->pruneIONodes(computeResources, max_nb_hosts);

        // Create one copy action per file and per host
        service_specific_args[copyJob->getName()] = std::map<std::string, std::string>();
        auto computeResourcesIt = computeResources.begin();
        for (const auto &read_file : read_files) {
            auto source_location = wrench::FileLocation::LOCATION(this->storage_service, this->config->pstor.mount_prefix + "/" + this->config->pstor.read_path, read_file);
            wrench::StorageService::createFileAtLocation(source_location);
            auto action_id = "SC_" + read_file->getID();
            copyJob->addFileCopyAction(
                action_id,
                source_location,
                wrench::FileLocation::LOCATION(this->compound_storage_service, read_file));

            // register the future file within the CSS
            this->compound_storage_service->lookupOrDesignateStorageService(wrench::FileLocation::LOCATION(this->compound_storage_service, read_file));

            service_specific_args[copyJob->getName()][action_id] = computeResourcesIt->first;
            if (++computeResourcesIt == computeResources.end())
                computeResourcesIt = computeResources.begin();
        }

        return read_files;
    }

    void Controller::readFromTemporary(std::shared_ptr<wrench::BareMetalComputeService> bare_metal,
                                       std::shared_ptr<wrench::CompoundJob> readJob,
                                       std::string jobID,
                                       unsigned int runID,
                                       std::map<std::string, std::map<std::string, std::string>> &service_specific_args,
                                       uint64_t readBytes,
                                       std::vector<std::shared_ptr<wrench::DataFile>> inputs,
                                       unsigned int max_nb_hosts) {

        WRENCH_INFO("[%s] Creating read sub-job for  %lu file(s) with cumulative size %ld bytes, using %u IO nodes (0 means all avail)", readJob->getName().c_str(), inputs.size(), readBytes, max_nb_hosts);

        auto computeResources = bare_metal->getPerHostNumCores();

        if (max_nb_hosts == 0)
            max_nb_hosts = computeResources.size();

        std::map<std::shared_ptr<wrench::DataFile>, unsigned int> stripes_per_file{};
        std::map<std::shared_ptr<wrench::DataFile>, std::vector<unsigned int>> stripes_per_host_per_file{};
        for (const auto &read_file : inputs) {
            // We manually invoke this in order to know how the files will be striped before starting partial writes.
            WRENCH_DEBUG("[%s] Calling lookupFileLocation for file %s", readJob->getName().c_str(), read_file->getID().c_str());
            auto file_stripes = this->compound_storage_service->lookupFileLocation(wrench::FileLocation::LOCATION(this->compound_storage_service, read_file));

            unsigned int nb_stripes = file_stripes.size();
            stripes_per_file[read_file] = nb_stripes;
            stripes_per_host_per_file[read_file] = std::vector<unsigned int>();

            if (nb_stripes < max_nb_hosts) {
                for (unsigned int i = 0; i < nb_stripes; i++) {
                    stripes_per_host_per_file[read_file].push_back(1);
                }
                WRENCH_DEBUG("[%s] readFromTemporary: For file %s (size %f) : we have only %u stripes, but %u hosts => reading 1 stripes from the first %u hosts",
                             readJob->getName().c_str(), read_file->getID().c_str(), read_file->getSize(), nb_stripes, max_nb_hosts, nb_stripes);
            } else {
                unsigned int stripes_per_host = std::floor(nb_stripes / max_nb_hosts);
                unsigned int remainder = nb_stripes % max_nb_hosts;
                for (unsigned int i = 0; i < max_nb_hosts; i++) {
                    stripes_per_host_per_file[read_file].push_back(stripes_per_host);
                }
                for (unsigned int i = 0; i < remainder; i++) {
                    stripes_per_host_per_file[read_file][i] += 1;
                }
                WRENCH_DEBUG("[%s] readFromTemporary: For file %s (size %f) : %u stripes, reading %u +- %u stripes from each host (%u hosts will read more)",
                             readJob->getName().c_str(), read_file->getID().c_str(), read_file->getSize(), nb_stripes, stripes_per_host, remainder, remainder);
            }
        }

        // Randomly remove nodes from resources in order to never use more than 'max_nb_hosts'
        this->pruneIONodes(computeResources, max_nb_hosts);

        auto computeResourcesIt = computeResources.begin();
        unsigned int action_cnt = 0;
        service_specific_args[readJob->getName()] = std::map<std::string, std::string>();
        for (const auto &read_file : inputs) {

            auto stripe_size = read_file->getSize() / stripes_per_file[read_file];

            for (const auto &stripes_per_host : stripes_per_host_per_file[read_file]) {
                WRENCH_DEBUG("[%s] readFromTemporary: Creating read action for file %s and host %s", readJob->getName().c_str(), read_file->getID().c_str(), computeResourcesIt->first.c_str());

                auto action_id = "FR_" + read_file->getID() + "_" + computeResourcesIt->first + "_act" + std::to_string(action_cnt);
                double read_byte_per_node = 0;
                if (stripes_per_file[read_file] == stripes_per_host) {
                    // Only one host reading all the stripes
                    read_byte_per_node = read_file->getSize();
                    WRENCH_DEBUG("[%s] readFromTemporary: We'll be reading the entire file from this host.", readJob->getName().c_str());
                } else {
                    read_byte_per_node = stripe_size * stripes_per_host;
                    WRENCH_DEBUG("[%s] readFromTemporary:   We'll be reading %d stripes from this host, for a total of %f bytes from this host", readJob->getName().c_str(), stripes_per_host, read_byte_per_node);
                }

                this->stripes_per_action[jobID][runID][action_id] = stripes_per_host;

                // dirty debug
                if (read_byte_per_node > read_file->getSize()) {
                    std::cout << "READ_BYTE_PER_NODE > READ FILE SIZE" << std::endl;
                    std::cout << read_byte_per_node << " > " << read_file->getSize() << std::endl;
                    std::cout << "Max nb host is " << max_nb_hosts << std::endl;
                    std::cout << "There are  " << inputs.size() << " files " << std::endl;
                    std::cout << "the job is " << readJob->getName() << std::endl;
                    std::cout << "Stripe size is : " << stripe_size << std::endl;
                    std::cout << "Number of stripes for this file " << stripes_per_file[read_file] << std::endl;
                    std::cout << "Number of stripes to read for this host : " << stripes_per_host << std::endl;
                }

                readJob->addFileReadAction(action_id, wrench::FileLocation::LOCATION(this->compound_storage_service, read_file), read_byte_per_node);
                action_cnt++;

                service_specific_args[readJob->getName()][action_id] = computeResourcesIt->first;
                if (++computeResourcesIt == computeResources.end())
                    computeResourcesIt = computeResources.begin();
            }
        }
    }

    std::vector<std::shared_ptr<wrench::DataFile>> Controller::writeToTemporary(std::shared_ptr<wrench::BareMetalComputeService> bare_metal,
                                                                                std::shared_ptr<wrench::CompoundJob> writeJob,
                                                                                std::string jobID,
                                                                                unsigned int runID,
                                                                                std::map<std::string, std::map<std::string, std::string>> &service_specific_args,
                                                                                uint64_t writtenBytes,
                                                                                unsigned int nb_files,
                                                                                unsigned int max_nb_hosts) {

        WRENCH_INFO("[%s] Creating write sub-job with size %ld bytes, on %u files and at most %u IO nodes (0 means all avail)",
                    writeJob->getName().c_str(), writtenBytes, nb_files, max_nb_hosts);

        auto computeResources = bare_metal->getPerHostNumCores();

        if (nb_files < 1) {
            throw std::runtime_error("writeToTemporary: At least one host is needed to perform a write");
        }
        if (max_nb_hosts == 0) {
            max_nb_hosts = computeResources.size();
        }

        // Subdivide the amount of written bytes between as many files as requested (one allocation per file)
        auto prefix = "outputFile_" + writeJob->getName();
        auto write_files = this->createFileParts(writtenBytes, nb_files, prefix);

        // Compute what should be written by each host to each file (including remainder if values are not round)
        std::map<std::shared_ptr<wrench::DataFile>, unsigned int> stripes_per_file{};
        std::map<std::shared_ptr<wrench::DataFile>, std::vector<unsigned int>> stripes_per_host_per_file{};

        for (auto write_file : write_files) {
            // We manually invoke this in order to know how the files will be striped before starting partial writes.
            WRENCH_DEBUG("Calling lookupOrDesignate for file %s", write_file->getID().c_str());

            unsigned int local_stripe_count = this->determineWriteStripeCount(this->compound_jobs[jobID].first.cumulWriteBW);

            auto file_stripes = this->compound_storage_service->lookupOrDesignateStorageService(wrench::FileLocation::LOCATION(this->compound_storage_service, write_file), local_stripe_count);

            unsigned int nb_stripes = file_stripes.size();
            stripes_per_file[write_file] = nb_stripes;
            stripes_per_host_per_file[write_file] = std::vector<unsigned int>();

            // How many stripes should be written by each host in use ?
            if (nb_stripes < max_nb_hosts) {
                for (unsigned int i = 0; i < nb_stripes; i++) {
                    stripes_per_host_per_file[write_file].push_back(1);
                }
                WRENCH_DEBUG("[%s] writeToTemporary: For file %s (size %f) : we have only %u stripes, but %u hosts, writing 1 stripes from the first %u hosts",
                             writeJob->getName().c_str(), write_file->getID().c_str(), write_file->getSize(), nb_stripes, max_nb_hosts, nb_stripes);
            } else {
                unsigned int stripes_per_host = std::floor(nb_stripes / max_nb_hosts); //
                unsigned int remainder = nb_stripes % max_nb_hosts;
                for (unsigned int i = 0; i < max_nb_hosts; i++) {
                    stripes_per_host_per_file[write_file].push_back(stripes_per_host);
                }
                for (unsigned int i = 0; i < remainder; i++) {
                    stripes_per_host_per_file[write_file][i] += 1;
                }
                WRENCH_DEBUG("[%s] writeToTemporary: For file %s (size %f) : %u stripes, writing %u stripes from each host and last host writes %u stripes",
                             writeJob->getName().c_str(), write_file->getID().c_str(), write_file->getSize(), nb_stripes, stripes_per_host, stripes_per_host + remainder);
            }
        }

        // DEBUG : AT THIS POINT, THE CORRECT STORAGE SPACE IS RESERVED ONTO THE SELECTED NODES

        // Randomly remove nodes from resources in order to never use more than 'max_nb_hosts'
        this->pruneIONodes(computeResources, max_nb_hosts);

        // Create one write action per file (each one will run on one host, unless there are more actions than 'max_nb_hosts',
        // in which case multiple actions can be scheduled on the same host)
        auto computeResourcesIt = computeResources.begin();
        unsigned int action_cnt = 0;
        service_specific_args[writeJob->getName()] = std::map<std::string, std::string>();

        for (auto &write_file : write_files) {

            auto stripe_size = write_file->getSize() / stripes_per_file[write_file];

            for (const auto &stripes_per_host : stripes_per_host_per_file[write_file]) {
                WRENCH_DEBUG("[%s] writeToTemporary: Creating custom write action for file %s and host %s", writeJob->getName().c_str(), write_file->getID().c_str(), computeResourcesIt->first.c_str());

                auto action_id = "FW_" + write_file->getID() + "_" + computeResourcesIt->first + "_act" + std::to_string(action_cnt);

                double write_bytes_per_node = 0;
                if (stripes_per_file[write_file] == stripes_per_host) {
                    // Only one host reading all the stripes (stripes_per_host is actually the total number or stripes)
                    write_bytes_per_node = write_file->getSize();
                    WRENCH_DEBUG("[%s] writeToTemporary: We'll be writing the entire file from this host.", writeJob->getName().c_str());
                } else {
                    write_bytes_per_node = stripe_size * stripes_per_host;
                    WRENCH_DEBUG("[%s] writeToTemporary: We'll be writing %d stripes from this host, for a total of %f bytes from this host", writeJob->getName().c_str(), stripes_per_host, write_bytes_per_node);
                }

                auto customWriteAction = std::make_shared<PartialWriteCustomAction>(
                    action_id, 0, 0,
                    [this, write_file, write_bytes_per_node](const std::shared_ptr<wrench::ActionExecutor> &action_executor) {
                        this->compound_storage_service->writeFile(S4U_Daemon::getRunningActorRecvCommPort(),
                                                                  wrench::FileLocation::LOCATION(this->compound_storage_service, write_file),
                                                                  write_bytes_per_node,
                                                                  true);
                    },
                    [action_id, write_bytes_per_node, writeJob](std::shared_ptr<wrench::ActionExecutor> executor) {
                        WRENCH_DEBUG("[%s] writeToTemporary: %s terminating - wrote %f", writeJob->getName().c_str(), action_id.c_str(), write_bytes_per_node);
                    },
                    write_file, write_bytes_per_node);
                writeJob->addCustomAction(customWriteAction);

                this->stripes_per_action[jobID][runID][action_id] = stripes_per_host;
                action_cnt++;

                // One copy per compute node, looping over if there are more files than nodes
                service_specific_args[writeJob->getName()][action_id] = computeResourcesIt->first;
                if (++computeResourcesIt == computeResources.end())
                    computeResourcesIt = computeResources.begin();
            }
        }

        // internalJobManager->submitJob(writeJob, bare_metal, service_specific_args);
        // WRENCH_INFO("[%s-%u] writeToTemporary: job submitted with %lu actions on bare_metal %s", jobPair.first.id.c_str(), record.id, write_files.size() * max_nb_hosts, bare_metal->getName().c_str());
        // auto nextEvent = action_executor->waitForNextEvent();
        // if (not std::dynamic_pointer_cast<wrench::CompoundJobCompletedEvent>(nextEvent)) {
        //     auto failedEvent = std::dynamic_pointer_cast<wrench::CompoundJobFailedEvent>(nextEvent);
        //     for (const auto &action : writeJob->getActions()) {
        //         if (action->getState() == wrench::Action::FAILED) {
        //             WRENCH_WARN("Action failure cause : %s", action->getFailureCause()->toString().c_str());
        //         }
        //     }
        //     WRENCH_WARN("Failed event cause : %s", failedEvent->failure_cause->toString().c_str());
        //     throw std::runtime_error("Sub-job 'write to CSS' " + writeJob->getName() + " failed");
        // }
        // WRENCH_INFO("[%s] Write job executed with %lu actions", writeJob->getName().c_str(), writeJob->getActions().size());
        return write_files;
    }

    void Controller::copyToPermanent(std::shared_ptr<wrench::BareMetalComputeService> bare_metal,
                                     std::shared_ptr<wrench::CompoundJob> copyJob,
                                     std::map<std::string, std::map<std::string, std::string>> &service_specific_args,
                                     uint64_t writtenBytes,
                                     std::vector<std::shared_ptr<wrench::DataFile>> outputs,
                                     unsigned int nb_hosts) {

        WRENCH_INFO("[%s] Creating copy sub-job (out) %lu file(s) with cumulative size %ld bytes, using %u IO nodes (0 means all avail)",
                    copyJob->getName().c_str(), outputs.size(), writtenBytes, nb_hosts);

        // Randomly remove nodes from resources in order to never use more than 'max_nb_hosts'
        auto computeResources = bare_metal->getPerHostNumCores();
        if (nb_hosts == 0) {
            nb_hosts = computeResources.size();
        }
        this->pruneIONodes(computeResources, nb_hosts);

        /* Note here if a write operations was previously sliced between n compute hosts, we virtually created n pseudo files,
         * and we're now goind to run the copy from n compute hosts once again. In future works, we might want to differentiate
         * the number of hosts used in the write operation and the number of hosts used in the following copy.
         */
        service_specific_args[copyJob->getName()] = std::map<std::string, std::string>();
        auto computeResourcesIt = computeResources.begin();
        for (const auto &output_data : outputs) {
            auto action_id = "AC_" + output_data->getID();
            auto archiveAction = copyJob->addFileCopyAction(
                action_id,
                wrench::FileLocation::LOCATION(this->compound_storage_service, output_data),
                wrench::FileLocation::LOCATION(this->storage_service, this->config->pstor.mount_prefix + "/" + this->config->pstor.write_path, output_data));

            service_specific_args[copyJob->getName()][action_id] = computeResourcesIt->first;
            if (++computeResourcesIt == computeResources.end())
                computeResourcesIt = computeResources.begin();
        }

        // internalJobManager->submitJob(copyJob, bare_metal, service_specific_args);
        // WRENCH_INFO("[%s] copyToPermanent: job submitted with %lu actions on bare_metal %s", jobPair.first.id.c_str(), record.id, outputs.size(), bare_metal->getName().c_str());
        // if (not std::dynamic_pointer_cast<wrench::CompoundJobCompletedEvent>(action_executor->waitForNextEvent()))
        //     throw std::runtime_error("Sub-job 'copy to permanent' " + copyJob->getName() + " failed");
        // WRENCH_INFO("[%s] CopyTo job executed with %lu actions", copyJob->getName().c_str(), copyJob->getActions().size());
    }

    void Controller::cleanupInput(std::shared_ptr<wrench::BareMetalComputeService> bare_metal,
                                  std::shared_ptr<wrench::CompoundJob> cleanupJob,
                                  std::map<std::string, std::map<std::string, std::string>> &service_specific_args,
                                  std::vector<std::shared_ptr<wrench::DataFile>> inputs,
                                  bool cleanup_external) {

        WRENCH_INFO("[%s] Creating cleanup actions for input file(s)", cleanupJob->getName().c_str());

        auto computeResources = bare_metal->getPerHostNumCores();

        service_specific_args[cleanupJob->getName()] = std::map<std::string, std::string>();
        auto computeResourcesIt = computeResources.begin();
        auto action_cnt = 0;
        for (const auto &input_data : inputs) {
            auto del_id = "DRF_" + input_data->getID();
            auto deleteReadAction = cleanupJob->addFileDeleteAction(del_id, wrench::FileLocation::LOCATION(this->compound_storage_service, input_data));
            if (computeResourcesIt == computeResources.end()) {
                computeResourcesIt = computeResources.begin();
            }
            service_specific_args[cleanupJob->getName()][del_id] = computeResourcesIt->first;
            action_cnt++;
        }

        if (cleanup_external) {
            for (const auto &input_data : inputs) {
                auto del_id = "DERF_" + input_data->getID();
                auto deleteExternalReadAction = cleanupJob->addFileDeleteAction(del_id, wrench::FileLocation::LOCATION(this->storage_service, this->config->pstor.mount_prefix + "/" + this->config->pstor.read_path, input_data));
                if (computeResourcesIt == computeResources.end()) {
                    computeResourcesIt = computeResources.begin();
                }
                service_specific_args[cleanupJob->getName()][del_id] = computeResourcesIt->first;
                action_cnt++;
            }
        }

        // internalJobManager->submitJob(cleanupJob, bare_metal, {});
        // WRENCH_INFO("[%s] CleanupInput: inputCleanUp job submitted with %lu actions on bare_metal %s", jobPair.first.id.c_str(), record.id, inputs.size() * 2, bare_metal->getName().c_str());
        // if (not std::dynamic_pointer_cast<wrench::CompoundJobCompletedEvent>(action_executor->waitForNextEvent()))
        //     throw std::runtime_error("Sub-job 'cleanup input(s)' " + cleanupJob->getName() + " failed");
        // WRENCH_INFO("[%s] CleanupInput job executed with %lu actions", cleanupJob->getName().c_str(), cleanupJob->getActions().size());
    }

    void Controller::cleanupOutput(std::shared_ptr<wrench::BareMetalComputeService> bare_metal,
                                   std::shared_ptr<wrench::CompoundJob> cleanupJob,
                                   std::map<std::string, std::map<std::string, std::string>> &service_specific_args,
                                   std::vector<std::shared_ptr<wrench::DataFile>> outputs,
                                   bool cleanup_external) {

        WRENCH_INFO("[%s] Creating cleanup actions for output file(s)", cleanupJob->getName().c_str());

        auto computeResources = bare_metal->getPerHostNumCores();

        service_specific_args[cleanupJob->getName()] = std::map<std::string, std::string>();
        auto computeResourcesIt = computeResources.begin();
        auto action_cnt = 0;
        for (const auto &output_data : outputs) {
            auto del_id = "DWF_" + output_data->getID();
            auto deleteWriteAction = cleanupJob->addFileDeleteAction(del_id, wrench::FileLocation::LOCATION(this->compound_storage_service, output_data));
            if (computeResourcesIt == computeResources.end()) {
                computeResourcesIt = computeResources.begin();
            }
            service_specific_args[cleanupJob->getName()][del_id] = computeResourcesIt->first;
            action_cnt++;
        }

        if (cleanup_external) {
            for (const auto &output_data : outputs) {
                auto del_id = "DEWF_" + output_data->getID();
                auto deleteExternalWriteAction = cleanupJob->addFileDeleteAction(del_id, wrench::FileLocation::LOCATION(this->storage_service, this->config->pstor.mount_prefix + "/" + this->config->pstor.write_path, output_data));
                if (computeResourcesIt == computeResources.end()) {
                    computeResourcesIt = computeResources.begin();
                }
                service_specific_args[cleanupJob->getName()][del_id] = computeResourcesIt->first;
                action_cnt++;
            }
        }

        // internalJobManager->submitJob(cleanupJob, bare_metal, {});
        // WRENCH_INFO("[%s] cleanupOutput: outputCleanUp job submitted with %lu actions on bare_metal %s", jobPair.first.id.c_str(), record.id, outputs.size() * 2, bare_metal->getName().c_str());
        // if (not std::dynamic_pointer_cast<wrench::CompoundJobCompletedEvent>(action_executor->waitForNextEvent()))
        //     throw std::runtime_error("Sub-job 'cleanup output(s)' " + cleanupJob->getName() + " failed");
        // WRENCH_INFO("[%s] CleanupOutput job executed with %lu actions", cleanupJob->getName().c_str(), cleanupJob->getActions().size());
    }

    /**
     * @brief Process a compound job completion event
     *
     * @param event: the event
     */
    void Controller::processEventCompoundJobCompletion(std::shared_ptr<wrench::CompoundJobCompletedEvent> event) {
        auto job = event->job;
        WRENCH_INFO("[%s] Notified that this compound job has completed", job->getName().c_str());

        // Extract relevant informations from job and write them to file / send them to DB ?
        processCompletedJob(job->getName());
    }

    void Controller::processEventTimer(std::shared_ptr<wrench::TimerEvent> timerEvent) {
        this->submitJob(timerEvent->message);
    }

    /**
     * @brief Process a compound job failure event
     *
     * @param event: the event
     */
    void Controller::processEventCompoundJobFailure(std::shared_ptr<wrench::CompoundJobFailedEvent> event) {
        /* Retrieve the job that this event is for and failure cause*/
        auto job = event->job;
        auto cause = event->failure_cause;
        wrench::TerminalOutput::setThisProcessLoggingColor(wrench::TerminalOutput::COLOR_RED);
        WRENCH_WARN("[%s] Notified that this compound job has failed: %s", job->getName().c_str(), cause->toString().c_str());
        wrench::TerminalOutput::setThisProcessLoggingColor(wrench::TerminalOutput::COLOR_GREEN);
        this->failed_jobs_count += 1;
        this->compound_jobs.erase(job->getName());
    }

    std::pair<std::string, std::string> updateIoUsageDelete(std::map<std::string, StorageServiceIOCounters> &volume_records, const std::shared_ptr<wrench::FileLocation> &location) {

        auto storage_service = location->getStorageService()->getName();
        auto mount_pt = location->getPath(); // TODO : RECENT FIX, NEED TO CHECK IF DATA IS CORRECT
        auto volume = location->getFile()->getSize();

        // Update volume record with new write/copy/delete value
        volume_records[storage_service].total_allocation_count -= 1;
        volume_records[storage_service].total_capacity_used -= volume;
        volume_records[storage_service].disks[mount_pt].total_allocation_count -= 1;   // allocation count on disk
        volume_records[storage_service].disks[mount_pt].total_capacity_used -= volume; // byte volume count

        // Return latest updated keys for ease of use
        return std::make_pair(storage_service, mount_pt);
    }

    std::pair<std::string, std::string> updateIoUsageCopy(std::map<std::string, StorageServiceIOCounters> &volume_records, const std::shared_ptr<wrench::FileLocation> &location) {

        auto dst_storage_service = location->getStorageService()->getName();
        auto dst_path = location->getPath(); // TODO : RECENT FIX, NEED TO CHECK IF DATA IS CORRECT
        auto dst_volume = location->getFile()->getSize();

        if (volume_records.find(dst_storage_service) == volume_records.end()) {
            // Add entry for previously unseen storage service
            volume_records[dst_storage_service].disks = std::map<std::string, DiskIOCounters>();
        }

        if (volume_records[dst_storage_service].disks.find(dst_path) == volume_records[dst_storage_service].disks.end()) {
            // Add entry for previously unseen storage service disk
            volume_records[dst_storage_service].disks[dst_path] = DiskIOCounters();
        }

        // Update volume record with new write/copy/delete value
        volume_records[dst_storage_service].total_allocation_count += 1;                       // allocation count on disk
        volume_records[dst_storage_service].total_capacity_used += dst_volume;                 // byte volume count
        volume_records[dst_storage_service].disks[dst_path].total_allocation_count += 1;       // allocation count on disk
        volume_records[dst_storage_service].disks[dst_path].total_capacity_used += dst_volume; // byte volume count

        // Return latest updated keys for ease of use
        return std::make_pair(dst_storage_service, dst_path);
    }

    std::pair<std::string, std::string> updateIoUsageWrite(std::map<std::string, StorageServiceIOCounters> &volume_records, const std::shared_ptr<wrench::FileLocation> &location) {

        auto storage_service = location->getStorageService()->getName();
        auto path = location->getPath(); // TODO : RECENT FIX, NEED TO CHECK IF DATA IS CORRECT
        auto volume = location->getFile()->getSize();

        if (volume_records.find(storage_service) == volume_records.end()) {
            volume_records[storage_service].disks = std::map<std::string, DiskIOCounters>();
        }

        if (volume_records[storage_service].disks.find(path) == volume_records[storage_service].disks.end()) {
            volume_records[storage_service].disks[path] = DiskIOCounters();
        }

        // Update volume record with new write/copy/delete value
        volume_records[storage_service].total_allocation_count += 1;               // allocation count on disk
        volume_records[storage_service].total_capacity_used += volume;             // byte volume count
        volume_records[storage_service].disks[path].total_allocation_count += 1;   // allocation count on disk
        volume_records[storage_service].disks[path].total_capacity_used += volume; // byte volume count

        // Return latest updated keys for ease of use
        return std::make_pair(storage_service, path);
    }

    void Controller::processActions(YAML::Emitter &out_jobs, const std::set<std::shared_ptr<wrench::Action>> &actions, double &job_start_time, const std::string &job_id) {

        for (const auto &action : actions) {

            if (action->getState() != wrench::Action::COMPLETED) {
                WRENCH_WARN("Action %s is in state %s", action->getName().c_str(), action->getStateAsString().c_str());
                continue;
                // throw std::runtime_error("Action is not in COMPLETED state");
            }

            out_jobs << YAML::BeginMap; // action map
            out_jobs << YAML::Key << "act_name" << YAML::Value << action->getName();
            out_jobs << YAML::Key << "sub_job" << YAML::Value << action->getJob()->getName();
            auto act_type = wrench::Action::getActionTypeAsString(action);
            act_type.pop_back(); // removing a useless '-' at the end
            out_jobs << YAML::Key << "act_type" << YAML::Value << act_type;
            out_jobs << YAML::Key << "act_status" << YAML::Value << action->getStateAsString();
            out_jobs << YAML::Key << "act_start_ts" << YAML::Value << action->getStartDate();
            out_jobs << YAML::Key << "act_end_ts" << YAML::Value << action->getEndDate();
            out_jobs << YAML::Key << "act_duration" << YAML::Value << (action->getEndDate() - action->getStartDate());

            auto subjob = action->getJob()->getName();
            auto last_ = subjob.find_last_of("_");
            int run_id = -1;
            if (last_ != std::string::npos and act_type != "FILEDELETE" and act_type != "FILECOPY") {
                auto run_id_str = subjob.substr(last_ + 5, subjob.size());
                try {
                    run_id = stoi(run_id_str);
                } catch (const std::invalid_argument &e) {
                    std::cout << e.what() << std::endl;
                    std::cout << subjob << "::" << run_id_str << std::endl;
                }
            }

            job_start_time = min(job_start_time, action->getStartDate());

            // WRENCH_DEBUG("  - Action type : %s at ts: %f", act_type.c_str(), action->getStartDate());

            if (auto fileRead = std::dynamic_pointer_cast<wrench::FileReadAction>(action)) {

                auto usedLocation = fileRead->getUsedFileLocation();
                auto usedFile = usedLocation->getFile();

                out_jobs << YAML::Key << "file_name" << YAML::Value << usedFile->getID();
                out_jobs << YAML::Key << "file_size_bytes" << YAML::Value << usedFile->getSize();
                out_jobs << YAML::Key << "io_size_bytes" << YAML::Value << fileRead->getNumBytesToRead();
                if (run_id != -1) {
                    out_jobs << YAML::Key << "nb_stripes" << YAML::Value << this->stripes_per_action[job_id][run_id][action->getName()];
                }
            } else if (auto fileWrite = std::dynamic_pointer_cast<storalloc::PartialWriteCustomAction>(action)) {
                // auto usedLocation = fileWrite->getFileLocation();
                auto usedFile = fileWrite->getFile();

                auto write_trace = this->compound_storage_service->write_traces[usedFile->getID()];
                out_jobs << YAML::Key << "internal_locations" << YAML::Value << write_trace.internal_locations.size();
                out_jobs << YAML::Key << "parts_count" << YAML::Value << write_trace.parts_count;
                out_jobs << YAML::Key << "file_name" << YAML::Value << usedFile->getID();
                out_jobs << YAML::Key << "file_size_bytes" << YAML::Value << usedFile->getSize();
                out_jobs << YAML::Key << "io_size_bytes" << YAML::Value << fileWrite->getWrittenSize();
                if (run_id != -1) {
                    out_jobs << YAML::Key << "nb_stripes" << YAML::Value << this->stripes_per_action[job_id][run_id][action->getName()];
                }

                /*for (const auto &trace_loc : write_trace.internal_locations) {
                    auto keys = updateIoUsageWrite(this->volume_per_storage_service_disk, trace_loc);
                    out_actions << YAML::BeginMap;
                    out_actions << YAML::Key << "ts" << YAML::Value << action->getEndDate(); // end_date, because we record the IO change when the write is completed
                    out_actions << YAML::Key << "action_type" << YAML::Value << act_type;
                    out_actions << YAML::Key << "action_job" << YAML::Value << action->getJob()->getName();
                    out_actions << YAML::Key << "action_name" << YAML::Value << action->getName();
                    out_actions << YAML::Key << "filename" << YAML::Value << trace_loc->getFile()->getID();
                    out_actions << YAML::Key << "storage_service" << YAML::Value << keys.first;
                    out_actions << YAML::Key << "disk" << YAML::Value << keys.second;
                    out_actions << YAML::Key << "volume_change_bytes" << YAML::Value << usedFile->getSize();
                    out_actions << YAML::Key << "total_allocation_server" << YAML::Value << this->volume_per_storage_service_disk[keys.first].total_allocation_count;
                    out_actions << YAML::Key << "total_used_volume_bytes_server" << YAML::Value << this->volume_per_storage_service_disk[keys.first].total_capacity_used;
                    out_actions << YAML::Key << "total_allocation_disk" << YAML::Value << this->volume_per_storage_service_disk[keys.first].disks[keys.second].total_allocation_count;
                    out_actions << YAML::Key << "total_used_volume_bytes_disk" << YAML::Value << this->volume_per_storage_service_disk[keys.first].disks[keys.second].total_capacity_used;
                    out_actions << YAML::EndMap;
                }*/
            } else if (auto fileCopy = std::dynamic_pointer_cast<wrench::FileCopyAction>(action)) {

                std::shared_ptr<wrench::FileLocation> css;
                std::shared_ptr<wrench::FileLocation> sss;

                // !!! We don't handle a sss <-> sss or css <-> css copy
                if (std::dynamic_pointer_cast<wrench::CompoundStorageService>(
                        fileCopy->getSourceFileLocation()->getStorageService())) {
                    css = fileCopy->getSourceFileLocation();
                    sss = fileCopy->getDestinationFileLocation();
                    out_jobs << YAML::Key << "copy_direction" << YAML::Value << "css_to_sss";
                } else {
                    sss = fileCopy->getSourceFileLocation();
                    css = fileCopy->getDestinationFileLocation();
                    out_jobs << YAML::Key << "copy_direction" << YAML::Value << "sss_to_css";
                }

                auto copy_trace = this->compound_storage_service->copy_traces[css->getFile()->getID()];
                out_jobs << YAML::Key << "parts_count" << YAML::Value << copy_trace.internal_locations.size();
                out_jobs << YAML::Key << "sss" << YAML::Value << sss->getStorageService()->getName();
                out_jobs << YAML::Key << "sss_server" << YAML::Value << sss->getStorageService()->getHostname();
                out_jobs << YAML::Key << "sss_file_path" << YAML::Value << sss->getPath();
                out_jobs << YAML::Key << "sss_file_name" << YAML::Value << sss->getFile()->getID();
                out_jobs << YAML::Key << "sss_file_size_bytes" << YAML::Value << sss->getFile()->getSize();
                out_jobs << YAML::Key << "css" << YAML::Value << css->getStorageService()->getName();
                out_jobs << YAML::Key << "css_server" << YAML::Value << css->getStorageService()->getHostname();
                out_jobs << YAML::Key << "css_file_path" << YAML::Value << css->getPath();
                out_jobs << YAML::Key << "css_file_name" << YAML::Value << css->getFile()->getID();
                out_jobs << YAML::Key << "css_file_size_bytes" << YAML::Value << css->getFile()->getSize();

                // only record a write if it's from SSS (external permanent storage) to CSS
                /*
                if (fileCopy->getDestinationFileLocation()->getStorageService()->getHostname() != "permanent_storage") {

                    for (const auto &trace_loc : copy_trace.internal_locations) {
                        auto keys = updateIoUsageCopy(this->volume_per_storage_service_disk, trace_loc);
                        out_actions << YAML::BeginMap;
                        out_actions << YAML::Key << "ts" << YAML::Value << action->getEndDate(); // end_date, because we record the IO change when the write is completed
                        out_actions << YAML::Key << "action_type" << YAML::Value << act_type;
                        out_actions << YAML::Key << "action_name" << YAML::Value << action->getName();
                        out_actions << YAML::Key << "filename" << YAML::Value << trace_loc->getFile()->getID();
                        out_actions << YAML::Key << "action_job" << YAML::Value << action->getJob()->getName();
                        out_actions << YAML::Key << "storage_service" << YAML::Value << keys.first;
                        out_actions << YAML::Key << "disk" << YAML::Value << keys.second;
                        out_actions << YAML::Key << "volume_change_bytes" << YAML::Value << css->getFile()->getSize(); // dest file, because it's the one being written
                        out_actions << YAML::Key << "total_allocation_server" << YAML::Value << this->volume_per_storage_service_disk[keys.first].total_allocation_count;
                        out_actions << YAML::Key << "total_used_volume_bytes_server" << YAML::Value << this->volume_per_storage_service_disk[keys.first].total_capacity_used;
                        out_actions << YAML::Key << "total_allocation_disk" << YAML::Value << this->volume_per_storage_service_disk[keys.first].disks[keys.second].total_allocation_count;
                        out_actions << YAML::Key << "total_used_volume_bytes_disk" << YAML::Value << this->volume_per_storage_service_disk[keys.first].disks[keys.second].total_capacity_used;
                        out_actions << YAML::EndMap;
                    }
                }*/
            } else if (auto fileDelete = std::dynamic_pointer_cast<wrench::FileDeleteAction>(action)) {
                auto usedLocation = fileDelete->getFileLocation();
                auto usedFile = usedLocation->getFile();

                out_jobs << YAML::Key << "storage_service" << YAML::Value << usedLocation->getStorageService()->getName();
                out_jobs << YAML::Key << "storage_server" << YAML::Value << usedLocation->getStorageService()->getHostname();
                out_jobs << YAML::Key << "file_path" << YAML::Value << usedLocation->getPath();
                out_jobs << YAML::Key << "file_name" << YAML::Value << usedFile->getID();
                out_jobs << YAML::Key << "file_size_bytes" << YAML::Value << usedFile->getSize();

                if (usedLocation->getStorageService()->getHostname() != "permanent_storage") {

                    auto delete_trace = this->compound_storage_service->delete_traces[usedFile->getID()];
                    out_jobs << YAML::Key << "parts_count" << YAML::Value << delete_trace.internal_locations.size();

                    /*
                    for (const auto &trace_loc : delete_trace.internal_locations) {
                        auto keys = updateIoUsageDelete(this->volume_per_storage_service_disk, trace_loc);
                        out_actions << YAML::BeginMap;
                        out_actions << YAML::Key << "ts" << YAML::Value << action->getEndDate(); // end_date, because we record the IO change when the write is completed
                        out_actions << YAML::Key << "action_type" << YAML::Value << act_type;
                        out_actions << YAML::Key << "action_name" << YAML::Value << action->getName();
                        out_actions << YAML::Key << "filename" << YAML::Value << trace_loc->getFile()->getID();
                        out_actions << YAML::Key << "action_job" << YAML::Value << action->getJob()->getName();
                        out_actions << YAML::Key << "storage_service" << YAML::Value << keys.first;
                        out_actions << YAML::Key << "disk" << YAML::Value << keys.second;
                        out_actions << YAML::Key << "volume_change_bytes" << YAML::Value << usedFile->getSize();
                        out_actions << YAML::Key << "total_allocation_server" << YAML::Value << this->volume_per_storage_service_disk[keys.first].total_allocation_count;
                        out_actions << YAML::Key << "total_used_volume_bytes_server" << YAML::Value << this->volume_per_storage_service_disk[keys.first].total_capacity_used;
                        out_actions << YAML::Key << "total_allocation_disk" << YAML::Value << this->volume_per_storage_service_disk[keys.first].disks[keys.second].total_allocation_count;
                        out_actions << YAML::Key << "total_used_volume_bytes_disk" << YAML::Value << this->volume_per_storage_service_disk[keys.first].disks[keys.second].total_capacity_used;
                        out_actions << YAML::EndMap;
                    }*/
                }
            }
            out_jobs << YAML::EndMap;
        }
    }

    void Controller::processCompletedJob(const std::string &job_id) {

        auto job_entry = this->compound_jobs[job_id];

        const auto &yaml_job = job_entry.first;
        const auto job_list = job_entry.second;
        const auto &parent_job = job_list[0];

        // Ignore preload jobs in the output results
        if (parent_job->getName().substr(0, 7) == "preload")
            return;

        this->completed_jobs << YAML::BeginMap; // Job map

        // ## High level 'parent' job informations
        WRENCH_INFO("[%s] Processing metrics...", parent_job->getName().c_str());
        this->completed_jobs << YAML::Key << "job_uid" << YAML::Value << parent_job->getName();
        this->completed_jobs << YAML::Key << "job_status" << YAML::Value << parent_job->getStateAsString();
        this->completed_jobs << YAML::Key << "job_submit_ts" << YAML::Value << parent_job->getSubmitDate();
        this->completed_jobs << YAML::Key << "job_end_ts" << YAML::Value << parent_job->getEndDate();
        this->completed_jobs << YAML::Key << "real_runtime_s" << YAML::Value << yaml_job.runtimeSeconds;
        this->completed_jobs << YAML::Key << "real_read_bytes" << YAML::Value << yaml_job.readBytes;
        this->completed_jobs << YAML::Key << "real_written_bytes" << YAML::Value << yaml_job.writtenBytes;
        this->completed_jobs << YAML::Key << "real_cores_used" << YAML::Value << yaml_job.coresUsed;
        this->completed_jobs << YAML::Key << "real_waiting_time_s" << YAML::Value << yaml_job.waitingTimeSeconds;
        this->completed_jobs << YAML::Key << "real_cReadTime_s" << YAML::Value << yaml_job.readTimeSeconds;
        this->completed_jobs << YAML::Key << "real_cWriteTime_s" << YAML::Value << yaml_job.writeTimeSeconds;
        this->completed_jobs << YAML::Key << "real_cMetaTime_s" << YAML::Value << yaml_job.metaTimeSeconds;
        this->completed_jobs << YAML::Key << "sim_sleep_time" << YAML::Value << yaml_job.sleepSimulationSeconds;
        this->completed_jobs << YAML::Key << "cumul_read_bw" << YAML::Value << yaml_job.cumulReadBW;
        this->completed_jobs << YAML::Key << "cumul_write_bw" << YAML::Value << yaml_job.cumulWriteBW;

        // this->completed_jobs << YAML::Key << "sum_nprocs" << YAML::Value << yaml_job.sum_nprocs;

        // ## Processing actions for all sub jobs related to the current top-level job being processed
        this->completed_jobs << YAML::Key << "actions" << YAML::Value << YAML::BeginSeq;

        // Actual values determined after processing all actions from different sub jobs
        double earliest_action_start_time = UINT64_MAX;
        // Note that we start at ++begin(), to skip the first job (parent) in vector
        for (auto it = job_list.begin(); it < job_list.end(); it++) {
            WRENCH_DEBUG("Actions of job %s : ", it->get()->getName().c_str());
            auto actions = (it->get())->getActions();
            WRENCH_DEBUG(" ->> %lu", actions.size());
            processActions(this->completed_jobs, actions, earliest_action_start_time, job_id);
        }

        this->completed_jobs << YAML::EndSeq; // actions

        // ## High level keys that can be updated only after all actions are processed.
        this->completed_jobs << YAML::Key << "job_start_ts" << YAML::Value << earliest_action_start_time;
        this->completed_jobs << YAML::Key << "job_waiting_time_s" << YAML::Value << earliest_action_start_time - parent_job->getSubmitDate();
        this->completed_jobs << YAML::Key << "job_runtime_s" << YAML::Value << parent_job->getEndDate() - earliest_action_start_time;

        this->completed_jobs << YAML::EndMap; // job map

        this->compound_jobs.erase(job_id); // cleanup job map once the job is analysed
    }

    /**
     * @brief Explore the list of completed jobs and prepare two yaml outputs for further analysis :
     *          - The first one, "simulatedJobs_*", includes the full list of jobs and the list of actions for each jobs.
     *          It also contains data about the actual jobs, taken from the original yaml dataset (for easier comparison later)
     *          - The second one contains a list of all actions, with more details on storage services for each action.
     *        Both files can be correlated with the jobs ID.
     */
    void Controller::processCompletedJobs(const std::string &jobsFilename, const std::string &configVersion, const std::string &tag) {

        ofstream simulatedJobs;
        simulatedJobs.open(this->config->out.job_filename_prefix + jobsFilename + "__" + configVersion + "_" + tag + ".yml");
        simulatedJobs << "---\n";
        simulatedJobs << this->completed_jobs.c_str();
        simulatedJobs << "\n...\n";
        simulatedJobs.close();
    }

    /**
     * @brief Extract data from internal storage service usage history (collected for each write, delete or copy operation)
     *        These traces are snapshots of the state of each service involved in the IO operation, at the time of the IO operation
     *        (right before and right after).
     *        We write that information into a CSV which can be used to replay the storage service activity (excluding copies from the CSS
     *        and reads on the CSS, which don't incur changes in the free capacity or number of files on the storage services).
     */
    void Controller::extractSSSIO(const std::string &jobsFilename, const std::string &configVersion, const std::string &tag) {

        if (not this->hasReturnedFromMain()) {
            throw std::runtime_error("Cannot extract IO traces before the controller has returned from main()");
        }

        ofstream io_ops;
        io_ops.setf(std::ios_base::fixed);
        io_ops.open(this->config->out.storage_svc_prefix + jobsFilename + "__" + configVersion + "_" + tag + ".csv");
        io_ops << "ts,action_name,storage_service_name,storage_hostname,disk_id,disk_capacity,disk_file_count,disk_free_space,file_name,parts_count\n";
        // io_ops << setprecision(10);

        for (const auto &entry : this->compound_storage_service->internal_storage_use) {

            auto ts = entry.first;     // ts
            auto alloc = entry.second; // AllocationTrace structure

            for (const auto &disk_usage : alloc.disk_usage) {

                auto simple_storage = std::dynamic_pointer_cast<wrench::SimpleStorageService>(disk_usage.service);
                std::string disk_capacity = simple_storage->getDiskForPathOrNull("/")->get_property("size");
                auto capacity_bytes = wrench::UnitParser::parse_size(disk_capacity.c_str());

                io_ops << ts << ",";                                               // timestamp
                io_ops << std::to_string(static_cast<u_int8_t>(alloc.act)) << ","; // action code
                io_ops << simple_storage->getName() << ",";                        // storage_service_name
                io_ops << simple_storage->getHostname() << ",";                    // storage_hostname
                io_ops << simple_storage->getBaseRootPath() << ",";                // disk_id
                io_ops << capacity_bytes << ",";                                   // disk_capacity
                io_ops << disk_usage.file_count << ",";                            // current file count on disk
                io_ops << disk_usage.free_space << ",";                            // disk_free_space
                io_ops << alloc.file_name << ",";                                  // file_name
                io_ops << alloc.parts_count << "\n";                               // number of parts for file
            }
        }

        io_ops.close();
    }

} // namespace storalloc
