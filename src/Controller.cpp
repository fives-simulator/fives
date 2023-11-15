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

        auto services = this->compound_storage_service->getAllServices();
        for (const auto &service : services) {
            for (const auto &ost : service.second) {
                WRENCH_INFO("NUMBER OF FILES CURRENTLY ON OST %s : %f", ost->getName().c_str(), ost->getTotalFilesZeroTime());
            }
        }

        // Process any remaining events (number unknown, depends on jobs runtime vs simulation sleep between jobs)
        while (processed_events < total_events) {
            this->waitForAndProcessNextEvent();
            WRENCH_INFO("[Outside of submission loop] Processing new event...");
            processed_events += 1;
        }

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
        // std::cout << "Mean runtime (s) : " << this->preload_header->mean_runtime_s << std::endl;
        // std::cout << "Median runtime (s) : " << this->preload_header->median_runtime_s << std::endl;
        // std::cout << "Runtime var : " << this->preload_header->var_runtime_s << std::endl;
        // std::cout << "Max runtime (s) : " << this->preload_header->max_runtime_s << std::endl;

        std::vector<int> rand_runtimes_s;
        std::lognormal_distribution<> dr(
            std::log(this->preload_header->mean_runtime_s / std::sqrt(this->preload_header->var_runtime_s / std::pow(this->preload_header->mean_runtime_s, 2) + 1)),
            std::sqrt(std::log((this->preload_header->var_runtime_s / std::pow(this->preload_header->mean_runtime_s, 2)) + 1)));
        while (rand_runtimes_s.size() != preloadJobsCount) {
            auto val = std::floor(dr(gen));
            if (val <= this->preload_header->max_runtime_s) {
                rand_runtimes_s.push_back(val);
                // std::cout << "Adding " << val << " to runtimes" << std::endl;
            }
        }

        // Interval between jobs :
        // std::cout << "Mean interval (s) : " << this->preload_header->mean_interval_s << std::endl;
        // std::cout << "Median interval (s) : " << this->preload_header->median_interval_s << std::endl;
        // std::cout << "Interval var : " << this->preload_header->var_interval_s << std::endl;
        // std::cout << "Max interval (s) : " << this->preload_header->max_interval_s << std::endl;

        std::vector<int> rand_intervals_s;
        std::lognormal_distribution<> di(
            std::log(this->preload_header->mean_interval_s / std::sqrt(this->preload_header->var_interval_s / std::pow(this->preload_header->mean_interval_s, 2) + 1)),
            std::sqrt(std::log((this->preload_header->var_interval_s / std::pow(this->preload_header->mean_interval_s, 2)) + 1)));
        while (rand_intervals_s.size() != preloadJobsCount) {
            auto val = std::floor(di(gen));
            if (val <= this->preload_header->max_interval_s) {
                rand_intervals_s.push_back(val);
                // std::cout << "Adding " << val << " to intervals" << std::endl;
            }
        }

        // Nodes used:
        // std::cout << "Mean nodes used : " << this->preload_header->mean_nodes_used << std::endl;
        // std::cout << "Median nodes used : " << this->preload_header->median_nodes_used << std::endl;
        // std::cout << "Nodes used var : " << this->preload_header->var_nodes_used << std::endl;
        // std::cout << "Max nodes used : " << this->preload_header->max_nodes_used << std::endl;

        std::vector<int> rand_nodes_count;
        std::lognormal_distribution<> dn(
            std::log(this->preload_header->mean_nodes_used / std::sqrt(this->preload_header->var_nodes_used / std::pow(this->preload_header->mean_nodes_used, 2) + 1)),
            std::sqrt(std::log((this->preload_header->var_nodes_used / std::pow(this->preload_header->mean_nodes_used, 2)) + 1)));
        while (rand_nodes_count.size() != preloadJobsCount) {
            auto val = std::floor(dn(gen));
            if (val <= this->preload_header->max_nodes_used) {
                rand_nodes_count.push_back(val);
                // std::cout << "Adding " << val << " to nodes count" << std::endl;
            }
        }

        // Bytes READ
        // std::cout << "Mean terabytes read : " << this->preload_header->mean_read_tbytes << std::endl;
        // std::cout << "Median terabytes read : " << this->preload_header->median_read_tbytes << std::endl;
        // std::cout << "Var terabytes read : " << this->preload_header->var_read_tbytes << std::endl;
        // std::cout << "Max terabytes read : " << this->preload_header->max_read_tbytes << std::endl;

        std::vector<double> rand_read_tbytes;
        std::lognormal_distribution<> drb(
            std::log(this->preload_header->mean_read_tbytes / std::sqrt(this->preload_header->var_read_tbytes / std::pow(this->preload_header->mean_read_tbytes, 2) + 1)),
            std::sqrt(std::log((this->preload_header->var_read_tbytes / std::pow(this->preload_header->mean_read_tbytes, 2)) + 1)));
        while (rand_read_tbytes.size() != preloadJobsCount) {
            auto val = drb(gen);
            if (val <= this->preload_header->max_read_tbytes) {
                rand_read_tbytes.push_back(val);
                // std::cout << "Adding " << val << " to read terabytes" << std::endl;
            }
        }

        // Bytes WRITTEN
        // std::cout << "Mean terabytes written : " << this->preload_header->mean_written_tbytes << std::endl;
        // std::cout << "Median terabytes written : " << this->preload_header->median_written_tbytes << std::endl;
        // std::cout << "Var terabytes written : " << this->preload_header->var_written_tbytes << std::endl;
        // std::cout << "Max terabytes written : " << this->preload_header->max_written_tbytes << std::endl;

        std::vector<double> rand_written_tbytes;
        std::lognormal_distribution<> dwb(
            std::log(this->preload_header->mean_written_tbytes / std::sqrt(this->preload_header->var_written_tbytes / std::pow(this->preload_header->mean_written_tbytes, 2) + 1)),
            std::sqrt(std::log((this->preload_header->var_written_tbytes / std::pow(this->preload_header->mean_written_tbytes, 2)) + 1)));
        while (rand_written_tbytes.size() != preloadJobsCount) {
            auto val = dwb(gen);
            if (val <= this->preload_header->max_written_tbytes) {
                rand_written_tbytes.push_back(val);
                // std::cout << "Adding " << val << " to written terabytes" << std::endl;
            }
        }

        auto cores_per_node = this->compute_service->getPerHostNumCores().begin()->second;
        // std::cout << "Using " << cores_per_node << " cores for each reserved node" << std::endl;
        std::vector<storalloc::YamlJob> preload_jobs;
        unsigned int i = 0;
        while (i < preloadJobsCount) {
            storalloc::YamlJob job = {};

            auto read_bytes = rand_read_tbytes[i] * 1'000'000'000'000;
            auto written_bytes = rand_written_tbytes[i] * 1'000'000'000'000;

            job.approxComputeTimeSeconds = rand_runtimes_s[i] * 0.9;
            job.coreHoursReq = 0;                             // not used
            job.coreHoursUsed = 0;                            // not used
            job.endTime = "NA";                               // not used
            job.id = "preload_" + std::to_string(i);          // used to filter out jobs in the end.
            job.metaTimeSeconds = 0;                          // not used
            job.model = storalloc::JobType::ReadComputeWrite; // Not bothering using != types of jobs
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

            preload_jobs.push_back(job);
            i++;
        }

        WRENCH_INFO("%u preload jobs created", preloadJobsCount);

        return preload_jobs;
    }

    void Controller::submitJob(std::string jobID) {

        auto yJob = this->jobsWithPreload[jobID];
        auto parentJob = this->job_manager->createCompoundJob(yJob.id);
        // Save job for future analysis (note : we'll need to find sub jobs one by one by ID)
        this->compound_jobs[yJob.id] = std::make_pair(yJob, std::vector<std::shared_ptr<wrench::CompoundJob>>());
        this->compound_jobs[yJob.id].second.push_back(parentJob);
        WRENCH_INFO("[%s] Preparing parent job of type %d for submission", yJob.id.c_str(), yJob.model);

        parentJob->addCustomAction(
            "parentJob" + yJob.id,
            0, 0, // RAM & num cores
            [this, jobID](const std::shared_ptr<wrench::ActionExecutor> &action_executor) {
                auto internalJobManager = action_executor->createJobManager();

                unsigned int nodes_nb_read = std::ceil(this->compound_jobs[jobID].first.nodesUsed * this->config->stor.io_read_node_ratio) + 1;
                nodes_nb_read = std::min(
                    std::min(nodes_nb_read, this->config->stor.max_read_node_cnt),
                    this->compound_jobs[jobID].first.nodesUsed);
                WRENCH_DEBUG(" - [%s] : %u nodes will be doing read IOs", jobID.c_str(), nodes_nb_read);
                unsigned int nodes_nb_write = std::ceil(this->compound_jobs[jobID].first.nodesUsed * this->config->stor.io_write_node_ratio) + 1;
                nodes_nb_write = std::min(
                    std::min(nodes_nb_write, this->config->stor.max_write_node_cnt),
                    this->compound_jobs[jobID].first.nodesUsed);
                WRENCH_DEBUG(" - [%s] : %u nodes will be doing write IOs", jobID.c_str(), nodes_nb_write);

                if (this->compound_jobs[jobID].first.model == storalloc::JobType::ReadComputeWrite) {

                    auto input_data = this->copyFromPermanent(action_executor, internalJobManager, this->compound_jobs[jobID],
                                                              this->config->stor.nb_files_per_read, nodes_nb_read);
                    this->readFromTemporary(action_executor, internalJobManager, this->compound_jobs[jobID], input_data, nodes_nb_read);
                    this->compute(action_executor, internalJobManager, this->compound_jobs[jobID]);
                    auto output_data = this->writeToTemporary(action_executor, internalJobManager, this->compound_jobs[jobID], this->config->stor.nb_files_per_write, nodes_nb_write);
                    this->copyToPermanent(action_executor, internalJobManager, this->compound_jobs[jobID], output_data, nodes_nb_write);
                    this->cleanupInput(action_executor, internalJobManager, this->compound_jobs[jobID], input_data);
                    this->cleanupOutput(action_executor, internalJobManager, this->compound_jobs[jobID], output_data);

                } else if (this->compound_jobs[jobID].first.model == storalloc::JobType::ComputeWrite) {

                    this->compute(action_executor, internalJobManager, this->compound_jobs[jobID]);
                    auto output_data = this->writeToTemporary(action_executor, internalJobManager, this->compound_jobs[jobID], this->config->stor.nb_files_per_write, nodes_nb_write);
                    this->copyToPermanent(action_executor, internalJobManager, this->compound_jobs[jobID], output_data, nodes_nb_write);
                    this->cleanupOutput(action_executor, internalJobManager, this->compound_jobs[jobID], output_data);

                } else if (this->compound_jobs[jobID].first.model == storalloc::JobType::ReadCompute) {

                    auto input_data = this->copyFromPermanent(action_executor, internalJobManager, this->compound_jobs[jobID],
                                                              this->config->stor.nb_files_per_read, nodes_nb_read);
                    this->readFromTemporary(action_executor, internalJobManager, this->compound_jobs[jobID], input_data, nodes_nb_read);
                    this->compute(action_executor, internalJobManager, this->compound_jobs[jobID]);
                    this->cleanupInput(action_executor, internalJobManager, this->compound_jobs[jobID], input_data);

                } else if (this->compound_jobs[jobID].first.model == storalloc::JobType::Compute) {

                    this->compute(action_executor, internalJobManager, this->compound_jobs[jobID]);

                } else {

                    throw std::runtime_error("Unknown job model for job " + jobID);
                }
            },
            [jobID](std::shared_ptr<wrench::ActionExecutor> action_executor) {
                WRENCH_INFO(" >> [parent customAction_%s] terminating", jobID.c_str());
            });

        // Submit job
        std::map<std::string, std::string> service_specific_args =
            {{"-N", std::to_string(yJob.nodesUsed)},                                                             // nb of nodes
             {"-c", std::to_string(yJob.coresUsed / yJob.nodesUsed)},                                            // cores per node
             {"-t", std::to_string(static_cast<int>(yJob.walltimeSeconds * this->config->walltime_extension))}}; // seconds
        WRENCH_INFO("[%s] Submitting parent job (%d nodes, %d cores per node, %ds of walltime)",
                    parentJob->getName().c_str(),
                    yJob.nodesUsed, yJob.coresUsed / yJob.nodesUsed, yJob.walltimeSeconds);
        job_manager->submitJob(parentJob, this->compute_service, service_specific_args);
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
            auto file_name = prefix_name + "_sub" + std::to_string(i);
            files.push_back(wrench::Simulation::addFile(file_name, fileSize));
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
    std::vector<std::shared_ptr<wrench::DataFile>> Controller::copyFromPermanent(std::shared_ptr<wrench::ActionExecutor> action_executor,
                                                                                 std::shared_ptr<wrench::JobManager> internalJobManager,
                                                                                 std::pair<YamlJob, std::vector<std::shared_ptr<wrench::CompoundJob>>> &jobPair,
                                                                                 unsigned int nb_files, unsigned int max_nb_hosts) {

        WRENCH_INFO("[%s] Creating copy sub-job (in) for a total read size of %ld bytes, on %u files and at most %u IO nodes (0 means all avail)",
                    jobPair.first.id.c_str(), jobPair.first.readBytes, nb_files, max_nb_hosts);

        if (nb_files < 1)
            throw std::runtime_error("Copy should happen on 1 file at least");

        auto actionExecutorService = action_executor->getActionExecutionService();
        auto bare_metal = std::dynamic_pointer_cast<wrench::BareMetalComputeService>(actionExecutorService->getParentService());
        auto computeResources = bare_metal->getPerHostNumCores();
        auto copyJob = internalJobManager->createCompoundJob(jobPair.first.id + "_copyFromPermanent");
        jobPair.second.push_back(copyJob);

        // Subdivide the amount of copied bytes between as many files as requested (one allocation per file)
        auto prefix = "input_data_file_" + copyJob->getName();
        auto read_files = this->createFileParts(jobPair.first.readBytes, nb_files, prefix);

        // Randomly remove nodes from resources in order to never use more than 'max_nb_hosts'
        this->pruneIONodes(computeResources, max_nb_hosts);

        // Create one copy action per file and per host
        std::map<std::string, std::string> service_specific_args = {};
        auto computeResourcesIt = computeResources.begin();

        for (const auto &read_file : read_files) {
            auto source_location = wrench::FileLocation::LOCATION(this->storage_service, this->config->pstor.mount_prefix + "/" + this->config->pstor.read_path, read_file);
            wrench::StorageService::createFileAtLocation(source_location);
            auto action_id = "stagingCopy_" + jobPair.first.id + "_f" + read_file->getID();
            copyJob->addFileCopyAction(
                action_id,
                source_location,
                wrench::FileLocation::LOCATION(this->compound_storage_service, read_file));

            service_specific_args[action_id] = computeResourcesIt->first;
            if (++computeResourcesIt == computeResources.end())
                computeResourcesIt = computeResources.begin();
        }

        internalJobManager->submitJob(copyJob, bare_metal, service_specific_args);
        WRENCH_INFO("copyFromPermanent: job submitted with %lu actions on bare_metal %s", read_files.size(), bare_metal->getName().c_str());
        if (not std::dynamic_pointer_cast<wrench::CompoundJobCompletedEvent>(action_executor->waitForNextEvent()))
            throw std::runtime_error("Sub-job 'copy from permanent' " + copyJob->getName() + " failed");

        WRENCH_INFO("[%s] CopyFrom job executed with %lu actions", copyJob->getName().c_str(), copyJob->getActions().size());

        return read_files;
    }

    void Controller::readFromTemporary(const std::shared_ptr<wrench::ActionExecutor> &action_executor,
                                       const std::shared_ptr<wrench::JobManager> &internalJobManager,
                                       std::pair<YamlJob, std::vector<std::shared_ptr<wrench::CompoundJob>>> &jobPair,
                                       std::vector<std::shared_ptr<wrench::DataFile>> inputs,
                                       unsigned int max_nb_hosts) {

        WRENCH_INFO("[%s] Creating read sub-job for  %lu file(s) with cumulative size %ld bytes, using %u IO nodes (0 means all avail)", jobPair.first.id.c_str(), inputs.size(), jobPair.first.readBytes, max_nb_hosts);

        auto actionExecutorService = action_executor->getActionExecutionService();
        auto bare_metal = std::dynamic_pointer_cast<wrench::BareMetalComputeService>(actionExecutorService->getParentService());
        auto computeResources = bare_metal->getPerHostNumCores();
        auto readJob = internalJobManager->createCompoundJob(jobPair.first.id + "_readFiles");
        jobPair.second.push_back(readJob);

        if (max_nb_hosts == 0)
            max_nb_hosts = computeResources.size();

        std::map<std::shared_ptr<wrench::DataFile>, unsigned int> stripes_per_file{};
        std::map<std::shared_ptr<wrench::DataFile>, std::vector<unsigned int>> stripes_per_host_per_file{};
        for (const auto &read_file : inputs) {
            // We manually invoke this in order to know how the files will be striped before starting partial writes.
            WRENCH_DEBUG("[%s] Calling lookupFileLocation for file %s", jobPair.first.id.c_str(), read_file->getID().c_str());
            auto file_stripes = this->compound_storage_service->lookupFileLocation(wrench::FileLocation::LOCATION(this->compound_storage_service, read_file));

            unsigned int nb_stripes = file_stripes.size();
            stripes_per_file[read_file] = nb_stripes;
            stripes_per_host_per_file[read_file] = std::vector<unsigned int>();

            if (nb_stripes < max_nb_hosts) {
                for (unsigned int i = 0; i < nb_stripes; i++) {
                    stripes_per_host_per_file[read_file].push_back(1);
                }
                WRENCH_DEBUG("[%s] readFromTemporary: For file %s (size %f) : we have only %u stripes, but %u hosts, reading 1 stripes from the first %u hosts",
                             jobPair.first.id.c_str(), read_file->getID().c_str(), read_file->getSize(), nb_stripes, max_nb_hosts, nb_stripes);
            } else {
                unsigned int stripes_per_host = std::floor(nb_stripes / max_nb_hosts);
                unsigned int remainder = nb_stripes % max_nb_hosts;
                for (unsigned int i = 0; i < max_nb_hosts; i++) {
                    stripes_per_host_per_file[read_file].push_back(stripes_per_host);
                }
                for (unsigned int i = 0; i < remainder; i++) {
                    stripes_per_host_per_file[read_file][i] += 1;
                }
                WRENCH_DEBUG("[%s] readFromTemporary: For file %s (size %f) : %u stripes, reading %u stripes from each host and last host writes %u stripes",
                             jobPair.first.id.c_str(), read_file->getID().c_str(), read_file->getSize(), nb_stripes, stripes_per_host, stripes_per_host + remainder);
            }
        }

        // Randomly remove nodes from resources in order to never use more than 'max_nb_hosts'
        this->pruneIONodes(computeResources, max_nb_hosts);

        auto computeResourcesIt = computeResources.begin();
        unsigned int action_cnt = 0;
        std::map<std::string, std::string> service_specific_args = {};
        for (const auto &read_file : inputs) {

            auto stripe_size = read_file->getSize() / stripes_per_file[read_file];

            for (const auto &stripes_per_host : stripes_per_host_per_file[read_file]) {
                WRENCH_DEBUG("[%s] readFromTemporary: Creating read action for file %s and host %s", jobPair.first.id.c_str(), read_file->getID().c_str(), computeResourcesIt->first.c_str());

                auto action_id = "fRead_f" + read_file->getID() + "_" + computeResourcesIt->first + "_act" + std::to_string(action_cnt);
                auto read_byte_per_node = stripe_size * stripes_per_host;
                WRENCH_DEBUG("[%s] readFromTemporary:   We'll be reading %d stripes from this host, for a total of %f bytes from this host", jobPair.first.id.c_str(), stripes_per_host, read_byte_per_node);

                readJob->addFileReadAction(action_id, wrench::FileLocation::LOCATION(this->compound_storage_service, read_file), read_byte_per_node);
                action_cnt++;

                service_specific_args[action_id] = computeResourcesIt->first;
                if (++computeResourcesIt == computeResources.end())
                    computeResourcesIt = computeResources.begin();
            }
        }

        internalJobManager->submitJob(readJob, bare_metal, service_specific_args);
        WRENCH_INFO("[%s] readFromTemporary: job submitted with %lu actions on bare_metal %s", jobPair.first.id.c_str(), inputs.size() * max_nb_hosts, bare_metal->getName().c_str());
        if (not std::dynamic_pointer_cast<wrench::CompoundJobCompletedEvent>(action_executor->waitForNextEvent()))
            throw std::runtime_error("Sub-job 'read from CSS' " + readJob->getName() + " failed");

        WRENCH_INFO("[%s] Read job executed with %lu actions", readJob->getName().c_str(), readJob->getActions().size());
    }

    void Controller::compute(const std::shared_ptr<wrench::ActionExecutor> &action_executor,
                             const std::shared_ptr<wrench::JobManager> &internalJobManager,
                             std::pair<YamlJob, std::vector<std::shared_ptr<wrench::CompoundJob>>> &jobPair) {

        WRENCH_INFO("[%s] Creating compute sub-job", jobPair.first.id.c_str());

        auto actionExecutorService = action_executor->getActionExecutionService();
        auto bare_metal = std::dynamic_pointer_cast<wrench::BareMetalComputeService>(actionExecutorService->getParentService());
        auto computeResources = bare_metal->getPerHostNumCores();
        auto computeJob = internalJobManager->createCompoundJob(jobPair.first.id + "_compute");
        jobPair.second.push_back(computeJob);

        // Random values (flops, ram, ...) to be adjusted.
        // We could start from the theoreticak peak performance of modelled platform, and specify the flops based on the
        // % of available compute resources used by the job and its execution time (minor a factor of the time spend in IO ?)
        auto cores_per_node = jobPair.first.coresUsed / jobPair.first.nodesUsed;

        // Create one compute action per node // OR setup the sched to simulate compute as sleep and use the job's compute time for the sleep.
        std::map<std::string, std::string> service_specific_args = {};
        auto computeResourcesIt = computeResources.begin();
        auto action_id = "compute_" + jobPair.first.id + "_1";
        computeJob->addComputeAction(
            action_id,
            this->flopRate * jobPair.first.approxComputeTimeSeconds,
            this->config->compute.ram * GBYTE, // Not used
            cores_per_node, cores_per_node,
            wrench::ParallelModel::AMDAHL(this->config->amdahl));
        service_specific_args[action_id] = (computeResourcesIt++)->first;

        internalJobManager->submitJob(computeJob, bare_metal); // , service_specific_args);
        WRENCH_INFO("[%s] compute: job submitted on bare_metal %s", jobPair.first.id.c_str(), bare_metal->getName().c_str());
        if (not std::dynamic_pointer_cast<wrench::CompoundJobCompletedEvent>(action_executor->waitForNextEvent()))
            throw std::runtime_error("Sub-job 'compute' " + computeJob->getName() + " failed");

        WRENCH_INFO("[%s] Compute job executed with %lu actions", computeJob->getName().c_str(), computeJob->getActions().size());
    }

    std::vector<std::shared_ptr<wrench::DataFile>> Controller::writeToTemporary(const std::shared_ptr<wrench::ActionExecutor> &action_executor,
                                                                                const std::shared_ptr<wrench::JobManager> &internalJobManager,
                                                                                std::pair<YamlJob, std::vector<std::shared_ptr<wrench::CompoundJob>>> &jobPair,
                                                                                unsigned int nb_files, unsigned int max_nb_hosts) {

        WRENCH_INFO("[%s] Creating write sub-job with size %ld bytes, on %u files and at most %u IO nodes (0 means all avail)",
                    jobPair.first.id.c_str(), jobPair.first.writtenBytes, nb_files, max_nb_hosts);

        auto actionExecutorService = action_executor->getActionExecutionService();
        auto bare_metal = std::dynamic_pointer_cast<wrench::BareMetalComputeService>(actionExecutorService->getParentService());
        auto computeResources = bare_metal->getPerHostNumCores();
        auto writeJob = internalJobManager->createCompoundJob(jobPair.first.id + "_writeFiles");
        jobPair.second.push_back(writeJob);

        if (nb_files < 1) {
            throw std::runtime_error("writeToTemporary: At least one host is needed to perform a write");
        }
        if (max_nb_hosts == 0) {
            max_nb_hosts = computeResources.size();
        }

        // Subdivide the amount of written bytes between as many files as requested (one allocation per file)
        auto prefix = "output_data_file_" + writeJob->getName();
        auto write_files = this->createFileParts(jobPair.first.writtenBytes, nb_files, prefix);

        // Compute what should be written by each host to each file (including remainder if values are not round)
        std::map<std::shared_ptr<wrench::DataFile>, unsigned int> stripes_per_file{};
        std::map<std::shared_ptr<wrench::DataFile>, std::vector<unsigned int>> stripes_per_host_per_file{};

        for (auto write_file : write_files) {
            // We manually invoke this in order to know how the files will be striped before starting partial writes.
            WRENCH_DEBUG("Calling lookupOrDesignate for file %s", write_file->getID().c_str());
            auto file_stripes = this->compound_storage_service->lookupOrDesignateStorageService(wrench::FileLocation::LOCATION(this->compound_storage_service, write_file));

            unsigned int nb_stripes = file_stripes.size();
            stripes_per_file[write_file] = nb_stripes;
            stripes_per_host_per_file[write_file] = std::vector<unsigned int>();

            if (nb_stripes < max_nb_hosts) {
                for (unsigned int i = 0; i < nb_stripes; i++) {
                    stripes_per_host_per_file[write_file].push_back(1);
                }
                WRENCH_DEBUG("[%s] writeToTemporary: For file %s (size %f) : we have only %u stripes, but %u hosts, writing 1 stripes from the first %u hosts",
                             jobPair.first.id.c_str(), write_file->getID().c_str(), write_file->getSize(), nb_stripes, max_nb_hosts, nb_stripes);
            } else {
                unsigned int stripes_per_host = std::floor(nb_stripes / max_nb_hosts);
                unsigned int remainder = nb_stripes % max_nb_hosts;
                for (unsigned int i = 0; i < max_nb_hosts; i++) {
                    stripes_per_host_per_file[write_file].push_back(stripes_per_host);
                }
                for (unsigned int i = 0; i < remainder; i++) {
                    stripes_per_host_per_file[write_file][i] += 1;
                }
                WRENCH_DEBUG("[%s] writeToTemporary: For file %s (size %f) : %u stripes, writing %u stripes from each host and last host writes %u stripes",
                             jobPair.first.id.c_str(), write_file->getID().c_str(), write_file->getSize(), nb_stripes, stripes_per_host, stripes_per_host + remainder);
            }
        }

        // DEBUG : AT THIS POINT, THE CORRECT STORAGE SPACE IS RESERVED ONTO THE SELECTED NODES

        // Randomly remove nodes from resources in order to never use more than 'max_nb_hosts'
        this->pruneIONodes(computeResources, max_nb_hosts);

        // Create one write action per file (each one will run on one host, unless there are more actions than 'max_nb_hosts',
        // in which case multiple actions can be scheduled on the same host)
        auto computeResourcesIt = computeResources.begin();
        unsigned int action_cnt = 0;
        std::map<std::string, std::string> service_specific_args = {};
        for (auto &write_file : write_files) {

            auto stripe_size = write_file->getSize() / stripes_per_file[write_file];

            for (const auto &stripes_per_host : stripes_per_host_per_file[write_file]) {
                WRENCH_DEBUG("[%s] writeToTemporary: Creating custom write action for file %s and host %s", jobPair.first.id.c_str(), write_file->getID().c_str(), computeResourcesIt->first.c_str());

                auto action_id = "fWrite_" + write_file->getID() + "_" + computeResourcesIt->first + "_act" + std::to_string(action_cnt);
                auto write_byte_per_node = stripe_size * stripes_per_host;
                WRENCH_DEBUG("[%s] writeToTemporary:   We'll be writing %d stripes from this host, for a total of %f bytes from this host", jobPair.first.id.c_str(), stripes_per_host, write_byte_per_node);

                auto customWriteAction = std::make_shared<PartialWriteCustomAction>(
                    action_id, 0, 0,
                    [this, write_file, write_byte_per_node](const std::shared_ptr<wrench::ActionExecutor> &action_executor) {
                        this->compound_storage_service->writeFile(S4U_Daemon::getRunningActorRecvMailbox(),
                                                                  wrench::FileLocation::LOCATION(this->compound_storage_service, write_file),
                                                                  write_byte_per_node,
                                                                  true);
                    },
                    [action_id, write_byte_per_node, jobPair](std::shared_ptr<wrench::ActionExecutor> executor) {
                        WRENCH_DEBUG("[%s] writeToTemporary: %s terminating - wrote %f", jobPair.first.id.c_str(), action_id.c_str(), write_byte_per_node);
                    },
                    write_file, write_byte_per_node);
                writeJob->addCustomAction(customWriteAction);
                action_cnt++;

                // One copy per compute node, looping over if there are more files than nodes
                service_specific_args[action_id] = computeResourcesIt->first;
                if (++computeResourcesIt == computeResources.end())
                    computeResourcesIt = computeResources.begin();
            }
        }

        internalJobManager->submitJob(writeJob, bare_metal, service_specific_args);
        WRENCH_INFO("[%s] writeToTemporary: job submitted with %lu actions on bare_metal %s", jobPair.first.id.c_str(), write_files.size() * max_nb_hosts, bare_metal->getName().c_str());
        auto nextEvent = action_executor->waitForNextEvent();
        if (not std::dynamic_pointer_cast<wrench::CompoundJobCompletedEvent>(nextEvent)) {
            auto failedEvent = std::dynamic_pointer_cast<wrench::CompoundJobFailedEvent>(nextEvent);
            for (const auto &action : writeJob->getActions()) {
                if (action->getState() == wrench::Action::FAILED) {
                    WRENCH_WARN("Action failure cause : %s", action->getFailureCause()->toString().c_str());
                }
            }
            WRENCH_WARN("Failed event cause : %s", failedEvent->failure_cause->toString().c_str());
            throw std::runtime_error("Sub-job 'write to CSS' " + writeJob->getName() + " failed");
        }
        WRENCH_INFO("[%s] Write job executed with %lu actions", writeJob->getName().c_str(), writeJob->getActions().size());
        return write_files;
    }

    void Controller::copyToPermanent(const std::shared_ptr<wrench::ActionExecutor> &action_executor,
                                     const std::shared_ptr<wrench::JobManager> &internalJobManager,
                                     std::pair<YamlJob, std::vector<std::shared_ptr<wrench::CompoundJob>>> &jobPair,
                                     std::vector<std::shared_ptr<wrench::DataFile>> outputs,
                                     unsigned int nb_hosts) {

        WRENCH_INFO("[%s] Creating copy sub-job (out) %lu file(s) with cumulative size %ld bytes, using %u IO nodes (0 means all avail)",
                    jobPair.first.id.c_str(), outputs.size(), jobPair.first.writtenBytes, nb_hosts);

        auto actionExecutorService = action_executor->getActionExecutionService();
        auto bare_metal = std::dynamic_pointer_cast<wrench::BareMetalComputeService>(actionExecutorService->getParentService());
        auto computeResources = bare_metal->getPerHostNumCores();
        auto copyJob = internalJobManager->createCompoundJob(jobPair.first.id + "_copyToPermanent");
        jobPair.second.push_back(copyJob);

        if (nb_hosts == 0) {
            nb_hosts = computeResources.size();
        }

        // Randomly remove nodes from resources in order to never use more than 'max_nb_hosts'
        this->pruneIONodes(computeResources, nb_hosts);

        /* Note here if a write operations was previously sliced between n compute hosts, we virtually created n pseudo files,
         * and we're now goind to run the copy from n compute hosts once again. In future works, we might want to differentiate
         * the number of hosts used in the write operation and the number of hosts used in the following copy.
         */
        std::map<std::string, std::string> service_specific_args = {};
        auto computeResourcesIt = computeResources.begin();
        for (const auto &output_data : outputs) {
            auto action_id = "archiveCopy_" + jobPair.first.id + "_f" + output_data->getID();
            auto archiveAction = copyJob->addFileCopyAction(
                action_id,
                wrench::FileLocation::LOCATION(this->compound_storage_service, output_data),
                wrench::FileLocation::LOCATION(this->storage_service, this->config->pstor.mount_prefix + "/" + this->config->pstor.write_path, output_data));

            service_specific_args[action_id] = computeResourcesIt->first;
            if (++computeResourcesIt == computeResources.end())
                computeResourcesIt = computeResources.begin();
        }

        internalJobManager->submitJob(copyJob, bare_metal, service_specific_args);
        WRENCH_INFO("[%s] copyToPermanent: job submitted with %lu actions on bare_metal %s", jobPair.first.id.c_str(), outputs.size(), bare_metal->getName().c_str());
        if (not std::dynamic_pointer_cast<wrench::CompoundJobCompletedEvent>(action_executor->waitForNextEvent()))
            throw std::runtime_error("Sub-job 'copy to permanent' " + copyJob->getName() + " failed");
        WRENCH_INFO("[%s] CopyTo job executed with %lu actions", copyJob->getName().c_str(), copyJob->getActions().size());
    }

    void Controller::cleanupInput(const std::shared_ptr<wrench::ActionExecutor> &action_executor,
                                  const std::shared_ptr<wrench::JobManager> &internalJobManager,
                                  std::pair<YamlJob, std::vector<std::shared_ptr<wrench::CompoundJob>>> &jobPair,
                                  std::vector<std::shared_ptr<wrench::DataFile>> inputs) {

        WRENCH_INFO("[%s] Creating cleanup actions for input file(s) with cumulative size %ld", jobPair.first.id.c_str(), jobPair.first.readBytes);

        auto actionExecutorService = action_executor->getActionExecutionService();
        auto bare_metal = std::dynamic_pointer_cast<wrench::BareMetalComputeService>(actionExecutorService->getParentService());
        auto computeResources = bare_metal->getPerHostNumCores();
        auto cleanupJob = internalJobManager->createCompoundJob(jobPair.first.id + "_cleanupInputFiles");
        jobPair.second.push_back(cleanupJob);

        std::map<std::string, std::string> service_specific_args = {};
        auto computeResourcesIt = computeResources.begin();
        auto action_cnt = 0;
        for (const auto &input_data : inputs) {
            auto del_id_1 = "delRF_" + jobPair.first.id + "_" + std::to_string(action_cnt);
            auto del_id_2 = "delERF_" + jobPair.first.id + "_" + std::to_string(action_cnt);
            auto deleteReadAction = cleanupJob->addFileDeleteAction(del_id_1, wrench::FileLocation::LOCATION(this->compound_storage_service, input_data));
            auto deleteExternalReadAction = cleanupJob->addFileDeleteAction(del_id_2, wrench::FileLocation::LOCATION(this->storage_service, this->config->pstor.mount_prefix + "/" + this->config->pstor.read_path, input_data));
            cleanupJob->addActionDependency(deleteReadAction, deleteExternalReadAction);
            if (computeResourcesIt == computeResources.end()) {
                computeResourcesIt = computeResources.begin();
            }
            service_specific_args[del_id_1] = computeResourcesIt->first;
            if (computeResourcesIt == computeResources.end()) {
                computeResourcesIt = computeResources.begin();
            }
            service_specific_args[del_id_2] = computeResourcesIt->first;
            action_cnt++;
        }

        internalJobManager->submitJob(cleanupJob, bare_metal, {});
        WRENCH_INFO("[%s] CleanupInput: inputCleanUp job submitted with %lu actions on bare_metal %s", jobPair.first.id.c_str(), inputs.size() * 2, bare_metal->getName().c_str());
        if (not std::dynamic_pointer_cast<wrench::CompoundJobCompletedEvent>(action_executor->waitForNextEvent()))
            throw std::runtime_error("Sub-job 'cleanup input(s)' " + cleanupJob->getName() + " failed");
        WRENCH_INFO("[%s] CleanupInput job executed with %lu actions", cleanupJob->getName().c_str(), cleanupJob->getActions().size());
    }

    void Controller::cleanupOutput(const std::shared_ptr<wrench::ActionExecutor> &action_executor,
                                   const std::shared_ptr<wrench::JobManager> &internalJobManager,
                                   std::pair<YamlJob, std::vector<std::shared_ptr<wrench::CompoundJob>>> &jobPair,
                                   std::vector<std::shared_ptr<wrench::DataFile>> outputs) {

        WRENCH_INFO("[%s] Creating cleanup actions for output file(s) with cumulative size %ld", jobPair.first.id.c_str(), jobPair.first.writtenBytes);

        auto actionExecutorService = action_executor->getActionExecutionService();
        auto bare_metal = std::dynamic_pointer_cast<wrench::BareMetalComputeService>(actionExecutorService->getParentService());
        auto computeResources = bare_metal->getPerHostNumCores();
        auto cleanupJob = internalJobManager->createCompoundJob(jobPair.first.id + "_cleanupOutputFiles");
        jobPair.second.push_back(cleanupJob);

        std::map<std::string, std::string> service_specific_args = {};
        auto computeResourcesIt = computeResources.begin();
        auto action_cnt = 0;
        for (const auto &output_data : outputs) {
            auto del_id_1 = "delWF_" + jobPair.first.id + "_" + std::to_string(action_cnt);
            auto del_id_2 = "delEWF_" + jobPair.first.id + "_" + std::to_string(action_cnt);
            auto deleteWriteAction = cleanupJob->addFileDeleteAction(del_id_1, wrench::FileLocation::LOCATION(this->compound_storage_service, output_data));
            auto deleteExternalWriteAction = cleanupJob->addFileDeleteAction(del_id_2, wrench::FileLocation::LOCATION(this->storage_service, this->config->pstor.mount_prefix + "/" + this->config->pstor.write_path, output_data));
            cleanupJob->addActionDependency(deleteWriteAction, deleteExternalWriteAction);
            if (computeResourcesIt == computeResources.end()) {
                computeResourcesIt = computeResources.begin();
            }
            service_specific_args[del_id_1] = computeResourcesIt->first;
            if (computeResourcesIt == computeResources.end()) {
                computeResourcesIt = computeResources.begin();
            }
            service_specific_args[del_id_2] = computeResourcesIt->first;
            action_cnt++;
        }

        internalJobManager->submitJob(cleanupJob, bare_metal, {});
        WRENCH_INFO("[%s] cleanupOutput: outputCleanUp job submitted with %lu actions on bare_metal %s", jobPair.first.id.c_str(), outputs.size() * 2, bare_metal->getName().c_str());
        if (not std::dynamic_pointer_cast<wrench::CompoundJobCompletedEvent>(action_executor->waitForNextEvent()))
            throw std::runtime_error("Sub-job 'cleanup output(s)' " + cleanupJob->getName() + " failed");
        WRENCH_INFO("[%s] CleanupOutput job executed with %lu actions", cleanupJob->getName().c_str(), cleanupJob->getActions().size());
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

    void Controller::processActions(YAML::Emitter &out_jobs, YAML::Emitter &out_actions, const std::set<std::shared_ptr<wrench::Action>> &actions, double &job_start_time) {

        for (const auto &action : actions) {

            if (action->getState() != wrench::Action::COMPLETED) {
                WRENCH_WARN("Action %s is in state %s", action->getName().c_str(), action->getStateAsString().c_str());
                throw std::runtime_error("Action is not in COMPLETED state");
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

            job_start_time = min(job_start_time, action->getStartDate());

            // WRENCH_DEBUG("  - Action type : %s at ts: %f", act_type.c_str(), action->getStartDate());

            if (auto fileRead = std::dynamic_pointer_cast<wrench::FileReadAction>(action)) {

                auto usedLocation = fileRead->getUsedFileLocation();
                auto usedFile = usedLocation->getFile();

                out_jobs << YAML::Key << "file_name" << YAML::Value << usedFile->getID();
                out_jobs << YAML::Key << "file_size_bytes" << YAML::Value << usedFile->getSize();
                out_jobs << YAML::Key << "io_size_bytes" << YAML::Value << fileRead->getNumBytesToRead();

            } else if (auto fileWrite = std::dynamic_pointer_cast<storalloc::PartialWriteCustomAction>(action)) {
                // auto usedLocation = fileWrite->getFileLocation();
                auto usedFile = fileWrite->getFile();

                auto write_trace = this->compound_storage_service->write_traces[usedFile->getID()];
                out_jobs << YAML::Key << "internal_locations" << YAML::Value << write_trace.internal_locations.size();
                out_jobs << YAML::Key << "parts_count" << YAML::Value << write_trace.parts_count;
                out_jobs << YAML::Key << "file_name" << YAML::Value << usedFile->getID();
                out_jobs << YAML::Key << "file_size_bytes" << YAML::Value << usedFile->getSize();
                out_jobs << YAML::Key << "io_size_bytes" << YAML::Value << fileWrite->getWrittenSize();

                for (const auto &trace_loc : write_trace.internal_locations) {
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
                }

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
                out_jobs << YAML::Key << "src_file_path" << YAML::Value << sss->getPath();
                out_jobs << YAML::Key << "src_file_name" << YAML::Value << sss->getFile()->getID();
                out_jobs << YAML::Key << "src_file_size_bytes" << YAML::Value << sss->getFile()->getSize();
                out_jobs << YAML::Key << "css" << YAML::Value << css->getStorageService()->getName();
                out_jobs << YAML::Key << "css_server" << YAML::Value << css->getStorageService()->getHostname();
                out_jobs << YAML::Key << "dst_file_path" << YAML::Value << css->getPath();
                out_jobs << YAML::Key << "dst_file_name" << YAML::Value << css->getFile()->getID();
                out_jobs << YAML::Key << "dst_file_size_bytes" << YAML::Value << css->getFile()->getSize();

                // only record a write if it's from SSS (external permanent storage) to CSS
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
                }

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
                    }
                }
            }
            out_jobs << YAML::EndMap;
        }
    }

    /**
     * @brief Explore the list of completed jobs and prepare two yaml outputs for further analysis :
     *          - The first one, "simulatedJobs_*", includes the full list of jobs and the list of actions for each jobs.
     *          It also contains data about the actual jobs, taken from the original yaml dataset (for easier comparison later)
     *          - The second one contains a list of all actions, with more details on storage services for each action.
     *        Both files can be correlated with the jobs ID.
     */
    void Controller::processCompletedJobs(const std::string &jobsFilename, const std::string &configVersion, const std::string &tag) {

        this->volume_per_storage_service_disk.clear();

        YAML::Emitter out_actions, out_jobs;
        out_actions << YAML::BeginSeq;
        out_jobs << YAML::BeginSeq;

        // Loop through top-level parent CompoundJobs / job-lists
        for (const auto &job_entry : this->compound_jobs) {

            const auto &job_pair = job_entry.second;
            const auto &yaml_job = job_pair.first;
            const auto job_list = job_pair.second;
            const auto &parent_job = job_list[0];

            // Ignore preload jobs in the output results
            if (parent_job->getName().substr(0, 7) == "preload")
                continue;

            out_jobs << YAML::BeginMap; // Job map

            // ## High level 'parent' job informations
            WRENCH_INFO("[%s] Processing metrics...", parent_job->getName().c_str());
            out_jobs << YAML::Key << "job_uid" << YAML::Value << parent_job->getName();
            out_jobs << YAML::Key << "job_status" << YAML::Value << parent_job->getStateAsString();
            out_jobs << YAML::Key << "job_submit_ts" << YAML::Value << parent_job->getSubmitDate();
            out_jobs << YAML::Key << "job_end_ts" << YAML::Value << parent_job->getEndDate();
            out_jobs << YAML::Key << "real_runtime_s" << YAML::Value << yaml_job.runtimeSeconds;
            out_jobs << YAML::Key << "real_read_bytes" << YAML::Value << yaml_job.readBytes;
            out_jobs << YAML::Key << "real_written_bytes" << YAML::Value << yaml_job.writtenBytes;
            out_jobs << YAML::Key << "real_cores_used" << YAML::Value << yaml_job.coresUsed;
            out_jobs << YAML::Key << "real_waiting_time_s" << YAML::Value << yaml_job.waitingTimeSeconds;
            out_jobs << YAML::Key << "real_cReadTime_s" << YAML::Value << yaml_job.readTimeSeconds;
            out_jobs << YAML::Key << "real_cWriteTime_s" << YAML::Value << yaml_job.writeTimeSeconds;
            out_jobs << YAML::Key << "real_cMetaTime_s" << YAML::Value << yaml_job.metaTimeSeconds;
            out_jobs << YAML::Key << "approx_cComputeTime_s" << YAML::Value << yaml_job.approxComputeTimeSeconds;
            out_jobs << YAML::Key << "sim_sleep_time" << YAML::Value << yaml_job.sleepSimulationSeconds;

            // ## Processing actions for all sub jobs related to the current top-level job being processed
            out_jobs << YAML::Key << "actions" << YAML::Value << YAML::BeginSeq;

            // Actual values determined after processing all actions from different sub jobs
            double earliest_action_start_time = UINT64_MAX;
            // Note that we start at ++begin(), to skip the first job (parent) in vector
            for (auto it = ++job_list.begin(); it < job_list.end(); it++) {
                // WRENCH_DEBUG("Actions of job %s : ", it->get()->getName().c_str());
                auto actions = (it->get())->getActions();
                // WRENCH_DEBUG(" ->> %lu", actions.size());
                processActions(out_jobs, out_actions, actions, earliest_action_start_time);
            }

            out_jobs << YAML::EndSeq; // actions

            // ## High level keys that can be updated only after all actions are processed.
            out_jobs << YAML::Key << "job_start_ts" << YAML::Value << earliest_action_start_time;
            out_jobs << YAML::Key << "job_waiting_time_s" << YAML::Value << earliest_action_start_time - parent_job->getSubmitDate();
            out_jobs << YAML::Key << "job_runtime_s" << YAML::Value << parent_job->getEndDate() - earliest_action_start_time;

            out_jobs << YAML::EndMap; // job map
        }
        out_jobs << YAML::EndSeq;
        out_actions << YAML::EndSeq;

        // Write to YAML file
        ofstream simulatedJobs;
        simulatedJobs.open(this->config->out.job_filename_prefix + jobsFilename + "__" + configVersion + "_" + tag + ".yml");
        simulatedJobs << "---\n";
        simulatedJobs << out_jobs.c_str();
        simulatedJobs << "\n...\n";
        simulatedJobs.close();

        // Write to YAML file
        ofstream io_ss_actions;
        io_ss_actions.open(this->config->out.io_actions_prefix + jobsFilename + "__" + configVersion + "_" + tag + ".yml");
        io_ss_actions << "---\n";
        io_ss_actions << out_actions.c_str();
        io_ss_actions << "\n...\n";
        io_ss_actions.close();
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
