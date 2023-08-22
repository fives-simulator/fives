
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

#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <wrench/util/UnitParser.h>

#include "yaml-cpp/yaml.h"

WRENCH_LOG_CATEGORY(storalloc_controller, "Log category for storalloc controller");

namespace storalloc {

    template <typename E>
    constexpr auto
    toUType(E enumerator) noexcept {
        return static_cast<std::underlying_type_t<E>>(enumerator);
    }

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
        WRENCH_INFO("Controller starting");
        WRENCH_INFO("Got %s jobs to prepare and submit", std::to_string(jobs.size()).c_str());

        std::cout << "Getting flop rate" << std::endl;
        this->flopRate = this->compute_service->getCoreFlopRate().begin()->second; // flop rate from first compute node
        std::cout << "Flop rate is " << std::to_string(this->flopRate) << std::endl;

        this->job_manager = this->createJobManager();

        // Simulate 'fake' load before the actual dataset is used
        auto preload_jobs = this->createPreloadJobs();

        // concat preload jobs with dataset
        std::vector<storalloc::YamlJob> jobsWithPreload(preload_jobs);
        jobsWithPreload.insert(jobsWithPreload.end(), this->jobs.begin(), this->jobs.end());

        // Simulate jobs
        auto total_events = 0;
        auto processed_events = 0;

        for (const auto &yaml_entry : jobsWithPreload) {

            // Cleanup our temporary variables
            this->current_yaml_job = yaml_entry; // this is the job we're going to submit next
            this->actions.clear();

            WRENCH_DEBUG("# Setting timer for = %d s", this->current_yaml_job.sleepSimulationSeconds);
            double timer_off_date = wrench::Simulation::getCurrentSimulatedDate() + this->current_yaml_job.sleepSimulationSeconds + 1; // some simulation sleep values are 0, we don't want that
            total_events += 1;
            this->setTimer(timer_off_date, "SleepBeforeNextJob_" + this->current_yaml_job.id);

            auto nextSubmission = false;
            while (!nextSubmission) {
                auto event = this->waitForNextEvent(3600);
                if (!event) {
                    continue;
                }

                processed_events += 1;

                if (auto timer_event = std::dynamic_pointer_cast<wrench::TimerEvent>(event)) {
                    this->processEventTimer(timer_event);
                    total_events += 1; // job that was just submitted
                    nextSubmission = true;
                } else if (auto completion_event = std::dynamic_pointer_cast<wrench::CompoundJobCompletedEvent>(event)) {
                    this->processEventCompoundJobCompletion(completion_event);
                } else if (auto failure_event = std::dynamic_pointer_cast<wrench::CompoundJobFailedEvent>(event)) {
                    this->processEventCompoundJobFailure(failure_event);
                } else {
                    throw std::runtime_error("Unexpected Controller Event : " + event->toString());
                }
            }
        }

        // Process any remaining events (number unknown, depends on jobs runtime vs simulation sleep between jobs)
        while (processed_events < total_events) {
            this->waitForAndProcessNextEvent();
            processed_events += 1;
        }

        WRENCH_INFO("Controller execution complete");
        return 0;
    }

    std::shared_ptr<wrench::CompoundJob> Controller::getCompletedJobById(std::string id) {

        auto job_pair = this->compound_jobs.find(id);
        if (job_pair == this->compound_jobs.end()) {
            return nullptr;
        }

        return job_pair->second.second;
    }

    bool Controller::actionsAllCompleted() {

        // List all action that failed states and times
        bool all_good = true;

        for (const auto &job : this->compound_jobs) {

            /* Access pair of yaml data / compound job, and then compound job inside the pair */
            for (const auto &a : job.second.second->getActions()) {

                if (a->getState() != wrench::Action::State::COMPLETED) {
                    wrench::TerminalOutput::setThisProcessLoggingColor(wrench::TerminalOutput::COLOR_RED);
                    WRENCH_WARN("Error for action %s: %.2fs - %.2fs", a->getName().c_str(), a->getStartDate(), a->getEndDate());
                    std::cout << "Failed action for job : " << job.first << " : " << a->getName() << std::endl;
                    if (a->getFailureCause()) {
                        WRENCH_WARN("-> Failure cause: %s", a->getFailureCause()->toString().c_str());
                    }
                    wrench::TerminalOutput::setThisProcessLoggingColor(wrench::TerminalOutput::COLOR_GREEN);
                    all_good = false;
                }
            }
        }

        return all_good;
    }

    std::vector<storalloc::YamlJob> Controller::createPreloadJobs() const {

        // How many preload jobs to create (20% of the total number of jobs):
        auto preloadJobsCount = std::ceil(this->preload_header->job_count * this->config->preload_percent);
        std::cout << "Preparing " << preloadJobsCount << " preload jobs" << std::endl;

        std::random_device rd;
        std::mt19937 gen(rd());

        // Runtimes :
        std::cout << "Mean runtime (s) : " << this->preload_header->mean_runtime_s << std::endl;
        std::cout << "Median runtime (s) : " << this->preload_header->median_runtime_s << std::endl;
        std::cout << "Runtime var : " << this->preload_header->var_runtime_s << std::endl;
        std::cout << "Max runtime (s) : " << this->preload_header->max_runtime_s << std::endl;

        std::vector<int> rand_runtimes_s;
        std::lognormal_distribution<> dr(
            std::log(this->preload_header->mean_runtime_s / std::sqrt(this->preload_header->var_runtime_s / std::pow(this->preload_header->mean_runtime_s, 2) + 1)),
            std::sqrt(std::log((this->preload_header->var_runtime_s / std::pow(this->preload_header->mean_runtime_s, 2)) + 1)));
        while (rand_runtimes_s.size() != preloadJobsCount) {
            auto val = std::floor(dr(gen));
            if (val <= this->preload_header->max_runtime_s) {
                rand_runtimes_s.push_back(val);
                std::cout << "Adding " << val << " to runtimes" << std::endl;
            }
        }

        // Interval between jobs :
        std::cout << "Mean interval (s) : " << this->preload_header->mean_interval_s << std::endl;
        std::cout << "Median interval (s) : " << this->preload_header->median_interval_s << std::endl;
        std::cout << "Interval var : " << this->preload_header->var_interval_s << std::endl;
        std::cout << "Max interval (s) : " << this->preload_header->max_interval_s << std::endl;

        std::vector<int> rand_intervals_s;
        std::lognormal_distribution<> di(
            std::log(this->preload_header->mean_interval_s / std::sqrt(this->preload_header->var_interval_s / std::pow(this->preload_header->mean_interval_s, 2) + 1)),
            std::sqrt(std::log((this->preload_header->var_interval_s / std::pow(this->preload_header->mean_interval_s, 2)) + 1)));
        while (rand_intervals_s.size() != preloadJobsCount) {
            auto val = std::floor(di(gen));
            if (val <= this->preload_header->max_interval_s) {
                rand_intervals_s.push_back(val);
                std::cout << "Adding " << val << " to intervals" << std::endl;
            }
        }

        // Nodes used:
        std::cout << "Mean nodes used : " << this->preload_header->mean_nodes_used << std::endl;
        std::cout << "Median nodes used : " << this->preload_header->median_nodes_used << std::endl;
        std::cout << "Nodes used var : " << this->preload_header->var_nodes_used << std::endl;
        std::cout << "Max nodes used : " << this->preload_header->max_nodes_used << std::endl;

        std::vector<int> rand_nodes_count;
        std::lognormal_distribution<> dn(
            std::log(this->preload_header->mean_nodes_used / std::sqrt(this->preload_header->var_nodes_used / std::pow(this->preload_header->mean_nodes_used, 2) + 1)),
            std::sqrt(std::log((this->preload_header->var_nodes_used / std::pow(this->preload_header->mean_nodes_used, 2)) + 1)));
        while (rand_nodes_count.size() != preloadJobsCount) {
            auto val = std::floor(dn(gen));
            if (val <= this->preload_header->max_nodes_used) {
                rand_nodes_count.push_back(val);
                std::cout << "Adding " << val << " to nodes count" << std::endl;
            }
        }

        // Bytes READ
        std::cout << "Mean terabytes read : " << this->preload_header->mean_read_tbytes << std::endl;
        std::cout << "Median terabytes read : " << this->preload_header->median_read_tbytes << std::endl;
        std::cout << "Var terabytes read : " << this->preload_header->var_read_tbytes << std::endl;
        std::cout << "Max terabytes read : " << this->preload_header->max_read_tbytes << std::endl;

        std::vector<double> rand_read_tbytes;
        std::lognormal_distribution<> drb(
            std::log(this->preload_header->mean_read_tbytes / std::sqrt(this->preload_header->var_read_tbytes / std::pow(this->preload_header->mean_read_tbytes, 2) + 1)),
            std::sqrt(std::log((this->preload_header->var_read_tbytes / std::pow(this->preload_header->mean_read_tbytes, 2)) + 1)));
        while (rand_read_tbytes.size() != preloadJobsCount) {
            auto val = drb(gen);
            if (val <= this->preload_header->max_read_tbytes) {
                rand_read_tbytes.push_back(val);
                std::cout << "Adding " << val << " to read terabytes" << std::endl;
            }
        }

        // Bytes WRITTEN
        std::cout << "Mean terabytes written : " << this->preload_header->mean_written_tbytes << std::endl;
        std::cout << "Median terabytes written : " << this->preload_header->median_written_tbytes << std::endl;
        std::cout << "Var terabytes written : " << this->preload_header->var_written_tbytes << std::endl;
        std::cout << "Max terabytes written : " << this->preload_header->max_written_tbytes << std::endl;

        std::vector<double> rand_written_tbytes;
        std::lognormal_distribution<> dwb(
            std::log(this->preload_header->mean_written_tbytes / std::sqrt(this->preload_header->var_written_tbytes / std::pow(this->preload_header->mean_written_tbytes, 2) + 1)),
            std::sqrt(std::log((this->preload_header->var_written_tbytes / std::pow(this->preload_header->mean_written_tbytes, 2)) + 1)));
        while (rand_written_tbytes.size() != preloadJobsCount) {
            auto val = dwb(gen);
            if (val <= this->preload_header->max_written_tbytes) {
                rand_written_tbytes.push_back(val);
                std::cout << "Adding " << val << " to written terabytes" << std::endl;
            }
        }

        auto cores_per_node = this->compute_service->getPerHostNumCores().begin()->second;
        std::cout << "Using " << cores_per_node << " cores for each reserved node" << std::endl;
        std::vector<storalloc::YamlJob> preload_jobs;
        auto i = 0;
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

        std::cout << "Preload jobs created" << std::endl;

        return preload_jobs;
    }

    void Controller::submitJob() {

        WRENCH_DEBUG("# New Job - SUBMISSION TIME = %s", this->current_yaml_job.submissionTime.c_str());
        auto yJob = this->current_yaml_job;

        auto job = this->job_manager->createCompoundJob(yJob.id);
        WRENCH_DEBUG(" # Job created with ID %s", yJob.id.c_str());
        this->current_job = job;

        if (this->current_yaml_job.model == storalloc::JobType::ReadComputeWrite) {
            auto input_data = this->copyFromPermanent();
            this->readFromTemporary(input_data);
            this->compute();
            auto output_data = this->writeToTemporary();
            this->copyToPermanent(output_data);
            this->cleanupInput(input_data);
            this->cleanupOutput(output_data);
        } else if (this->current_yaml_job.model == storalloc::JobType::ComputeWrite) {
            this->compute();
            auto output_data = this->writeToTemporary();
            this->copyToPermanent(output_data);
            this->cleanupOutput(output_data);
        } else if (this->current_yaml_job.model == storalloc::JobType::ReadCompute) {
            auto input_data = this->copyFromPermanent();
            this->readFromTemporary(input_data);
            this->compute();
            this->cleanupInput(input_data);
        } else if (this->current_yaml_job.model == storalloc::JobType::Compute) {
            this->compute();
        } /*else if (this->current_yaml_job.model == storalloc::JobType::ReadWrite) {       // Currently still adding a fake tiny compute phase to "RW" jobs
            auto input_data = this->copyFromPermanent();
            this->readFromTemporary(input_data);
            auto output_data = this->writeToTemporary();
            this->copyToPermanent(output_data);
            this->cleanupInput(input_data);
            this->cleanupOutput(output_data);
        }*/
        else {
            throw std::runtime_error("Unknown job model for job " + yJob.id);
        }

        // Submit job
        std::map<std::string, std::string> service_specific_args =
            {{"-N", std::to_string(yJob.nodesUsed)},                  // nb of nodes
             {"-c", std::to_string(yJob.coresUsed / yJob.nodesUsed)}, // cores per node
             {"-t", std::to_string(yJob.walltimeSeconds)}};           // seconds
        WRENCH_DEBUG("Submitting job %s (%d nodes, %d cores per node, %d minutes) for executing actions",
                     job->getName().c_str(),
                     yJob.nodesUsed, yJob.coresUsed / yJob.nodesUsed, yJob.walltimeSeconds);
        job_manager->submitJob(job, this->compute_service, service_specific_args);

        // Save job for future analysis
        this->compound_jobs[yJob.id] = std::make_pair(yJob, job);
    }

    std::shared_ptr<wrench::DataFile> Controller::copyFromPermanent() {

        WRENCH_DEBUG("Creating copy action for a file with size %ld", this->current_yaml_job.readBytes);

        auto read_file = wrench::Simulation::addFile("input_data_file_" + this->current_yaml_job.id, this->current_yaml_job.readBytes);

        wrench::StorageService::createFileAtLocation(wrench::FileLocation::LOCATION(this->storage_service, "/dev/disk0/read/", read_file));
        auto fileCopyAction = current_job->addFileCopyAction(
            "stagingCopy_" + this->current_yaml_job.id,
            wrench::FileLocation::LOCATION(this->storage_service, "/dev/disk0/read/", read_file),
            wrench::FileLocation::LOCATION(this->compound_storage_service, read_file));

        // Actions dependencies
        if (!this->actions.empty()) {
            this->current_job->addActionDependency(this->actions.back(), fileCopyAction);
        }
        this->actions.push_back(fileCopyAction);

        return read_file;
    }

    void Controller::readFromTemporary(std::shared_ptr<wrench::DataFile> input_data) {

        WRENCH_DEBUG("Creating read action for a file with size %ld", this->current_yaml_job.readBytes);

        auto fileReadAction = this->current_job->addFileReadAction(
            "fRead_" + this->current_yaml_job.id,
            wrench::FileLocation::LOCATION(this->compound_storage_service, input_data));

        // Actions dependencies
        if (!this->actions.empty()) {
            this->current_job->addActionDependency(this->actions.back(), fileReadAction);
        }
        this->actions.push_back(fileReadAction);
    }

    void Controller::compute() {

        WRENCH_DEBUG("Creating compute action");

        // Random values (flops, ram, ...) to be adjusted.
        // We could start from the theoreticak peak performance of modelled platform, and specify the flops based on the
        // % of available compute resources used by the job and its execution time (minor a factor of the time spend in IO ?)
        auto cores_per_node = this->current_yaml_job.coresUsed / this->current_yaml_job.nodesUsed;

        auto computeAction = this->current_job->addComputeAction(
            "compute_" + this->current_yaml_job.id,
            this->flopRate * this->current_yaml_job.approxComputeTimeSeconds,
            16 * GBYTE,
            cores_per_node, cores_per_node,
            wrench::ParallelModel::AMDAHL(0.75));

        // Actions dependencies
        if (!this->actions.empty()) {
            this->current_job->addActionDependency(this->actions.back(), computeAction);
        }
        this->actions.push_back(computeAction);
    }

    std::shared_ptr<wrench::DataFile> Controller::writeToTemporary() {

        WRENCH_DEBUG("Creating write action for a file with size %ld", this->current_yaml_job.writtenBytes);

        auto write_file = wrench::Simulation::addFile("output_data_file_" + this->current_yaml_job.id, this->current_yaml_job.writtenBytes);
        auto fileWriteAction = this->current_job->addFileWriteAction(
            "fWrite_" + this->current_yaml_job.id,
            wrench::FileLocation::LOCATION(this->compound_storage_service, write_file));

        // Actions dependencies
        if (!this->actions.empty()) {
            this->current_job->addActionDependency(this->actions.back(), fileWriteAction);
        }
        this->actions.push_back(fileWriteAction);

        return write_file;
    }

    void Controller::copyToPermanent(std::shared_ptr<wrench::DataFile> output_data) {

        WRENCH_DEBUG("Creating copy action for a file with size %ld", this->current_yaml_job.writtenBytes);

        auto archiveAction = this->current_job->addFileCopyAction(
            "archiveCopy_" + this->current_yaml_job.id,
            wrench::FileLocation::LOCATION(this->compound_storage_service, output_data),
            wrench::FileLocation::LOCATION(this->storage_service, "/dev/disk0/write/", output_data));

        // Actions dependencies
        this->current_job->addActionDependency(this->actions.back(), archiveAction);
        this->actions.push_back(archiveAction);
    }

    void Controller::cleanupInput(std::shared_ptr<wrench::DataFile> input_data) {

        WRENCH_DEBUG("Creating cleanup actions for an input file with size %ld", this->current_yaml_job.readBytes);

        auto readSleep = this->current_job->addSleepAction("sleepBeforeReadCleanup" + this->current_yaml_job.id, 1.0);
        this->current_job->addActionDependency(this->actions.back(), readSleep);
        this->actions.push_back(readSleep);

        auto deleteReadAction = this->current_job->addFileDeleteAction("delRF_" + this->current_yaml_job.id, wrench::FileLocation::LOCATION(this->compound_storage_service, input_data));
        this->current_job->addActionDependency(this->actions.back(), deleteReadAction);
        this->actions.push_back(deleteReadAction);

        auto deleteExternalReadAction = this->current_job->addFileDeleteAction("delERF_" + this->current_yaml_job.id, wrench::FileLocation::LOCATION(this->storage_service, "/dev/disk0/read/", input_data));
        this->current_job->addActionDependency(this->actions.back(), deleteExternalReadAction);
        this->actions.push_back(deleteExternalReadAction);
    }

    void Controller::cleanupOutput(std::shared_ptr<wrench::DataFile> output_data) {

        WRENCH_DEBUG("Creating cleanup actions for an input file with size %ld", this->current_yaml_job.readBytes);

        auto writeSleep = this->current_job->addSleepAction("sleepBeforeWriteCleanup" + this->current_yaml_job.id, 1.0);
        this->current_job->addActionDependency(this->actions.back(), writeSleep);
        this->actions.push_back(writeSleep);

        auto deleteWriteAction = this->current_job->addFileDeleteAction("delWF_" + this->current_yaml_job.id, wrench::FileLocation::LOCATION(this->compound_storage_service, output_data));
        this->current_job->addActionDependency(this->actions.back(), deleteWriteAction);
        this->actions.push_back(deleteWriteAction);

        auto deleteExternalWriteAction = this->current_job->addFileDeleteAction("delEWF_" + this->current_yaml_job.id, wrench::FileLocation::LOCATION(this->storage_service, "/dev/disk0/write/", output_data));
        this->current_job->addActionDependency(this->actions.back(), deleteExternalWriteAction);
        this->actions.push_back(deleteExternalWriteAction);
    }

    /**
     * @brief Process a compound job completion event
     *
     * @param event: the event
     */
    void Controller::processEventCompoundJobCompletion(std::shared_ptr<wrench::CompoundJobCompletedEvent> event) {
        auto job = event->job;
        std::cout << "# Notified that compound job " << job->getName() << " has completed" << std::endl;
        WRENCH_INFO("# Notified that compound job %s has completed:", job->getName().c_str());

        // Extract relevant informations from job and write them to file / send them to DB ?
    }

    void Controller::processEventTimer(std::shared_ptr<wrench::TimerEvent> timerEvent) {
        std::cout << "Timer Event : " << timerEvent->toString() << std::endl;
        this->submitJob();
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
        WRENCH_WARN("Notified that compound job %s has failed: %s", job->getName().c_str(), cause->toString().c_str());
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

    /**
     * @brief Explore the list of completed jobs and prepare two yaml outputs for further analysis :
     *          - The first one, "simulatedJobs_*", includes the full list of jobs and the list of actions for each jobs.
     *          It also contains data about the actual jobs, taken from the original yaml dataset (for easier comparison later)
     *          - The second one contains a list of all actions, with more details on storage services for each action.
     *        Both files can be correlated with the jobs ID.
     */
    void Controller::processCompletedJobs(const std::string &jobsFilename, const std::string &configVersion) {

        std::map<std::string, StorageServiceIOCounters> volume_per_storage_service_disk = {};

        YAML::Emitter out_actions;
        out_actions << YAML::BeginSeq;

        YAML::Emitter out_jobs;
        out_jobs << YAML::BeginSeq;

        for (const auto &job_entry : this->compound_jobs) {

            const auto &job_pair = job_entry.second;
            const auto &yaml_job = job_pair.first;
            const auto job = job_pair.second;

            // Ignore preload jobs in the output results
            if (job->getName().substr(0, 7) == "preload") {
                continue;
            }

            auto actions = job->getActions();
            auto set_cmp = [](const std::shared_ptr<wrench::Action> &a, const std::shared_ptr<wrench::Action> &b) {
                return (a->getStartDate() < b->getStartDate());
            };
            auto sorted_actions = std::set<std::shared_ptr<wrench::Action>, decltype(set_cmp)>(set_cmp);
            sorted_actions.insert(actions.begin(), actions.end());

            out_jobs << YAML::BeginMap; // Job map

            auto job_uid = job->getName();
            WRENCH_DEBUG("In job %s", job_uid.c_str());
            out_jobs << YAML::Key << "job_uid" << YAML::Value << job_uid;
            out_jobs << YAML::Key << "job_status" << YAML::Value << job->getStateAsString();
            out_jobs << YAML::Key << "job_submit_ts" << YAML::Value << job->getSubmitDate();
            out_jobs << YAML::Key << "job_start_ts" << YAML::Value << sorted_actions.begin()->get()->getStartDate();
            auto waiting_time = sorted_actions.begin()->get()->getStartDate() - job->getSubmitDate();
            //  if (waiting_time <= 0.001)
            //      waiting_time = 0;
            out_jobs << YAML::Key << "job_waiting_time_s" << YAML::Value << waiting_time;
            out_jobs << YAML::Key << "job_end_ts" << YAML::Value << job->getEndDate();
            out_jobs << YAML::Key << "job_runtime_s" << YAML::Value << (job->getEndDate() - job->getSubmitDate());
            out_jobs << YAML::Key << "real_runtime_s" << YAML::Value << yaml_job.runtimeSeconds;
            out_jobs << YAML::Key << "real_read_bytes" << YAML::Value << yaml_job.readBytes;
            out_jobs << YAML::Key << "real_written_bytes" << YAML::Value << yaml_job.writtenBytes;
            out_jobs << YAML::Key << "real_cores_used" << YAML::Value << yaml_job.coresUsed;
            // out_jobs << YAML::Key << "real_mpi_procs" << YAML::Value << yaml_job.nprocs;
            out_jobs << YAML::Key << "real_waiting_time_s" << YAML::Value << yaml_job.waitingTimeSeconds;
            out_jobs << YAML::Key << "real_cReadTime_s" << YAML::Value << yaml_job.readTimeSeconds;
            out_jobs << YAML::Key << "real_cWriteTime_s" << YAML::Value << yaml_job.writeTimeSeconds;
            out_jobs << YAML::Key << "real_cMetaTime_s" << YAML::Value << yaml_job.metaTimeSeconds;
            out_jobs << YAML::Key << "approx_cComputeTime_s" << YAML::Value << yaml_job.approxComputeTimeSeconds;
            out_jobs << YAML::Key << "sim_sleep_time" << YAML::Value << yaml_job.sleepSimulationSeconds;

            out_jobs << YAML::Key << "actions" << YAML::Value << YAML::BeginSeq; // action sequence

            for (const auto &action : sorted_actions) {

                if (action->getState() != wrench::Action::COMPLETED) {
                    WRENCH_WARN("Action %s is in state %s", action->getName().c_str(), action->getStateAsString().c_str());
                    throw std::runtime_error("Action is not in COMPLETED state");
                }

                out_jobs << YAML::BeginMap; // action map
                out_jobs << YAML::Key << "act_name" << YAML::Value << action->getName();
                auto act_type = wrench::Action::getActionTypeAsString(action);
                act_type.pop_back(); // removing a useless '-' at the end
                out_jobs << YAML::Key << "act_type" << YAML::Value << act_type;
                out_jobs << YAML::Key << "act_status" << YAML::Value << action->getStateAsString();
                out_jobs << YAML::Key << "act_start_ts" << YAML::Value << action->getStartDate();
                out_jobs << YAML::Key << "act_end_ts" << YAML::Value << action->getEndDate();
                out_jobs << YAML::Key << "act_duration" << YAML::Value << (action->getEndDate() - action->getStartDate());

                WRENCH_DEBUG("ACTION TYPE: %s AT TS: %f", act_type.c_str(), action->getStartDate());

                if (auto fileRead = std::dynamic_pointer_cast<wrench::FileReadAction>(action)) {

                    auto usedLocation = fileRead->getUsedFileLocation();
                    auto usedFile = usedLocation->getFile();

                    out_jobs << YAML::Key << "file_name" << YAML::Value << usedFile->getID();
                    out_jobs << YAML::Key << "file_size_bytes" << YAML::Value << usedFile->getSize();

                } else if (auto fileWrite = std::dynamic_pointer_cast<wrench::FileWriteAction>(action)) {
                    auto usedLocation = fileWrite->getFileLocation();
                    auto usedFile = usedLocation->getFile();

                    auto write_trace = this->compound_storage_service->write_traces[usedFile->getID()];
                    out_jobs << YAML::Key << "internal_locations" << YAML::Value << write_trace.internal_locations.size();
                    out_jobs << YAML::Key << "parts_count" << YAML::Value << write_trace.parts_count;
                    out_jobs << YAML::Key << "file_name" << YAML::Value << usedFile->getID();
                    out_jobs << YAML::Key << "file_size_bytes" << YAML::Value << usedFile->getSize();

                    for (const auto &trace_loc : write_trace.internal_locations) {
                        auto keys = updateIoUsageWrite(volume_per_storage_service_disk, trace_loc);
                        out_actions << YAML::BeginMap;
                        out_actions << YAML::Key << "ts" << YAML::Value << action->getEndDate(); // end_date, because we record the IO change when the write is completed
                        out_actions << YAML::Key << "action_type" << YAML::Value << act_type;
                        out_actions << YAML::Key << "action_job" << YAML::Value << job_uid;
                        out_actions << YAML::Key << "action_name" << YAML::Value << action->getName();
                        out_actions << YAML::Key << "filename" << YAML::Value << trace_loc->getFile()->getID();
                        out_actions << YAML::Key << "storage_service" << YAML::Value << keys.first;
                        out_actions << YAML::Key << "disk" << YAML::Value << keys.second;
                        out_actions << YAML::Key << "volume_change_bytes" << YAML::Value << usedFile->getSize();
                        out_actions << YAML::Key << "total_allocation_server" << YAML::Value << volume_per_storage_service_disk[keys.first].total_allocation_count;
                        out_actions << YAML::Key << "total_used_volume_bytes_server" << YAML::Value << volume_per_storage_service_disk[keys.first].total_capacity_used;
                        out_actions << YAML::Key << "total_allocation_disk" << YAML::Value << volume_per_storage_service_disk[keys.first].disks[keys.second].total_allocation_count;
                        out_actions << YAML::Key << "total_used_volume_bytes_disk" << YAML::Value << volume_per_storage_service_disk[keys.first].disks[keys.second].total_capacity_used;
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
                            auto keys = updateIoUsageCopy(volume_per_storage_service_disk, trace_loc);
                            out_actions << YAML::BeginMap;
                            out_actions << YAML::Key << "ts" << YAML::Value << action->getEndDate(); // end_date, because we record the IO change when the write is completed
                            out_actions << YAML::Key << "action_type" << YAML::Value << act_type;
                            out_actions << YAML::Key << "action_name" << YAML::Value << action->getName();
                            out_actions << YAML::Key << "filename" << YAML::Value << trace_loc->getFile()->getID();
                            out_actions << YAML::Key << "action_job" << YAML::Value << job_uid;
                            out_actions << YAML::Key << "storage_service" << YAML::Value << keys.first;
                            out_actions << YAML::Key << "disk" << YAML::Value << keys.second;
                            out_actions << YAML::Key << "volume_change_bytes" << YAML::Value << css->getFile()->getSize(); // dest file, because it's the one being written
                            out_actions << YAML::Key << "total_allocation_server" << YAML::Value << volume_per_storage_service_disk[keys.first].total_allocation_count;
                            out_actions << YAML::Key << "total_used_volume_bytes_server" << YAML::Value << volume_per_storage_service_disk[keys.first].total_capacity_used;
                            out_actions << YAML::Key << "total_allocation_disk" << YAML::Value << volume_per_storage_service_disk[keys.first].disks[keys.second].total_allocation_count;
                            out_actions << YAML::Key << "total_used_volume_bytes_disk" << YAML::Value << volume_per_storage_service_disk[keys.first].disks[keys.second].total_capacity_used;
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
                            auto keys = updateIoUsageDelete(volume_per_storage_service_disk, trace_loc);
                            out_actions << YAML::BeginMap;
                            out_actions << YAML::Key << "ts" << YAML::Value << action->getEndDate(); // end_date, because we record the IO change when the write is completed
                            out_actions << YAML::Key << "action_type" << YAML::Value << act_type;
                            out_actions << YAML::Key << "action_name" << YAML::Value << action->getName();
                            out_actions << YAML::Key << "filename" << YAML::Value << trace_loc->getFile()->getID();
                            out_actions << YAML::Key << "action_job" << YAML::Value << job_uid;
                            out_actions << YAML::Key << "storage_service" << YAML::Value << keys.first;
                            out_actions << YAML::Key << "disk" << YAML::Value << keys.second;
                            out_actions << YAML::Key << "volume_change_bytes" << YAML::Value << usedFile->getSize();
                            out_actions << YAML::Key << "total_allocation_server" << YAML::Value << volume_per_storage_service_disk[keys.first].total_allocation_count;
                            out_actions << YAML::Key << "total_used_volume_bytes_server" << YAML::Value << volume_per_storage_service_disk[keys.first].total_capacity_used;
                            out_actions << YAML::Key << "total_allocation_disk" << YAML::Value << volume_per_storage_service_disk[keys.first].disks[keys.second].total_allocation_count;
                            out_actions << YAML::Key << "total_used_volume_bytes_disk" << YAML::Value << volume_per_storage_service_disk[keys.first].disks[keys.second].total_capacity_used;
                            out_actions << YAML::EndMap;
                        }
                    }
                }
                out_jobs << YAML::EndMap;
            }
            out_jobs << YAML::EndSeq;
            out_jobs << YAML::EndMap;
        }
        out_jobs << YAML::EndSeq;
        out_actions << YAML::EndSeq;

        // Write to YAML file
        ofstream simulatedJobs;
        simulatedJobs.open("simulatedJobs_" + jobsFilename + "__" + configVersion + ".yml");
        simulatedJobs << "---\n";
        simulatedJobs << out_jobs.c_str();
        simulatedJobs << "\n...\n";
        simulatedJobs.close();

        // Write to YAML file
        ofstream io_ss_actions;
        io_ss_actions.open("io_actions_ts_" + jobsFilename + "__" + configVersion + ".yml");
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
    void Controller::extractSSSIO(const std::string &jobsFilename, const std::string &configVersion) {

        if (not this->hasReturnedFromMain()) {
            throw std::runtime_error("Cannot extract IO traces before the controller has returned from main()");
        }

        ofstream io_ops;
        io_ops.setf(std::ios_base::fixed);
        io_ops.open("storage_services_operations_" + jobsFilename + "__" + configVersion + ".csv");
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
                io_ops << disk_usage.file_count << ";";                            // current file count on disk
                io_ops << disk_usage.free_space << ",";                            // disk_free_space
                io_ops << alloc.file_name << ";";                                  // file_name
                io_ops << alloc.parts_count << "\n";                               // number of parts for file
            }
        }

        io_ops.close();
    }

} // namespace storalloc
