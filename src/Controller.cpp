/**
 ** An execution controller to execute a workflow
 **/

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
#include <xbt.h>

#include "yaml-cpp/yaml.h"

#define GFLOP (1000.0 * 1000.0 * 1000.0)
#define MBYTE (1000.0 * 1000.0)
#define GBYTE (1000.0 * 1000.0 * 1000.0)

WRENCH_LOG_CATEGORY(fives_controller, "Log category for Fives Controller");

namespace fives {

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
                           const std::map<std::string, YamlJob> &jobs,
                           const std::shared_ptr<fives::Config> &fives_config) : ExecutionController(hostname, "controller"),
                                                                                 compute_service(compute_service),
                                                                                 storage_service(storage_service),
                                                                                 compound_storage_service(compound_storage_service),
                                                                                 jobs(jobs),
                                                                                 config(fives_config) {}

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
        this->ost_count = this->compound_storage_service->getAllServices().size();

        this->job_manager = this->createJobManager();

        this->preloadData();

        auto total_events = 0;
        auto processed_events = 0;
        for (const auto &yaml_entry : this->jobs) {

            WRENCH_INFO("[%s] Setting %ds timer", yaml_entry.first.c_str(), yaml_entry.second.sleepSimulationSeconds);
            double timer_off_date = wrench::Simulation::getCurrentSimulatedDate() + yaml_entry.second.sleepSimulationSeconds;
            this->setTimer(timer_off_date, yaml_entry.first.c_str());
            total_events += 1;

            auto nextSubmission = false;
            while (!nextSubmission) {
                WRENCH_INFO("[%s] Now waiting for next event...", yaml_entry.first.c_str());
                auto event = this->waitForNextEvent();
                processed_events += 1;

                if (auto timer_event = std::dynamic_pointer_cast<wrench::TimerEvent>(event)) {
                    WRENCH_INFO("[During loop for %s] Timer event received", yaml_entry.first.c_str());
                    this->processEventTimer(timer_event);
                    total_events += 1; // job that was just submitted
                    nextSubmission = true;
                    WRENCH_INFO("[During loop for %s] Timer event processed", yaml_entry.first.c_str());
                } else if (auto completion_event = std::dynamic_pointer_cast<wrench::CompoundJobCompletedEvent>(event)) {
                    WRENCH_INFO("[During loop for %s] Job completed event received ()", yaml_entry.first.c_str());
                    this->processEventCompoundJobCompletion(completion_event);
                } else if (auto failure_event = std::dynamic_pointer_cast<wrench::CompoundJobFailedEvent>(event)) {
                    WRENCH_INFO("[During loop for %s] Job failure event received ()", yaml_entry.first.c_str());
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
     * @brief Find a reservation job by id in the internal job list.
     * @param id String id of the reservation job to look for
     * @return shared_ptr on CompoundJob, pointing to a 'reservation job'
     */
    std::shared_ptr<wrench::CompoundJob> Controller::getReservationJobById(const std::string &id) {

        auto job_entry = this->sim_jobs.find(id);
        if (job_entry == this->sim_jobs.end()) {
            WRENCH_WARN("Controller::getCompletedJobsById: Job ID %s not found", id.c_str());
            return nullptr;
        }

        return job_entry->second.reservationJob;
    }

    /**
     * @brief Find compound jobs by id in the internal job list.
     *        Note that every compound job in this map is actually a list of sub-jobs started from
     *        inside the customAction of the reservation job.
     * @param id String id of the reservation job to look for
     * @return Vector of shared_ptr on Compound jobs, including all sub-jobs but not the 'reservation' job
     */
    std::map<uint32_t, std::vector<std::shared_ptr<wrench::CompoundJob>>> Controller::getCompletedJobsById(const std::string &id) {

        auto job_entry = this->sim_jobs.find(id);
        if (job_entry == this->sim_jobs.end()) {
            WRENCH_WARN("Controller::getCompletedJobsById: Job ID %s not found", id.c_str());
            return {};
        }

        return job_entry->second.subJobs;
    }

    /**
     * @brief Check all actions from all jobs for any failed action
     * @return True if no action has failed, false otherwise
     */
    bool Controller::actionsAllCompleted() {

        bool success = true;

        for (const auto &[job_id, trace] : this->sim_jobs) {
            for (const auto &[run, jobs] : trace.subJobs) {
                for (const auto &job : jobs) {
                    for (const auto &a : job->getActions()) {
                        if (a->getState() != wrench::Action::State::COMPLETED) {
                            wrench::TerminalOutput::setThisProcessLoggingColor(wrench::TerminalOutput::COLOR_RED);
                            WRENCH_WARN("Error for action %s: %.2fs - %.2fs", a->getName().c_str(), a->getStartDate(), a->getEndDate());
                            std::cout << "Failed action for job : " << job_id << " : " << a->getName() << std::endl;
                            if (a->getFailureCause()) {
                                WRENCH_WARN("-> Failure cause: %s", a->getFailureCause()->toString().c_str());
                            }
                            wrench::TerminalOutput::setThisProcessLoggingColor(wrench::TerminalOutput::COLOR_GREEN);
                            success = false;
                        }
                    }
                }
            }
        }

        return success;
    }

    void Controller::preloadData() {

        for (const auto &job : this->jobs) {
            for (const auto &run : job.second.runs) {
                if ((run.readBytes > 0) and (run.readBytes >= this->config->stor.read_bytes_preload_thres)) {

                    unsigned int local_stripe_count = this->getReadStripeCount(job.second.cumulReadBW);
                    auto nb_read_files = getReadFileCount(local_stripe_count);

                    auto prefix = "pInputFile_id" + job.first + "_exec" + std::to_string(run.id);
                    WRENCH_DEBUG("preloading file prefix %s on external storage system", prefix.c_str());
                    auto read_files = this->createFileParts(run.readBytes, nb_read_files, prefix);

                    for (const auto &read_file : read_files) {
                        auto file_stripes = this->compound_storage_service->lookupOrDesignateStorageService(wrench::FileLocation::LOCATION(this->compound_storage_service, read_file), local_stripe_count);
                        for (const auto &stripe : file_stripes) {
                            wrench::StorageService::createFileAtLocation(stripe);
                        }
                    }

                    this->preloadedData[job.first + "_" + std::to_string(run.id)] = read_files;
                }
            }
        }
    }

    unsigned int Controller::getReadNodeCount(unsigned int max_nodes, double cumul_read_bw,
                                              unsigned int stripe_count) const {
        /* Note : this model uses the disk bw, which is easy to determine in our case (no matter where the file
        is located, all disks are the same), but won't be in case of a heterogeneous storage system, where another
        model will probably be required */
        if (cumul_read_bw <= this->config->stor.read_node_thres) {
            return 1;
        } else {
            auto ratio = (cumul_read_bw / 1e6) / (stripe_count * this->config->stor.disk_templates.begin()->second.read_bw);
            ratio = std::min(ratio, 1.0);
            unsigned int node_count = std::ceil(max_nodes * ratio);
            return std::max(2u, node_count);
        }
    }

    unsigned int Controller::getWriteNodeCount(unsigned int max_nodes, double cumul_write_bw,
                                               unsigned int stripe_count) const {
        /* Note : this model uses the disk bw, which is easy to determine in our case (no matter where the file
        is located, all disks are the same), but won't be in case of a heterogeneous storage system, where another
        model will probably be required */
        if (cumul_write_bw <= this->config->stor.write_node_thres) {
            return 1;
        } else {
            auto ratio = (cumul_write_bw / 1e6) / (stripe_count * this->config->stor.disk_templates.begin()->second.write_bw);
            ratio = std::min(ratio, 1.0);
            unsigned int node_count = std::ceil(max_nodes * ratio);
            return std::max(2u, node_count);
        }
    }

    unsigned int Controller::getReadStripeCount(double cumul_read_bw) const {

        unsigned int local_stripe_count = this->config->lustre.stripe_count;
        if (this->config->lustre.stripe_count_high_thresh_read && cumul_read_bw >= this->config->lustre.stripe_count_high_thresh_read) {
            auto ratio = std::ceil(cumul_read_bw / this->config->lustre.stripe_count_high_thresh_read);
            local_stripe_count = this->config->lustre.stripe_count_high_read_add * ratio;
            local_stripe_count = std::min(local_stripe_count, this->ost_count);
        }

        return local_stripe_count;
    }

    unsigned int Controller::getWriteStripeCount(double cumul_write_bw) const {

        unsigned int local_stripe_count = this->config->lustre.stripe_count;
        if (this->config->lustre.stripe_count_high_thresh_write && cumul_write_bw >= this->config->lustre.stripe_count_high_thresh_write) {
            auto ratio = std::ceil(cumul_write_bw / this->config->lustre.stripe_count_high_thresh_write);
            local_stripe_count = this->config->lustre.stripe_count_high_write_add * ratio;
            local_stripe_count = std::min(local_stripe_count, this->ost_count);
        }

        return local_stripe_count;
    }

    unsigned int Controller::getReadFileCount(unsigned int stripe_count) const {

        unsigned int nb_files = 1;
        nb_files = this->config->stor.nb_files_per_read * stripe_count;
        nb_files = std::max(nb_files, 1u);

        return nb_files;
    }

    unsigned int Controller::getWriteFileCount(unsigned int stripe_count) const {

        unsigned int nb_files = 1;
        nb_files = this->config->stor.nb_files_per_write * stripe_count;
        nb_files = std::max(nb_files, 1u);
        return nb_files;
    }

    void Controller::registerJob(const std::string &jobId,
                                 uint32_t runId,
                                 std::shared_ptr<wrench::CompoundJob> job,
                                 bool child) {
        if (this->sim_jobs[jobId].subJobs[runId].size() != 0 && child) {
            this->sim_jobs[jobId].subJobs[runId].back()->addChildJob(job);
        }
        this->sim_jobs[jobId].subJobs[runId].push_back(job);
    }

    void Controller::addSleepJob(JobManagementStruct &jms,
                                 const std::string &jobID,
                                 const DarshanRecord &run) {

        auto sleepJob = jms.jobManager->createCompoundJob("sleep_id" + jobID + "_run" + std::to_string(run.id));
        sleepJob->addSleepAction("sleep", run.sleepDelay);
        this->registerJob(jobID, run.id, sleepJob, false);
        jms.serviceSpecificArgs[sleepJob->getName()] = std::map<std::string, std::string>();
        jms.serviceSpecificArgs[sleepJob->getName()]["sleep"] = {};
        WRENCH_DEBUG("[%s-%u] Sleep job added for %lu s", jobID.c_str(), run.id, run.sleepDelay);
    }

    /**
     * Add a read sub job inside the custom action of a reservation.
     * The read job is possibly preceded by a copy job if the file read have not
     * already been created by a previous call to 'preloadData'() and/or are out of scope
     * for 'preloadData()'
     */
    void Controller::addReadJob(JobManagementStruct &jms,
                                const std::string &jobID,
                                const DarshanRecord &run) {

        std::vector<std::shared_ptr<wrench::DataFile>> input_files;

        unsigned int max_nodes_run = max(std::ceil(run.nprocs / this->config->compute.core_count), 1.0);
        auto read_stripe_count = this->getReadStripeCount(this->sim_jobs[jobID].yamlJob.cumulReadBW);
        auto nb_nodes_read = this->getReadNodeCount(max_nodes_run, this->sim_jobs[jobID].yamlJob.cumulReadBW, read_stripe_count);
        // auto read_stripe_count = this->getReadStripeCount(this->compound_jobs[jobID].first.cumulReadBW);
        // auto nb_nodes_read = this->getReadNodeCount(max_nodes_run, this->compound_jobs[jobID].first.cumulReadBW, read_stripe_count);

        // Optional COPY job
        if (this->preloadedData.find(jobID + "_" + std::to_string(run.id)) == this->preloadedData.end()) {
            auto nb_read_files = this->getReadFileCount(read_stripe_count);
            auto copyJob = jms.jobManager->createCompoundJob("inCopy_id" + jobID + "_run" + std::to_string(run.id));
            input_files = this->copyFromPermanent(jms.bareMetalCS, copyJob, jms.serviceSpecificArgs, run.readBytes, nb_read_files, nb_nodes_read);
            this->registerJob(jobID, run.id, copyJob, true);
            // this->compound_jobs[jobID].second.push_back(copyJob);
            WRENCH_DEBUG("[%s-%u] 'In' copy job added with %d nodes", jobID.c_str(), run.id, nb_nodes_read);
        } else { // No need for copy jobs, files have been created already
            input_files = this->preloadedData[jobID + "_" + std::to_string(run.id)];
        }

        auto readJob = jms.jobManager->createCompoundJob("rdFiles_id" + jobID + "_run" + std::to_string(run.id));
        this->readFromTemporary(jms.bareMetalCS, readJob, jobID, run.id, jms.serviceSpecificArgs, run.readBytes, input_files, nb_nodes_read);
        this->registerJob(jobID, run.id, readJob, true);

        WRENCH_DEBUG("[%s-%u] Read job added with %d nodes", jobID.c_str(), run.id, nb_nodes_read);
    }

    void Controller::addWriteJob(JobManagementStruct &jms,
                                 const std::string &jobID,
                                 const DarshanRecord &run) {

        std::vector<std::shared_ptr<wrench::DataFile>> output_data;
        unsigned int max_nodes_run = max(std::ceil(run.nprocs / this->config->compute.core_count), 1.0);
        auto write_stripe_count = this->getWriteStripeCount(this->sim_jobs[jobID].yamlJob.cumulWriteBW);
        auto nb_nodes_write = this->getReadNodeCount(max_nodes_run, this->sim_jobs[jobID].yamlJob.cumulWriteBW, write_stripe_count);
        // auto write_stripe_count = this->getWriteStripeCount(this->compound_jobs[jobID].first.cumulWriteBW);
        // auto nb_nodes_write = this->getReadNodeCount(max_nodes_run, this->compound_jobs[jobID].first.cumulWriteBW, write_stripe_count);
        auto nb_write_files = this->getWriteFileCount(write_stripe_count);
        WRENCH_DEBUG("[%s-%u] : %d nodes will be doing write/copy IOs", jobID.c_str(), run.id, nb_nodes_write);

        auto writeJob = jms.jobManager->createCompoundJob("wrFiles_id" + jobID + "_run" + std::to_string(run.id));
        output_data = this->writeToTemporary(jms.bareMetalCS, writeJob, jobID, run.id, jms.serviceSpecificArgs, run.writtenBytes, nb_write_files, nb_nodes_write);
        this->registerJob(jobID, run.id, writeJob, true);

        // Optional COPY job
        if (run.writtenBytes <= this->config->stor.write_bytes_copy_thres) {
            auto copyJob = jms.jobManager->createCompoundJob("outCopy_id" + jobID + "_run" + std::to_string(run.id));
            this->copyToPermanent(jms.bareMetalCS, copyJob, jms.serviceSpecificArgs, run.writtenBytes, output_data, nb_nodes_write);
            this->registerJob(jobID, run.id, copyJob, true);
            WRENCH_DEBUG("[%s-%u] 'Out' copy job added with %d nodes", jobID.c_str(), run.id, nb_nodes_write);
        }
    }

    void Controller::submitJob(const std::string &jobID) {

        wrench::TerminalOutput::setThisProcessLoggingColor(wrench::TerminalOutput::COLOR_GREEN);

        // ########################################################################################
        // Creating the top level-job (logical equivalent of a reservation on the resource manager)
        // Save job for future analysis (note : we'll need to find sub jobs one by one by ID)
        this->sim_jobs[jobID] = {
            this->jobs.find(jobID)->second,                                         // Yaml Job
            this->job_manager->createCompoundJob(jobID),                            // reservationJob
            std::map<uint32_t, std::vector<std::shared_ptr<wrench::CompoundJob>>>() // sub-jobs
        };

        auto yJob = this->sim_jobs[jobID].yamlJob; // for convenience only

        /* Make sure the reservation lasts for as long as the real one, no matter what happens inside. */
        this->sim_jobs[jobID].reservationJob->addSleepAction("fullRuntimeSleep",
                                                             static_cast<double>(std::min(
                                                                 yJob.runtimeSeconds,
                                                                 yJob.walltimeSeconds - 1)));
        WRENCH_INFO("[%s] Preparing reservation job for submission", jobID.c_str());
        // ########################################################################################

        // ########################################################################################
        // Add the actual workload found in Darshan traces (if any)
        if (this->sim_jobs[jobID].yamlJob.runs.size() != 0) {

            WRENCH_INFO("[%s] Preparing workload custom action", yJob.id.c_str());

            /* Custom action holds a specific job manager, allowing for multiple possibly
             * subjobs instead of sequential actions */
            this->sim_jobs[jobID].reservationJob->addCustomAction(
                "workload_" + yJob.id,
                0, 0, // RAM & num cores, unused
                [this, jobID](const std::shared_ptr<wrench::ActionExecutor> &action_executor) {
                    wrench::TerminalOutput::setThisProcessLoggingColor(wrench::TerminalOutput::COLOR_WHITE);

                    XBT_CDEBUG(fives_controller, "[%s] Testing debug message inside lambda action", jobID.c_str());

                    fives::JobManagementStruct jms;
                    jms.jobManager = action_executor->createJobManager();
                    jms.executionService = action_executor->getActionExecutionService();
                    jms.bareMetalCS = std::dynamic_pointer_cast<wrench::BareMetalComputeService>(jms.executionService->getParentService());

                    // Keep trace of all exec_jobs for each of the Darshan records / monitored runs of an application
                    // std::map<unsigned int, std::vector<std::shared_ptr<wrench::CompoundJob>>> exec_jobs;
                    this->stripes_per_action[jobID] = std::map<unsigned int, std::map<std::string, unsigned int>>(); // empty map for new job

                    /**
                     *  SUB-JOBS CREATION
                     *  Loop on all 'runs' entries (Darshan records) for this job/reservation
                     */
                    for (const auto &run : this->sim_jobs[jobID].yamlJob.runs) {

                        WRENCH_INFO("[%s-%u] Preparing sub-jobs", jobID.c_str(), run.id);

                        this->stripes_per_action[jobID][run.id] = std::map<std::string, unsigned int>();
                        this->sim_jobs[jobID].subJobs[run.id] = std::vector<std::shared_ptr<wrench::CompoundJob>>();

                        /* 1. Create a sleep sub-job if I/O operations don't start right away in this run */
                        if (run.sleepDelay != 0) {
                            this->addSleepJob(jms, jobID, run);
                        }

                        /* 2. Create I/O jobs */
                        if (run.readBytes != 0) {
                            this->addReadJob(jms, jobID, run);
                        }
                        if (run.writtenBytes != 0) {
                            this->addWriteJob(jms, jobID, run);
                        }
                    }

                    /**
                     *  SUB-JOBS SUBMISSION
                     *  Loop a second time on the Darshan records associated with the
                     *  current reservation and submit them
                     */
                    WRENCH_INFO("[%s] Submitting sub-jobs", jobID.c_str());
                    for (const auto &run : this->sim_jobs[jobID].yamlJob.runs) {
                        for (const auto &subJob : this->sim_jobs[jobID].subJobs[run.id]) {

                            if (jms.bareMetalCS->hasReturnedFromMain()) {
                                WRENCH_WARN("Bare metal service has already returned from main");
                            }
                            if (jms.bareMetalCS->getState() != wrench::S4U_Daemon::State::UP) {
                                WRENCH_WARN("Bare metal service is not up anymore");
                            }
                            WRENCH_DEBUG("[%s-%u] Using internal job manager %s and bare_metal %s", jobID.c_str(), run.id, jms.jobManager->getName().c_str(), jms.bareMetalCS->getName().c_str());
                            WRENCH_INFO("[%s-%u] Submitting job %s with %lu actions", jobID.c_str(), run.id, subJob->getName().c_str(), subJob->getActions().size());
                            jms.jobManager->submitJob(subJob, jms.bareMetalCS, jms.serviceSpecificArgs[subJob->getName()]);
                        }
                    }

                    /**
                     *  Waiting for job completion (this synchronous wait is one of the reasons we have to encapsulate
                     *  the reservation inside a custom action with it's own job manager)
                     */
                    WRENCH_INFO("[%s] Waiting for sub jobs completion", jobID.c_str());
                    for (const auto &run : this->sim_jobs[jobID].yamlJob.runs) {
                        for (const auto &subJob : this->sim_jobs[jobID].subJobs[run.id]) {
                            auto event = action_executor->waitForNextEvent();
                            auto completed_event = std::dynamic_pointer_cast<wrench::CompoundJobCompletedEvent>(event);
                            if (not completed_event) {
                                auto failure = std::dynamic_pointer_cast<wrench::CompoundJobFailedEvent>(event);
                                throw std::runtime_error("One of the subjobs failed -> " + failure->toString() + " // " + failure->failure_cause->toString());
                            }
                            WRENCH_DEBUG("[%s] completed", completed_event->job->getName().c_str());
                        }
                    }
                },
                [jobID](std::shared_ptr<wrench::ActionExecutor> action_executor) {
                    WRENCH_INFO(" >> [reservation customAction_%s] terminating", jobID.c_str());
                });
        }
        // ########################################################################################

        // Submit job
        std::map<std::string, std::string> service_specific_args =
            {{"-N", std::to_string(yJob.nodesUsed)},                                                             // nb of nodes
             {"-c", std::to_string(yJob.coresUsed / yJob.nodesUsed)},                                            // cores per node
             {"-t", std::to_string(static_cast<int>(yJob.walltimeSeconds * this->config->walltime_extension))}}; // seconds
        WRENCH_INFO("[%s] Submitting reservation job (%d nodes, %d cores per node, %ds of walltime)",
                    this->sim_jobs[jobID].reservationJob->getName().c_str(),
                    yJob.nodesUsed, yJob.coresUsed / yJob.nodesUsed, yJob.walltimeSeconds);
        job_manager->submitJob(this->sim_jobs[jobID].reservationJob, this->compute_service, service_specific_args);
        WRENCH_INFO("[%s] Job successfully submitted", this->sim_jobs[jobID].reservationJob->getName().c_str());
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
        XBT_CDEBUG(fives_controller, "[%s] Testing debug message read from temporary", jobID.c_str());

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
                WRENCH_DEBUG("[%s] readFromTemporary: For file %s (size %lld) : we have only %u stripes, but %u hosts => reading 1 stripes from the first %u hosts",
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
                WRENCH_DEBUG("[%s] readFromTemporary: For file %s (size %lld) : %u stripes, reading %u +- %u stripes from each host (%u hosts will read more)",
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

                auto overhead = readJob->addSleepAction("read_overhead_" + action_id, this->config->stor.static_read_overhead_seconds);
                auto file_read = readJob->addFileReadAction(action_id, wrench::FileLocation::LOCATION(this->compound_storage_service, read_file), read_byte_per_node);
                readJob->addActionDependency(overhead, file_read);
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

        WRENCH_INFO("[%s] Creating sub-job %s (%ld bytes, on %u files and at most %u IO nodes [0=all])",
                    jobID.c_str(), writeJob->getName().c_str(), writtenBytes, nb_files, max_nb_hosts);
        XBT_CDEBUG(fives_controller, "[%s] Testing debug message write to temporary", jobID.c_str());

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

            unsigned int local_stripe_count = this->getWriteStripeCount(this->sim_jobs[jobID].yamlJob.cumulWriteBW);

            auto file_stripes = this->compound_storage_service->lookupOrDesignateStorageService(wrench::FileLocation::LOCATION(this->compound_storage_service, write_file), local_stripe_count);

            unsigned int nb_stripes = file_stripes.size();
            stripes_per_file[write_file] = nb_stripes;
            stripes_per_host_per_file[write_file] = std::vector<unsigned int>();

            // How many stripes should be written by each host in use ?
            if (nb_stripes < max_nb_hosts) {
                for (unsigned int i = 0; i < nb_stripes; i++) {
                    stripes_per_host_per_file[write_file].push_back(1);
                }
                WRENCH_DEBUG("[%s] writeToTemporary: For file %s (size %lld) : we have only %u stripes, but %u hosts, writing 1 stripes from the first %u hosts",
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
                WRENCH_DEBUG("[%s] writeToTemporary: For file %s (size %lld) : %u stripes, writing %u stripes from each host and last host writes %u stripes",
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

                auto overhead = writeJob->addSleepAction("write_overhead_" + action_id, this->config->stor.static_write_overhead_seconds);
                auto write_file = writeJob->addCustomAction(customWriteAction);
                writeJob->addActionDependency(overhead, write_file);

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
    void Controller::processEventCompoundJobCompletion(const std::shared_ptr<wrench::CompoundJobCompletedEvent> &event) {
        auto job = event->job;
        WRENCH_INFO("[%s] Notified that this compound job has completed", job->getName().c_str());

        // Extract relevant informations from job and write them to file / send them to DB ?
        processCompletedJob(job->getName());
    }

    void Controller::processEventTimer(const std::shared_ptr<wrench::TimerEvent> &timerEvent) {
        this->submitJob(timerEvent->message);
    }

    /**
     * @brief Process a compound job failure event
     *
     * @param event: the event
     */
    void Controller::processEventCompoundJobFailure(const std::shared_ptr<wrench::CompoundJobFailedEvent> &event) {
        /* Retrieve the job that this event is for and failure cause*/
        auto job = event->job;
        auto cause = event->failure_cause;
        wrench::TerminalOutput::setThisProcessLoggingColor(wrench::TerminalOutput::COLOR_RED);
        WRENCH_WARN("[%s] Notified that this compound job has failed: %s", job->getName().c_str(), cause->toString().c_str());
        wrench::TerminalOutput::setThisProcessLoggingColor(wrench::TerminalOutput::COLOR_GREEN);
        this->failed_jobs_count += 1;
        this->sim_jobs.erase(job->getName());
    }

    std::pair<std::string, std::string> updateIoUsageDelete(std::map<std::string, StorageServiceIOCounters> &volume_records, const std::shared_ptr<wrench::FileLocation> &location) {

        auto storage_service = location->getStorageService()->getName();
        auto mount_pt = location->getFilePath(); // TODO : RECENT FIX, NEED TO CHECK IF DATA IS CORRECT
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
        auto dst_path = location->getFilePath(); // TODO : RECENT FIX, NEED TO CHECK IF DATA IS CORRECT
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
        auto path = location->getFilePath(); // TODO : RECENT FIX, NEED TO CHECK IF DATA IS CORRECT
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

    void Controller::processActions(YAML::Emitter &out_jobs, const std::set<std::shared_ptr<wrench::Action>> &actions, double &job_start_time, const std::string &job_id, uint32_t run_id) {

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

            job_start_time = min(job_start_time, action->getStartDate());

            // WRENCH_DEBUG("  - Action type : %s at ts: %f", act_type.c_str(), action->getStartDate());

            if (auto fileRead = std::dynamic_pointer_cast<wrench::FileReadAction>(action)) {

                auto usedLocation = fileRead->getUsedFileLocation();
                auto usedFile = usedLocation->getFile();

                out_jobs << YAML::Key << "file_name" << YAML::Value << usedFile->getID();
                out_jobs << YAML::Key << "file_size_bytes" << YAML::Value << usedFile->getSize();
                out_jobs << YAML::Key << "io_size_bytes" << YAML::Value << fileRead->getNumBytesToRead();
                out_jobs << YAML::Key << "nb_stripes" << YAML::Value << this->stripes_per_action[job_id][run_id][action->getName()];

            } else if (auto fileWrite = std::dynamic_pointer_cast<fives::PartialWriteCustomAction>(action)) {
                // auto usedLocation = fileWrite->getFileLocation();
                auto usedFile = fileWrite->getFile();

                auto write_trace = this->compound_storage_service->write_traces[usedFile->getID()];
                out_jobs << YAML::Key << "internal_locations" << YAML::Value << write_trace.internal_locations.size();
                out_jobs << YAML::Key << "parts_count" << YAML::Value << write_trace.parts_count;
                out_jobs << YAML::Key << "file_name" << YAML::Value << usedFile->getID();
                out_jobs << YAML::Key << "file_size_bytes" << YAML::Value << usedFile->getSize();
                out_jobs << YAML::Key << "io_size_bytes" << YAML::Value << fileWrite->getWrittenSize();
                out_jobs << YAML::Key << "nb_stripes" << YAML::Value << this->stripes_per_action[job_id][run_id][action->getName()];

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
                out_jobs << YAML::Key << "sss_file_path" << YAML::Value << sss->getFilePath();
                out_jobs << YAML::Key << "sss_file_name" << YAML::Value << sss->getFile()->getID();
                out_jobs << YAML::Key << "sss_file_size_bytes" << YAML::Value << sss->getFile()->getSize();
                out_jobs << YAML::Key << "css" << YAML::Value << css->getStorageService()->getName();
                out_jobs << YAML::Key << "css_server" << YAML::Value << css->getStorageService()->getHostname();
                out_jobs << YAML::Key << "css_file_path" << YAML::Value << css->getFilePath();
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
                out_jobs << YAML::Key << "file_path" << YAML::Value << usedLocation->getFilePath();
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

        auto job_entry = this->sim_jobs[job_id];

        const auto &yaml_job = job_entry.yamlJob;
        const auto job_list = job_entry.subJobs;
        const auto &reservation_job = job_entry.reservationJob;

        // Ignore preload jobs in the output results
        if (reservation_job->getName().substr(0, 7) == "preload")
            return;

        this->completed_jobs << YAML::BeginMap; // Job map

        // ## High level 'reservation' job informations
        WRENCH_INFO("[%s] Processing metrics...", reservation_job->getName().c_str());
        this->completed_jobs << YAML::Key << "job_uid" << YAML::Value << reservation_job->getName();
        this->completed_jobs << YAML::Key << "job_status" << YAML::Value << reservation_job->getStateAsString();
        this->completed_jobs << YAML::Key << "job_submit_ts" << YAML::Value << reservation_job->getSubmitDate();
        this->completed_jobs << YAML::Key << "job_end_ts" << YAML::Value << reservation_job->getEndDate();
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
        this->completed_jobs << YAML::Key << "category" << YAML::Value << yaml_job.category;

        // ## Processing actions for all sub jobs related to the current top-level job being processed
        this->completed_jobs << YAML::Key << "actions" << YAML::Value << YAML::BeginSeq;

        // Actual values determined after processing all actions from different sub jobs
        double earliest_action_start_time = UINT64_MAX;
        // Note that we start at ++begin(), to skip the first job (reservation job) in vector
        for (const auto &[run_id, subjobs] : job_list) {
            for (const auto &subjob : subjobs) {
                WRENCH_DEBUG("Now processing actions of sub job %s", subjob->getName().c_str());
                auto actions = subjob->getActions();
                processActions(this->completed_jobs, std::move(actions), earliest_action_start_time, job_id, run_id);
            }
        }

        // for (auto it = job_list.begin(); it < job_list.end(); it++) {
        //     WRENCH_DEBUG("Actions of job %s : ", it->get()->getName().c_str());
        //     auto actions = (it->get())->getActions();
        //     WRENCH_DEBUG(" ->> %lu", actions.size());
        //     processActions(this->completed_jobs, actions, earliest_action_start_time, job_id);
        // }

        this->completed_jobs << YAML::EndSeq; // actions

        // ## High level keys that can be updated only after all actions are processed.
        this->completed_jobs << YAML::Key << "job_start_ts" << YAML::Value << earliest_action_start_time;
        this->completed_jobs << YAML::Key << "job_waiting_time_s" << YAML::Value << earliest_action_start_time - reservation_job->getSubmitDate();
        this->completed_jobs << YAML::Key << "job_runtime_s" << YAML::Value << reservation_job->getEndDate() - earliest_action_start_time;

        this->completed_jobs << YAML::EndMap; // job map

        if (not config->testing) {
            this->sim_jobs.erase(job_id); // cleanup job map once the job is analysed
        }

        WRENCH_INFO("Controller::processCompletedJob: Job %s processed and removed from job map", job_id.c_str());
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

} // namespace fives
