/*
 * An execution controller to execute a workflow
 */

#include "Controller.h"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <utility>
#include <wrench/services/helper_services/action_execution_service/ActionExecutionService.h>
#include <wrench/util/UnitParser.h>
#include <xbt.h>

#include "yaml-cpp/yaml.h"

// Uncomment to print Job submissions and completion on stdout (was to lazy to link this to the cmake, it's only debug)
// #define CONSOLE_OUTPUT

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
     *        Note that:
     *          - shared_ptr are passed by value: we could have gone for references, as the Controller
     *            will always be destroyed before the pointers become invalid, but it doesn't cost much
     *            and it looks slightly cleaner
     *          - Yaml jobs map is also passed by value, but allowed to be moved, and effectively not used
     *            anywhere in the caller after the controller is created. In principle, the compiler should
     *            avoid the copy
     *
     * @param compute_service   A compute services available to run actions
     * @param storage_services  A set of storage services available to store files
     * @param compound_storage_service  A pointer to a compound storage service
     * @param hostname the name of the host on which to start the WMS
     * @param jobs A map (by id) of YamlJobs from the job data file used for the simulation
     * @param fives_config A pointer to the parsed configuration file for the simulation
     */
    Controller::Controller(std::shared_ptr<wrench::ComputeService> compute_service,
                           std::shared_ptr<wrench::SimpleStorageService> storage_service,
                           std::shared_ptr<wrench::CompoundStorageService> compound_storage_service,
                           const std::string &hostname,
                           std::map<std::string, YamlJob> jobs,
                           std::shared_ptr<fives::Config> fives_config) : ExecutionController(hostname, "controller"),
                                                                          compute_service(compute_service),
                                                                          storage_service(storage_service),
                                                                          compound_storage_service(compound_storage_service),
                                                                          jobs(std::move(jobs)),
                                                                          config(fives_config) {}

    /**
     * @brief main method of the Controller. Responsible for:
     *         - reading and setting a few parameters
     *         - running the data preload function (creating some file on storage *before* any job starts)
     *         - parsing all yaml jobs and triggering their submission into the simulation:
     *              - each job has a 'sleepSimulationSeconds' fields that indicates at which point in the
     *                simulation it should be submitted (how long after the previous job)
     *              - this value is used to setup a WRENCH timer for each job, which upon releasing an event,
     *                will trigger the actual creation and submission of the job, at the correct simulated time
     *              - while waiting for the trigger of a job submission, we may alwo have to handle the completion
     *                of another job, which we wait for in a while loop (timer off, completion or failure events
     *                may happen, but only the timer will also make us leave the while loop and setup a new timer
     *                for the next job)
     *          - waiting for any remaining running jobs to complete (second loop)
     *          - 'closing' the yaml document containing simulation results
     *
     * When the controller returns, the main of the simulators knows the results are ready to be written to file,
     * and the simulation can be completed.
     *
     * @return 0 on completion
     *
     * @throw std::runtime_error
     */
    int Controller::main() {

        wrench::TerminalOutput::setThisProcessLoggingColor(wrench::TerminalOutput::COLOR_GREEN);
        WRENCH_INFO("Controller : %s jobs to create and submit", std::to_string(jobs.size()).c_str());

        this->completed_jobs << YAML::BeginSeq;

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
                    throw std::runtime_error("[During loop for " + yaml_entry.first + "] Unexpected Controller Event : " + event->toString());
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
     * @brief Getter for a reservation job by its id.
     * @param id String id of the reservation job to look for ('id' field in the yaml data)
     * @return shared_ptr on CompoundJob, pointing to a 'reservation job' OR a nullptr (in a shared)
     */
    std::shared_ptr<wrench::CompoundJob> Controller::getReservationJobById(const std::string &id) {

        auto job_entry = this->sim_jobs.find(id);
        if (job_entry == this->sim_jobs.end()) {
            WRENCH_WARN("Controller::getCompletedJobsById: Job ID %s not found", id.c_str());
            return std::shared_ptr<wrench::CompoundJob>(nullptr);
        }

        return job_entry->second.reservationJob;
    }

    /**
     * @brief Getter for subjobs of a given yaml job, found by the reservation job id.
     *        Note that every compound job in this map is actually a list of sub-jobs started from
     *        inside the customAction of the reservation job.
     * @param id String id of the reservation job for which you want subjobs
     * @return Vector of shared_ptr on Compound jobs, including all sub-jobs but not the 'reservation' job
     */
    fives::subjobsPerRunMap Controller::getCompletedJobsById(const std::string &id) {

        auto job_entry = this->sim_jobs.find(id);
        if (job_entry == this->sim_jobs.end()) {
            WRENCH_WARN("Controller::getCompletedJobsById: Job ID %s not found", id.c_str());
            return {};
        }

        return job_entry->second.subJobs;
    }

    /**
     * @brief Create files on the CSS prior to running any job. This is used in order to start the simulation with some of the
     *        job data already in place, on a non-empty PFS model (as if dataset had been setup in a scratch space before running
     *        experiments, and were to be used by multiple runs over a given period of time)
     *        This concerns only the read actions of jobs, and jobs for which data is preloaded are chosen based on a configured
     *        threshold on the amount of read bytes (read_bytes_preload_thres).
     *
     *        Files created during preload are prefixed "plIn_id<jobID>_run<run number>" (plIn -> "preload input")
     */
    void Controller::preloadData() {

        for (const auto &job : this->jobs) {
            for (const auto &run : job.second.runs) {

                if ((run.readBytes > 0) and (run.readBytes >= this->config->stor.read_bytes_preload_thres)) {

                    unsigned int local_stripe_count = this->getReadStripeCount(job.second.cumulReadBW);

                    auto prefix = "plIn_id" + job.first + "_run" + std::to_string(run.id);
                    WRENCH_DEBUG("[%s] preloading file prefix %s on external storage system", job.first.c_str(), prefix.c_str());
                    auto read_files = this->createFileParts(run.readBytes, run.read_files_count, prefix);

                    for (const auto &read_file : read_files) {
                        auto file_stripes = this->compound_storage_service->lookupOrDesignateStorageService(
                            wrench::FileLocation::LOCATION(
                                this->compound_storage_service,
                                read_file),
                            local_stripe_count);
                        for (const auto &stripe : file_stripes) {
                            wrench::StorageService::createFileAtLocation(stripe);
                        }
                    }

                    this->preloadedData[job.first + "_" + std::to_string(run.id)] = read_files;
                }
            }
        }
    }

    /**
     * @brief Determine the number of compute nodes that should be used for read file actions (in a given job)
     *        If the computed cumulated read bw of a job is below a configured threshold, only 1 node is ever used.
     *        Alternatively, a percentage of the number of available nodes is used. It is computed from other parameters
     *        such as the currently used stripe_count and the job cumulated read bandwidth. It can't be less than 2 nodes
     * @return Number of compute nodes onto which read file actions should be load balanced
     */
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

    /**
     * @brief Determine the number of compute nodes that should be used for write file actions (in a given job)
     *        If the computed cumulated write bw of a job is below a configured threshold, only 1 node is ever used.
     *        Alternatively, a percentage of the number of available nodes is used. It is computed from other parameters
     *        such as the currently used stripe_count and the job cumulated write bandwidth. It can't be less than 2 nodes
     * @return Number of compute nodes onto which write file actions should be load balanced
     */
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

    /**
     * @brief Compute and return the stripe count to be applied for a given job for read files.
     *        By default, the strip_count is set for all jobs and both read and written files in a single
     *        configuration value (lustre.stripe_count), but if an additional threshold is set, we can end up
     *        increasing the stripe count based on cumulated read bandwidth for jobs that seem to be reading faster
     *        than most others.
     * @return Chosen stripe_count value (read file case)
     */
    unsigned int Controller::getReadStripeCount(double cumul_read_bw) const {

        unsigned int local_stripe_count = this->config->lustre.stripe_count;

        if (this->config->lustre.stripe_count_high_thresh_read && cumul_read_bw >= this->config->lustre.stripe_count_high_thresh_read) {
            auto ratio = std::ceil(cumul_read_bw / this->config->lustre.stripe_count_high_thresh_read);
            local_stripe_count = this->config->lustre.stripe_count_high_read_add * ratio;
            local_stripe_count = std::min(local_stripe_count, this->ost_count);
        }

        return local_stripe_count;
    }

    /**
     * @brief Compute and return the stripe count to be applied for a given job for written files.
     *        By default, the strip_count is set for all jobs and both read and written files in a single
     *        configuration value (lustre.stripe_count), but if an additional threshold is set, we can end up
     *        increasing the stripe count based on cumulated write bandwidth for jobs that seem to be reading faster
     *        than most others.
     * @return Chosen stripe_count value (written file case)
     */
    unsigned int Controller::getWriteStripeCount(double cumul_write_bw) const {

        unsigned int local_stripe_count = this->config->lustre.stripe_count;

        if (this->config->lustre.stripe_count_high_thresh_write && cumul_write_bw >= this->config->lustre.stripe_count_high_thresh_write) {
            auto ratio = std::ceil(cumul_write_bw / this->config->lustre.stripe_count_high_thresh_write);
            local_stripe_count = this->config->lustre.stripe_count_high_write_add * ratio;
            local_stripe_count = std::min(local_stripe_count, this->ost_count);
        }

        return local_stripe_count;
    }

    /**
     * @brief Compute and return the number of file that should be created on the storage system to
     *        account for the bytes volume read by the job.
     *        The default is 1 file (then possibly striped into lots of chunks over multiple OST), but this value
     *        is then updated using a configuration value and the currently chosen stripe_count
     * @return Number of files to create in order to support file read actions
     */
    // unsigned int Controller::getReadFileCount(unsigned int stripe_count) const {

    //     unsigned int nb_files = 1;
    //     nb_files = this->config->stor.nb_files_per_read * stripe_count;
    //     nb_files = std::max(nb_files, 1u);

    //     return nb_files;
    // }

    /**
     * @brief Compute and return the number of file that should be created on the storage system to
     *        account for the bytes volume written by the job.
     *        The default is 1 file (then possibly striped into lots of chunks over multiple OST), but this value
     *        is then updated using a configuration value and the currently chosen stripe_count
     * @return Number of files to create in order to support file write actions
     */
    // unsigned int Controller::getWriteFileCount(unsigned int stripe_count) const {

    //     unsigned int nb_files = 1;
    //     nb_files = this->config->stor.nb_files_per_write * stripe_count;
    //     nb_files = std::max(nb_files, 1u);
    //     return nb_files;
    // }

    /**
     * @brief Add a job to the global job map (sim_jobs). The map is organized by 'reservation' job ID
     *        (top-level job created by an HPC batch scheduler), and 'run' ID, which correspond to every single
     *        application run traced by Darshan within a single reservation.
     * @param jobId ID of the top level job ('id' field in yaml data)
     * @param runId ID (number) of the traced application run (starting at 1)
     * @param job   Pointer to CompoundJob that needs to be registered
     * @param child Whether or not to set this job as the child of the previous job in the same run
     *              (which conditions the sequential or // execution of jobs)
     */
    void Controller::registerJob(const std::string &jobId,
                                 uint32_t runId,
                                 std::shared_ptr<wrench::CompoundJob> job,
                                 bool child) {
        if (this->sim_jobs[jobId].subJobs[runId].size() != 0 && child) {
            this->sim_jobs[jobId].subJobs[runId].back()->addChildJob(job);
        }
        this->sim_jobs[jobId].subJobs[runId].push_back(job);
    }

    /**
     * @brief Add a sleep job inside the custom action of a reservation.
     *
     */
    void Controller::addSleepJob(JobManagementStruct &jms,
                                 const std::string &jobID,
                                 const DarshanRecord &run) {

        auto sleepJob = jms.jobManager->createCompoundJob("sleep_id" + jobID + "_run" + std::to_string(run.id));
        sleepJob->addSleepAction("sleep", run.sleepDelay);
        this->registerJob(jobID, run.id, sleepJob, true);
        jms.serviceSpecificArgs[sleepJob->getName()] = std::map<std::string, std::string>();
        jms.serviceSpecificArgs[sleepJob->getName()]["sleep"] = {};
        WRENCH_DEBUG("[%s-%u] addSleepJob: Sleep job added for %lu s", jobID.c_str(), run.id, run.sleepDelay);
    }

    /**
     * @brief Add a read sub-job inside the custom action of a reservation.
     *        The read job is possibly preceded by a copy job if the file read have not
     *        already been created by a previous call to 'preloadData'() and/or are out of scope
     *        for 'preloadData()'
     *
     *        This is called for each run of reservation job (-> if multiple Darshan traces are associated
     *        with a single batch scheduler job, it will be called once of each application traced with Darshan)
     */
    void Controller::addReadJob(JobManagementStruct &jms,
                                const std::string &jobID,
                                const DarshanRecord &run) {

        double run_mean_read_bw = run.readBytes / run.readTimeSeconds;
        auto read_stripe_count = this->getReadStripeCount(run_mean_read_bw); // lustre param
        // Assuming 1 proc/thread per core, maximum number of nodes used by the recorded application
        // (not necessarily all the nodes from the reservation)
        unsigned int max_nodes_run = max(std::ceil(run.nprocs / this->config->compute.core_count), 1.0);
        auto nb_nodes_read = this->getReadNodeCount(max_nodes_run, run_mean_read_bw, read_stripe_count);
        WRENCH_DEBUG("[%s-%u] addReadJob: %d nodes will be doing copy/read IOs", jobID.c_str(), run.id, nb_nodes_read);

        std::vector<std::shared_ptr<wrench::DataFile>> input_files;

        // Optional COPY sub-job
        if (this->preloadedData.find(jobID + "_" + std::to_string(run.id)) == this->preloadedData.end()) {

            auto copyJob = jms.jobManager->createCompoundJob("inCopy_id" + jobID + "_run" + std::to_string(run.id));
            input_files = this->copyFromPermanent(jms, copyJob, run, nb_nodes_read);
            this->registerJob(jobID, run.id, copyJob, true);

            WRENCH_DEBUG("[%s-%u] addReadJob: 'In' copy job added with %d nodes", jobID.c_str(), run.id, nb_nodes_read);

        } else { // No need for copy jobs, files have been created already
            input_files = this->preloadedData[jobID + "_" + std::to_string(run.id)];
        }

        // READ sub-job
        auto readJob = jms.jobManager->createCompoundJob("rdFiles_id" + jobID + "_run" + std::to_string(run.id));
        this->readFromTemporary(jms, readJob, run, this->stripes_per_action[jobID][run.id], input_files, nb_nodes_read);
        this->registerJob(jobID, run.id, readJob, true);

        WRENCH_DEBUG("[%s-%u] addReadJob: Read job added with %d nodes", jobID.c_str(), run.id, nb_nodes_read);
    }

    void Controller::addWriteJob(JobManagementStruct &jms,
                                 const std::string &jobID,
                                 const DarshanRecord &run) {

        double run_mean_write_bw = run.writtenBytes / run.writeTimeSeconds;
        auto write_stripe_count = this->getWriteStripeCount(run_mean_write_bw);
        // Assuming 1 proc/thread per core, maximum number of nodes used by the recorded application
        // (not necessarily all the nodes from the reservation)
        unsigned int max_nodes_run = max(std::ceil(run.nprocs / this->config->compute.core_count), 1.0);
        auto nb_nodes_write = this->getWriteNodeCount(max_nodes_run, run_mean_write_bw, write_stripe_count);
        WRENCH_DEBUG("[%s-%u] addWriteJob: %d nodes will be doing write/copy IOs", jobID.c_str(), run.id, nb_nodes_write);

        std::vector<std::shared_ptr<wrench::DataFile>> output_data;

        auto writeJob = jms.jobManager->createCompoundJob("wrFiles_id" + jobID + "_run" + std::to_string(run.id));
        output_data = this->writeToTemporary(jms, writeJob, run, this->stripes_per_action[jobID][run.id], nb_nodes_write, write_stripe_count);
        this->registerJob(jobID, run.id, writeJob, true);

        // Optional COPY job
        if (run.writtenBytes <= this->config->stor.write_bytes_copy_thres) {
            auto copyJob = jms.jobManager->createCompoundJob("outCopy_id" + jobID + "_run" + std::to_string(run.id));
            this->copyToPermanent(jms, copyJob, run, output_data, nb_nodes_write);
            this->registerJob(jobID, run.id, copyJob, true);
            WRENCH_DEBUG("[%s-%u] addWriteJob: 'Out' copy job added with %d nodes", jobID.c_str(), run.id, nb_nodes_write);
        }
    }

    /**
     * @brief Find a YAML job in the dataset (by ID)
     */
    void Controller::submitJob(const std::string &jobID) {

        wrench::TerminalOutput::setThisProcessLoggingColor(wrench::TerminalOutput::COLOR_GREEN);
        static uint32_t job_submission_counter = 0;

        WRENCH_INFO("[%s] Preparing reservation job for submission", jobID.c_str());

        // ########################################################################################
        // Creating the top level-job (logical equivalent of a reservation on the resource manager)
        // Save job for future processing (note : we'll need to find sub jobs one by one by ID)

        const YamlJob *yJob = &(this->jobs.find(jobID)->second);
        this->sim_jobs[jobID] = {
            yJob,                                        // Yaml Job
            this->job_manager->createCompoundJob(jobID), // reservationJob
            fives::subjobsPerRunMap()                    // sub-jobs
        };

        /* Make sure the reservation lasts for as long as the real one, no matter what happens inside. */
        this->sim_jobs[jobID].reservationJob->addSleepAction("fullRuntimeSleep",
                                                             static_cast<double>(std::min(
                                                                 yJob->runtimeSeconds,
                                                                 yJob->walltimeSeconds - 1)));
        // ########################################################################################

        // ########################################################################################
        // Add the actual workload found in Darshan traces (if any)
        if (this->sim_jobs[jobID].yamlJob->runs.size() != 0) {

            WRENCH_INFO("[%s] Preparing workload custom action", yJob->id.c_str());

            /* Custom action holds a specific job manager, allowing for multiple possibly
             * subjobs instead of sequential actions */
            this->sim_jobs[jobID].reservationJob->addCustomAction(
                "workload_" + yJob->id,
                0, 0, // RAM & num cores, unused
                [this, jobID](const std::shared_ptr<wrench::ActionExecutor> &action_executor) {
                    fives::JobManagementStruct jms;
                    jms.jobManager = action_executor->createJobManager();
                    jms.executionService = action_executor->getActionExecutionService();
                    jms.bareMetalCS = std::dynamic_pointer_cast<wrench::BareMetalComputeService>(jms.executionService->getParentService());

                    this->stripes_per_action[jobID] = std::map<unsigned int, std::map<std::string, unsigned int>>(); // empty map for new job

                    /**
                     *  SUB-JOBS CREATION
                     *  Loop on all 'runs' entries (Darshan records) for this job/reservation
                     */
                    for (const auto &run : this->sim_jobs[jobID].yamlJob->runs) {

                        WRENCH_INFO("[%s-%u] Preparing sub-jobs", jobID.c_str(), run.id);

                        this->stripes_per_action[jobID][run.id] = std::map<std::string, unsigned int>();
                        this->sim_jobs[jobID].subJobs[run.id] = std::vector<std::shared_ptr<wrench::CompoundJob>>();

                        /* 1. Create a sleep sub-job if I/O operations don't start right away in this run */
                        if (run.sleepDelay != 0) {
                            this->addSleepJob(jms, jobID, run);
                        }

                        /* 2. Create I/O sub-jobs (basic version: read then write) */
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
                    for (const auto &run : this->sim_jobs[jobID].yamlJob->runs) {
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
                    for (const auto &run : this->sim_jobs[jobID].yamlJob->runs) {
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
            {{"-N", std::to_string(yJob->nodesUsed)},                                                             // nb of nodes
             {"-c", std::to_string(yJob->coresUsed / yJob->nodesUsed)},                                           // cores per node
             {"-t", std::to_string(static_cast<int>(yJob->walltimeSeconds * this->config->walltime_extension))}}; // seconds
        WRENCH_INFO("[%s] Submitting reservation job (%d nodes, %d cores per node, %ds of walltime)",
                    this->sim_jobs[jobID].reservationJob->getName().c_str(),
                    yJob->nodesUsed, yJob->coresUsed / yJob->nodesUsed, yJob->walltimeSeconds);
        job_manager->submitJob(this->sim_jobs[jobID].reservationJob, this->compute_service, service_specific_args);
        WRENCH_INFO("[%s] Job successfully submitted", this->sim_jobs[jobID].reservationJob->getName().c_str());

#ifdef CONSOLE_OUTPUT
        std::cout << "Job " << jobID << " submitted [" << ++job_submission_counter << "/" << this->jobs.size() << "]" << std::endl;
#endif
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

    /** @brief For each read subjob, we need to match every stripe of the targetted file to a compute host from which
     *         the I/O will take place. This function does the load-balancing of stripes to participating compute hosts.
     *
     *  @param inputs A list of files for which stripes will need to be load-balanced onto multiple hosts
     *  @param participating_hosts_count Number of compute hosts that will ultimately receive the read actions to stripes.
     *  @param stripes_per_file A map to keep track of how many stripes are created for each file
     *  @param stripes_per_host_per_file A map to keep of how many stripes of each file are supposed to be handled by each compute hosts
     *  @param jobName Name of the read or write sub-job that called this (for logs) (default = "unknown")
     *
     *  @return Nothing, but both maps (stripes_per_file / stripes_per_host_per_file) are updated in place.
     */
    void Controller::setReadStripesPerHost(const std::vector<std::shared_ptr<wrench::DataFile>> &inputs,
                                           unsigned int participating_hosts_count,
                                           std::map<std::shared_ptr<wrench::DataFile>, unsigned int> &stripes_per_file,
                                           std::map<std::shared_ptr<wrench::DataFile>, std::vector<unsigned int>> &stripes_per_host_per_file,
                                           const std::string &jobName) {

        for (const auto &file : inputs) {

            // We manually invoke this in order to know how the files will be striped before starting I/O
            WRENCH_DEBUG("[%s] setStripesPerHost: Calling lookupFileLocation for file %s", jobName.c_str(), file->getID().c_str());

            auto file_stripes = this->compound_storage_service->lookupFileLocation(wrench::FileLocation::LOCATION(this->compound_storage_service, file));
            unsigned int nb_stripes = file_stripes.size();
            stripes_per_file[file] = nb_stripes;

            if (nb_stripes < participating_hosts_count) {

                stripes_per_host_per_file[file] = std::vector<unsigned int>(nb_stripes, 1);

                WRENCH_DEBUG("[%s] setStripesPerHost: For file %s (size %lld) : we have only %u stripes, but %u hosts => IO to 1 stripes from the first %u hosts",
                             jobName.c_str(), file->getID().c_str(), file->getSize(), nb_stripes, participating_hosts_count, nb_stripes);

            } else {

                unsigned int stripes_per_host = std::floor(nb_stripes / participating_hosts_count);
                unsigned int remainder = nb_stripes % participating_hosts_count;
                stripes_per_host_per_file[file] = std::vector<unsigned int>(participating_hosts_count, stripes_per_host);

                for (unsigned int i = 0; i < remainder; i++) {
                    stripes_per_host_per_file[file][i] += 1;
                }

                WRENCH_DEBUG("[%s] setStripesPerHost: For file %s (size %lld) : %u stripes, IO to %u +- %u stripes from each host (%u hosts will read more)",
                             jobName.c_str(), file->getID().c_str(), file->getSize(), nb_stripes, stripes_per_host, remainder, remainder);
            }
        }
    }

    /** @brief For each write subjob, we need to match every stripe of the targetted file to a compute host from which
     *         the I/O will take place. This function does the load-balancing of stripes to participating compute hosts.
     *         The logic is slighty different to that of the read version, as it needs to call lookupOrDesignateStorageService()
     *         instead of lookupFileLocation() (as files to write do not exist yet), and this process also requires to have
     *         a stripe_count value to feed lookupOrDesignateStorageService.
     *
     *  @param inputs A list of files for which stripes will need to be load-balanced onto multiple hosts
     *  @param participating_hosts_count Number of compute hosts that will ultimately received I/O actions to stripes.
     *  @param stripes_per_file A map to keep track of how many stripes are created for each file
     *  @param stripes_per_host_per_file A map to keep of how many stripes of each file are supposed to be handled by each compute hosts
     *  @param stripe_count A lustre stripe_count value for the current Darshan record, computed from I/O performance indicators
     *  @param jobName Name of the read or write sub-job that called this (for logs) (default = "unknown")
     *
     *  @return Nothing, but both maps (stripes_per_file / stripes_per_host_per_file) are updated in place.
     */
    // this->setWriteStripesPerHost(write_files, max_nb_hosts, stripes_per_file, stripes_per_host_per_file, write_stripe_count, writeJob->getName());
    void Controller::setWriteStripesPerHost(const std::vector<std::shared_ptr<wrench::DataFile>> &inputs,
                                            unsigned int participating_hosts_count,
                                            std::map<std::shared_ptr<wrench::DataFile>, unsigned int> &stripes_per_file,
                                            std::map<std::shared_ptr<wrench::DataFile>, std::vector<unsigned int>> &stripes_per_host_per_file,
                                            unsigned int stripe_count, // stripe count computed for each Darshan run
                                            const std::string &jobName) {

        for (const auto &file : inputs) {

            // We manually invoke this in order to know how the files will be striped before starting partial writes.
            WRENCH_DEBUG("[%s] setWriteStripesPerHost: Calling lookupOrDesignate for file %s", jobName.c_str(), file->getID().c_str());
            auto file_stripes = this->compound_storage_service->lookupOrDesignateStorageService(wrench::FileLocation::LOCATION(this->compound_storage_service, file), stripe_count);

            unsigned int nb_stripes = file_stripes.size();
            stripes_per_file[file] = nb_stripes;

            // How many stripes should be written by each host in use ?
            if (nb_stripes < participating_hosts_count) {

                stripes_per_host_per_file[file] = std::vector<unsigned int>(nb_stripes, 1);

                WRENCH_DEBUG("[%s] setWriteStripesPerHost: For file %s (size %lld) : we have only %u stripes, but %u hosts, writing 1 stripes from the first %u hosts",
                             jobName.c_str(), file->getID().c_str(), file->getSize(), nb_stripes, participating_hosts_count, nb_stripes);

            } else {
                unsigned int stripes_per_host = std::floor(nb_stripes / participating_hosts_count);
                unsigned int remainder = nb_stripes % participating_hosts_count;
                stripes_per_host_per_file[file] = std::vector<unsigned int>(participating_hosts_count, stripes_per_host);

                for (unsigned int i = 0; i < remainder; i++) {
                    stripes_per_host_per_file[file][i] += 1;
                }

                WRENCH_DEBUG("[%s] setWriteStripesPerHost: For file %s (size %lld) : %u stripes, writing %u stripes from each host and last host writes %u stripes",
                             jobName.c_str(), file->getID().c_str(), file->getSize(), nb_stripes, stripes_per_host, stripes_per_host + remainder);
            }
        }
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
    std::vector<std::shared_ptr<wrench::DataFile>> Controller::copyFromPermanent(JobManagementStruct &jms,
                                                                                 std::shared_ptr<wrench::CompoundJob> copyJob,
                                                                                 const DarshanRecord &run,
                                                                                 unsigned int max_nb_hosts) {

        WRENCH_INFO("[%s] copyFromPermanent: Creating copy sub-job (in) for a total read size of %ld bytes, on %u files and at most %u IO nodes (0 means all avail)",
                    copyJob->getName().c_str(), run.readBytes, run.read_files_count, max_nb_hosts);

        if (run.read_files_count < 1)
            throw std::runtime_error("[" + copyJob->getName() + "] copyFromPermanent: Copy should happen on 1 file at least");

        // Subdivide the amount of copied bytes between as many files as requested (one allocation per file)
        auto prefix = "inputFile_" + copyJob->getName();
        auto read_files = this->createFileParts(run.readBytes, run.read_files_count, prefix);

        // Prepare the list of compute host which may participate in the copy
        auto computeResources = jms.bareMetalCS->getPerHostNumCores();
        // Randomly remove nodes from resources in order to never use more than 'max_nb_hosts'
        this->pruneIONodes(computeResources, max_nb_hosts);
        auto computeResourcesIt = computeResources.begin();

        // Create one copy action per file and per host
        jms.serviceSpecificArgs[copyJob->getName()] = std::map<std::string, std::string>();
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

            jms.serviceSpecificArgs[copyJob->getName()][action_id] = computeResourcesIt->first;
            if (++computeResourcesIt == computeResources.end())
                computeResourcesIt = computeResources.begin();
        }

        return read_files;
    }

    /** @brief Creates read (and possibly metadata overhead sleep) actions inside a read sub-job.
     *
     *  @param jms  Ref. to the current job management services (from parent job)
     *  @param readJob Newly created subjob for reading phase
     *  @param run  Data for
     *  @param current_stripes_per_action Reference to an entry in the stripes_per_action map, for this job and this run.
     *  @param inputs  List of files which will be read
     *  @param max_nb_hosts Maximum number of hosts that should be handling the FileRead actions.
     */
    void Controller::readFromTemporary(JobManagementStruct &jms,
                                       std::shared_ptr<wrench::CompoundJob> readJob,
                                       const DarshanRecord &run,
                                       std::map<std::string, unsigned int> &current_stripes_per_action,
                                       std::vector<std::shared_ptr<wrench::DataFile>> inputs,
                                       unsigned int max_nb_hosts) {

        WRENCH_INFO("[%s] Creating read sub-job for  %lu file(s) with cumulative size %ld bytes, using %u IO nodes (0 means all avail)",
                    readJob->getName().c_str(), inputs.size(), run.readBytes, max_nb_hosts);

        // Prepare stripes-to-hosts matching
        std::map<std::shared_ptr<wrench::DataFile>, unsigned int> stripes_per_file{};
        std::map<std::shared_ptr<wrench::DataFile>, std::vector<unsigned int>> stripes_per_host_per_file{};
        this->setReadStripesPerHost(inputs, max_nb_hosts, stripes_per_file, stripes_per_host_per_file, readJob->getName());

        /* Get the list of currently allocated compute resources and randomly remove nodes from resources
           in order to never use more than 'max_nb_hosts' */
        auto computeResources = jms.bareMetalCS->getPerHostNumCores();
        this->pruneIONodes(computeResources, max_nb_hosts);

        // Using recorded meta time to add a sleep action before every read action
        std::shared_ptr<wrench::SleepAction> overhead;
        if (run.metaTimeSeconds) {
            if (run.writtenBytes) { // simple approach : divide metadata time between read and write
                overhead = readJob->addSleepAction("read_overhead_" + readJob->getName(), run.metaTimeSeconds / 2);
            } else { // no written bytes, metadata is for read only
                overhead = readJob->addSleepAction("read_overhead_" + readJob->getName(), run.metaTimeSeconds);
            }
        }

        // Init counters and iterators to follow which action goes to which resource
        auto computeResourcesIt = computeResources.begin();
        unsigned int action_cnt = 0;
        jms.serviceSpecificArgs[readJob->getName()] = std::map<std::string, std::string>();

        // Main loop, over the 'n' files read by this sub job
        for (const auto &read_file : inputs) {

            auto stripe_size = read_file->getSize() / stripes_per_file[read_file];

            // Secondary loop, to create one action per stripe, with the correct read volume
            for (const auto &stripes_per_host : stripes_per_host_per_file[read_file]) { // each entry in the vector stands for one host

                WRENCH_DEBUG("[%s] readFromTemporary: Creating read action for file %s and host %s",
                             readJob->getName().c_str(), read_file->getID().c_str(), computeResourcesIt->first.c_str());

                auto action_id = "FR_" + read_file->getID() + "_" + computeResourcesIt->first + "_act" + std::to_string(action_cnt++);
                double read_byte_per_node = 0;
                if (stripes_per_file[read_file] == stripes_per_host) {
                    // Only one host reading all the stripes
                    read_byte_per_node = read_file->getSize();
                    WRENCH_DEBUG("[%s] readFromTemporary: We'll be reading the entire file from this host.",
                                 readJob->getName().c_str());
                } else {
                    read_byte_per_node = stripe_size * stripes_per_host;
                    WRENCH_DEBUG("[%s] readFromTemporary:   We'll be reading %d stripes from this host, for a total of %f bytes from this host",
                                 readJob->getName().c_str(), stripes_per_host, read_byte_per_node);
                }

                // This is a reference to a particular map entry in the this->stripes_per_action, for the current job and run
                // It's used to keep trace the count of stripes used for all actions throughout the simulation (in a dirty way)
                current_stripes_per_action[action_id] = stripes_per_host;

                // ------------------------------------------------------------------
                // Create FILEREAD action (for a specific stripe and a specific host)
                auto file_read = readJob->addFileReadAction(
                    action_id,
                    wrench::FileLocation::LOCATION(this->compound_storage_service, read_file),
                    read_byte_per_node);
                // ------------------------------------------------------------------

                if (overhead) // wait for overhead sleep to be done before starting this action.
                    readJob->addActionDependency(overhead, file_read);

                // Record which compute host should be used to complete this read action.
                jms.serviceSpecificArgs[readJob->getName()][action_id] = computeResourcesIt->first;
                if (++computeResourcesIt == computeResources.end())
                    computeResourcesIt = computeResources.begin();
            }
        }
    }

    std::vector<std::shared_ptr<wrench::DataFile>> Controller::writeToTemporary(JobManagementStruct &jms,
                                                                                std::shared_ptr<wrench::CompoundJob> writeJob,
                                                                                const DarshanRecord &run,
                                                                                std::map<std::string, unsigned int> &current_stripes_per_action,
                                                                                unsigned int write_stripe_count,
                                                                                unsigned int max_nb_hosts) {

        WRENCH_INFO("[%s] Creating sub-job (%ld bytes, on %u files and at most %u IO nodes [0=all])",
                    writeJob->getName().c_str(), run.writtenBytes, run.written_files_count, max_nb_hosts);

        if (run.written_files_count < 1) {
            throw std::runtime_error("[" + writeJob->getName() + "] writeToTemporary: At least one file is needed to perform a write");
        }

        // Subdivide the amount of written bytes between as many files as requested (one allocation per file)
        auto prefix = "outputFile_" + writeJob->getName();
        auto write_files = this->createFileParts(run.writtenBytes, run.written_files_count, prefix);

        // Prepare stripes-to-hosts matching
        std::map<std::shared_ptr<wrench::DataFile>, unsigned int> stripes_per_file{};
        std::map<std::shared_ptr<wrench::DataFile>, std::vector<unsigned int>> stripes_per_host_per_file{};
        this->setWriteStripesPerHost(write_files, max_nb_hosts, stripes_per_file, stripes_per_host_per_file, write_stripe_count, writeJob->getName());

        // Using recorded meta time to add a sleep action before every read action
        std::shared_ptr<wrench::SleepAction> overhead;
        if (run.metaTimeSeconds) {
            if (run.readBytes) { // simple approach : divide metadata time between read and write
                overhead = writeJob->addSleepAction("write_overhead_" + writeJob->getName(), run.metaTimeSeconds / 2);
            } else { // no read bytes, metadata is for write only
                overhead = writeJob->addSleepAction("write_overhead_" + writeJob->getName(), run.metaTimeSeconds);
            }
        }

        // Create one write action per file (each one will run on one host, unless there are more actions than 'max_nb_hosts',
        // in which case multiple actions can be scheduled on the same host)
        // Randomly remove nodes from resources in order to never use more than 'max_nb_hosts'
        auto computeResources = jms.bareMetalCS->getPerHostNumCores();
        this->pruneIONodes(computeResources, max_nb_hosts);
        auto computeResourcesIt = computeResources.begin();
        unsigned int action_cnt = 0;
        jms.serviceSpecificArgs[writeJob->getName()] = std::map<std::string, std::string>();

        for (auto &write_file : write_files) {

            auto stripe_size = write_file->getSize() / stripes_per_file[write_file];

            for (const auto &stripes_per_host : stripes_per_host_per_file[write_file]) { // each entry in the vector stands for one host
                WRENCH_DEBUG("[%s] writeToTemporary: Creating custom write action for file %s and host %s", writeJob->getName().c_str(), write_file->getID().c_str(), computeResourcesIt->first.c_str());

                auto action_id = "FW_" + write_file->getID() + "_" + computeResourcesIt->first + "_act" + std::to_string(action_cnt++);

                double write_bytes_per_node = 0;
                if (stripes_per_file[write_file] == stripes_per_host) {
                    // Only one host reading all the stripes (stripes_per_host is actually the total number or stripes)
                    write_bytes_per_node = write_file->getSize();
                    WRENCH_DEBUG("[%s] writeToTemporary: We'll be writing the entire file from this host.", writeJob->getName().c_str());
                } else {
                    write_bytes_per_node = stripe_size * stripes_per_host;
                    WRENCH_DEBUG("[%s] writeToTemporary: We'll be writing %d stripes from this host, for a total of %f bytes from this host", writeJob->getName().c_str(), stripes_per_host, write_bytes_per_node);
                }

                // This is a reference to a particular map entry in the this->stripes_per_action, for the current job and run
                // It's used to keep trace the count of stripes used for all actions throughout the simulation (in a dirty way)
                current_stripes_per_action[action_id] = stripes_per_host;

                // ------------------------------------------------------------------
                // Create FILEWRITE action (as a custom action, because we need to pass a number of bytes to write,
                // and that's currently not permitted by WRENCH) -- Specific to a stripe and a host
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
                auto write_file = writeJob->addCustomAction(customWriteAction);
                // ------------------------------------------------------------------

                if (overhead) // wait for overhead sleep to be done before starting this action.
                    writeJob->addActionDependency(overhead, write_file);

                // One copy per compute node, looping over if there are more files than nodes
                jms.serviceSpecificArgs[writeJob->getName()][action_id] = computeResourcesIt->first;
                if (++computeResourcesIt == computeResources.end())
                    computeResourcesIt = computeResources.begin();
            }
        }

        return write_files;
    }

    void Controller::copyToPermanent(JobManagementStruct &jms,
                                     std::shared_ptr<wrench::CompoundJob> copyJob,
                                     const DarshanRecord &run,
                                     std::vector<std::shared_ptr<wrench::DataFile>> outputs,
                                     unsigned int max_nb_hosts) {

        WRENCH_INFO("[%s] copyToPermanent: Creating copy sub-job (out) %lu file(s) with cumulative size %ld bytes, using %u IO nodes (0 means all avail)",
                    copyJob->getName().c_str(), outputs.size(), run.writtenBytes, run.written_files_count);

        /* Note here if a write operations was previously sliced between n compute hosts, we virtually created n pseudo files,
         * and we're now going to run the copy from n compute hosts once again. In future works, we might want to differentiate
         * the number of hosts used in the write operation and the number of hosts used in the following copy.
         */

        // Prepare the list of compute host which may participate in the copy
        auto computeResources = jms.bareMetalCS->getPerHostNumCores();
        // Randomly remove nodes from resources in order to never use more than 'max_nb_hosts'
        this->pruneIONodes(computeResources, max_nb_hosts);
        auto computeResourcesIt = computeResources.begin();

        // Create one copy action per file and per host
        jms.serviceSpecificArgs[copyJob->getName()] = std::map<std::string, std::string>();
        for (const auto &output_data : outputs) {
            auto action_id = "AC_" + output_data->getID();
            auto archiveAction = copyJob->addFileCopyAction(
                action_id,
                wrench::FileLocation::LOCATION(this->compound_storage_service, output_data),
                wrench::FileLocation::LOCATION(this->storage_service, this->config->pstor.mount_prefix + "/" + this->config->pstor.write_path, output_data));

            jms.serviceSpecificArgs[copyJob->getName()][action_id] = computeResourcesIt->first;
            if (++computeResourcesIt == computeResources.end())
                computeResourcesIt = computeResources.begin();
        }
    }

    /**
     * @brief Process a compound job completion event
     *
     * @param event: the event
     */
    void Controller::processEventCompoundJobCompletion(const std::shared_ptr<wrench::CompoundJobCompletedEvent> &event) {
        static uint32_t completed_jobs = 0;
        auto job = event->job;
        WRENCH_INFO("[%s] Notified that this compound job has completed", job->getName().c_str());
#ifdef CONSOLE_OUTPUT
        std::cout << "                                      Job " << job->getName().c_str() << " completed [" << ++completed_jobs << "/" << this->jobs.size() << "]" << std::endl;
#endif
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

        auto &job_entry = this->sim_jobs[job_id];

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
        this->completed_jobs << YAML::Key << "real_runtime_s" << YAML::Value << yaml_job->runtimeSeconds;
        this->completed_jobs << YAML::Key << "real_read_bytes" << YAML::Value << yaml_job->readBytes;
        this->completed_jobs << YAML::Key << "real_written_bytes" << YAML::Value << yaml_job->writtenBytes;
        this->completed_jobs << YAML::Key << "real_cores_used" << YAML::Value << yaml_job->coresUsed;
        this->completed_jobs << YAML::Key << "real_waiting_time_s" << YAML::Value << yaml_job->waitingTimeSeconds;
        this->completed_jobs << YAML::Key << "real_cReadTime_s" << YAML::Value << yaml_job->readTimeSeconds;
        this->completed_jobs << YAML::Key << "real_cWriteTime_s" << YAML::Value << yaml_job->writeTimeSeconds;
        this->completed_jobs << YAML::Key << "real_cMetaTime_s" << YAML::Value << yaml_job->metaTimeSeconds;
        this->completed_jobs << YAML::Key << "sim_sleep_time" << YAML::Value << yaml_job->sleepSimulationSeconds;
        this->completed_jobs << YAML::Key << "cumul_read_bw" << YAML::Value << yaml_job->cumulReadBW;
        this->completed_jobs << YAML::Key << "cumul_write_bw" << YAML::Value << yaml_job->cumulWriteBW;
        this->completed_jobs << YAML::Key << "category" << YAML::Value << yaml_job->category;

        // ## Processing actions for all sub jobs related to the current top-level job being processed
        this->completed_jobs << YAML::Key << "actions" << YAML::Value << YAML::BeginSeq;

        // Actual values determined after processing all actions from different sub jobs
        double earliest_action_start_time = UINT64_MAX;
        // Note that we start at ++begin(), to skip the first job (reservation job) in vector
        for (const auto &[run_id, subjobs] : job_list) {
            for (const auto &subjob : subjobs) {
                WRENCH_DEBUG("Now processing actions of subjob %s", subjob->getName().c_str());
                auto actions = subjob->getActions();
                processActions(this->completed_jobs, std::move(actions), earliest_action_start_time, job_id, run_id);
            }
        }

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
