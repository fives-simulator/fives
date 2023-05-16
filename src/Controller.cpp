
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

#include <iostream>
#include <fstream>
#include <iomanip>
#include <wrench/util/UnitParser.h>

#include "yaml-cpp/yaml.h"

WRENCH_LOG_CATEGORY(storalloc_controller, "Log category for Controller");

namespace wrench {

    template<typename E>
    constexpr auto
    toUType(E enumerator) noexcept
    {
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
    Controller::Controller(const std::shared_ptr<ComputeService> &compute_service,
                           const std::shared_ptr<SimpleStorageService> &storage_service,
                           const std::shared_ptr<CompoundStorageService> &compound_storage_service,
                           const std::string &hostname,
                           const std::vector<storalloc::YamlJob>& jobs) :
            ExecutionController(hostname,"controller"),
            compute_service(compute_service), 
            storage_service(storage_service), 
            compound_storage_service(compound_storage_service),
            jobs(jobs) {}

    /**
     * @brief main method of the Controller
     *
     * @return 0 on completion
     *
     * @throw std::runtime_error
     */
    int Controller::main() {

        /* Set the logging output to GREEN */
        TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_GREEN);
        WRENCH_INFO("Controller starting");
        WRENCH_INFO("Got %s jobs to prepare and submit", std::to_string(jobs.size()).c_str());

        /* Create a job manager so that we can create/submit jobs */
        auto job_manager = this->createJobManager();
        std::vector<std::pair<storalloc::YamlJob, std::shared_ptr<wrench::CompoundJob>>> compound_jobs;
        std::vector<std::shared_ptr<wrench::Action>> actions;

        /**
         *  Create CompoundJobs from the list provided in the yaml file.
         *  All jobs have the same model so far (optional copy + read then compute, 
         *  optional write, and cleanup with file delete).
         * 
        */
        auto cumul_sleep = 0;
        for (const auto& yaml_job : jobs) {
            
            WRENCH_DEBUG("JOB SUBMISSION TIME = %s", yaml_job.submissionTime.c_str());

            WRENCH_DEBUG("SIMULATION SLEEP BEFORE SUBMIT = %d", yaml_job.sleepTime);
            // Sleep in simulation so that jobs are submitted sequentially with the same schedule as in
            // the original DARSHAN traces.
            simulation->sleep(yaml_job.sleepTime);
            std::shared_ptr<wrench::Action> latest_dependency = nullptr;

            auto job = job_manager->createCompoundJob(yaml_job.id);
            WRENCH_DEBUG("Creating a compound job for ID %s", yaml_job.id.c_str());

            // Create file for read operation
            std::shared_ptr<wrench::DataFile> read_file = nullptr;
            std::shared_ptr<wrench::FileReadAction> fileReadAction = nullptr;
            if (yaml_job.readBytes != 0) {
                WRENCH_DEBUG("Creating copy&read for a file with size %ld", yaml_job.readBytes);
                read_file = wrench::Simulation::addFile("input_data_file_" + yaml_job.id, yaml_job.readBytes);
                // "storage_service" represents a user shared storage area (/home, any NFS, ...) or any storage located outside the cluster.
                wrench::StorageService::createFileAtLocation(wrench::FileLocation::LOCATION(this->storage_service, "/dev/disk0/read/", read_file));
                auto fileCopyAction = job->addFileCopyAction(
                    "stagingCopy_" + yaml_job.id, 
                    wrench::FileLocation::LOCATION(this->storage_service, "/dev/disk0/read/", read_file),
                    wrench::FileLocation::LOCATION(this->compound_storage_service, read_file)
                );
                fileReadAction = job->addFileReadAction("fRead_" + yaml_job.id, wrench::FileLocation::LOCATION(this->compound_storage_service, read_file));
                actions.push_back(fileCopyAction);
                actions.push_back(fileReadAction);
                job->addActionDependency(fileCopyAction, fileReadAction);
                latest_dependency = fileReadAction;
                WRENCH_DEBUG("Copy and read action added to job ID %s", yaml_job.id.c_str());
            }

            // Compute action - Add one for every job.
            // Random values (flops, ram, ...) to be adjusted.
            // We could start from the theoreticak peak performance of modelled platform, and specify the flops based on the 
            // % of available compute resources used by the job and its execution time (minor a factor of the time spend in IO ?)
            auto cores_per_node = yaml_job.coresUsed / yaml_job.nodesUsed;
            auto compute = job->addComputeAction("compute_" + yaml_job.id, 1000 * GFLOP, 64 * GBYTE, cores_per_node, cores_per_node, wrench::ParallelModel::AMDAHL(0.8));
            actions.push_back(compute);
            if (latest_dependency) {
                job->addActionDependency(latest_dependency, compute);
            }
            latest_dependency = compute;
            WRENCH_DEBUG("Compute action added to job ID %s", yaml_job.id.c_str());
            // Possibly model some waiting time / bootstrapping with a sleep action ?
            // auto sleep = job2->addSleepAction("sleep", 20.0);

            // Create file for write operation and copy written file back to long-term storage
            std::shared_ptr<wrench::DataFile> write_file = nullptr;
            std::shared_ptr<wrench::FileWriteAction> fileWriteAction = nullptr;
            if (yaml_job.writtenBytes != 0) {
                write_file = wrench::Simulation::addFile("ouptut_data_file_" + yaml_job.id, yaml_job.writtenBytes);
                fileWriteAction = job->addFileWriteAction("fWrite_" + yaml_job.id, wrench::FileLocation::LOCATION(this->compound_storage_service, write_file));
                auto archiveAction = job->addFileCopyAction(
                    "archiveCopy_" + yaml_job.id, 
                    wrench::FileLocation::LOCATION(this->compound_storage_service, write_file),
                    wrench::FileLocation::LOCATION(this->storage_service, "/dev/disk0/write/", write_file)
                );
                actions.push_back(fileWriteAction);
                actions.push_back(archiveAction);
                job->addActionDependency(latest_dependency, fileWriteAction);
                job->addActionDependency(fileWriteAction, archiveAction);
                latest_dependency = archiveAction;
                WRENCH_DEBUG("Write and copy to archive actions added to job ID %s", yaml_job.id.c_str());
            }

            // Dependencies and cleanup by delete files (if needed) 
            if (fileReadAction) {
                auto readSleep = job->addSleepAction("sleepBeforeReadCleanup" + yaml_job.id, 1.0);
                job->addActionDependency(latest_dependency, readSleep);
                latest_dependency = readSleep;
                // job->addActionDependency(fileReadAction, compute);
                auto deleteReadAction = job->addFileDeleteAction("delRF_" + yaml_job.id, wrench::FileLocation::LOCATION(this->compound_storage_service, read_file));
                auto deleteExternalReadAction = job->addFileDeleteAction("delERF_" + yaml_job.id, wrench::FileLocation::LOCATION(this->storage_service, "/dev/disk0/read/", read_file));
                job->addActionDependency(latest_dependency, deleteReadAction);
                job->addActionDependency(deleteReadAction, deleteExternalReadAction);
                actions.push_back(deleteReadAction);
                actions.push_back(deleteExternalReadAction);
                latest_dependency = deleteExternalReadAction;
            }

            if (fileWriteAction) {
                auto writeSleep = job->addSleepAction("sleepBeforeWriteCleanup" + yaml_job.id, 1.0);
                job->addActionDependency(latest_dependency, writeSleep);
                latest_dependency = writeSleep;
                auto deleteWriteAction = job->addFileDeleteAction("delWF_" + yaml_job.id, wrench::FileLocation::LOCATION(this->compound_storage_service, write_file));
                auto deleteExternalWriteAction = job->addFileDeleteAction("delEWF_" + yaml_job.id, wrench::FileLocation::LOCATION(this->storage_service, "/dev/disk0/write/", write_file));
                actions.push_back(deleteWriteAction);
                actions.push_back(deleteExternalWriteAction);
                job->addActionDependency(latest_dependency, deleteWriteAction);
                job->addActionDependency(deleteWriteAction, deleteExternalWriteAction);
            }

            auto runtime = 0;
            if (yaml_job.runTime < 1000) {
                runtime = ((yaml_job.runTime + 20000) * 60);
            } else {
                runtime = ((yaml_job.runTime) * 60);     // We artificially increase the runtime just to make sure no job times out, but this needs to be adjusted
            }
            
            std::map<std::string, std::string> service_specific_args =
                    {{"-N", std::to_string(yaml_job.nodesUsed)},                            // nb of nodes
                     {"-c", std::to_string(yaml_job.coresUsed / yaml_job.nodesUsed)},       // core per node
                     {"-t", std::to_string(runtime)}};                                      // seconds
            WRENCH_DEBUG("Submitting job %s (%d nodes, %d cores per node, %d minutes) for executing actions",
                        job->getName().c_str(),
                        yaml_job.nodesUsed, yaml_job.coresUsed / yaml_job.nodesUsed, runtime
            );
            compound_jobs.push_back(std::make_pair(yaml_job, job));
            job_manager->submitJob(job, this->compute_service, service_specific_args);
        }
            
        TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_BLUE);
        WRENCH_INFO("### All jobs submitted to the BatchComputeService");
        WRENCH_INFO("### Waiting for execution events");
        TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_GREEN);
        auto nb_jobs = compound_jobs.size();
        for (size_t i = 0; i < nb_jobs; i++) {
            this->waitForAndProcessNextEvent();
        }

        WRENCH_INFO("Execution complete");

        // List all action states and times
        for (auto const &a : actions) {
            if (a->getState() != Action::State::COMPLETED) {
                TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_RED);
            }
            WRENCH_DEBUG("Action %s: %.2fs - %.2fs", a->getName().c_str(), a->getStartDate(), a->getEndDate());
            if (a->getState() != Action::State::COMPLETED) {
                WRENCH_WARN("Action %s: %.2fs - %.2fs", a->getName().c_str(), a->getStartDate(), a->getEndDate());
                WRENCH_WARN("-> Failure cause: %s", a->getFailureCause()->toString().c_str());
                TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_GREEN);
                return 1;
            }
        }

        this->processCompletedJobs(compound_jobs);
        this->extractSSSIO();

        return 0;
    }

    /**
     * @brief Process a compound job completion event
     *
     * @param event: the event
     */
    void Controller::processEventCompoundJobCompletion(std::shared_ptr<CompoundJobCompletedEvent> event) {
        auto job = event->job;
        WRENCH_INFO("# Notified that compound job %s has completed:", job->getName().c_str());
    }


    /**
     * @brief Process a compound job failure event
     *
     * @param event: the event
     */
    void Controller::processEventCompoundJobFailure(std::shared_ptr<CompoundJobFailedEvent> event) {
        /* Retrieve the job that this event is for and failure cause*/
        auto job = event->job;
        auto cause = event->failure_cause;
        TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_RED);
        WRENCH_WARN("Notified that compound job %s has failed: %s", job->getName().c_str(), cause->toString().c_str());
        TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_GREEN);
    }

    // DELETE Overload
    std::pair<std::string, std::string> updateIoUsageDelete(std::map<std::string, StorageServiceIOCounters>& volume_records, const std::shared_ptr<wrench::FileLocation>& location) {

        auto storage_service = location->getStorageService()->getName();
        auto mount_pt = location->getPath();                   // TODO : RECENT FIX, NEED TO CHECK IF DATA IS CORRECT
        auto volume = location->getFile()->getSize(); 

        // Update volume record with new write/copy/delete value
        volume_records[storage_service].total_allocation_count -= 1;
        volume_records[storage_service].total_capacity_used -= volume;
        volume_records[storage_service].disks[mount_pt].total_allocation_count -= 1;           // allocation count on disk
        volume_records[storage_service].disks[mount_pt].total_capacity_used -= volume;     // byte volume count

        // Return latest updated keys for ease of use
        return std::make_pair(storage_service, mount_pt);
    }

    // COPY overload
    std::pair<std::string, std::string> updateIoUsageCopy(std::map<std::string, StorageServiceIOCounters>& volume_records, const std::shared_ptr<wrench::FileLocation>& location) {
        
        auto dst_storage_service = location->getStorageService()->getName();
        auto dst_path = location->getPath();        // TODO : RECENT FIX, NEED TO CHECK IF DATA IS CORRECT
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
        volume_records[dst_storage_service].total_allocation_count += 1;  // allocation count on disk
        volume_records[dst_storage_service].total_capacity_used += dst_volume;  // byte volume count
        volume_records[dst_storage_service].disks[dst_path].total_allocation_count += 1;  // allocation count on disk
        volume_records[dst_storage_service].disks[dst_path].total_capacity_used  += dst_volume;  // byte volume count

        // Return latest updated keys for ease of use
        return std::make_pair(dst_storage_service, dst_path);
    }

    // WRITE overload
    std::pair<std::string, std::string> updateIoUsageWrite(std::map<std::string, StorageServiceIOCounters>& volume_records, const std::shared_ptr<wrench::FileLocation>& location) {

        auto storage_service = location->getStorageService()->getName();
        auto path = location->getPath();        // TODO : RECENT FIX, NEED TO CHECK IF DATA IS CORRECT
        auto volume = location->getFile()->getSize(); 

        if (volume_records.find(storage_service) == volume_records.end()) {
            volume_records[storage_service].disks = std::map<std::string, DiskIOCounters>();
        }

        if (volume_records[storage_service].disks.find(path) == volume_records[storage_service].disks.end()) {
            volume_records[storage_service].disks[path] = DiskIOCounters();
        }

        // Update volume record with new write/copy/delete value
        volume_records[storage_service].total_allocation_count += 1;                    // allocation count on disk
        volume_records[storage_service].total_capacity_used += volume;                  // byte volume count
        volume_records[storage_service].disks[path].total_allocation_count += 1;        // allocation count on disk
        volume_records[storage_service].disks[path].total_capacity_used += volume;      // byte volume count

        // Return latest updated keys for ease of use
        return std::make_pair(storage_service, path);
    }

    void Controller::processCompletedJobs(const std::vector<std::pair<storalloc::YamlJob, std::shared_ptr<wrench::CompoundJob>>>& jobs) {
        
        std::map<std::string, StorageServiceIOCounters> volume_per_storage_service_disk = {};

        YAML::Emitter out_ss;
        out_ss << YAML::BeginSeq;

        YAML::Emitter out;
        out << YAML::BeginSeq;

        for (const auto& job_pair : jobs) {

            auto yaml_job = job_pair.first;
            auto job = job_pair.second;

            out << YAML::BeginMap;  // Job map

            auto job_uid = job->getName();
            std::cout << "In job " << job_uid << std::endl;
            auto job_id = job_uid.substr(0, job_uid.find("-"));
            out << YAML::Key << "job_uid" << YAML::Value << job_uid;
            out << YAML::Key << "job_id" << YAML::Value << job_id;          // for use in group-by 
            out << YAML::Key << "job_status" << YAML::Value << job->getStateAsString();
            out << YAML::Key << "job_submit_ts" << YAML::Value << job->getSubmitDate();
            out << YAML::Key << "job_end_ts" << YAML::Value << job->getEndDate();
            out << YAML::Key << "job_duration" << YAML::Value << (job->getEndDate() - job->getSubmitDate());
            out << YAML::Key << "origin_runtime" << YAML::Value << yaml_job.runTime;
            out << YAML::Key << "origin_read_bytes" << YAML::Value << yaml_job.readBytes;
            out << YAML::Key << "origin_written_bytes" << YAML::Value << yaml_job.writtenBytes;
            out << YAML::Key << "origin_core_used" << YAML::Value << yaml_job.coresUsed;
            out << YAML::Key << "origin_mpi_procs" << YAML::Value << yaml_job.mpiProcs;
            out << YAML::Key << "sim_sleep_time" << YAML::Value << yaml_job.sleepTime;
            
            out << YAML::Key << "job_actions" << YAML::Value << YAML::BeginSeq;   // action sequence
            auto actions = job->getActions();
            auto set_cmp = [](const std::shared_ptr<Action>& a, const std::shared_ptr<Action>& b)
                                  {
                                      return (a->getStartDate() < b->getStartDate());
                                  };
            auto sorted_actions = std::set<std::shared_ptr<Action>, decltype(set_cmp)>(set_cmp);
            sorted_actions.insert(actions.begin(), actions.end());
        
            for (const auto& action : sorted_actions) {

                if (action->getState() != wrench::Action::COMPLETED) {
                    std::cout << "Action " << action->getName() << " is in state " << action->getStateAsString() << std::endl;
                    throw std::runtime_error("Action is not in COMPLETED state");
                }

                out << YAML::BeginMap;  // action map
                out << YAML::Key << "act_name" << YAML::Value << action->getName();
                auto act_type = wrench::Action::getActionTypeAsString(action);
                act_type.pop_back();  // removing a useless '-' at the end
                out << YAML::Key << "act_type" << YAML::Value << act_type;  
                out << YAML::Key << "act_status" << YAML::Value << action->getStateAsString();
                out << YAML::Key << "act_start_ts" << YAML::Value << action->getStartDate();
                out << YAML::Key << "act_end_ts" << YAML::Value << action->getEndDate();
                out << YAML::Key << "act_duration" << YAML::Value << (action->getEndDate() - action->getStartDate());

                std::cout << "ACTION TYPE:  " << act_type << " AT TS : " << std::to_string(action->getStartDate()) << std::endl;

                if (auto fileRead = std::dynamic_pointer_cast<FileReadAction>(action)) {

                    auto usedLocation = fileRead->getUsedFileLocation();
                    auto usedFile = usedLocation->getFile();

                    auto read_trace = this->compound_storage_service->read_traces[usedFile->getID()];
                    out << YAML::Key << "parts_count" << YAML::Value << read_trace.internal_locations.size();
                    out << YAML::Key << "parts" << YAML::Value << YAML::BeginSeq;
                    for (const auto& trace_loc : read_trace.internal_locations) {
                        out << YAML::BeginMap;
                        out << YAML::Key << "storage_service" << YAML::Value << trace_loc->getStorageService()->getName();
                        out << YAML::Key << "storage_server" << YAML::Value << trace_loc->getStorageService()->getHostname();
                        out << YAML::Key << "part_path" << YAML::Value << trace_loc->getPath();
                        out << YAML::Key << "part_name" << YAML::Value << trace_loc->getFile()->getID();
                        out << YAML::Key << "part_size_bytes" << YAML::Value << trace_loc->getFile()->getSize();
                        out << YAML::EndMap;
                    }
                    out << YAML::EndSeq;

                    out << YAML::Key << "css" << YAML::Value << usedLocation->getStorageService()->getName();
                    out << YAML::Key << "css_server" << YAML::Value << usedLocation->getStorageService()->getHostname();
                    out << YAML::Key << "file_path" << YAML::Value << usedLocation->getPath();
                    out << YAML::Key << "file_name" << YAML::Value << usedFile->getID();
                    out << YAML::Key << "file_size_bytes" << YAML::Value << usedFile->getSize();
                    
                } else if (auto fileWrite = std::dynamic_pointer_cast<FileWriteAction>(action)) {
                    auto usedLocation = fileWrite->getFileLocation();
                    auto usedFile = usedLocation->getFile();

                    auto write_trace = this->compound_storage_service->write_traces[usedFile->getID()];
                    out << YAML::Key << "parts_count" << YAML::Value << write_trace.internal_locations.size();
                    out << YAML::Key << "parts" << YAML::Value << YAML::BeginSeq;
                    for (const auto& trace_loc : write_trace.internal_locations) {
                        out << YAML::BeginMap;
                        out << YAML::Key << "storage_service" << YAML::Value << trace_loc->getStorageService()->getName();
                        out << YAML::Key << "storage_server" << YAML::Value << trace_loc->getStorageService()->getHostname();
                        out << YAML::Key << "part_path" << YAML::Value << trace_loc->getPath();
                        out << YAML::Key << "part_name" << YAML::Value << trace_loc->getFile()->getID();
                        out << YAML::Key << "part_size_bytes" << YAML::Value << trace_loc->getFile()->getSize();
                        out << YAML::EndMap;
                    }
                    out << YAML::EndSeq;

                    out << YAML::Key << "css" << YAML::Value << usedLocation->getStorageService()->getName();
                    out << YAML::Key << "css_server" << YAML::Value << usedLocation->getStorageService()->getHostname();
                    // out << YAML::Key << "dst_storage_disk" << YAML::Value << usedLocation->getMountPoint();
                    out << YAML::Key << "file_path" << YAML::Value << usedLocation->getPath();
                    out << YAML::Key << "file_name" << YAML::Value << usedFile->getID();
                    out << YAML::Key << "file_size_bytes" << YAML::Value << usedFile->getSize();

                    for (const auto& trace_loc : write_trace.internal_locations) {
                        auto keys = updateIoUsageWrite(volume_per_storage_service_disk, trace_loc);
                        out_ss << YAML::BeginMap;
                        out_ss << YAML::Key << "ts" << YAML::Value << action->getEndDate();         // end_date, because we record the IO change when the write is completed
                        out_ss << YAML::Key << "action_type" << YAML::Value << act_type;
                        out_ss << YAML::Key << "action_job" << YAML::Value << job_uid;
                        out_ss << YAML::Key << "action_name" << YAML::Value << action->getName();
                        out_ss << YAML::Key << "storage_service" << YAML::Value << keys.first;
                        out_ss << YAML::Key << "disk" << YAML::Value << keys.second;
                        out_ss << YAML::Key << "volume_change_bytes" << YAML::Value << usedFile->getSize();
                        out_ss << YAML::Key << "total_allocation_server" << YAML::Value << volume_per_storage_service_disk[keys.first].total_allocation_count;
                        out_ss << YAML::Key << "total_used_volume_bytes_server" << YAML::Value << volume_per_storage_service_disk[keys.first].total_capacity_used;
                        out_ss << YAML::Key << "total_allocation_disk" << YAML::Value << volume_per_storage_service_disk[keys.first].disks[keys.second].total_allocation_count;
                        out_ss << YAML::Key << "total_used_volume_bytes_disk" << YAML::Value << volume_per_storage_service_disk[keys.first].disks[keys.second].total_capacity_used;
                        out_ss << YAML::EndMap;
                    }

                } else if (auto fileCopy = std::dynamic_pointer_cast<FileCopyAction>(action)) {

                    std::shared_ptr<wrench::FileLocation> css;
                    std::shared_ptr<wrench::FileLocation> sss;

                    // !!! We don't handle a sss <-> sss or css <-> css copy
                    if (std::dynamic_pointer_cast<wrench::CompoundStorageService>(
                        fileCopy->getSourceFileLocation()->getStorageService()
                    )) {
                        css = fileCopy->getSourceFileLocation();
                        sss = fileCopy->getDestinationFileLocation();
                        out << YAML::Key << "copy_direction" << YAML::Value << "css_to_sss";
                    } else {
                        sss = fileCopy->getSourceFileLocation();
                        css = fileCopy->getDestinationFileLocation();
                        out << YAML::Key << "copy_direction" << YAML::Value << "sss_to_css";
                    }

                    auto copy_trace = this->compound_storage_service->copy_traces[css->getFile()->getID()];
                    out << YAML::Key << "parts_count" << YAML::Value << copy_trace.internal_locations.size();
                    out << YAML::Key << "parts" << YAML::Value << YAML::BeginSeq;
                    for (const auto& trace_loc : copy_trace.internal_locations) {
                        out << YAML::BeginMap;
                        out << YAML::Key << "storage_service" << YAML::Value << trace_loc->getStorageService()->getName();
                        out << YAML::Key << "storage_server" << YAML::Value << trace_loc->getStorageService()->getHostname();
                        out << YAML::Key << "part_path" << YAML::Value << trace_loc->getPath();
                        out << YAML::Key << "part_name" << YAML::Value << trace_loc->getFile()->getID();
                        out << YAML::Key << "part_size_bytes" << YAML::Value << trace_loc->getFile()->getSize();
                        out << YAML::EndMap;
                    }
                    out << YAML::EndSeq;

                    out << YAML::Key << "sss" << YAML::Value << sss->getStorageService()->getName();
                    out << YAML::Key << "sss_server" << YAML::Value << sss->getStorageService()->getHostname();
                    // out << YAML::Key << "src_storage_disk" << YAML::Value << src->getMountPoint();
                    out << YAML::Key << "src_file_path" << YAML::Value << sss->getPath();
                    out << YAML::Key << "src_file_name" << YAML::Value << sss->getFile()->getID();
                    out << YAML::Key << "src_file_size_bytes" << YAML::Value << sss->getFile()->getSize();

                    out << YAML::Key << "css" << YAML::Value << css->getStorageService()->getName();
                    out << YAML::Key << "css_server" << YAML::Value << css->getStorageService()->getHostname();
                    // out << YAML::Key << "dst_storage_disk" << YAML::Value << dest->getMountPoint();
                    out << YAML::Key << "dst_file_path" << YAML::Value << css->getPath();
                    out << YAML::Key << "dst_file_name" << YAML::Value << css->getFile()->getID();
                    out << YAML::Key << "dst_file_size_bytes" << YAML::Value << css->getFile()->getSize();

                    // only record a write if it's from SSS (external permanent storage) to CSS 
                    if (fileCopy->getDestinationFileLocation()->getStorageService()->getHostname() != "permanent_storage") {
                        
                        for (const auto& trace_loc : copy_trace.internal_locations) {
                            auto keys = updateIoUsageCopy(volume_per_storage_service_disk, trace_loc);
                            out_ss << YAML::BeginMap;
                            out_ss << YAML::Key << "ts" << YAML::Value << action->getEndDate();         // end_date, because we record the IO change when the write is completed
                            out_ss << YAML::Key << "action_type" << YAML::Value << act_type;
                            out_ss << YAML::Key << "action_name" << YAML::Value << action->getName();
                            out_ss << YAML::Key << "action_job" << YAML::Value << job_uid;
                            out_ss << YAML::Key << "storage_service" << YAML::Value << keys.first;
                            out_ss << YAML::Key << "disk" << YAML::Value << keys.second;
                            out_ss << YAML::Key << "volume_change_bytes" << YAML::Value << css->getFile()->getSize();      // dest file, because it's the one being written
                            out_ss << YAML::Key << "total_allocation_server" << YAML::Value << volume_per_storage_service_disk[keys.first].total_allocation_count;
                            out_ss << YAML::Key << "total_used_volume_bytes_server" << YAML::Value << volume_per_storage_service_disk[keys.first].total_capacity_used;
                            out_ss << YAML::Key << "total_allocation_disk" << YAML::Value << volume_per_storage_service_disk[keys.first].disks[keys.second].total_allocation_count;
                            out_ss << YAML::Key << "total_used_volume_bytes_disk" << YAML::Value << volume_per_storage_service_disk[keys.first].disks[keys.second].total_capacity_used;
                            out_ss << YAML::EndMap;
                        }
                    }

                } else if (auto fileDelete = std::dynamic_pointer_cast<FileDeleteAction>(action)) {
                    auto usedLocation = fileDelete->getFileLocation();
                    auto usedFile = usedLocation->getFile();

                    out << YAML::Key << "storage_service" << YAML::Value << usedLocation->getStorageService()->getName();
                    out << YAML::Key << "storage_server" << YAML::Value << usedLocation->getStorageService()->getHostname();
                    out << YAML::Key << "file_path" << YAML::Value << usedLocation->getPath();
                    out << YAML::Key << "file_name" << YAML::Value << usedFile->getID();
                    out << YAML::Key << "file_size_bytes" << YAML::Value << usedFile->getSize();

                    
                    if (usedLocation->getStorageService()->getHostname() != "permanent_storage") {

                        auto delete_trace = this->compound_storage_service->delete_traces[usedFile->getID()];
                        out << YAML::Key << "parts_count" << YAML::Value << delete_trace.internal_locations.size();
                        out << YAML::Key << "parts" << YAML::Value << YAML::BeginSeq;
                        for (const auto& trace_loc : delete_trace.internal_locations) {
                            out << YAML::BeginMap;
                            out << YAML::Key << "storage_service" << YAML::Value << trace_loc->getStorageService()->getName();
                            out << YAML::Key << "storage_server" << YAML::Value << trace_loc->getStorageService()->getHostname();
                            out << YAML::Key << "part_path" << YAML::Value << trace_loc->getPath();
                            out << YAML::Key << "part_name" << YAML::Value << trace_loc->getFile()->getID();
                            out << YAML::Key << "part_size_bytes" << YAML::Value << trace_loc->getFile()->getSize();
                            out << YAML::EndMap;
                        }
                        out << YAML::EndSeq;

                        for (const auto& trace_loc : delete_trace.internal_locations) {
                            auto keys = updateIoUsageDelete(volume_per_storage_service_disk, trace_loc);
                            out_ss << YAML::BeginMap;
                            out_ss << YAML::Key << "ts" << YAML::Value << action->getEndDate();         // end_date, because we record the IO change when the write is completed
                            out_ss << YAML::Key << "action_type" << YAML::Value << act_type;
                            out_ss << YAML::Key << "action_name" << YAML::Value << action->getName();
                            out_ss << YAML::Key << "action_job" << YAML::Value << job_uid;
                            out_ss << YAML::Key << "storage_service" << YAML::Value << keys.first;
                            out_ss << YAML::Key << "disk" << YAML::Value << keys.second;
                            out_ss << YAML::Key << "volume_change_bytes" << YAML::Value << usedFile->getSize();
                            out_ss << YAML::Key << "total_allocation_server" << YAML::Value << volume_per_storage_service_disk[keys.first].total_allocation_count;
                            out_ss << YAML::Key << "total_used_volume_bytes_server" << YAML::Value << volume_per_storage_service_disk[keys.first].total_capacity_used;
                            out_ss << YAML::Key << "total_allocation_disk" << YAML::Value << volume_per_storage_service_disk[keys.first].disks[keys.second].total_allocation_count;
                            out_ss << YAML::Key << "total_used_volume_bytes_disk" << YAML::Value << volume_per_storage_service_disk[keys.first].disks[keys.second].total_capacity_used;
                            out_ss << YAML::EndMap;
                        }
                    }
                }
                out << YAML::EndMap;
            }
            out << YAML::EndSeq;
            out << YAML::EndMap;
        }
        out << YAML::EndSeq;
        out_ss << YAML::EndSeq;

        // Write to YAML file
        ofstream io_ops;
        io_ops.open ("io_operations.yml");
        io_ops << "---\n";
        io_ops << out.c_str();
        io_ops << "\n...\n";
        io_ops.close();

        // Write to YAML file
        ofstream io_ss_ops;
        io_ss_ops.open ("storage_service_operations.yml");
        io_ss_ops << "---\n";
        io_ss_ops << out_ss.c_str();
        io_ss_ops << "\n...\n";
        io_ss_ops.close();
         	
    }

    /*
        struct AllocationTrace {
            double ts;
            IOAction act;
            std::string file_name;
            std::vector<DiskUsage> disk_usage;                              // new usage stats for updated disks
            std::vector<std::shared_ptr<FileLocation>> internal_locations;  
        };

        struct DiskUsage {
            std::shared_ptr<StorageService> service;
            double free_space;
            double load;        // not actually used so far
        };
    */

    void Controller::extractSSSIO() {

        ofstream io_ops;
        io_ops.setf(std::ios_base::fixed);
        io_ops.open("timestamped_io_operations.csv");
        io_ops << "ts,action_name,storage_service_name,storage_hostname,disk_id,disk_capacity,disk_free_space,file_name\n";
        // io_ops << setprecision(10);

        for(const auto& entry : this->compound_storage_service->internal_storage_use) {

            auto ts = entry.first;        // ts
            auto alloc = entry.second;    // AllocationTrace structure

            for (const auto& disk_usage : alloc.disk_usage) {

                auto  simple_storage = std::dynamic_pointer_cast<wrench::SimpleStorageService>(disk_usage.service);
                std::string disk_capacity = simple_storage->getDiskForPathOrNull("/")->get_property("size");
                auto capacity_bytes = wrench::UnitParser::parse_size(disk_capacity.c_str());

                io_ops << ts << ",";                                                            // timestamp
                io_ops <<  std::to_string(static_cast<u_int8_t>(alloc.act)) << ",";             // action code
                io_ops << simple_storage->getName() << ",";                                     // storage_service_name
                io_ops << simple_storage->getHostname() << ",";                                 // storage_hostname
                io_ops << simple_storage->getBaseRootPath() << ",";                             // disk_id
                io_ops << capacity_bytes << ",";                                                // disk_capacity
                io_ops << disk_usage.free_space << ",";                                         // disk_free_space
                io_ops << (disk_usage.file_name.empty() ? "NoFile" : disk_usage.file_name) << "\n";                                         // file_name

            }

            /*
            for (const auto& map_entry : sss_map) {
          
                auto sss = std::dynamic_pointer_cast<wrench::SimpleStorageService>(map_entry.first);
                std::string disk_capacity = sss->getDiskForPathOrNull("/")->get_property("size");
                auto capacity_bytes = wrench::UnitParser::parse_size(disk_capacity.c_str());

                io_ops << ts << ",";                                    // timestamp
                io_ops << std::to_string(static_cast<int>(map_entry.second.act)) << ",";       // traced action type 
                io_ops << map_entry.first->getName() << ",";            // storage_service_name
                io_ops << map_entry.first->getHostname() << ",";        // storage_hostname
                io_ops << map_entry.first->getBaseRootPath() << ",";    // disk_id
                io_ops << capacity_bytes << ",";                        // disk_capacity
                io_ops << map_entry.second.free_space << "\n";           // disk_free_space
                // io_ops << map_entry.second.load << "";                // disk_load
            }
            */
        }

        io_ops.close();
    }

} // namespace wrench
