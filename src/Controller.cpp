
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

#include "yaml-cpp/yaml.h"

WRENCH_LOG_CATEGORY(controller, "Log category for Controller");

namespace wrench {

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
        for (const auto& yaml_job : jobs) {
            
            WRENCH_INFO("JOB SUBMISSION TIME = %s", yaml_job.submissionTime.c_str());

            WRENCH_INFO("SIMULATION SLEEP BEFORE SUBMIT = %d", yaml_job.sleepTime);
            // Sleep in simulation so that jobs are submitted sequentially with the same schedule as in
            // the original DARSHAN traces.
            simulation->sleep(yaml_job.sleepTime);

            auto job = job_manager->createCompoundJob(yaml_job.id);
            WRENCH_INFO("Creating a compound job for ID %s", yaml_job.id.c_str());

            // Create file for read operation
            std::shared_ptr<wrench::DataFile> read_file = nullptr;
            std::shared_ptr<wrench::FileReadAction> fileReadAction = nullptr;
            if (yaml_job.readBytes != 0) {
                read_file = wrench::Simulation::addFile("input_data_file_" + yaml_job.id, yaml_job.readBytes);
                // "storage_service" represents a user shared storage area (/home, any NFS, ...) or any storage located outside the cluster.
                wrench::Simulation::createFile(wrench::FileLocation::LOCATION(this->storage_service, "/dev/disk0/", read_file));
                auto fileCopyAction = job->addFileCopyAction(
                    "fileCopyForStaging" + yaml_job.id, 
                    wrench::FileLocation::LOCATION(this->storage_service, "/dev/disk0/", read_file),
                    wrench::FileLocation::LOCATION(this->compound_storage_service, read_file)
                );
                fileReadAction = job->addFileReadAction("fileRead" + yaml_job.id, wrench::FileLocation::LOCATION(this->compound_storage_service, read_file));
                job->addActionDependency(fileCopyAction, fileReadAction);   // Copy must always happen firstso that file is correctly placed on storage before being read
                actions.push_back(fileCopyAction);
                actions.push_back(fileReadAction);
                WRENCH_INFO("Copy and read action added to job ID %s", yaml_job.id.c_str());
            }

            // Compute action - Add one for every job.
            // Random values (flops, ram, ...) to be adjusted.
            // We could start from the theoreticak peak performance of modelled platform, and specify the flops based on the 
            // % of available compute resources used by the job and its execution time (minor a factor of the time spend in IO ?)
            auto compute = job->addComputeAction("compute" + yaml_job.id, 100 * GFLOP, 200 * MBYTE, yaml_job.coresUsed, yaml_job.coresUsed, wrench::ParallelModel::AMDAHL(0.8));
            actions.push_back(compute);
            WRENCH_INFO("Compute action added to job ID %s", yaml_job.id.c_str());
            // Possibly model some waiting time / bootstrapping with a sleep action ?
            // auto sleep = job2->addSleepAction("sleep", 20.0);

            // Create file for write operation
            std::shared_ptr<wrench::DataFile> write_file = nullptr;
            std::shared_ptr<wrench::FileWriteAction> fileWriteAction = nullptr;
            if (yaml_job.writtenBytes != 0) {
                write_file = wrench::Simulation::addFile("ouptut_data_file_" + yaml_job.id, yaml_job.writtenBytes);
                fileWriteAction = job->addFileWriteAction("fileWrite" + yaml_job.id, wrench::FileLocation::LOCATION(this->compound_storage_service, write_file));
                actions.push_back(fileWriteAction);
                WRENCH_INFO("Write action added to job ID %s", yaml_job.id.c_str());
            }

            // Dependencies and cleanup by delete files (if needed) 
            if (fileReadAction) {
                job->addActionDependency(fileReadAction, compute);
                auto deleteReadAction = job->addFileDeleteAction("deleteReadFile", wrench::FileLocation::LOCATION(this->compound_storage_service, read_file));
                auto deleteExternalReadAction = job->addFileDeleteAction("deleteExternalReadFile", wrench::FileLocation::LOCATION(this->storage_service, "/dev/disk0/", read_file));
                if (fileWriteAction) {
                    job->addActionDependency(fileWriteAction, deleteReadAction);
                    job->addActionDependency(fileWriteAction, deleteExternalReadAction);
                } else {
                    job->addActionDependency(compute, deleteReadAction);
                    job->addActionDependency(compute, deleteExternalReadAction);
                }  
            }
            if (fileWriteAction) {
                job->addActionDependency(compute, fileWriteAction);
                auto deleteWriteAction = job->addFileDeleteAction("deleteWriteFile", wrench::FileLocation::LOCATION(this->compound_storage_service, write_file));
                // Maybe we need a sleep action or a copy action here to 
                job->addActionDependency(fileWriteAction, deleteWriteAction);
            }

            std::map<std::string, std::string> service_specific_args =
                    {{"-N", std::to_string(yaml_job.nodesUsed)},
                     {"-c", std::to_string(yaml_job.coresUsed)},
                     {"-t", std::to_string(yaml_job.runTime)}};
            WRENCH_INFO("Submitting job %s (%d nodes, %d cores per node, %d minutes) for executing actions",
                        job->getName().c_str(),
                        yaml_job.nodesUsed, yaml_job.coresUsed, yaml_job.runTime
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
            WRENCH_INFO("Action %s: %.2fs - %.2fs\n", a->getName().c_str(), a->getStartDate(), a->getEndDate());
            if (a->getState() != Action::State::COMPLETED) {
                WRENCH_INFO("  - action failure cause: %s", a->getFailureCause()->toString().c_str());
            }
            TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_GREEN);
        }


        this->processCompletedJobs(compound_jobs);

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

    void Controller::processCompletedJobs(const std::vector<std::pair<storalloc::YamlJob, std::shared_ptr<wrench::CompoundJob>>>& jobs) {
        
        YAML::Emitter out;
        out << YAML::BeginSeq;

        for (const auto& job_pair : jobs) {

            auto yaml_job = job_pair.first;
            auto job = job_pair.second;

            out << YAML::BeginMap;  // Job map

            out << YAML::Key << "job_id" << YAML::Value << job->getName();
            out << YAML::Key << "job_status" << YAML::Value << job->getStateAsString();
            out << YAML::Key << "job_submit_ts" << YAML::Value << job->getSubmitDate();
            out << YAML::Key << "job_end_ts" << YAML::Value << job->getEndDate();
            out << YAML::Key << "job_duration" << YAML::Value << (job->getEndDate() - job->getSubmitDate());
            out << YAML::Key << "origin_runtime" << YAML::Value << yaml_job.runTime;
            out << YAML::Key << "origin_read_bytes" << YAML::Value << yaml_job.readBytes;
            out << YAML::Key << "origin_written_bytes" << YAML::Value << yaml_job.writtenBytes;
            out << YAML::Key << "origin_core_used" << YAML::Value << yaml_job.coresUsed;
            out << YAML::Key << "origin_mpi_procs" << YAML::Value << yaml_job.mpiProcs;
            out << YAML::Key << "job_sleep_time" << YAML::Value << yaml_job.sleepTime;
            
            out << YAML::Key << "job_actions" << YAML::Value << YAML::BeginSeq;   // action sequence
            auto actions = job->getActions();
            for (const auto& action : actions) {
                
                out << YAML::BeginMap;  // action map
                out << YAML::Key << "act_name" << YAML::Value << action->getName();
                out << YAML::Key << "act_type" << YAML::Value << wrench::Action::getActionTypeAsString(action);
                out << YAML::Key << "act_status" << YAML::Value << action->getStateAsString();
                out << YAML::Key << "act_start_ts" << YAML::Value << action->getStartDate();
                out << YAML::Key << "act_end_ts" << YAML::Value << action->getEndDate();
                out << YAML::Key << "act_duration" << YAML::Value << (action->getEndDate() - action->getStartDate());

                if (auto fileRead = std::dynamic_pointer_cast<FileReadAction>(action)) {

                    auto usedLocation = fileRead->getUsedFileLocation();
                    auto usedFile = usedLocation->getFile();

                    out << YAML::Key << "src_storage_service" << YAML::Value << usedLocation->getStorageService()->getName();
                    out << YAML::Key << "src_storage_server" << YAML::Value << usedLocation->getStorageService()->getHostname();
                    out << YAML::Key << "src_storage_disk" << YAML::Value << usedLocation->getMountPoint();
                    out << YAML::Key << "src_file_path" << YAML::Value << usedLocation->getFullAbsolutePath();
                    out << YAML::Key << "src_file_name" << YAML::Value << usedFile->getID();
                    out << YAML::Key << "src_file_size_bytes" << YAML::Value << usedFile->getSize();
                    
                } else if (auto fileWrite = std::dynamic_pointer_cast<FileWriteAction>(action)) {
                    auto usedLocation = fileWrite->getFileLocation();
                    auto usedFile = usedLocation->getFile();

                    out << YAML::Key << "dst_storage_service" << YAML::Value << usedLocation->getStorageService()->getName();
                    out << YAML::Key << "dst_storage_server" << YAML::Value << usedLocation->getStorageService()->getHostname();
                    out << YAML::Key << "dst_storage_disk" << YAML::Value << usedLocation->getMountPoint();
                    out << YAML::Key << "dst_file_path" << YAML::Value << usedLocation->getFullAbsolutePath();
                    out << YAML::Key << "dst_file_name" << YAML::Value << usedFile->getID();
                    out << YAML::Key << "dst_file_size_bytes" << YAML::Value << usedFile->getSize();

                } else if (auto fileCopy = std::dynamic_pointer_cast<FileCopyAction>(action)) {
                    auto src = fileCopy->getSourceFileLocation();
                    auto dest = fileCopy->getDestinationFileLocation();

                    out << YAML::Key << "src_storage_service" << YAML::Value << src->getStorageService()->getName();
                    out << YAML::Key << "src_storage_server" << YAML::Value << src->getStorageService()->getHostname();
                    out << YAML::Key << "src_storage_disk" << YAML::Value << src->getMountPoint();
                    out << YAML::Key << "src_file_path" << YAML::Value << src->getFullAbsolutePath();
                    out << YAML::Key << "src_file_name" << YAML::Value << src->getFile()->getID();
                    out << YAML::Key << "src_file_size_bytes" << YAML::Value << src->getFile()->getSize();

                    out << YAML::Key << "dst_storage_service" << YAML::Value << dest->getStorageService()->getName();
                    out << YAML::Key << "dst_storage_server" << YAML::Value << dest->getStorageService()->getHostname();
                    out << YAML::Key << "dst_storage_disk" << YAML::Value << dest->getMountPoint();
                    out << YAML::Key << "dst_file_path" << YAML::Value << dest->getFullAbsolutePath();
                    out << YAML::Key << "dst_file_name" << YAML::Value << dest->getFile()->getID();
                    out << YAML::Key << "dst_file_size_bytes" << YAML::Value << dest->getFile()->getSize();

                } else if (auto fileDelete = std::dynamic_pointer_cast<FileDeleteAction>(action)) {
                    auto usedLocation = fileDelete->getFileLocation();
                    auto usedFile = usedLocation->getFile();

                    out << YAML::Key << "dst_storage_service" << YAML::Value << usedLocation->getStorageService()->getName();
                    out << YAML::Key << "dst_storage_server" << YAML::Value << usedLocation->getStorageService()->getHostname();
                    out << YAML::Key << "dst_storage_disk" << YAML::Value << usedLocation->getMountPoint();
                    out << YAML::Key << "dst_file_path" << YAML::Value << usedLocation->getFullAbsolutePath();
                    out << YAML::Key << "dst_file_name" << YAML::Value << usedFile->getID();
                    out << YAML::Key << "dst_file_size_bytes" << YAML::Value << usedFile->getSize();

                }
                out << YAML::EndMap;
            }
            out << YAML::EndSeq;
            out << YAML::EndMap;
        }
        out << YAML::EndSeq;
        
        // Write to YAML file
        ofstream io_ops;
        io_ops.open ("io_operations.yml");
        io_ops << "---\n";
        io_ops << out.c_str();
        io_ops << "\n...\n";
        io_ops.close();
         	
    }

} // namespace wrench
