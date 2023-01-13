
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

#include <iostream>

#include "Controller.h"

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

        WRENCH_INFO("Got %s jobs", std::to_string(jobs.size()).c_str());

        /* Create a job manager so that we can create/submit jobs */
        auto job_manager = this->createJobManager();
        std::vector<std::shared_ptr<wrench::CompoundJob>> compound_jobs;
        std::vector<std::shared_ptr<wrench::Action>> actions;

        for (const auto& yaml_job : jobs) {
            
            // Create WRENCH compound jobs
            auto job = job_manager->createCompoundJob(std::to_string(yaml_job.id));
            WRENCH_INFO("Creating a compound job for ID %s", std::to_string(yaml_job.id).c_str());

            // Create file for read operation
            std::shared_ptr<wrench::DataFile> read_file = nullptr;
            std::shared_ptr<wrench::FileReadAction> fileReadAction = nullptr;
            if (yaml_job.readBytes != 0) {
                read_file = wrench::Simulation::addFile("input_data_file_" + std::to_string(yaml_job.id), yaml_job.readBytes);
                // "storage_service" represents a user shared storage area (/home, any NFS, ...) or any storage located outside the cluster.
                wrench::Simulation::createFile(wrench::FileLocation::LOCATION(this->storage_service, "/dev/hdd0/", read_file));
                job->addFileCopyAction(
                    "fileCopyForStaging", 
                    wrench::FileLocation::LOCATION(this->storage_service, "/dev/hdd0/", read_file),
                    wrench::FileLocation::LOCATION(this->compound_storage_service, read_file)
                );
                fileReadAction = job->addFileReadAction("fileRead", wrench::FileLocation::LOCATION(this->compound_storage_service, read_file));
                actions.push_back(fileReadAction);
                WRENCH_INFO("Read action added to job ID %s", std::to_string(yaml_job.id).c_str());
            }

            auto compute = job->addComputeAction("compute", 300 * GFLOP, 50 * MBYTE, yaml_job.coresUsed, yaml_job.coresUsed, wrench::ParallelModel::AMDAHL(0.8));
            actions.push_back(compute);
            WRENCH_INFO("Compute action added to job ID %s", std::to_string(yaml_job.id).c_str());

            // Possibly model some waiting time / bootstrapping with a sleep action ?
            // auto sleep = job2->addSleepAction("sleep", 20.0);

            // Create file for write operation
            std::shared_ptr<wrench::DataFile> write_file = nullptr;
            std::shared_ptr<wrench::FileWriteAction> fileWriteAction = nullptr;
            if (yaml_job.writtenBytes != 0) {
                write_file = wrench::Simulation::addFile("ouptut_data_file_" + std::to_string(yaml_job.id), yaml_job.writtenBytes);
                fileWriteAction = job->addFileWriteAction("fileWrite", wrench::FileLocation::LOCATION(this->compound_storage_service, write_file));
                actions.push_back(fileWriteAction);
                WRENCH_INFO("Write action added to job ID %s", std::to_string(yaml_job.id).c_str());
            }
            
            // Dependencies (if any)
            if (fileReadAction)
                job->addActionDependency(fileReadAction, compute);
            if (fileWriteAction)
                job->addActionDependency(compute, fileWriteAction);

            std::map<std::string, std::string> service_specific_args =
                    {{"-N", std::to_string(yaml_job.nodesUsed)},
                     {"-c", std::to_string(yaml_job.coresUsed)},
                     {"-t", std::to_string(yaml_job.runTime)}};
            WRENCH_INFO("Submitting job %s (%d nodes, %d cores per node, %d minutes) for executing actions",
                        job->getName().c_str(),
                        yaml_job.nodesUsed, yaml_job.coresUsed, yaml_job.runTime
            );
            job_manager->submitJob(job, this->compute_service, service_specific_args);

        }
    
        WRENCH_INFO("All jobs submitted to the BatchComputeService");
        
        /*
        auto wr_loc = wrench::FileLocation::LOCATION(this->storage_service, "/dev/ssd0/temp_write", result_file);
        auto wr_loc2 = wrench::FileLocation::LOCATION(this->storage_service, "/dev/ssd0/temp_write", result_file);
        WRENCH_INFO(wr_loc->getAbsolutePathAtMountPoint().c_str());
        WRENCH_INFO(wr_loc->getFile()->getID().c_str());
        WRENCH_INFO(wr_loc->getFullAbsolutePath().c_str());
        WRENCH_INFO(wr_loc->toString().c_str()); 
        WRENCH_INFO(wr_loc->equal(wr_loc2) ? "Both wr_loc equal" : "wr_loc are different");
        */
       
        auto nb_jobs = compound_jobs.size();
        for (size_t i = 0; i < nb_jobs; i++) {
            this->waitForAndProcessNextEvent();
        }

        WRENCH_INFO("Execution complete!");

        for (auto const &a : actions) {
            printf("Action %s: %.2fs - %.2fs\n", a->getName().c_str(), a->getStartDate(), a->getEndDate());
        }

        /*
            // cleanup
            if (read_file)
                wrench::simulation::removeFile(read_file);

            if (write_file)
                wrench::simulation::removeFile(write_file);
        */

        return 0;
    }

    /**
     * @brief Process a compound job completion event
     *
     * @param event: the event
     */
    void Controller::processEventCompoundJobCompletion(std::shared_ptr<CompoundJobCompletedEvent> event) {
        /* Retrieve the job that this event is for */
        auto job = event->job;
        /* Print info about all actions in the job */
        WRENCH_INFO("Notified that compound job %s has completed:", job->getName().c_str());
    }
}
