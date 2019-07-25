package com.batch.akbatch.service;

import java.util.Date;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LoadRemoteFileService {

	@Autowired
    private JobLauncher jobLauncher;
	
    
    @Autowired
    private Job remoteFileLoadJob;

    @Scheduled(cron = "${scheduler.expression}", zone = "IST")
    public BatchStatus loadFile() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {

    	JobExecution jobExecution = jobLauncher.run(remoteFileLoadJob, new JobParametersBuilder()
		        .addDate("date", new Date())
		        .toJobParameters());
        log.info("JobExecution: " + jobExecution.getStatus());

        log.info("Batch is Running...");
        while (jobExecution.isRunning()) {
        	log.info("...");
        }

        return jobExecution.getStatus();
    }
}
