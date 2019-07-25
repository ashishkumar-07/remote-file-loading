package com.batch.akbatch.controller;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.batch.akbatch.service.LoadRemoteFileService;

@RestController
@RequestMapping("/")
public class RemoteFileController {

	@Autowired
	LoadRemoteFileService loadRemoteFileService;

	@GetMapping("/load")
	public BatchStatus loadRemote() throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
			JobRestartException, JobInstanceAlreadyCompleteException {
		return loadRemoteFileService.loadFile();

	}
}