package com.batch.akbatch.config;

import java.time.LocalDate;

import javax.sql.DataSource;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.repository.dao.AbstractJdbcBatchMetadataDao;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteJobRepsitoryTasklet implements Tasklet, InitializingBean {
	/**
	 * SQL statements removing step and job executions compared to a given date.
	 */
	private static final String SQL_DELETE_BATCH_STEP_EXECUTION_CONTEXT = "DELETE FROM %PREFIX%STEP_EXECUTION_CONTEXT WHERE STEP_EXECUTION_ID IN (SELECT STEP_EXECUTION_ID FROM %PREFIX%STEP_EXECUTION WHERE JOB_EXECUTION_ID IN (SELECT JOB_EXECUTION_ID FROM  %PREFIX%JOB_EXECUTION where TRUNC(CREATE_TIME) < ?))";
	private static final String SQL_DELETE_BATCH_STEP_EXECUTION = "DELETE FROM %PREFIX%STEP_EXECUTION WHERE JOB_EXECUTION_ID IN (SELECT JOB_EXECUTION_ID FROM %PREFIX%JOB_EXECUTION where TRUNC(CREATE_TIME) < ?)";
	private static final String SQL_DELETE_BATCH_JOB_EXECUTION_CONTEXT = "DELETE FROM %PREFIX%JOB_EXECUTION_CONTEXT WHERE JOB_EXECUTION_ID IN (SELECT JOB_EXECUTION_ID FROM  %PREFIX%JOB_EXECUTION where TRUNC(CREATE_TIME) < ?)";
	private static final String SQL_DELETE_BATCH_JOB_EXECUTION_PARAMS = "DELETE FROM %PREFIX%JOB_EXECUTION_PARAMS WHERE JOB_EXECUTION_ID IN (SELECT JOB_EXECUTION_ID FROM %PREFIX%JOB_EXECUTION where TRUNC(CREATE_TIME) < ?)";
	private static final String SQL_DELETE_BATCH_JOB_EXECUTION = "DELETE FROM %PREFIX%JOB_EXECUTION where TRUNC(CREATE_TIME) < ?";
	private static final String SQL_DELETE_BATCH_JOB_INSTANCE = "DELETE FROM %PREFIX%JOB_INSTANCE WHERE JOB_INSTANCE_ID NOT IN (SELECT JOB_INSTANCE_ID FROM %PREFIX%JOB_EXECUTION)";

	/**
	 * Default value for the table prefix property.
	 */
	private static final String DEFAULT_TABLE_PREFIX = AbstractJdbcBatchMetadataDao.DEFAULT_TABLE_PREFIX;

	/**
	 * Default value for the data retention (in month)
	 */

	private String tablePrefix = DEFAULT_TABLE_PREFIX;

	private Integer noOfDaysToKeep;

	public Integer getNoOfDaysToKeep() {
		return noOfDaysToKeep;
	}

	private JdbcTemplate jdbcTemplate;

	public DeleteJobRepsitoryTasklet(DataSource dataSource) {
		jdbcTemplate = new JdbcTemplate(dataSource);
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
		int totalCount = 0;
		LocalDate dateToDelete = LocalDate.now().minusDays(noOfDaysToKeep);

		log.info("Delete the Spring Batch Repository before the {}", dateToDelete);

		int rowCount = jdbcTemplate.update(getQuery(SQL_DELETE_BATCH_STEP_EXECUTION_CONTEXT), dateToDelete);
		log.info("Deleted rows number from the BATCH_STEP_EXECUTION_CONTEXT table: {}", rowCount);
		totalCount += rowCount;

		rowCount = jdbcTemplate.update(getQuery(SQL_DELETE_BATCH_STEP_EXECUTION), dateToDelete);
		log.info("Deleted rows number from the BATCH_STEP_EXECUTION table: {}", rowCount);
		totalCount += rowCount;

		rowCount = jdbcTemplate.update(getQuery(SQL_DELETE_BATCH_JOB_EXECUTION_CONTEXT), dateToDelete);
		log.info("Deleted rows number from the BATCH_JOB_EXECUTION_CONTEXT table: {}", rowCount);
		totalCount += rowCount;

		rowCount = jdbcTemplate.update(getQuery(SQL_DELETE_BATCH_JOB_EXECUTION_PARAMS), dateToDelete);
		log.info("Deleted rows number from the BATCH_JOB_EXECUTION_PARAMS table: {}", rowCount);
		totalCount += rowCount;

		rowCount = jdbcTemplate.update(getQuery(SQL_DELETE_BATCH_JOB_EXECUTION), dateToDelete);
		log.info("Deleted rows number from the BATCH_JOB_EXECUTION table: {}", rowCount);
		totalCount += rowCount;

		rowCount = jdbcTemplate.update(getQuery(SQL_DELETE_BATCH_JOB_INSTANCE));
		log.info("Deleted rows number from the BATCH_JOB_INSTANCE table: {}", rowCount);
		totalCount += rowCount;

		contribution.incrementWriteCount(totalCount);

		return RepeatStatus.FINISHED;
	}

	protected String getQuery(String base) {
		return StringUtils.replace(base, "%PREFIX%", tablePrefix);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(jdbcTemplate, "The jdbcTemplate must not be null");
	}

	public void setTablePrefix(String tablePrefix) {
		this.tablePrefix = tablePrefix;
	}

	public void setNoOfDaysToKeep(Integer noOfDaysToKeep) {
		this.noOfDaysToKeep = noOfDaysToKeep;
	}

}
