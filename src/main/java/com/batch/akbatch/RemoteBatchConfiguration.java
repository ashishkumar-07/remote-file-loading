package com.batch.akbatch;

import javax.sql.DataSource;

import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import lombok.Getter;

@EnableBatchProcessing
@Configuration
/** This class configures the JOB repository in In-memory data base; separate from application DB **/
@Getter
public class RemoteBatchConfiguration extends DefaultBatchConfigurer {
    
	private DataSource embeddedDataSource;
	
	public DataSource embeddedDataSource() {

		EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
		EmbeddedDatabase embeddedDatabase = builder
				.addScript("classpath:org/springframework/batch/core/schema-drop-h2.sql")
				.addScript("classpath:org/springframework/batch/core/schema-h2.sql").setType(EmbeddedDatabaseType.H2)
				.build();
		embeddedDataSource=embeddedDatabase;
		return embeddedDataSource;
	}

	@Override
	protected JobRepository createJobRepository() throws Exception {
		JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
		factory.setDataSource(embeddedDataSource());
		factory.setTransactionManager(transactionManager());
		factory.afterPropertiesSet();
		factory.setValidateTransactionState(false);

		return (JobRepository) factory.getObject();
	}

	private ResourcelessTransactionManager transactionManager() {
		return new ResourcelessTransactionManager();
	}

}
