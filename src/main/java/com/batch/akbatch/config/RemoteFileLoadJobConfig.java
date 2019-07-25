package com.batch.akbatch.config;

import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;

import javax.persistence.EntityManagerFactory;
import javax.validation.ConstraintViolationException;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import com.batch.akbatch.RemoteBatchConfiguration;
import com.batch.akbatch.dto.Book;
import com.batch.akbatch.dto.BookDto;
import com.batch.akbatch.util.FtpProperties;
import com.batch.akbatch.util.MakeFtpConnection;
import com.batch.akbatch.util.RemoteResource;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class RemoteFileLoadJobConfig {

	private Resource[] inputResources;

	private MakeFtpConnection ftpCon;

	@Autowired
	private FtpProperties ftpProperties;

	@Value("${jobrepo.no-of-retention-days}")
	private Integer noOfDaysToKeepJobRunHist;

	@Bean
	public Job remoteFileLoadJob(StepBuilderFactory stepBuilder, JobBuilderFactory jobBuilderFactory,
			MultiResourceItemReader<BookDto> multiFileReader, ItemWriter<Book> fileWriter,
			RemoteBatchConfiguration remoteBatchConfiguration, PlatformTransactionManager jpaTransactionManager) {

		Step step = stepBuilder.get("load-remote-file").<BookDto, Book>chunk(1)
				.transactionManager(jpaTransactionManager).listener(listner())
				.reader(multiFileReader).processor(processNew(multiFileReader)).listener(listnerWrite())
				.writer(fileWriter).faultTolerant().skipPolicy(skipPolicy()).listener(stepExecutionListener()).build();

		DeleteJobRepsitoryTasklet deleteJobRepsitoryTasklet = new DeleteJobRepsitoryTasklet(
				remoteBatchConfiguration.getEmbeddedDataSource());
		deleteJobRepsitoryTasklet.setNoOfDaysToKeep(noOfDaysToKeepJobRunHist);

		Step deleteJobRepsitoryTaskletStep = stepBuilder.get("delete-job-repository").tasklet(deleteJobRepsitoryTasklet)
				.build();

		return jobBuilderFactory.get("remote-load").start(step).next(deleteJobRepsitoryTaskletStep).build();

	}

	@Bean
	@StepScope
	public MultiResourceItemReader<BookDto> multiFileReader() throws JSchException, SftpException {

		Resource[] tempResource = { new FileSystemResource("dummy") };
		Vector<LsEntry> files = new Vector<>();

		ftpCon = new MakeFtpConnection(ftpProperties.getHost(), ftpProperties.getUsername(),
				ftpProperties.getPassword(), ftpProperties.getPort(), null, null);

		ftpCon.openConnection();
		files = ftpCon.getSftpChannel().ls(ftpProperties.getSourcePath() + ftpProperties.getFilePattern());

		inputResources = files.stream().map(m -> m.getFilename())
				// .filter(e -> e.endsWith("csv"))
				.map(m -> new RemoteResource(ftpProperties.getSourcePath() + m, ftpCon)).collect(Collectors.toList())
				.toArray(tempResource);

		return new MultiResourceItemReaderBuilder<BookDto>().name("multi-file-reader").resources(inputResources)
				.delegate(singleFileReader()).build();
	}

	public FlatFileItemReader<BookDto> singleFileReader() {

		FlatFileItemReader<BookDto> flatFileItemReader = new FlatFileItemReader<BookDto>();

		flatFileItemReader.setLineMapper(new DefaultLineMapper<BookDto>() {
			{
				// 3 columns in each row
				setLineTokenizer(new DelimitedLineTokenizer(",") {
					{
						setNames(new String[] { "isbn", "bookName", "authorName", "genre" });
					}
				});
				// Set values in Employee class
				setFieldSetMapper(new BeanWrapperFieldSetMapper<BookDto>() {
					{
						setTargetType(BookDto.class);
					}
				});
			}
		});

		return flatFileItemReader;
	}

	@Bean
	@StepScope
	public ItemWriter<Book> fileWriter(EntityManagerFactory entityManagerFactory) {
		return new JpaItemWriterBuilder<Book>().entityManagerFactory(entityManagerFactory).build();

	}

	public ItemProcessor<BookDto, Book> processNew(MultiResourceItemReader<BookDto> multiReader) {
		return (item) -> {
			Book pr = new Book();
			pr.setIsbn(item.getIsbn());
			pr.setBookName(item.getBookName());
			pr.setAuthorName(item.getAuthorName());
			pr.setGenre(item.getGenre());
			pr.setFileName(multiReader.getCurrentResource().getFilename());
			return pr;
		};
	}

	public ItemReadListener<BookDto> listner() {
		return new ItemReadListener<BookDto>() {

			@Override
			public void beforeRead() {
			}

			@Override
			public void afterRead(BookDto item) {

			}

			@Override
			public void onReadError(Exception ex) {
				log.error(ex.getMessage());
			}

		};

	}

	private SkipPolicy skipPolicy() {
		return new SkipPolicy() {

			@Override
			public boolean shouldSkip(Throwable t, int skipCount) throws SkipLimitExceededException {
				if (t instanceof FlatFileParseException || t instanceof ConstraintViolationException )
					return true;
				else
					return false;
			}

		};
	}

	public ItemWriteListener<Book> listnerWrite() {
		return new ItemWriteListener<Book>() {

			@Override
			public void beforeWrite(List<? extends Book> items) {
			}

			@Override
			public void afterWrite(List<? extends Book> items) {
			}

			@Override
			public void onWriteError(Exception exception, List<? extends Book> items) {
				log.error(exception.getMessage());
			}

		};

	}

	public StepExecutionListener stepExecutionListener() {

		return new StepExecutionListener() {

			@Override
			public void beforeStep(StepExecution stepExecution) {
			}

			@Override
			public ExitStatus afterStep(StepExecution stepExecution) {
				log.info("StepExecutionListener - afterStep");
				log.info("------------------------------------------------------------------------------------");
				log.info("StepExecutionListener - afterStep:getCommitCount=" + stepExecution.getCommitCount());
				log.info("StepExecutionListener - afterStep:getFilterCount=" + stepExecution.getFilterCount());
				log.info(
						"StepExecutionListener - afterStep:getProcessSkipCount=" + stepExecution.getProcessSkipCount());
				log.info("StepExecutionListener - afterStep:getReadCount=" + stepExecution.getReadCount());
				log.info("StepExecutionListener - afterStep:getReadSkipCount=" + stepExecution.getReadSkipCount());
				log.info("StepExecutionListener - afterStep:getRollbackCount=" + stepExecution.getRollbackCount());
				log.info("StepExecutionListener - afterStep:getWriteCount=" + stepExecution.getWriteCount());
				log.info("StepExecutionListener - afterStep:getWriteSkipCount=" + stepExecution.getWriteSkipCount());
				log.info("StepExecutionListener - afterStep:getStepName=" + stepExecution.getStepName());
				log.info("StepExecutionListener - afterStep:getSummary=" + stepExecution.getSummary());
				log.info("StepExecutionListener - afterStep:getStartTime=" + stepExecution.getStartTime());
				log.info("StepExecutionListener - afterStep:getStartTime=" + stepExecution.getEndTime());
				log.info("StepExecutionListener - afterStep:getLastUpdated=" + stepExecution.getLastUpdated());
				log.info("StepExecutionListener - afterStep:getExitStatus=" + stepExecution.getExitStatus());
				log.info("StepExecutionListener - afterStep:getFailureExceptions="
						+ stepExecution.getFailureExceptions());
				log.info("------------------------------------------------------------------------------------");

				ftpCon.closeConnection();

				return null;
			}

		};

	}
	
	@Bean
	public PlatformTransactionManager jpaTransactionManager(EntityManagerFactory emf) {
	       return new JpaTransactionManager(emf);
	}

}
