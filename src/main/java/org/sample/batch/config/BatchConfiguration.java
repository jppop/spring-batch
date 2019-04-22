package org.sample.batch.config;

import org.apache.logging.log4j.util.Strings;
import org.sample.batch.csv.CsvNameExtractor;
import org.sample.batch.csv.SimpleSkipListener;
import org.sample.batch.listener.ChunkListener;
import org.sample.batch.listener.JobCompletionNotificationListener;
import org.sample.batch.model.InvalidDataException;
import org.sample.batch.model.Person;
import org.sample.batch.processor.PersonItemProcessor;
import org.sample.batch.service.NationalService;
import org.sample.batch.service.impl.NationalServiceImpl;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

  public static final String SHOULD_BE_OVERRIDDEN = "should be overridden";

  @Autowired
  public JobBuilderFactory jobBuilderFactory;

  @Autowired
  public StepBuilderFactory stepBuilderFactory;

  @Autowired
  private ResourcePatternResolver resoursePatternResolver;

  @Bean
  public DataSource dataSource() {
    EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
    EmbeddedDatabase db = builder
      .setType(EmbeddedDatabaseType.HSQL) //.H2 or .DERBY
      .addScripts("schema-all.sql")
      .build();
    return db;
  }

  @Bean
  public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
    return jobBuilderFactory.get("importUserJob")
      .incrementer(new RunIdIncrementer())
      .listener(listener)
      .flow(partitionStep())
      .end()
      .build();
  }

  @Bean
  public Step partitionStep() {
    return stepBuilderFactory.get("partitionStep")
      .partitioner("slaveStep", partitioner(SHOULD_BE_OVERRIDDEN, SHOULD_BE_OVERRIDDEN))
      .step(step1())
      .taskExecutor(taskExecutor())
      .build();
  }

  @Bean
  public Step step1() {
    return stepBuilderFactory.get("step1")
      .<Person, Person>chunk(2)
      .reader(reader(SHOULD_BE_OVERRIDDEN))
      .processor(processor())
      .writer(writer())
      .faultTolerant()
      .skipLimit(2)
      .skip(InvalidDataException.class)
      .skip(FlatFileParseException.class)
      .listener(chunkListener())
      .listener(skipListener(SHOULD_BE_OVERRIDDEN))
      .build();
  }

  @Bean
  @StepScope
  public FlatFileItemReader<Person> reader(@Value("#{stepExecutionContext['input.file']}") String inputFile) {

    CsvNameExtractor<Person> csvNameExtractor = new CsvNameExtractor<>(Person.class);
    List<String> fields = csvNameExtractor.getNames();
    String[] header = fields.toArray(new String[fields.size()]);
    return new FlatFileItemReaderBuilder<Person>()
      .name("personItemReader")
      .resource(new FileSystemResource(inputFile))
      .lineMapper(new DefaultLineMapper<Person>() {
        {
          setLineTokenizer(new DelimitedLineTokenizer() {
            {
              setStrict(true);
              setDelimiter(";");
              setNames(header);
            }
          });
          setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
            setTargetType(Person.class);
          }});
        }
      })
      .linesToSkip(1)
      .build();
  }

  @Bean
  public PersonItemProcessor processor() {
    return new PersonItemProcessor(nationalService());
  }

  @Bean
  NationalService nationalService() {
    return new NationalServiceImpl();
  }

  @Bean
  public JdbcBatchItemWriter<Person> writer() {
    return new JdbcBatchItemWriterBuilder<Person>()
      .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
      .sql("INSERT INTO people (first_name, last_name, age) VALUES (:firstName, :lastName, :age)")
      .dataSource(dataSource())
      .build();
  }

  @Bean
  @JobScope
  public CustomMultiResourcePartitioner partitioner(
    @Value("#{jobParameters['input.dir']}") String inboudsDirJobParam,
    @Value("#{jobParameters['input.file']}") String inputFile
  ) {
    CustomMultiResourcePartitioner partitioner = new CustomMultiResourcePartitioner();
    Resource[] resources;
    if (!Strings.isBlank(inboudsDirJobParam)) {
      try {
        String locationPattern = "file://" + Paths.get(inboudsDirJobParam, "*.csv").toString();
        resources = resoursePatternResolver.getResources(locationPattern);
      } catch (IOException e) {
        throw new RuntimeException("I/O problems when resolving the input file pattern.", e);
      }

    } else if (!Strings.isBlank(inputFile)) {

      Resource resource = new FileSystemResource(inputFile);
      resources = new Resource[]{resource};

    } else {
      throw new RuntimeException("Either 'input.dir' or 'input.file' is mandatory");
    }
    partitioner.setResources(resources);
    return partitioner;
  }

  @Bean
  public ChunkListener chunkListener() {
    return new ChunkListener();
  }

  @Bean
  @StepScope
  public SimpleSkipListener skipListener(@Value("#{stepExecutionContext['output.error.file']}") String errorFilename) {
    return new SimpleSkipListener<>(Person.class, errorFilename);
  }

  @Bean
  public JobCompletionNotificationListener jobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
    return new JobCompletionNotificationListener(jdbcTemplate);
  }

  @Bean
  public TaskExecutor taskExecutor() {
    ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
    taskExecutor.setMaxPoolSize(2);
    taskExecutor.setCorePoolSize(2);
//        taskExecutor.setQueueCapacity(2);
    taskExecutor.setThreadNamePrefix("step-#");
    taskExecutor.afterPropertiesSet();
    return taskExecutor;
  }

}
