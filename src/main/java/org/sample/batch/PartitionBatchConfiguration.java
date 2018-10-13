package org.sample.batch;

import org.apache.logging.log4j.util.Strings;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.PathResource;
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

@Configuration
@EnableBatchProcessing
public class PartitionBatchConfiguration {

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

    // tag::readerwriterprocessor[]
    @Bean
    @StepScope
    public FlatFileItemReader<Person> reader(@Value("#{stepExecutionContext['input.file']}") String inputFile) {
        return new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                .resource(new PathResource(inputFile))
                .lineMapper(new DefaultLineMapper<Person>() {
                    {
                        setLineTokenizer(new DelimitedLineTokenizer() {
                            {
                                setStrict(false);
                                setDelimiter(";");
                                setNames(new String[] { "firstName", "lastName", "age" });
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
        return new PersonItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Person> writer() {
        return new JdbcBatchItemWriterBuilder<Person>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO people (first_name, last_name, age) VALUES (:firstName, :lastName, :age)")
                .dataSource(dataSource())
                .build();
    }
    // end::readerwriterprocessor[]

    @Bean
    public JobCompletionNotificationListener jobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
        return new JobCompletionNotificationListener(jdbcTemplate);
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
                String locationPattern = "file://" + Paths.get(inboudsDirJobParam,"*.csv").toString();
                resources = resoursePatternResolver.getResources(locationPattern);
            } catch (IOException e) {
                throw new RuntimeException("I/O problems when resolving the input file pattern.", e);
            }

        } else if (!Strings.isBlank(inputFile)) {

            Resource resource = new PathResource(inputFile);
            resources = new Resource[] { resource };

        } else {
            throw new RuntimeException("Either 'input.dir' or 'input.file' is mandatory");
        }
        partitioner.setResources(resources);
        return partitioner;
    }

    // tag::jobstep[]
    @Bean
    public Step partitionStep() {
        return stepBuilderFactory.get("partitionStep")
                .partitioner("slaveStep", partitioner(SHOULD_BE_OVERRIDDEN, SHOULD_BE_OVERRIDDEN))
                .step(step1())
                .taskExecutor(taskExecutor())
                .build();
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
    public ChunkListener chunkListener() {
        return new ChunkListener();
    }

    @Bean
    public SkipListener skipListener() {
        return new SkipListener(errorItemWriter(SHOULD_BE_OVERRIDDEN));
    }

    @Bean
    @StepScope
    public FlatFileItemWriterDual<Person> errorItemWriter(@Value("#{stepExecutionContext['output.error.file']}") String outputFile) {
        FlatFileItemWriterDual<Person> errorItemWriter = new FlatFileItemWriterDual<>();
        FlatFileItemWriter<String> csvFileWriter = new FlatFileItemWriter<>();

        String exportFileHeader = "firstName;lastName;age;status;errorCode";
        StringHeaderWriter headerWriter = new StringHeaderWriter(exportFileHeader);
        csvFileWriter.setHeaderCallback(headerWriter);

        csvFileWriter.setResource(new FileSystemResource(outputFile));
        DelimitedLineAggregator<String> rawLineAggregator = new DelimitedLineAggregator<>();
        rawLineAggregator.setDelimiter(";");
        csvFileWriter.setLineAggregator(rawLineAggregator);

        LineAggregator<Person> lineAggregator = createPersonLineAggregator();

        errorItemWriter.setDelegate(csvFileWriter);
        errorItemWriter.setLineAggregator(lineAggregator);
        return errorItemWriter;
    }

    @Bean
    public SkipListener skipListener(FlatFileItemWriterDual<Person> errorItemWriter) {
        return new SkipListener(errorItemWriter);
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
                .stream(errorItemWriter(SHOULD_BE_OVERRIDDEN))
//                .listener(stepExecutionListener())
                .listener(chunkListener())
                .listener(skipListener())
                .build();
    }
    // end::jobstep[]

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(2);
        taskExecutor.setCorePoolSize(2);
//        taskExecutor.setQueueCapacity(2);
        taskExecutor.setThreadNamePrefix("step-executor");
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

    private LineAggregator<Person> createPersonLineAggregator() {
        DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(";");

        FieldExtractor<Person> fieldExtractor = createPersonFieldExtractor();
        lineAggregator.setFieldExtractor(fieldExtractor);

        return lineAggregator;
    }

    private FieldExtractor<Person> createPersonFieldExtractor() {
        BeanWrapperFieldExtractor<Person> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(new String[]{"firstName", "lastName", "age", "status", "errorCode"});
        return extractor;
    }

}
