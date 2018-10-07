package org.sample.batch;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.transform.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    // tag::readerwriterprocessor[]
    @Bean
    public FlatFileItemReader<Person> reader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                .resource(new ClassPathResource("sample-data.csv"))
                .delimited()
                .delimiter(";")
                .names(new String[]{"firstName", "lastName", "age"})
                .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                    setTargetType(Person.class);
                }})
                .linesToSkip(1)
                .build();
    }

    @Bean
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO people (first_name, last_name, age) VALUES (:firstName, :lastName, :age)")
                .dataSource(dataSource)
                .build();
    }
    // end::readerwriterprocessor[]

    // tag::jobstep[]
    @Bean
    public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .end()
                .build();
    }

    @Bean
    public ChunkListener chunkListener() {
        return new ChunkListener();
    }

    @Bean
    public FlatFileItemWriterEx<Person> errorItemWriter() {
        FlatFileItemWriterEx<Person> errorItemWriter = new FlatFileItemWriterEx<>();
        FlatFileItemWriter<String> csvFileWriter = new FlatFileItemWriter<>();

        String exportFileHeader = "firstName;lastName;age;errorCode";
        StringHeaderWriter headerWriter = new StringHeaderWriter(exportFileHeader);
        csvFileWriter.setHeaderCallback(headerWriter);

        String exportFilePath = "error.csv";
        csvFileWriter.setResource(new FileSystemResource(exportFilePath));
        DelimitedLineAggregator<String> rawLineAggregator = new DelimitedLineAggregator<>();
        rawLineAggregator.setDelimiter(";");
        csvFileWriter.setLineAggregator(rawLineAggregator);

        LineAggregator<Person> lineAggregator = createPersonLineAggregator();

        errorItemWriter.setDelegate(csvFileWriter);
        errorItemWriter.setLineAggregator(lineAggregator);
        return errorItemWriter;
    }

    @Bean
    public SkipListener skipListener(FlatFileItemWriterEx<Person> errorItemWriter) {
        return new SkipListener(errorItemWriter);
    }

    @Bean
    public Step step1(JdbcBatchItemWriter<Person> writer, ChunkListener chunkListener, SkipListener skipListner, FlatFileItemWriterEx<Person> errorItemWriter) {
        return stepBuilderFactory.get("step1")
                .<Person, Person>chunk(2)
                .reader(reader())
                .processor(processor())
                .writer(writer)
                .faultTolerant()
                .skipLimit(2)
                .skip(IllegalArgumentException.class)
                .skip(FlatFileParseException.class)
                .stream(errorItemWriter)
                .listener(chunkListener)
                .listener(skipListner)
                .build();
    }
    // end::jobstep[]

    private LineAggregator<Person> createPersonLineAggregator() {
        DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(";");

        FieldExtractor<Person> fieldExtractor = createPersonFieldExtractor();
        lineAggregator.setFieldExtractor(fieldExtractor);

        return lineAggregator;
    }

    private FieldExtractor<Person> createPersonFieldExtractor() {
        BeanWrapperFieldExtractor<Person> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(new String[]{"firstName", "lastName", "age", "errorCode"});
        return extractor;
    }

}
