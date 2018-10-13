package org.sample.batch;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {BatchConfiguration.class, TestConfig.class})
public class ApplicationPartitionTest {

    private static final String SAMPLE_DATA_PATH = "src/main/resources/sample-data.csv";
    public static final String COUNT_PEOPLE = "SELECT COUNT(*) FROM PEOPLE";

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

	@Before
    public void cleanUp() {
	    jdbcTemplate.update("delete from people");
    }
    @Rule
    public TemporaryFolder dataFolder = new TemporaryFolder();

    @Test
    public void shouldProcessAllFilesSuccessfully() throws Exception {

        PersonFaker personFaker = new PersonFaker();

        final int fileCount = 10;
        final int itemCount = 10;
        final Integer[] skippedItems = new Integer[]{3, 7};

        for (int file = 0; file < fileCount; file++) {
            File dataFile = dataFolder.newFile(String.format("data-#%02d.csv", file));
            List<Person> persons = personFaker.buildPersons(itemCount, skippedItems);
            personFaker.writeCsvOfPerson(dataFile.getAbsolutePath(), persons);
        }

        JobParameters params = new JobParametersBuilder().addString("input.dir", dataFolder.getRoot().getAbsolutePath()).toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(params);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // assert executions
        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        assertEquals(fileCount + 1, stepExecutions.size());

//        stepExecutions.stream().forEach( System.out::println );

        // assert read/written items
        final int expectedGlobalReads = fileCount * itemCount;
        final int expectedGlobalWrites = fileCount * (itemCount - skippedItems.length);
        final int expectedGlobalSkips = fileCount * skippedItems.length;
        Optional<StepExecution> executionOpt =
                stepExecutions.stream().filter(e -> "partitionStep".equals(e.getStepName())).findFirst();
        assertTrue(executionOpt.isPresent());
        StepExecution execution = executionOpt.get();
        assertEquals(expectedGlobalReads, execution.getReadCount());
        assertEquals(expectedGlobalWrites, execution.getWriteCount());
        assertEquals(expectedGlobalSkips, execution.getSkipCount());

        // read/writes of each step
        List<StepExecution> executions = stepExecutions.stream()
                .filter(e -> e.getStepName().startsWith("step1:"))
                .collect(Collectors.toList());
        for (StepExecution stepExecution : executions) {
            assertEquals(itemCount, stepExecution.getReadCount());
            assertEquals(itemCount - skippedItems.length, stepExecution.getWriteCount());
            assertEquals(skippedItems.length, stepExecution.getSkipCount());
        }

        long result = jdbcTemplate.queryForObject(COUNT_PEOPLE, Long.class);
        assertEquals(fileCount * (itemCount - skippedItems.length), result); // two items skipped

        for (int file = 0; file < fileCount; file++) {
            File errorFile = new File(dataFolder.getRoot(), String.format("data-#%02d-errors.csv", file));
            assertTrue(errorFile.exists());
        }

    }

    @Test
    public void shouldProcessOneParticularFileSuccessfully() throws Exception {
        File dataFile = dataFolder.newFile("data.csv");
        PersonFaker personFaker = new PersonFaker();
        final int expectedCount = 100;
        final Integer[] skippedItems = new Integer[] { 3, 7 };
        List<Person> persons = personFaker.buildPersons(expectedCount, skippedItems);
        personFaker.writeCsvOfPerson(dataFile.getAbsolutePath(), persons);

        JobParameters params = new JobParametersBuilder().addString("input.file", dataFile.getAbsolutePath()).toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(params);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        // assert read/written items
        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        assertEquals(1 + 1, stepExecutions.size());
        Optional<StepExecution> executionOpt =
                stepExecutions.stream().filter(e -> "partitionStep".equals(e.getStepName())).findFirst();
        assertTrue(executionOpt.isPresent());
        StepExecution execution = executionOpt.get();

        assertEquals(expectedCount, execution.getReadCount());
        assertEquals(expectedCount - 2, execution.getWriteCount());
        assertEquals(skippedItems.length, execution.getSkipCount());

        long result = jdbcTemplate.queryForObject(COUNT_PEOPLE, Long.class);
        assertEquals(expectedCount - skippedItems.length, result); // two items skipped

        File errorFile = new File(dataFolder.getRoot(), "data-errors.csv");
        assertTrue(errorFile.exists());
    }

}