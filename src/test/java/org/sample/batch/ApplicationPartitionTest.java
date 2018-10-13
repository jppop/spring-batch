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
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {PartitionBatchConfiguration.class, TestConfig.class})
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
    public void shouldProcessOneParticularFileSuccessfully() throws Exception {
        File dataFile = dataFolder.newFile("data.csv");
        PersonFaker personFaker = new PersonFaker();
        final int expectedCount = 10;
        List<Person> persons = personFaker.buildPersons(expectedCount, new Integer[]{3, 7});
        personFaker.writeCsvOfPerson(dataFile.getAbsolutePath(), persons);

        JobParameters params = new JobParametersBuilder().addString("inboundsDir", dataFolder.getRoot().getAbsolutePath()).toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(params);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        // assert read/written items
        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            assertEquals(expectedCount, stepExecution.getReadCount());
            assertEquals(expectedCount - 2, stepExecution.getWriteCount());
            assertEquals(2, stepExecution.getSkipCount());
        }

        long result = jdbcTemplate.queryForObject(COUNT_PEOPLE, Long.class);
        assertEquals(expectedCount - 2, result); // two items skipped

        // TODO: check error.csv
    }

 }