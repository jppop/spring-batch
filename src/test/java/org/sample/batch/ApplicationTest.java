package org.sample.batch;

import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {BatchConfiguration.class, TestConfig.class})
public class ApplicationTest {

    private static final String SAMPLE_DATA_PATH = "src/main/resources/sample-data.csv";

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

	@Before
    public void cleanUp() {
	    jdbcTemplate.update("delete from people");
    }
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldBeSuccessfully() throws Exception {
        JobParameters params = new JobParametersBuilder().addString("inputFile", SAMPLE_DATA_PATH).toJobParameters();
        BatchStatus batchStatus = jobLauncherTestUtils.launchJob(params).getStatus();
        assertEquals(BatchStatus.COMPLETED, batchStatus);
    }

    @Test(expected = JobInstanceAlreadyCompleteException.class)
    public void shouldNotBeReExecuted() throws Exception {
        JobParameters params = new JobParametersBuilder().addString("inputFile", SAMPLE_DATA_PATH).toJobParameters();
        BatchStatus batchStatus = jobLauncherTestUtils.launchJob(params).getStatus();
        assertEquals(BatchStatus.COMPLETED, batchStatus);

        jobLauncherTestUtils.launchJob(params).getStatus();
    }

    @Test
    public void shouldBeFailed() throws Exception {

        File dataFile = folder.newFile("data.csv");
	    PersonFaker personFaker = new PersonFaker();
        List<Person> persons = personFaker.buildPersons(10, new Integer[]{3, 7, 9});
	    personFaker.writeCsvOfPerson(dataFile.getAbsolutePath(), persons);

        JobParameters params = new JobParametersBuilder().addString("inputFile", dataFile.getAbsolutePath()).toJobParameters();
        BatchStatus batchStatus = jobLauncherTestUtils.launchJob(params).getStatus();
        assertEquals(BatchStatus.FAILED, batchStatus);
    }
}