package org.sample.batch;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {BatchConfiguration.class, TestConfig.class})
public class ApplicationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void shouldBeSuccessfully() throws Exception {
        JobParameters params = new JobParametersBuilder().addString("inputFile", "src/resources/sample-data.csv").toJobParameters();
        BatchStatus batchStatus = jobLauncherTestUtils.launchJob(params).getStatus();
        assertEquals(BatchStatus.COMPLETED, batchStatus);
    }

    @Test(expected = JobInstanceAlreadyCompleteException.class)
    public void shouldNotBeReExecuted() throws Exception {
        JobParameters params = new JobParametersBuilder().addString("inputFile", "src/resources/sample-data.csv").toJobParameters();
        BatchStatus batchStatus = jobLauncherTestUtils.launchJob(params).getStatus();
        assertEquals(BatchStatus.COMPLETED, batchStatus);

        jobLauncherTestUtils.launchJob(params).getStatus();


    }
}