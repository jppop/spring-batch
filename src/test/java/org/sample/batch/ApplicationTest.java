package org.sample.batch;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.sample.batch.config.BatchConfiguration;
import org.sample.batch.model.Person;
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
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {BatchConfiguration.class, TestConfig.class})
public class ApplicationTest {

  public static final String COUNT_PEOPLE = "SELECT COUNT(*) FROM PEOPLE";
  public static final String INPUT_FILE_PARAM = "input.file";
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  @Autowired
  private JdbcTemplate jdbcTemplate;
  @Autowired
  private JobLauncherTestUtils jobLauncherTestUtils;

  @Before
  public void cleanUp() {
    jdbcTemplate.update("delete from people");
  }

  @Test
  public void shouldBeSuccessfully() throws Exception {
    File dataFile = folder.newFile("data.csv");
    PersonFaker personFaker = new PersonFaker();
    List<Person> persons = personFaker.buildPersons(10, new Integer[]{3, 7});
    personFaker.writeCsvOfPerson(dataFile.getAbsolutePath(), persons);

    JobParameters params =
      new JobParametersBuilder().addString(INPUT_FILE_PARAM, dataFile.getAbsolutePath()).toJobParameters();
    BatchStatus batchStatus = jobLauncherTestUtils.launchJob(params).getStatus();
    assertEquals(BatchStatus.COMPLETED, batchStatus);

    long result = jdbcTemplate.queryForObject(COUNT_PEOPLE, Long.class);
    assertEquals(8, result); // two items skipped

    // TODO: check error.csv
  }

  @Test(expected = JobInstanceAlreadyCompleteException.class)
  public void shouldNotBeReExecuted() throws Exception {
    File dataFile = folder.newFile("data.csv");
    PersonFaker personFaker = new PersonFaker();
    List<Person> persons = personFaker.buildPersons(10, new Integer[]{3, 7});
    personFaker.writeCsvOfPerson(dataFile.getAbsolutePath(), persons);

    JobParameters params = new JobParametersBuilder().addString(INPUT_FILE_PARAM, dataFile.getAbsolutePath()).toJobParameters();
    BatchStatus batchStatus = jobLauncherTestUtils.launchJob(params).getStatus();
    assertEquals(BatchStatus.COMPLETED, batchStatus);

    jobLauncherTestUtils.launchJob(params).getStatus();
  }

  @Test
  public void shouldBeFailedWhenTooManyErrors() throws Exception {

    File dataFile = folder.newFile("data.csv");
    PersonFaker personFaker = new PersonFaker();
    List<Person> persons = personFaker.buildPersons(10, new Integer[]{3, 7, 9});
    personFaker.writeCsvOfPerson(dataFile.getAbsolutePath(), persons);

    JobParameters params = new JobParametersBuilder().addString(INPUT_FILE_PARAM, dataFile.getAbsolutePath()).toJobParameters();
    BatchStatus batchStatus = jobLauncherTestUtils.launchJob(params).getStatus();
    assertEquals(BatchStatus.FAILED, batchStatus);
    long result = jdbcTemplate.queryForObject(COUNT_PEOPLE, Long.class);
    assertEquals(6, result); // two items skipped, the last chunk failed (chunk size is 2)
  }

  @Test
  public void shouldBeReprocessedWhenFixed() throws Exception {

    File dataFile = folder.newFile("data.csv");
    PersonFaker personFaker = new PersonFaker();
    List<Person> persons = personFaker.buildPersons(10, new Integer[]{3, 7, 9});
    personFaker.writeCsvOfPerson(dataFile.getAbsolutePath(), persons);

    JobParameters params = new JobParametersBuilder().addString(INPUT_FILE_PARAM, dataFile.getAbsolutePath()).toJobParameters();
    BatchStatus batchStatus = jobLauncherTestUtils.launchJob(params).getStatus();
    assertEquals(BatchStatus.FAILED, batchStatus);
    long result = jdbcTemplate.queryForObject(COUNT_PEOPLE, Long.class);
    assertEquals(6, result); // two items skipped, the last chunk failed (chunk size is 2)

    // fix errors
    persons.get(9).setAge(99);
    personFaker.writeCsvOfPerson(dataFile.getAbsolutePath(), persons);

    // restart job
    batchStatus = jobLauncherTestUtils.launchJob(params).getStatus();
    assertEquals(BatchStatus.COMPLETED, batchStatus);
    result = jdbcTemplate.queryForObject(COUNT_PEOPLE, Long.class);
    assertEquals(8, result); // two items skipped, the last chunk succeed

    // fix skipped errors
    persons.get(3).setAge(100);
    persons.get(7).setAge(101);
    List<Person> fixedPersons = new ArrayList<>(2);
    fixedPersons.add(persons.get(3));
    fixedPersons.add(persons.get(7));
    File fixedFile = folder.newFile("dataFixed.csv");
    personFaker.writeCsvOfPerson(fixedFile.getAbsolutePath(), fixedPersons);

    // start job with fixed file
    JobParameters fixedParams = new JobParametersBuilder().addString(INPUT_FILE_PARAM, fixedFile.getAbsolutePath()).toJobParameters();
    batchStatus = jobLauncherTestUtils.launchJob(fixedParams).getStatus();
    assertEquals(BatchStatus.COMPLETED, batchStatus);
    result = jdbcTemplate.queryForObject(COUNT_PEOPLE, Long.class);
    assertEquals(10, result);
  }
}