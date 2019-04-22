package org.sample.batch;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.sample.batch.config.BatchConfiguration;
import org.sample.batch.model.Person;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.batch.test.StepScopeTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.validation.BindException;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {BatchConfiguration.class, TestConfig.class})
public class ReaderTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Autowired
  private FlatFileItemReader<Person> reader;

  private static Person personOf(String firstName, String lastName, int age) {
    return new Person(firstName, lastName, age);
  }

  @Test
  public void read() throws Exception {

    // Arrange
    File dataFile = folder.newFile("data.csv");

    CsvFaker<Person> csvFaker = new CsvFaker<>(Person.class);
    csvFaker
      .with("001", personOf("john", "doe", 23))
      .build(dataFile);

    // Act
    List<Person> people = readAll(dataFile);

    // Assert
    assertThat(people).hasSize(1);
    assertThat(people)
      .extracting("firstName", "lastName", "age")
      .contains(tuple("john", "doe", 23));

  }

  @Test
  public void failWhenInputIsNotValid() throws Exception {

    // Arrange
    File dataFile = folder.newFile("data.csv");

    CsvFaker<Person> csvFaker = new CsvFaker<>(Person.class);
    csvFaker
      .with("001", personOf("john", "doe", 23))
      .withFaultyRecord("002", "donald", "duck", "xx")
      .build(dataFile);

    // Act and Assert
    assertThatThrownBy(() -> readAll(dataFile))
      .isInstanceOf(FlatFileParseException.class)
      .hasCauseInstanceOf(BindException.class);
  }

  private List<Person> readAll(File inputFile) throws Exception {

    // build step execution context
    StepExecution stepExecution = MetaDataInstanceFactory.createStepExecution();
    stepExecution.getExecutionContext().putString("input.file", inputFile.getAbsolutePath());

    return StepScopeTestUtils.doInStepScope(
      stepExecution,
      () -> {

        // init reader
        reader.open(stepExecution.getExecutionContext());

        List<Person> items = new ArrayList<>();

        Person person;
        while ((person = reader.read()) != null) {
          items.add(person);
        }
        return items;
      });
  }
}
