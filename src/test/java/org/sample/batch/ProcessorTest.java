package org.sample.batch;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.sample.batch.model.Person;
import org.sample.batch.processor.PersonItemProcessor;
import org.sample.batch.service.NationalService;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;

@RunWith(MockitoJUnitRunner.class)
public class ProcessorTest {

  @Mock
  private NationalService nationalService;

  @InjectMocks
  private PersonItemProcessor processor;

  private static Person personOf(String firstName, String lastName, int age) {
    return new Person(firstName, lastName, age);
  }

  @Test
  public void process() throws Exception {

    // Arrange

    // create John
    Person john = personOf("john", "doe", 34);
    // John's ID is 123
    Mockito.when(
      nationalService.findNationalIdentifier(
        eq("John"),
        eq(john.getLastName().toUpperCase())))
      .thenReturn(Optional.of("123"));

    // Act
    Person actualPerson = processor.process(john);

    // Assert

    assertThat(actualPerson.getFirstName()).isEqualTo("John");
    assertThat(actualPerson.getLastName()).isEqualTo("DOE");
    assertThat(actualPerson.getAge()).isEqualTo(34);

    assertThat(actualPerson.getNationalId()).isEqualTo("123");
  }

}
