package org.sample.batch.processor;

import org.sample.batch.model.InvalidDataException;
import org.sample.batch.model.Person;
import org.sample.batch.service.NationalService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.StringUtils;

import java.util.Optional;

public class PersonItemProcessor implements ItemProcessor<Person, Person> {

  private static final Logger log = LoggerFactory.getLogger(PersonItemProcessor.class);

  private final NationalService nationalService;

  public PersonItemProcessor(NationalService nationalService) {
    this.nationalService = nationalService;
  }

  @Override
  public Person process(final Person person) throws Exception {
    final String firstName = StringUtils.capitalize(person.getFirstName());
    final String lastName = person.getLastName().toUpperCase();

    if (person.getAge() <= 0) {
      throw new InvalidDataException("must be born");
    }
    final Person transformedPerson = new Person(firstName, lastName, person.getAge());

    // find national ID
    Optional<String> nationalIdentifier = nationalService.findNationalIdentifier(firstName, lastName);
    if (nationalIdentifier.isPresent()) {
      transformedPerson.setNationalId(nationalIdentifier.get());
    }

    log.info("Converting (" + person + ") into (" + transformedPerson + ")");

    return transformedPerson;
  }

}
