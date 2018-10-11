package org.sample.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.StringUtils;

public class PersonItemProcessor implements ItemProcessor<Person, Person> {

    private static final Logger log = LoggerFactory.getLogger(PersonItemProcessor.class);

    @Override
    public Person process(final Person person) throws Exception {
        final String firstName = StringUtils.capitalize(person.getFirstName());
        final String lastName = person.getLastName().toUpperCase();

        if (person.getAge() == 0) {
            throw new InvalidDataException("must be born");
        }
        final Person transformedPerson = new Person(firstName, lastName, person.getAge());

        log.info("Converting (" + person + ") into (" + transformedPerson + ")");

        return transformedPerson;
    }

}
