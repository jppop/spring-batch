package org.sample.batch;

import com.github.javafaker.Faker;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import org.sample.batch.model.Person;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class PersonFaker {

    private Faker faker = new Faker(Locale.FRENCH);
    private final String[] header = new String[] { "firstName", "lastName", "age" };

    public Person buildPerson() {
        Person person = new Person();

        person.setFirstName(faker.name().firstName());
        person.setLastName(faker.name().lastName());
        person.setAge(faker.number().numberBetween(10, 100));
        return person;
     }

    public Person buildBadPerson() {
        Person badPerson = buildPerson();
        badPerson.setAge(0);
        return badPerson;
    }

    public List<Person> buildPersons(final int count, final Integer[] errorPositions) {
        List<Person> persons = new ArrayList<>(count);
        Set errorSet = new HashSet<>(Arrays.asList(errorPositions));

        for (int i = 0; i < count; i++) {
            Person person;
            boolean generateError = errorSet.contains(i);
            if (generateError) {
                person = buildBadPerson();
            } else {
                person = buildPerson();
            }
            persons.add(person);
        }
        return persons;
    }

    public void buildCsvOfPerson(final String outputFile, final int count, final int errorCount) throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {

        if (errorCount > count) {
            throw new IllegalArgumentException("errorCount > count");
        }
        // Creating writer class to generate csv file
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8)) {

            try (CSVWriter csvWriter = new CSVWriter(
                    writer,
                    ';',
                    CSVWriter.NO_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END
            )) {

                csvWriter.writeNext(header);
                int errors = 0;
                final int successCount = Math.max(0, count - errorCount);
                for (int i = 0; i < successCount; i++) {
                    Person person = buildPerson();
                    csvWriter.writeNext(personToArray(person));
                    if (errors < errorCount) {
                        int random = faker.number().numberBetween(1, 100);
                        if (random > 75) {
                            // process error
                            Person badPerson = buildBadPerson();
                            csvWriter.writeNext(personToArray(badPerson));
                            errors++;
                        }
                    }
                }
                for (int i = errors; i < errorCount; i++) {
                    Person badPerson = buildBadPerson();
                    csvWriter.writeNext(personToArray(badPerson));
                }
            }

        }
    }

    public void writeCsvOfPerson(final String outputFile, List<Person> persons) throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {

        // Creating writer class to generate csv file
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8)) {

            try (CSVWriter csvWriter = new CSVWriter(
                    writer,
                    ';',
                    CSVWriter.NO_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END
            )) {

                csvWriter.writeNext(header);
                for (Person person : persons) {
                    csvWriter.writeNext(personToArray(person));
                }
            }

        }
    }

    private static String[] personToArray(Person person) {
        String[] columns = new String[] { person.getFirstName(), person.getLastName(), Integer.toString(person.getAge()) };
        return columns;
    }

}
