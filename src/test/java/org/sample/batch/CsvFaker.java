package org.sample.batch;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.sample.batch.csv.CsvNameExtractor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class CsvFaker<T> {

    public static final char SEPARATOR = ';';
    private final Class<T> type;

    private Map<String, Record<T>> records = new HashMap<>();
    private boolean includeOptional = false;

    public CsvFaker(Class<T> type) {
        this.type = type;
    }

    public CsvFaker includeOptional(boolean include) {
        this.includeOptional = include;
        return this;
    }

    public CsvFaker with(String key, T record) {
        records.put(key, new Record<>(record));
        return this;
    }

    public CsvFaker withFaultyRecord(String key, String... values) {
        records.put(key, new Record<>(values));
        return this;
    }

    public void build(File dataFile) throws IOException {
        final CsvNameExtractor<T> csvExtractor = new CsvNameExtractor<>(type);
        final String[] headerNames = csvExtractor.getColumnNames(includeOptional).toArray(new String[0]);

        try (
            BufferedWriter writer = Files.newBufferedWriter(Paths.get(dataFile.getAbsolutePath()));

            CSVPrinter csvPrinter =
                new CSVPrinter(
                    writer,
                    CSVFormat.DEFAULT
                        .withDelimiter(SEPARATOR)
                        .withHeader(headerNames))
        ) {

            this.records.values().forEach(record -> {
                try {
                    csvPrinter.printRecord(record.getValue(csvExtractor, includeOptional));
                } catch (IOException e) {
                    throw  new RuntimeException(e);
                }
            });

            csvPrinter.flush();
        }
    }

    public int itemCount() {
        return this.records.size();
    }

    public List<T> items() {
        return this.records.values().stream()
            .filter(r -> !r.isFaultyRecord())
            .map(r -> r.getRecord())
            .collect(Collectors.toList());
    }

    public T item(String id) {
        Optional<T> itemOpt = findItemById(id);
        return itemOpt.orElse(null);
    }

    public Optional<T> findItemById(String id) {
        Record<T> record = this.records.get(id);
        if (record == null || record.isFaultyRecord()) {
            return Optional.empty();
        } else {
            return Optional.of(record.getRecord());
        }
    }

    public static class Record<T> {
        private final T record;
        private final String[] rawRecord;
        private final boolean faultyRecord;

        public Record(T record) {
            this.record = record;
            this.faultyRecord = false;
            this.rawRecord = null;
        }

        public Record(String... values) {
            this.rawRecord = values;
            this.faultyRecord = true;
            this.record = null;
        }

        public T getRecord() {
            return record;
        }

        public String[] getRawRecord() {
            return rawRecord;
        }

        public boolean isFaultyRecord() {
            return faultyRecord;
        }

        public List<String> getValue(CsvNameExtractor<T> csvExtractor, boolean includeOptional) {
            if (this.isFaultyRecord()) {
                return Arrays.asList(this.rawRecord);
            } else {
                return csvExtractor.getValues(this.record, includeOptional);
            }
        }

        @Override
        public String toString() {
            return "Record{" +
                "record=" + record +
                ", rawRecord=" + Arrays.toString(rawRecord) +
                ", faultyRecord=" + faultyRecord +
                '}';
        }
    }
}