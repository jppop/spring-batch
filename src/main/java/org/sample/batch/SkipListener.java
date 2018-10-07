package org.sample.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.annotation.OnSkipInProcess;
import org.springframework.batch.core.annotation.OnSkipInRead;
import org.springframework.batch.item.file.FlatFileParseException;

import java.util.Collections;

public class SkipListener {

    private static final Logger log = LoggerFactory.getLogger(SkipListener.class);
    private final FlatFileItemWriterEx<Person> errorItemWriter;

    public SkipListener(FlatFileItemWriterEx<Person> errorItemWriter) {
        this.errorItemWriter = errorItemWriter;
    }

    @OnSkipInRead
    public void onSkipInProcess(java.lang.Throwable t) throws Exception {
        log.info("Skipping read due to error: {}", t.getMessage());
        if (t instanceof FlatFileParseException) {
            FlatFileParseException ffpe = (FlatFileParseException) t;
            StringBuilder line = new StringBuilder(ffpe.getInput());
            line.append(";[E(p)];").append("1000");
            errorItemWriter.writeRaw(Collections.singletonList(line.toString()));
        }
    }

    @OnSkipInProcess
    public void onSkipInProcess(Person person, java.lang.Throwable t) throws Exception {
        log.info("Skipping {} due to error: {}", person, t.getMessage());
        person.setStatus("[E(r)]");
        person.setErrorCode("9999");
        errorItemWriter.write(Collections.singletonList(person));
    }
}
