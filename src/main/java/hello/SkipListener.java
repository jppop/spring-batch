package hello;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.annotation.OnSkipInProcess;
import org.springframework.batch.core.annotation.OnSkipInRead;
import org.springframework.batch.item.ItemWriter;

import java.util.Collections;

public class SkipListener {

    private static final Logger log = LoggerFactory.getLogger(SkipListener.class);
    private final ItemWriter<Person> errorItemWriter;

    public SkipListener(ItemWriter<Person> errorItemWriter) {
        this.errorItemWriter = errorItemWriter;
    }

    @OnSkipInRead
    public void onSkipInProcess(java.lang.Throwable t) throws Exception {
        log.info("Skipping read due to error: {}", t.getMessage());
    }

    @OnSkipInProcess
    public void onSkipInProcess(Person person, java.lang.Throwable t) throws Exception {
        log.info("Skipping {} due to error: {}", person, t.getMessage());
        person.setInvalid(true);
        person.setErrorCode("9999");
        errorItemWriter.write(Collections.singletonList(person));
    }
}
