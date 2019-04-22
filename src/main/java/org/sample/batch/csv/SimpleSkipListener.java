package org.sample.batch.csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.PassThroughFieldExtractor;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Step listener used to output all errors to a file.
 * <br>
 * <p>
 *     Output a CSV file like:<br>
 *     subject;type;message<br>
 *     12345575;read;Syntax error at line 21<br>
 *     1452456;process;Value not allowed in this case<br>
 *     1257357;write;failed to send mail<br>
 * </p>
 *
 * @param <T> The type of item (read, processed and written).
 */
public class SimpleSkipListener<T> implements SkipListener<T, T>, StepExecutionListener, InitializingBean {

  private static final Logger logger = LoggerFactory.getLogger(SimpleSkipListener.class);
  public static final String DELIMITER = ";";

  private FlatFileItemWriter<ErrorItem> errorItemWriter;
  private final String errorOutputPath;
  private final Class<T> type;
  private final CsvNameExtractor<T> csvNameExtractor;
  private ExecutionContext executionContext;

  private static final String keyPrefix = ClassUtils.getShortName(SimpleSkipListener.class);
  private static final Function<Throwable, String> messageFromException = t -> Optional.ofNullable(t.getMessage()).orElse(t.toString());

  // counters
  private SkipCounter counters = new SkipCounter();

  public SimpleSkipListener(Class<T> itemType, String errorOutputPath) {
    this.errorOutputPath = errorOutputPath;
    type = itemType;
    csvNameExtractor = new CsvNameExtractor<>(itemType);
  }

  @Override
  public void onSkipInRead(Throwable t) {
    logger.debug("Skipping read due to error:", t);

    String message = messageFromException.apply(t);
    logger.info("Skipping read due to error: {}", message);
    ErrorItem errorItem = new ErrorItem(subjectFrom(t), "read", message);

    writeError(t, errorItem);
    counters.incReadError();
  }

  @Override
  public void onSkipInProcess(T item, Throwable t) {
    logger.debug("Skipping process of {} due to error:", item, t);
    String message = messageFromException.apply(t);
    logger.info("Skipping process of {} due to error: {}", item, message);
    ErrorItem errorItem = new ErrorItem(subjectFrom(item), "process", message);
    writeError(t, errorItem);
    counters.incProcessError();
  }

  @Override
  public void onSkipInWrite(T item, Throwable t) {
    logger.debug("Skipping process of {} due to error:", item, t);
    String message = messageFromException.apply(t);
    logger.info("Skipping process of {} due to error: {}", item, message);
    ErrorItem errorItem = new ErrorItem(subjectFrom(item), "write", message);
    writeError(t, errorItem);
    counters.incWriteError();
  }

  @Override
  public void beforeStep(StepExecution stepExecution) {
    this.executionContext = stepExecution.getExecutionContext();
    this.errorItemWriter.open(executionContext);
  }

  @Override
  public ExitStatus afterStep(StepExecution stepExecution) {
    this.errorItemWriter.close();
    executionContext.put(keyPrefix + ".counters", this.counters);
    return stepExecution.getExitStatus();
  }

  @Override
  public void afterPropertiesSet()  {

    DelimitedLineAggregator<ErrorItem> aggregator = new DelimitedLineAggregator<>();
    aggregator.setDelimiter(DELIMITER);
    aggregator.setFieldExtractor(new PassThroughFieldExtractor<>());

    errorItemWriter = new FlatFileItemWriter<>();
    errorItemWriter.setResource(new FileSystemResource(this.errorOutputPath));
    List<String> columnNames = csvNameExtractor.getColumnNames();
    columnNames.add("type");
    columnNames.add("error");
    String header = columnNames.stream()
            .map(String::toString)
            .collect(Collectors.joining(DELIMITER));
    errorItemWriter.setHeaderCallback(w -> w.write(header));
    errorItemWriter.setLineAggregator(aggregator);

    errorItemWriter.setAppendAllowed(true);

    Consumer.acceptWithRawException(errorItemWriter, FlatFileItemWriter::afterPropertiesSet);

  }

  public static SkipCounter getCounters(ExecutionContext executionContext) {
    Assert.notNull(executionContext, "An ExecutionContext must be provided");
    return (SkipCounter) executionContext.get(keyPrefix + ".counters");
  }

  private void writeError(Throwable t, ErrorItem item) {
    try {
      errorItemWriter.write(Collections.singletonList(item));
      errorItemWriter.update(this.executionContext);
    } catch (Exception e) {
      logger.warn("failed to write error. Subject: {}, error: {}", item.getSubject(), t);
    }
  }

  private Map<String, String> subjectFrom(T item) {
    return csvNameExtractor.from(item, false);
  }

  private Map<String, String> subjectFrom(Throwable t) {
    if (FlatFileParseException.class.isAssignableFrom(t.getClass())) {
      final String input = ((FlatFileParseException)t).getInput();
      String[] columns = input.split(DELIMITER);
      return csvNameExtractor.from(columns, false);
    }
    return csvNameExtractor.nonAvailable(false);
  }

  private static class ErrorItem<T> {
    private final Map<String, String> subject;

    private ErrorItem(Map<String, String> subject, String type, String message) {
      this.subject = subject;
      this.subject.put("type", type);
      this.subject.put("error", message);
    }

    public Map<String, String> getSubject() {
      return subject;
    }

    @Override
    public String toString() {
      return "ErrorItem{" +
        "subject='" + subject + '\'' +
        '}';
    }
  }

}