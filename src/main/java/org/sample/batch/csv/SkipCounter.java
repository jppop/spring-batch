package org.sample.batch.csv;

import java.io.Serializable;

public class SkipCounter implements Serializable {

  private long errors = 0;
  private long readErrors = 0;
  private long writeError = 0;
  private long processError = 0;

  public SkipCounter() {
  }

  public long getErrors() {
    return errors;
  }

  public void setErrors(long errors) {
    this.errors = errors;
  }

  public long getReadErrors() {
    return readErrors;
  }

  public void setReadErrors(long readErrors) {
    this.readErrors = readErrors;
  }

  public long getWriteError() {
    return writeError;
  }

  public void setWriteError(long writeError) {
    this.writeError = writeError;
  }

  public long getProcessError() {
    return processError;
  }

  public void setProcessError(long processError) {
    this.processError = processError;
  }

  public void incReadError() {
    this.readErrors++;
    this.errors++;
  }

  public void incProcessError() {
    this.processError++;
    this.errors++;
  }

  public void incWriteError() {
    this.writeError++;
    this.errors++;
  }

  @Override
  public String toString() {
    return "SkipCounter{" +
        "errors=" + errors +
        ", readErrors=" + readErrors +
        ", writeError=" + writeError +
        ", processError=" + processError +
        '}';
  }
}