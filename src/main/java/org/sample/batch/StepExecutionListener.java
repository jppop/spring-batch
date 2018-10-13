package org.sample.batch;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.util.StringUtils;

public class StepExecutionListener {

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {

        // if input file has not been set by the partitioner, get it from job param
        // (use case: the job is called to process one particular file)
        ExecutionContext executionContext = stepExecution.getExecutionContext();
        String inputDataFile = executionContext.getString("input.file", "");
        if (StringUtils.isEmpty(inputDataFile)) {
            JobParameters params = stepExecution.getJobParameters();
            String inputFileJobParam = params.getString("inputFile");
            if (!StringUtils.isEmpty(inputFileJobParam)) {
                executionContext.put("inputData", inputFileJobParam);
            }
        }
    }
}
