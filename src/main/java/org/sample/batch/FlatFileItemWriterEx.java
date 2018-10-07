package org.sample.batch;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.LineAggregator;

import java.util.ArrayList;
import java.util.List;

public class FlatFileItemWriterEx<T> implements ItemStreamWriter<T> {

    private FlatFileItemWriter<String> delegate;
    private LineAggregator<T> lineAggregator;

    public FlatFileItemWriterEx() {}

    public FlatFileItemWriterEx(FlatFileItemWriter<String> itemWriter, LineAggregator<T> lineAggregator) {
        super();
        this.delegate = itemWriter;
        this.lineAggregator = lineAggregator;
    }

    public ItemWriter<String> getDelegate() {
        return delegate;
    }

    public void setDelegate(FlatFileItemWriter<String> delegate) {
        this.delegate = delegate;
    }

    public LineAggregator<T> getLineAggregator() {
        return lineAggregator;
    }

    public void setLineAggregator(LineAggregator<T> lineAggregator) {
        this.lineAggregator = lineAggregator;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        delegate.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        delegate.update(executionContext);

    }

    @Override
    public void close() throws ItemStreamException {
        delegate.close();

    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        List<String> lines = new ArrayList<>();
        for (T item : items) {
            lines.add(lineAggregator.aggregate(item));
        }
        delegate.write(lines);
    }

    public void writeRaw(List<String> items) throws Exception {
        delegate.write(items);
    }
}
