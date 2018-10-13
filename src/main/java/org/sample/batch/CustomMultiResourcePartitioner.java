package org.sample.batch;

import com.google.common.io.Files;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class CustomMultiResourcePartitioner implements Partitioner {

    private static final String inputFileKey = "input.file";
    private static final String outputErrorFileKey = "output.error.file";
    private static final String PARTITION_KEY = "partition";

    private Resource[] resources;

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> map = new HashMap<>(gridSize);
        int i = 0, k = 1;
        for (Resource resource : resources) {
            ExecutionContext context = new ExecutionContext();
            Assert.state(resource.exists(), "Resource does not exist: "
              + resource);
            try {
                String folder = resource.getFile().getParent();
                File inputFile = resource.getFile();
                String fileNameWithOutExt = Files.getNameWithoutExtension(inputFile.getName());
                String errorFilename = String.format("%s-errors.csv", fileNameWithOutExt);
                String errorPathname = Paths.get(folder, errorFilename).toString();
                context.putString(inputFileKey, inputFile.getAbsolutePath());
                context.putString(outputErrorFileKey, errorPathname);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            map.put(PARTITION_KEY + i, context);
            i++;
        }
        return map;
    }

    public void setResources(Resource[] resources) {
        this.resources = resources;
    }
}