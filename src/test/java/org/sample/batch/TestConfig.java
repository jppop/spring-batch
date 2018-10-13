package org.sample.batch;

import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.MapJobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class TestConfig {

    @Bean
    public DataSource dataSource() {
        EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
        EmbeddedDatabase db = builder
                .setType(EmbeddedDatabaseType.HSQL) //.H2 or .DERBY
                .addScripts("schema-all.sql")
                .build();
        return db;
    }

    @Bean
    public JdbcTemplate jdbcTemplate() {
        return new JdbcTemplate(dataSource());
    }

    @Bean
    public JobLauncherTestUtils jobLauncherTestUtils() { return new JobLauncherTestUtils(); }

    @Configuration
    @EnableBatchProcessing
    protected static class BatchConfiguration implements BatchConfigurer {

        private ResourcelessTransactionManager transactionManager = new ResourcelessTransactionManager();

        private JobRepository jobRepository;

        private MapJobRepositoryFactoryBean jobRepositoryFactory = new MapJobRepositoryFactoryBean(
                this.transactionManager);

        public BatchConfiguration() throws Exception {
            this.jobRepository = this.jobRepositoryFactory.getObject();
        }

        public void clear() {
            this.jobRepositoryFactory.clear();
        }

        @Override
        public JobRepository getJobRepository() throws Exception {
            return this.jobRepository;
        }

        @Override
        public PlatformTransactionManager getTransactionManager() throws Exception {
            return this.transactionManager;
        }

        @Override
        public JobLauncher getJobLauncher() throws Exception {
            SimpleJobLauncher launcher = new SimpleJobLauncher();
            launcher.setJobRepository(this.jobRepository);
            launcher.setTaskExecutor(new SyncTaskExecutor());
            return launcher;
        }

        @Override
        public JobExplorer getJobExplorer() throws Exception {
            return new MapJobExplorerFactoryBean(this.jobRepositoryFactory).getObject();
        }

    }
}
