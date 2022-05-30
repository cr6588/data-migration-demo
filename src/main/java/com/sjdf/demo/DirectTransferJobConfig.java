package com.sjdf.demo;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DuplicateKeyException;

import javax.sql.DataSource;

@Configuration
public class DirectTransferJobConfig {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Bean
    @StepScope
    public MultiTableReader tableDataReader(@Value("#{jobParameters['in.url']}") String url,
                                                @Value("#{jobParameters['table']}") String table,
                                                @Value("#{jobParameters['in.username']}") String username,
                                                @Value("#{jobParameters['in.password']}") String password) {
        return new MultiTableReader(table.split(","), new DbConVo(url, username, password));
    }

    @Bean
    public TableDataItemProcessor tableDataItemProcessor() {
        return new TableDataItemProcessor();
    }

    @StepScope
    @Bean
    public TableDataWriter tableDataWriter(@Value("#{jobParameters['out.url']}") String url,
                                           @Value("#{jobParameters['out.username']}") String username,
                                           @Value("#{jobParameters['out.password']}") String password) {
        DataSource dataSource = DataSourceCache.getDataSource(url, username, password);
        return new TableDataWriter(dataSource);
    }

    @Bean
    public Job directTransferJob(Step directTransferJobStep) {
        return jobBuilderFactory.get("directTransferJob")
                .incrementer(new RunIdIncrementer())
                .start(directTransferJobStep)
                .build();
    }

    @Bean
    public Step directTransferJobStep(MultiTableReader reader, TableDataWriter tableDataWriter) {
        return stepBuilderFactory.get("directTransferJobStep")
                .<TableData, TableData> chunk(1000)//提交间隔
                .reader(reader)
//                .processor(tableDataItemProcessor())
                .writer(tableDataWriter)
                //配置跳过
                .faultTolerant()
                .skip(DuplicateKeyException.class) //跳过主键重复异常
                .skipLimit(Integer.MAX_VALUE)
                .allowStartIfComplete(true)
                .build();
    }

}
