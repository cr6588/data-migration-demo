package com.sjdf.demo;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;


@Configuration
@EnableBatchProcessing
public class CustomBatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public JdbcTemplate jdbcTemplate;

    @Bean
    public SqlPagingQueryProviderFactoryBean queryProvider(DataSource dataSource) {
        SqlPagingQueryProviderFactoryBean provider = new SqlPagingQueryProviderFactoryBean();

        provider.setSelectClause("select *");
        provider.setFromClause("from :table");
        provider.setSortKey("id");
        provider.setDataSource(dataSource);
        return provider;
    }

    @Bean
    @StepScope
    public JdbcPagingItemReader itemReader(DataSource dataSource, PagingQueryProvider queryProvider, @Value("#{jobParameters['table']}") String table) {
        Map<String, Object> parameterValues = new HashMap<>();
        parameterValues.put("table", table);
        MySqlPagingQueryProvider p = (MySqlPagingQueryProvider) queryProvider;
        p.setFromClause("from " + table);
        return new JdbcPagingItemReaderBuilder<Map<String, Object>>()
                .name("itemReader")
                .dataSource(dataSource)
                .queryProvider(queryProvider)
                .parameterValues(parameterValues)
                .rowMapper(personMapper())
                .pageSize(1000)
                .build();
    }

    private RowMapper<Map<String, Object>> personMapper() {
        return (rs, rowNum) -> {
            ResultSetMetaData rsm = rs.getMetaData();
            Map<String, Object> map = new HashMap<>();
            int col = rsm.getColumnCount();   //获得列的个数
            for (int i = 0; i < col; i++) {
                String columnName = rsm.getColumnName(i + 1);
                map.put(columnName, rs.getObject(columnName));
            }
            return map;
        };
    }

//    @Bean
//    public PersonItemProcessor processor() {
//        return new PersonItemProcessor();
//    }

    @Bean
    public FlatFileItemWriter<Map<String, Object>> writer() {
        return new FlatFileItemWriterBuilder<Map<String, Object>>()
                .name("itemWriter")
                .resource(new FileSystemResource("target/test-outputs/output.txt"))
                .lineAggregator(new PassThroughLineAggregator<>())
                .build();
    }


    @Bean
    public Job exportPersonJob(Step step1) {
        return jobBuilderFactory.get("exportPersonJob")
                .incrementer(new RunIdIncrementer())
                .flow(step1)
                .end()
                .build();
    }

    @Bean
    public Step step1(JdbcPagingItemReader<Map<String, Object>> reader) {
        return stepBuilderFactory.get("step1")
                .<Map<String, Object>, Map<String, Object>> chunk(10)//提交间隔
                .reader(reader)
//                .processor(processor())
                .writer(writer())
                .allowStartIfComplete(true)
                .build();
    }
}
