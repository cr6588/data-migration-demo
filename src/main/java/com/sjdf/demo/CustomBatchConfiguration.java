package com.sjdf.demo;

import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;


@Configuration
public class CustomBatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    public PagingQueryProvider queryProvider(DataSource dataSource, String table) throws Exception {
        SqlPagingQueryProviderFactoryBean provider = new SqlPagingQueryProviderFactoryBean();
        provider.setSelectClause("select *");
        provider.setSortKey("id");
        provider.setFromClause(table);
        provider.setDataSource(dataSource);
        return provider.getObject();
    }

    private Map<String, PagingQueryProvider> queryProviderMap = new HashMap<>();
    private Map<String, DataSource> dsMap = new HashMap<>();

    @Bean
    @StepScope
    public JdbcPagingItemReader tableDataReader(@Value("#{jobParameters['url']}") String url,
                                                @Value("#{jobParameters['table']}") String table,
                                                @Value("#{jobParameters['username']}") String username,
                                                @Value("#{jobParameters['password']}") String password) {
        DataSource dataSource = getDataSource(url, username, password);
        PagingQueryProvider queryProvider = queryProviderMap.get(url);
        if(queryProvider == null) {
            try {
                //设置分页构造器，初始化需要传一个表名
                queryProvider = queryProvider(dataSource, table);
                queryProviderMap.put(url, queryProvider);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
        //设置表名
        if(queryProvider instanceof MySqlPagingQueryProvider) {
            ((MySqlPagingQueryProvider)queryProvider).setFromClause(table);
        }
        return new JdbcPagingItemReaderBuilder<Map<String, Object>>()
                .name("tableDataReader")
                .dataSource(dataSource)
                .queryProvider(queryProvider)
                .rowMapper(mapMapper())
                .pageSize(1000)
                .build();
    }

    private DataSource getDataSource(String url, String username, String password) {
        DataSource dataSource = dsMap.get(url);
        if(dataSource != null) {
            return dataSource;
        }
        DruidDataSource userDb = new DruidDataSource();
        userDb.setDriverClassName("com.mysql.cj.jdbc.Driver");
        userDb.setUrl(url + "?useUnicode=true&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true");
        userDb.setUsername(username);
        userDb.setPassword(password);
        userDb.setInitialSize(1);
        userDb.setMinIdle(1);
        userDb.setMaxActive(100);
        userDb.setMaxWait(60000);
        userDb.setTimeBetweenEvictionRunsMillis(60000);
        userDb.setMinEvictableIdleTimeMillis(30000);
        userDb.setValidationQuery("SELECT 'x'");
        userDb.setTestWhileIdle(true);
        userDb.setTestOnBorrow(false);
        userDb.setTestOnReturn(false);
        userDb.setPoolPreparedStatements(false);
        userDb.setMaxPoolPreparedStatementPerConnectionSize(20);
        dsMap.put(url, userDb);
        return userDb;
    }

    private RowMapper<Map<String, Object>> mapMapper() {
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

    @Bean
    @StepScope
    public MapItemProcessor mapItemProcessor() {
        return new MapItemProcessor();
    }

    @Bean
    public FlatFileItemWriter<String> tableDataFileWriter() {
        FlatFileItemWriter<String> tableDataFileWriter = new FlatFileItemWriterBuilder<String>()
                .name("tableDataFileWriter")
                .resource(new FileSystemResource("target/test-outputs/output.txt"))
                .lineAggregator(new PassThroughLineAggregator<>())
                .build();
        return tableDataFileWriter;
    }

    @Bean
    public Job exportTableJob(Step exportTableStep) {
        return jobBuilderFactory.get("exportTableJob")
                .incrementer(new RunIdIncrementer())
                .start(exportTableStep)
                .build();
    }

    @Bean
    public Step exportTableStep(JdbcPagingItemReader<Map<String, Object>> reader) {
        return stepBuilderFactory.get("exportTableStep")
                .<Map<String, Object>, String> chunk(1000)//提交间隔
                .reader(reader)
                .processor(mapItemProcessor())
                .writer(tableDataFileWriter())
                .allowStartIfComplete(true)
                .build();
    }




    @Bean
    public FlatFileItemReader<String> tableFileDataReader() {
        return new FlatFileItemReaderBuilder<String>()
                .name("tableFileDataReader")
                .resource(new FileSystemResource("target/test-outputs/output.txt"))
                .lineMapper(new PassThroughLineMapper())
                .build();
    }

    @StepScope
    @Bean
    public TableDataWriter tableDataWriter(@Value("#{jobParameters['url']}") String url,
                                                       @Value("#{jobParameters['table']}") String table,
                                                       @Value("#{jobParameters['username']}") String username,
                                                       @Value("#{jobParameters['password']}") String password) {
        DataSource dataSource = getDataSource(url, username, password);
        return new TableDataWriter(dataSource);
    }


    @Bean
    public Job importTableJob(Step importTableStep) {
        return jobBuilderFactory.get("exportTableJob")
                .incrementer(new RunIdIncrementer())
                .start(importTableStep)
                .build();
    }

    @Bean
    public Step importTableStep(TableDataWriter tableDataWriter) {
        return stepBuilderFactory.get("exportTableStep")
                .<String, String> chunk(1000)//提交间隔
                .reader(tableFileDataReader())
//                .processor(mapItemProcessor())
                .writer(tableDataWriter)
                .allowStartIfComplete(true)
                .build();
    }
}
