package com.sjdf.demo;

import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class DirectTransferJobConfig {

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

    /**
     * 需要依赖datasource
     */
    private Map<String, PagingQueryProvider> queryProviderMap = new HashMap<>();
    private Map<String, DataSource> dsMap = new HashMap<>();

    @Bean
    @StepScope
    public JdbcPagingItemReader tableDataReader(@Value("#{jobParameters['in.url']}") String url,
                                                @Value("#{jobParameters['table']}") String table,
                                                @Value("#{jobParameters['in.username']}") String username,
                                                @Value("#{jobParameters['in.password']}") String password) {
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

    @StepScope
    @Bean
    public TableDataWriter tableDataWriter(@Value("#{jobParameters['out.url']}") String url,
                                           @Value("#{jobParameters['table']}") String table,
                                           @Value("#{jobParameters['out.username']}") String username,
                                           @Value("#{jobParameters['out.password']}") String password) {
        DataSource dataSource = getDataSource(url, username, password);
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
    public Step directTransferJobStep(JdbcPagingItemReader<Map<String, Object>> reader, TableDataWriter tableDataWriter) {
        return stepBuilderFactory.get("directTransferJobStep")
                .<Map<String, Object>, String> chunk(1000)//提交间隔
                .reader(reader)
                .processor(mapItemProcessor())
                .writer(tableDataWriter)
                //配置跳过
                .faultTolerant()
                .skip(DuplicateKeyException.class) //跳过主键重复异常
                .skipLimit(Integer.MAX_VALUE)
                .allowStartIfComplete(true)
                .build();
    }

}
