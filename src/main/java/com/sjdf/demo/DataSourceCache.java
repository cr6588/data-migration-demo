package com.sjdf.demo;

import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

public class DataSourceCache {

    private static Map<String, DataSource> dsMap = new HashMap<>();

    private static Map<String, PagingQueryProvider> queryProviderMap = new HashMap<>();

    public static synchronized DataSource getDataSource(String url, String username, String password) {
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

    public static synchronized PagingQueryProvider queryProvider(String url, DataSource dataSource, String table) {
        PagingQueryProvider queryProvider = queryProviderMap.get(url);
        if(queryProvider != null) {
            return queryProvider;
        }
        try {
            //设置分页构造器，初始化需要传一个表名
            SqlPagingQueryProviderFactoryBean provider = new SqlPagingQueryProviderFactoryBean();
            provider.setSelectClause("select *");
            provider.setSortKey("id");
            provider.setFromClause(table);
            provider.setDataSource(dataSource);
            queryProvider = provider.getObject();
            queryProviderMap.put(url, queryProvider);
            return queryProvider;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
