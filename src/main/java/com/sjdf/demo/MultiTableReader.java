package com.sjdf.demo;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;

@Data
@Slf4j
public class MultiTableReader implements ItemReader<TableData> {

    private String[] tables;
    private int curTableIndex = 0;
    private DbConVo in;

    private JdbcPagingItemReader<Map<String, Object>> reader;

    public MultiTableReader(String[] tables, DbConVo in) {
        this.tables = tables;
        this.in = in;
        //初始化第一张表的reader
        String table = tables[curTableIndex];
        try {
            reader = tableDataReader(in.getUrl(), table, in.getUsername(), in.getPassword());
        } catch (Exception e) {
            throw new RuntimeException("reader init error");
        }
    }

    @Override
    public TableData read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        Map<String, Object> o = reader.read();
        if(o != null) {

            return new TableData(tables[curTableIndex], o);
        }
        log.info("table {} read complete", tables[curTableIndex]);
        //查看表是否已读完，没有则继续读取下一张表
        curTableIndex++;
        if(curTableIndex < tables.length) {
            String table = tables[curTableIndex];
            reader = tableDataReader(in.getUrl(), table, in.getUsername(), in.getPassword());
            //递归读取
            return read();
        }
        return null;
    }

    public JdbcPagingItemReader<Map<String, Object>> tableDataReader(String url,
                                                String table,
                                                String username,
                                                String password) throws Exception {
        DataSource dataSource = DataSourceCache.getDataSource(url, username, password);
        PagingQueryProvider queryProvider = DataSourceCache.queryProvider(url, dataSource, table);
        //设置表名
        if(queryProvider instanceof MySqlPagingQueryProvider) {
            ((MySqlPagingQueryProvider)queryProvider).setFromClause(table);
        }
        JdbcPagingItemReader<Map<String, Object>> tableDataReader = new JdbcPagingItemReaderBuilder<Map<String, Object>>()
                .name("tableDataReader")
                .dataSource(dataSource)
                .queryProvider(queryProvider)
                .rowMapper(mapMapper())
                .pageSize(1000)
                .build();
        tableDataReader.afterPropertiesSet();
        return tableDataReader;
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

}
