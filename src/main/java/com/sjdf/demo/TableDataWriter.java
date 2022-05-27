package com.sjdf.demo;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.List;

public class TableDataWriter implements ItemWriter<String> {

    public TableDataWriter(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    private JdbcTemplate jdbcTemplate;

    //setp中设置跳过主键重复异常后，当批量插入报出该异常后，会自动单个传入再试一次
    @Override
    public void write(List<? extends String> list) throws Exception {
        String[] arrays = new String[list.size()];
        list.toArray(arrays);
        jdbcTemplate.batchUpdate(arrays);
    }
}
