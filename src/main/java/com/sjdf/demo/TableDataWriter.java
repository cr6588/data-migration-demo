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

    @Override
    public void write(List<? extends String> list) throws Exception {
        for (String sql : list) {
            jdbcTemplate.execute((java.lang.String) sql);
        }
    }
}
