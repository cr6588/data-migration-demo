package com.sjdf.demo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class TableDataWriter implements ItemWriter<TableData> {

    public TableDataWriter(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    private JdbcTemplate jdbcTemplate;

    //setp中设置跳过主键重复异常后，当批量插入报出该异常后，会自动单个传入再试一次
    @Override
    public void write(List<? extends TableData> list) throws Exception {
        Map<String, List<TableData>> tableNameList = list.stream().collect(
                Collectors.groupingBy(r -> r.getTable()));
        for (Map.Entry<String, List<TableData>> entry : tableNameList.entrySet()) {
            String sql = getBatchSql(entry.getValue());
            long start = System.currentTimeMillis();
            jdbcTemplate.execute(sql);
            log.info("batchUpdate Millis = {}", System.currentTimeMillis() - start);
        }
    }

    public String getBatchSql(List<TableData> list) {
        TableData first = list.get(0);
        String table = first.getTable();

//        INSERT INTO `sys_i18n`(`code`, `text`, `language`) VALUES
//                ('message.token.repeat.invalid', '您的请求正在处理，请勿重复提交!', 'zh_CN'),
//                ('message.token.repeat.invalid', 'Your request is being processed, please do not repeat!', 'en_US'),
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO `" + table + "`(");
        Set<String> keys = first.getData().keySet();
        for (String o : keys) {
            sb.append("`" + o + "`, ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append(") VALUES ");
        for (TableData tableData : list) {
            sb.append("(");
            Map<String, Object> data = tableData.getData();
            for (String o : keys) {
                Object v = data.get(o);
                if(v instanceof String) {
                    //值当中有引号时加入\转义
                    String val = ((String) v);
                    val = val.replace("'", "\\'");
                    sb.append("'" + val + "', ");
                } else {
                    sb.append(v + ", ");
                }
            }
            sb.delete(sb.length() - 2, sb.length());
            sb.append("),");
        }
        sb.delete(sb.length() - 1, sb.length());
        sb.append(";");
        return sb.toString();
    }
}
