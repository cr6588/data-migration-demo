package com.sjdf.demo;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;

public class TableDataItemProcessor implements ItemProcessor<TableData, String> {

    /**
     * 将reader中读取转成的map,转化成标准的sql语句
     * @param
     * @return
     * @throws Exception
     */
    @Override
    public String process(final TableData tableData) throws Exception {
        String table = tableData.getTable();
        Map<String, Object> data = tableData.getData();
//        INSERT INTO `sys_language` VALUES('1', 'en_US', 'English(US)');
//        INSERT INTO `sys_i18n`(`code`, `text`, `language`)
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO `" + table + "`(");
        for (String o : data.keySet()) {
            sb.append("`" + o + "`, ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append(") VALUES (");
        for (String o : data.keySet()) {
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
        sb.append(");");
        return sb.toString();
    }

}
