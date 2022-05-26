package com.sjdf.demo;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;

public class MapItemProcessor implements ItemProcessor<Map<String, Object>, String> {

    @Value("#{jobParameters['table']}")
    private String table;

    /**
     * 将reader中读取转成的map,转化成标准的sql语句
     * @param map
     * @return
     * @throws Exception
     */
    @Override
    public String process(final Map<String, Object> map) throws Exception {
//        INSERT INTO `sys_language` VALUES('1', 'en_US', 'English(US)');
//        INSERT INTO `sys_i18n`(`code`, `text`, `language`)
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO `" + table + "`(");
        for (String o : map.keySet()) {
            sb.append("`" + o + "`, ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append(") VALUES (");
        for (String o : map.keySet()) {
            Object v = map.get(o);
            //TODO 值当中有引号时待处理
            if(v instanceof String) {
                sb.append("'" + v + "', ");
            } else {
                sb.append(v + ", ");
            }
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append(");");
        return sb.toString();
    }

}
