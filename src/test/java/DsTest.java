import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.FormatterLineAggregator;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DsTest {

    @Test
    public void name() {
//        spring.datasource.url=jdbc:mysql://192.168.1.206:31111/test
//        spring.datasource.username=root
//        spring.datasource.password=tTdAdf2129
        String url = "jdbc:mysql://192.168.1.206:31111/erp";
        String username = "root";
        String password = "tTdAdf2129";
        DruidDataSource userDb = new DruidDataSource();
        userDb.setDriverClassName("com.mysql.cj.jdbc.Driver");
        userDb.setUrl(url + "?useUnicode=true&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true");
        userDb.setUsername(username);
        userDb.setPassword(password);
        userDb.setInitialSize(0);
        userDb.setMinIdle(0);
        userDb.setMaxActive(100);
        userDb.setMaxWait(60000);
        userDb.setTimeBetweenEvictionRunsMillis(6000);
        userDb.setMinEvictableIdleTimeMillis(6000);
        userDb.setValidationQuery("SELECT 'x'");
        userDb.setTestWhileIdle(true);
        userDb.setTestOnBorrow(false);
        userDb.setTestOnReturn(false);
        userDb.setPoolPreparedStatements(false);
        userDb.setMaxPoolPreparedStatementPerConnectionSize(20);
        try {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(userDb);
            List<String> maps = jdbcTemplate.query("show tables;", (rs, i) -> {
                return rs.getString(1);
            });
            System.out.println(maps.toString());
            while(true) {
                System.out.println(userDb.getPoolingCount());
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    class Name {
        private String first;
        private String last;
        private int born;

        public String getFirst() {
            return first;
        }

        public void setFirst(String first) {
            this.first = first;
        }

        public String getLast() {
            return last;
        }

        public void setLast(String last) {
            this.last = last;
        }

        public int getBorn() {
            return born;
        }

        public void setBorn(int born) {
            this.born = born;
        }

        public Name(String first, String last, int born) {
            this.first = first;
            this.last = last;
            this.born = born;
        }
    }
    @Test
    public void tttt() {
        BeanWrapperFieldExtractor<Name> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(new String[] { "first", "last", "born" });

        String first = "Alan";
        String last = "Turing";
        int born = 1912;

        Name n = new Name(first, last, born);
        Object[] values = extractor.extract(n);

        Assert.assertEquals(first, values[0]);
        Assert.assertEquals(last, values[1]);
        Assert.assertEquals(born, values[2]);

        BeanWrapperFieldExtractor<Name> fieldExtractor = new BeanWrapperFieldExtractor<>();
        FormatterLineAggregator<Map<String, Object>> lineAggregator = new FormatterLineAggregator<>();
        lineAggregator.setFormat("INSERT INTO `sys_language` VALUES('1', 'en_US', 'English(US)');");
//        lineAggregator.setFieldExtractor(fieldExtractor);
    }
}
