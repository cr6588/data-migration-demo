package com.sjdf.demo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.support.AutomaticJobRegistrar;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Set;

@RestController
@RequestMapping(path="/api/dataBatch")
@Slf4j
public class MainController {

    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private Job directTransferJob;
    @Autowired
    private JobOperator jobOperator;

    /**
     * 数据库直连传输
     * @return
     */
    @PostMapping("/directTransfer")
    public String directTransfer(@RequestBody DirectTransferVo vo) {
        try {
            JobParameters param = new JobParametersBuilder()
                    .addString("table", vo.getTable())
                    .addString("in.url", vo.getIn().getUrl())
                    .addString("in.username", vo.getIn().getUsername())
                    .addString("in.password", vo.getIn().getPassword())
                    .addString("out.url", vo.getOut().getUrl())
                    .addString("out.username", vo.getOut().getUsername())
                    .addString("out.password", vo.getOut().getPassword())
                    .addDate("date", new Date())
                    .toJobParameters();
            jobLauncher.run(directTransferJob, param);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "ok";
    }


    @PostMapping("/stopJob")
    public String stopJob(String jobName) {
        Set<Long> executions = null;
        try {
            executions = jobOperator.getRunningExecutions(jobName);
            jobOperator.stop(executions.iterator().next());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return "stop error " + e.getMessage();
        }
        return "stop " + jobName;
    }
}
