package com.sjdf.demo;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping(path="/demo") // This means URL's start with /demo (after Application path)
public class MainController {

	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	private Job importUserJob;
	@Autowired
	private Job exportTableJob;
	@Autowired
	private Job importTableJob;
	@Autowired
	private JdbcTemplate jdbcTemplate;

	@GetMapping("/startImportJob")
	public String startImportJob() {
		try {
			JobParameters param = new JobParametersBuilder()
					.toJobParameters();
			jobLauncher.run(importUserJob, param);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "ok";
	}

	@PostMapping("/startExportJob")
	public String startExportJob(@RequestParam("url") String url,
								 @RequestParam("table") String table,
								 @RequestParam("username") String username,
								 @RequestParam("password") String password) {
		try {
			JobParameters param = new JobParametersBuilder()
				.addString("url", url)
				.addString("table", table)
				.addString("username", username)
				.addString("password", password)
				.addDate("date", new Date())
				.toJobParameters();
			jobLauncher.run(exportTableJob, param);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "ok";
	}

	@PostMapping("/startImportTableJob")
	public String startImportTableJob(@RequestParam("url") String url,
								 @RequestParam("table") String table,
								 @RequestParam("username") String username,
								 @RequestParam("password") String password) {
		try {
			JobParameters param = new JobParametersBuilder()
					.addString("url", url)
					.addString("table", table)
					.addString("username", username)
					.addString("password", password)
					.addDate("date", new Date())
					.toJobParameters();
			jobLauncher.run(importTableJob, param);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "ok";
	}

	@GetMapping("/table")
	public String getTable() {
		List<Map<String, Object>> list = jdbcTemplate.queryForList("show full tables");
		List<String> tables = list.stream().map(r -> (String)r.get("Tables_in_test")).collect(Collectors.toList());
		return tables.toString();
	}
}
