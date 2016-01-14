/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * 
 */
package org.apache.eagle.jobrunning.crawler;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipException;

import org.apache.eagle.jobrunning.config.RunningJobCrawlConfig;
import org.apache.eagle.jobrunning.counter.parser.JobCountersParser;
import org.apache.eagle.jobrunning.counter.parser.JobCountersParserImpl;
import org.apache.eagle.jobrunning.ha.HAURLSelector;
import org.apache.eagle.jobrunning.ha.ResourceManagerHAURLSelectorImpl;
import org.apache.eagle.jobrunning.job.conf.JobConfParser;
import org.apache.eagle.jobrunning.job.conf.JobConfParserImpl;
import org.apache.eagle.jobrunning.url.*;
import org.apache.eagle.jobrunning.util.InputStreamUtils;
import org.apache.eagle.jobrunning.util.JobUtils;
import org.apache.eagle.jobrunning.util.URLConnectionUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.eagle.jobrunning.common.JobConstants;
import org.apache.eagle.jobrunning.yarn.model.AppWrapper;
import org.apache.eagle.jobrunning.yarn.model.JobCompleteWrapper;
import org.apache.eagle.jobrunning.yarn.model.JobCountersWrapper;
import org.apache.eagle.jobrunning.yarn.model.JobsWrapper;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.jobrunning.yarn.model.AppInfo;
import org.apache.eagle.jobrunning.yarn.model.AppsWrapper;
import org.apache.eagle.jobrunning.yarn.model.JobDetailInfo;
import org.apache.eagle.jobrunning.common.JobConstants.ResourceType;

public class RMResourceFetcher implements ResourceFetcher{
	
	private static final Logger LOG = LoggerFactory.getLogger(RMResourceFetcher.class);
	private final HAURLSelector selector;
	private final String historyBaseUrl;
	private static final int CONNECTION_TIMEOUT = 10000;
	private static final int READ_TIMEOUT = 10000;
	private static final String XML_HTTP_HEADER = "Accept";
	private static final String XML_FORMAT = "application/xml";
	
	private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();
	
	static {
		OBJ_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
	}
	
	public RMResourceFetcher(RunningJobCrawlConfig.RunningJobEndpointConfig config) {
		this.selector = new ResourceManagerHAURLSelectorImpl(config.RMBasePaths, JobConstants.CompressionType.GZIP);
		this.historyBaseUrl = config.HSBasePath;
	}
	
	private void checkUrl() throws IOException {
		JobListServiceURLBuilderImpl jobListServiceURLBuilder = new JobListServiceURLBuilderImpl();
		String url = jobListServiceURLBuilder.RMBaseUrl(selector.getSelectedUrl())
				     	                     .JobState(JobConstants.JobState.RUNNING.name())
										     .build();
		if (!selector.checkUrl(url)) {
			selector.reSelectUrl();
		}
	}
	
	private List<Object> doFetchApplicationsList(String state) throws Exception {		
		List<AppInfo> result = null;
		InputStream is = null;
		try {
			checkUrl();
			JobListServiceURLBuilderImpl jobListServiceURLBuilder = new JobListServiceURLBuilderImpl();
			String url = jobListServiceURLBuilder.RMBaseUrl(selector.getSelectedUrl())
					.JobState(state)
					.build();
			LOG.info("Going to call yarn api to fetch running job list: " + url);
			is = InputStreamUtils.getInputStream(url, JobConstants.CompressionType.GZIP);
			final AppsWrapper appWrapper = OBJ_MAPPER.readValue(is, AppsWrapper.class);
			if (appWrapper != null && appWrapper.getApps() != null
					&& appWrapper.getApps().getApp() != null) {
				result = appWrapper.getApps().getApp();
				return Arrays.asList((Object)result);
			}
			return null;
		}
		finally {
			if (is != null) { try {is.close();} catch (Exception e){} }
		}
	}
	
	private List<Object> doFetchRunningJobInfo(String appID) throws Exception{
		InputStream is = null;
		InputStream is2 = null;
		try {
			JobDetailServiceURLBuilderImpl jobDetailServiceURLBuilder = new JobDetailServiceURLBuilderImpl();
			String urlString = jobDetailServiceURLBuilder.RMBaseUrl(selector.getSelectedUrl())
														 .AppID(appID)
														 .build();
			LOG.info("Going to fetch job detail information for " + appID + " , url: " + urlString);
			try {
				is = InputStreamUtils.getInputStream(urlString, JobConstants.CompressionType.GZIP);
			}
			catch (ZipException ex) {
				// Here if job already completed, it will be redirected to job history page and throw java.util.zip.ZipException
				LOG.info(appID + " has finished, skip this job");
				return null;
			}
			final JobsWrapper jobWrapper = OBJ_MAPPER.readValue(is, JobsWrapper.class);
			JobDetailInfo jobDetail = null;
			if (jobWrapper != null && jobWrapper.getJobs() != null && jobWrapper.getJobs().getJob() != null
				&& jobWrapper.getJobs().getJob().size() > 0) {
				jobDetail = jobWrapper.getJobs().getJob().get(0);
			}

			JobCountersServiceURLBuilderImpl jobCounterServiceURLBuilder = new JobCountersServiceURLBuilderImpl();
			String urlString2 = jobCounterServiceURLBuilder.RMBaseUrl(selector.getSelectedUrl())
														   .AppID(appID)
														   .build();
			LOG.info("Going to fetch job counters for application " + appID + " , url: " + urlString2);
			is2 = InputStreamUtils.getInputStream(urlString2, JobConstants.CompressionType.GZIP);
			final JobCountersWrapper jobCounterWrapper = OBJ_MAPPER.readValue(is2,JobCountersWrapper.class);
			
			return Arrays.asList(jobDetail, jobCounterWrapper);
		}
		finally {
			if (is != null) { try {is.close();} catch (Exception e){} }
			if (is2 != null) { try {is2.close();} catch (Exception e){} }
		}
	}
	
	private List<Object> doFetchCompleteJobInfo(String appId) throws Exception{
		InputStream is = null;
		InputStream is2 = null;
		try {
			checkUrl();
			String jobID = JobUtils.getJobIDByAppID(appId);
			JobCompleteDetailServiceURLBuilderImpl jobCompleteDetailServiceURLBuilder = new JobCompleteDetailServiceURLBuilderImpl();
			String urlString = jobCompleteDetailServiceURLBuilder.RMBaseUrl(selector.getSelectedUrl())
															     .JobID(jobID)
																 .build();
			LOG.info("Going to fetch job completed information for " + jobID + " , url: " + urlString);
			is = InputStreamUtils.getInputStream(urlString, JobConstants.CompressionType.GZIP);
			final JobCompleteWrapper jobWrapper = OBJ_MAPPER.readValue(is, JobCompleteWrapper.class);

			JobCompleteCounterServiceURLBuilderImpl jobCompleteCounterServiceURLBuilder = new JobCompleteCounterServiceURLBuilderImpl();
			String urlString2 = jobCompleteCounterServiceURLBuilder.HistoryBaseUrl(historyBaseUrl)
																  .JobID(jobID)
																  .build();
			LOG.info("Going to fetch job completed counters for " + jobID + " , url: " + urlString2);
			is2 = InputStreamUtils.getInputStream(urlString2, JobConstants.CompressionType.NONE, (int) (2 * DateUtils.MILLIS_PER_MINUTE));
			final Document doc = Jsoup.parse(is2, StandardCharsets.UTF_8.name(), urlString2);
			JobCountersParser parser = new JobCountersParserImpl();
			Map<String, Long> counters = parser.parse(doc);
			return Arrays.asList(jobWrapper, counters);
		}
		finally {
			if (is != null) { try {is.close();} catch (Exception e){}  }
			if (is2 != null) { try {is2.close();} catch (Exception e){}  }
		}
	}
	
	private List<Object> doFetchRunningJobConfiguration(String appID) throws Exception {
		InputStream is = null;
		try {
			checkUrl();
			String jobID = JobUtils.getJobIDByAppID(appID);
			JobRunningConfigServiceURLBuilderImpl jobRunningConfigServiceURLBuilder = new JobRunningConfigServiceURLBuilderImpl();
			String urlString = jobRunningConfigServiceURLBuilder.RMBaseUrl(selector.getSelectedUrl())
																.JobID(jobID)
																.build();
			LOG.info("Going to fetch job completed information for " + jobID + " , url: " + urlString);
			final URLConnection connection = URLConnectionUtils.getConnection(urlString);
			connection.setRequestProperty(XML_HTTP_HEADER, XML_FORMAT);
			connection.setConnectTimeout(CONNECTION_TIMEOUT);
			connection.setReadTimeout(READ_TIMEOUT);
			is = connection.getInputStream();
			Map<String, String> configs = XmlHelper.getConfigs(is);
			return Arrays.asList((Object)configs);
		}
		finally {
			if (is != null) { try {is.close();} catch (Exception e){}  }
		}
	}
	
	private List<Object> doFetchCompletedJobConfiguration(String appID) throws Exception {
		InputStream is = null;
		try {
			JobCompletedConfigServiceURLBuilderImpl jobCompletedConfigServiceURLBuilder = new JobCompletedConfigServiceURLBuilderImpl();
			String urlString = jobCompletedConfigServiceURLBuilder.HistoryBaseUrl(historyBaseUrl)
																.JobID(JobUtils.getJobIDByAppID(appID))
																.build();

			is = InputStreamUtils.getInputStream(urlString, JobConstants.CompressionType.NONE);
			final Document doc = Jsoup.parse(is, "UTF-8", urlString);
			JobConfParser parser = new JobConfParserImpl();
			Map<String, String> configs = parser.parse(doc);
			return Arrays.asList((Object)configs);
		}
		finally {
			if (is != null) { try {is.close();} catch (Exception e){}  }
		}
	}
	
	public boolean checkIfJobIsRunning(String appID) throws Exception{
		InputStream is = null;
		try {
			checkUrl();
			JobStatusServiceURLBuilderImpl jobStatusServiceURLBuilder = new JobStatusServiceURLBuilderImpl();
			final String urlString = jobStatusServiceURLBuilder.RMBaseUrl(selector.getSelectedUrl())
					                                           .AppID(appID)
															   .build();
			LOG.info("Going to call yarn api to fetch job status: " + urlString);
			is = InputStreamUtils.getInputStream(urlString, JobConstants.CompressionType.GZIP);
			final AppWrapper appWrapper = OBJ_MAPPER.readValue(is, AppWrapper.class);
			if (appWrapper != null && appWrapper.getApp() != null) {
				AppInfo result = appWrapper.getApp();
				if (result.getState().equals(JobConstants.JOB_STATE_RUNNING)) {
					return true;
				}
				return false;
			}
			else {
				LOG.error("The status of " + appID + " is not available");
				throw new IllegalStateException("The status of " + appID + " is not available");
			}
		}
		finally {
			if (is != null) { try {is.close();} catch (Exception e){}  }
		}
	}

    public List<Object> getResource(String resourceType, String resourceId) throws Exception{
	    if (resourceType.equals(ResourceType.MR_JOB_LIST.name())) {
			return doFetchApplicationsList(resourceId);
		} else if (resourceType.equals(ResourceType.MR_JOB_RUNNING_INFO.name())){
			return doFetchRunningJobInfo(resourceId);
	    } else if (resourceType.equals(ResourceType.MR_JOB_COMPLETE_INFO.name())){
			return doFetchCompleteJobInfo(resourceId);
		} else if (resourceType.equals(ResourceType.MR_JOB_CONFIGURATION.name())) {
			boolean isRunning = checkIfJobIsRunning(resourceId);
			if (isRunning)
				return doFetchRunningJobConfiguration(resourceId);
			else
				return doFetchCompletedJobConfiguration(resourceId);
		} else throw new Exception("Not supported resourceType :" + resourceType);
    }
}
