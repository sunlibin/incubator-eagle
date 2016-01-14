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
package org.apache.eagle.jobrunning.url;

import org.apache.eagle.jobrunning.common.JobConstants;
import org.apache.eagle.jobrunning.util.JobUtils;

public class JobCountersServiceURLBuilderImpl implements ServiceURLBuilder {

	private String rmBaseUrl;
	private String appID;

	public JobCountersServiceURLBuilderImpl RMBaseUrl(String rmBaseUrl) {
		this.rmBaseUrl = rmBaseUrl;
		return this;
	}

	public JobCountersServiceURLBuilderImpl AppID(String appID) {
		this.appID = appID;
		return this;
	}
	
	public String build() {
		// {rmUrl}/proxy/application_xxxxxxxxxxxxx_xxxxxx/ws/v1/mapreduce/jobs/job_xxxxxxxxxxxxx_xxxxxx/counters?anonymous=true
		return rmBaseUrl + JobConstants.V2_PROXY_PREFIX_URL + appID + JobConstants.V2_MR_APPMASTER_PREFIX +
			   JobUtils.getJobIDByAppID(appID) + JobConstants.V2_MR_COUNTERS_URL + "?" + JobConstants.ANONYMOUS_PARAMETER;
	}
}
