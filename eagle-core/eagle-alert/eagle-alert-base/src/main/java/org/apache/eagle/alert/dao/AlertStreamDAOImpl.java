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
package org.apache.eagle.alert.dao;

import com.typesafe.config.Config;
import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.entity.AlertStreamEntity;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AlertStreamDAOImpl implements AlertStreamDAO{
    private final Logger LOG = LoggerFactory.getLogger(AlertStreamDAOImpl.class);
    private final EagleServiceConnector connector;

    public AlertStreamDAOImpl(EagleServiceConnector connector){
        this.connector = connector;
    }

    public List<AlertStreamEntity> findAlertStreamByDataSource(String dataSource) throws Exception{
        try {
            IEagleServiceClient client = new EagleServiceClientImpl(connector);
            String query = AlertConstants.ALERT_STREAM_SERVICE_ENDPOINT_NAME + "[@dataSource=\"" + dataSource + "\"]{*}";
            GenericServiceAPIResponseEntity<AlertStreamEntity> response =  client.search()
                    .startTime(0)
                    .endTime(10 * DateUtils.MILLIS_PER_DAY)
                    .pageSize(Integer.MAX_VALUE)
                    .query(query)
                    .send();
            client.close();
            if (response.getException() != null) {
                throw new Exception("Got an exception when query eagle service: " + response.getException());
            }
            return response.getObj();
        }
        catch (Exception ex) {
            LOG.error("Got an exception when query stream metadata service ", ex);
            throw new IllegalStateException(ex);
        }
    }
}
