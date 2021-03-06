/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.datastream

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.eagle.datastream.storm.StormExecutionEnvironment

object testStreamUnionExpansion extends App{
  val config : Config = ConfigFactory.load
  val env = new StormExecutionEnvironment(config)
  val tail1 = env.fromSpout(TestSpout()).flatMap(WordPrependForAlertExecutor("test")).map2(a => ("key1",a))
  val tail2 = env.fromSpout(TestSpout()).flatMap(WordAppendForAlertExecutor("test")).map2(a => ("key1",a))
  tail1.streamUnion(List(tail2)).map1(a => "xyz")
}

object testStreamGroupbyExpansion extends App{
  val config : Config = ConfigFactory.load;
  val env = new StormExecutionEnvironment(config)
  env.fromSpout(TestSpout()).flatMap(WordPrependForAlertExecutor("test")).groupBy(1).map2(a => ("key1",a))
  //env.execute
}

object testStreamUnionAndGroupbyExpansion extends App{
  val config : Config = ConfigFactory.load;
  val env = new StormExecutionEnvironment(config)
  val tail1 = env.fromSpout(TestSpout()).flatMap(WordPrependForAlertExecutor("test")).map2(a => ("key1",a)).groupBy(1)
  val tail2 = env.fromSpout(TestSpout()).flatMap(WordAppendForAlertExecutor("test")).map2(a => ("key1",a)).groupBy(0)
  tail1.streamUnion(List(tail2)).map1(a => "xyz")
//  env.execute()
}

/**
 * 1. stream schema
 * curl -X POST -H 'Content-Type:application/json' "http://localhost:38080/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" -d '[{"prefix":"alertStreamSchema","tags":{"dataSource":"ds1","streamName":"s1","attrName":"word"},"attrDescription":"word","attrType":"string","category":"","attrValueResolver":""}]'
 * 2. policy
 * curl -X POST -H 'Content-Type:application/json' "http://localhost:38080/eagle-service/rest/entities?serviceName=AlertDefinitionService" -d '[{"tags":{"site":"sandbox","dataSource":"ds1","alertExecutorId":"alert1","policyId":"testAlert","policyType":"siddhiCEPEngine"},"desc":"test alert","policyDef":"{\"type\":\"siddhiCEPEngine\",\"expression\":\"from s1 [(str:regexp(word,'\'.*test.*==true)] select * insert into outputStream ;\"}","dedupeDef":"","notificationDef":"","remediationDef":"","enabled":"true"}]'
 */
object testAlertExpansion extends App{
  val config : Config = ConfigFactory.load;
  val env = new StormExecutionEnvironment(config)
  val tail1 = env.fromSpout(TestSpout()).nameAs("testSpout1")
                  .flatMap(WordPrependForAlertExecutor("test")).nameAs("prepend")
                  .alertWithConsumer("s1", "alert1")
  //env.execute
}

/**
 * 1. stream schema
 * curl -X POST -H 'Content-Type:application/json' "http://localhost:38080/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" -d '[{"prefix":"alertStreamSchema","tags":{"dataSource":"ds1","streamName":"s1","attrName":"word"},"attrDescription":"word","attrType":"string","category":"","attrValueResolver":""}]'
 * curl -X POST -H 'Content-Type:application/json' "http://localhost:38080/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" -d '[{"prefix":"alertStreamSchema","tags":{"dataSource":"ds1","streamName":"s2","attrName":"word"},"attrDescription":"word","attrType":"string","category":"","attrValueResolver":""}]'
 * 2. policy
 * curl -X POST -H 'Content-Type:application/json' "http://localhost:38080/eagle-service/rest/entities?serviceName=AlertDefinitionService" -d '[{"tags":{"site":"sandbox","dataSource":"ds1","alertExecutorId":"alert1","policyId":"testAlert","policyType":"siddhiCEPEngine"},"desc":"test alert","policyDef":"{\"type\":\"siddhiCEPEngine\",\"expression\":\"from s1 [(str:regexp(word,'\'.*test.*\)==true)] select * insert into outputStream ;\"}","dedupeDef":"","notificationDef":"","remediationDef":"","enabled":"true"}]'
 */
object testAlertExpansionWithUnion extends App{
  val config : Config = ConfigFactory.load;
  val env = new StormExecutionEnvironment(config)
  val tail1 = env.fromSpout(TestSpout()).nameAs("testSpout1").flatMap(WordPrependForAlertExecutor("test")).nameAs("prepend") //.map2(a => ("key1",a))
  val tail2 = env.fromSpout(TestSpout()).flatMap(WordAppendForAlertExecutor("test")) //.map2(a => ("key1",a))
  tail1.streamUnion(List(tail2)).alert(Seq("s1","s2"), "alert1", consume = true)
  //env.execute
}


object testStreamUnionExpansionWithSharedSpout extends App{
  val config : Config = ConfigFactory.load;
  val env = new StormExecutionEnvironment(config)
  val source = env.fromSpout(TestSpout())
  val tail1 = source.flatMap(WordPrependForAlertExecutor("test")).map2(a => ("key1",a))
  val tail2 = source.flatMap(WordAppendForAlertExecutor("test")).map2(a => ("key1",a))
  tail1.streamUnion(List(tail2)).map1(a  => {
    println(a)
    "xyz"
  })
//    env.execute
}

object testStreamUnionExpansionWithSharedSpout_2 extends App{
  val config : Config = ConfigFactory.load;
  val env = new StormExecutionEnvironment(config)
  val source = env.fromSpout(TestSpout())
  val tail1 = source.flatMap(WordPrependForAlertExecutor("test")).map2(a => ("key1",a))
  source.streamUnion(List(tail1)).map1(a  => {
    println(a)
    "xyz"
  })
//  env.execute
}
