/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test.functional;

import java.io.IOException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;

public class ConfigurableMacIT extends AbstractMacIT {
  public static final Logger log = Logger.getLogger(ConfigurableMacIT.class);

  public MiniAccumuloCluster cluster;

  public void configure(MiniAccumuloConfig cfg) {}

  @Before
  public void setUp() throws Exception {
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(createTestDir(this.getClass().getName()), ROOT_PASSWORD);
    configure(cfg);
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
  }

  @After
  public void tearDown() throws Exception {
    cleanUp(cluster);
  }

  public MiniAccumuloCluster getCluster() {
    return cluster;
  }

  @Override
  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    return getCluster().getConnector("root", ROOT_PASSWORD);
  }

  public Process exec(Class<? extends Object> clazz, String... args) throws IOException {
    return getCluster().exec(clazz, args);
  }

  @Override
  public String rootPath() {
    return getCluster().getConfig().getDir().getAbsolutePath();
  }

  public String getMonitor() throws KeeperException, InterruptedException {
    Instance instance = new ZooKeeperInstance(getCluster().getInstanceName(), getCluster().getZooKeepers());
    ZooReader zr = new ZooReader(getCluster().getZooKeepers(), 5000);
    return new String(zr.getData(ZooUtil.getRoot(instance) + Constants.ZMONITOR, null));
  }

}
