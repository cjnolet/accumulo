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
package org.apache.accumulo.minicluster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.gc.SimpleGarbageCollector;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.master.state.SetGoalState;
import org.apache.accumulo.server.tabletserver.TabletServer;
import org.apache.accumulo.server.util.Initialize;
import org.apache.accumulo.server.util.PortUtils;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.start.Main;
import org.apache.accumulo.start.classloader.vfs.MiniDFSUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.zookeeper.server.ZooKeeperServerMain;

/**
 * A utility class that will create Zookeeper and Accumulo processes that write all of their data to a single local directory. This class makes it easy to test
 * code against a real Accumulo instance. Its much more accurate for testing than {@link org.apache.accumulo.core.client.mock.MockAccumulo}, but much slower.
 * 
 * @since 1.5.0
 */
public class MiniAccumuloCluster {

  public static class LogWriter extends Daemon {
    private BufferedReader in;
    private BufferedWriter out;

    public LogWriter(InputStream stream, File logFile) throws IOException {
      this.in = new BufferedReader(new InputStreamReader(stream));
      out = new BufferedWriter(new FileWriter(logFile));

      SimpleTimer.getInstance().schedule(new Runnable() {
        @Override
        public void run() {
          try {
            flush();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }, 1000, 1000);
    }

    public synchronized void flush() throws IOException {
      if (out != null)
        out.flush();
    }

    @Override
    public void run() {
      String line;

      try {
        while ((line = in.readLine()) != null) {
          out.append(line);
          out.append("\n");
        }

        synchronized (this) {
          out.close();
          out = null;
          in.close();
        }

      } catch (IOException e) {}
    }
  }

  private boolean initialized = false;
  private Process zooKeeperProcess = null;
  private Process masterProcess = null;
  private Process gcProcess = null;
  private List<Process> tabletServerProcesses = new ArrayList<Process>();

  private Set<Pair<ServerType,Integer>> debugPorts = new HashSet<Pair<ServerType,Integer>>();

  private File zooCfgFile;
  private String dfsUri;

  public List<LogWriter> getLogWriters() {
    return logWriters;
  }

  private List<LogWriter> logWriters = new ArrayList<MiniAccumuloCluster.LogWriter>();

  private MiniAccumuloConfig config;
  private MiniDFSCluster miniDFS = null;
  private List<Process> cleanup = new ArrayList<Process>();

  public Process exec(Class<? extends Object> clazz, String... args) throws IOException {
    Process proc = exec(clazz, Collections.singletonList("-Xmx" + config.getDefaultMemory()), args);
    cleanup.add(proc);
    return proc;
  }

  private boolean containsSiteFile(File f) {
    return f.isDirectory() && f.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith("site.xml");
      }
    }).length > 0;
  }

  private void append(StringBuilder classpathBuilder, URL url) throws URISyntaxException {
    File file = new File(url.toURI());
    // do not include dirs containing hadoop or accumulo site files
    if (!containsSiteFile(file))
      classpathBuilder.append(File.pathSeparator).append(file.getAbsolutePath());
  }

  private String getClasspath() throws IOException {

    try {
      ArrayList<ClassLoader> classloaders = new ArrayList<ClassLoader>();

      ClassLoader cl = this.getClass().getClassLoader();

      while (cl != null) {
        classloaders.add(cl);
        cl = cl.getParent();
      }

      Collections.reverse(classloaders);

      StringBuilder classpathBuilder = new StringBuilder();
      classpathBuilder.append(config.getConfDir().getAbsolutePath());

      if (config.getClasspathItems() == null) {

        // assume 0 is the system classloader and skip it
        for (int i = 1; i < classloaders.size(); i++) {
          ClassLoader classLoader = classloaders.get(i);

          if (classLoader instanceof URLClassLoader) {

            URLClassLoader ucl = (URLClassLoader) classLoader;

            for (URL u : ucl.getURLs()) {
              append(classpathBuilder, u);
            }

          } else if (classLoader instanceof VFSClassLoader) {

            VFSClassLoader vcl = (VFSClassLoader) classLoader;
            for (FileObject f : vcl.getFileObjects()) {
              append(classpathBuilder, f.getURL());
            }
          } else {
            throw new IllegalArgumentException("Unknown classloader type : " + classLoader.getClass().getName());
          }
        }
      } else {
        for (String s : config.getClasspathItems())
          classpathBuilder.append(File.pathSeparator).append(s);
      }

      return classpathBuilder.toString();

    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  private Process exec(Class<? extends Object> clazz, List<String> extraJvmOpts, String... args) throws IOException {
    String javaHome = System.getProperty("java.home");
    String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
    String classpath = getClasspath();

    String className = clazz.getName();

    ArrayList<String> argList = new ArrayList<String>();
    argList.addAll(Arrays.asList(javaBin, "-Dproc=" + clazz.getSimpleName(), "-cp", classpath));
    argList.add("-Djava.library.path=" + config.getLibDir());
    argList.addAll(extraJvmOpts);
    argList.addAll(Arrays.asList("-XX:+UseConcMarkSweepGC", "-XX:CMSInitiatingOccupancyFraction=75", Main.class.getName(), className));
    argList.addAll(Arrays.asList(args));

    ProcessBuilder builder = new ProcessBuilder(argList);

    builder.environment().put("ACCUMULO_HOME", config.getDir().getAbsolutePath());
    builder.environment().put("ACCUMULO_LOG_DIR", config.getLogDir().getAbsolutePath());
    builder.environment().put("ACCUMULO_CONF_DIR", config.getConfDir().getAbsolutePath());

    Process process = builder.start();

    LogWriter lw;
    lw = new LogWriter(process.getErrorStream(), new File(config.getLogDir(), clazz.getSimpleName() + "_" + process.hashCode() + ".err"));
    logWriters.add(lw);
    lw.start();
    lw = new LogWriter(process.getInputStream(), new File(config.getLogDir(), clazz.getSimpleName() + "_" + process.hashCode() + ".out"));
    logWriters.add(lw);
    lw.start();

    return process;
  }

  private Process exec(Class<? extends Object> clazz, ServerType serverType, String... args) throws IOException {

    List<String> jvmOpts = new ArrayList<String>();
    jvmOpts.add("-Xmx" + config.getMemory(serverType));

    if (config.isJDWPEnabled()) {
      Integer port = PortUtils.getRandomFreePort();
      jvmOpts.addAll(buildRemoteDebugParams(port));
      debugPorts.add(new Pair<ServerType,Integer>(serverType, port));
    }
    return exec(clazz, jvmOpts, args);
  }

  /**
   * 
   * @param dir
   *          An empty or nonexistant temp directoy that Accumulo and Zookeeper can store data in. Creating the directory is left to the user. Java 7, Guava,
   *          and Junit provide methods for creating temporary directories.
   * @param rootPassword
   *          Initial root password for instance.
   */
  public MiniAccumuloCluster(File dir, String rootPassword) throws IOException {
    this(new MiniAccumuloConfig(dir, rootPassword));
  }

  /**
   * @param config
   *          initial configuration
   */
  public MiniAccumuloCluster(MiniAccumuloConfig config) throws IOException {

    this.config = config.initialize();

    config.getConfDir().mkdirs();
    config.getAccumuloDir().mkdirs();
    config.getZooKeeperDir().mkdirs();
    config.getLogDir().mkdirs();
    config.getWalogDir().mkdirs();
    config.getLibDir().mkdirs();

    if (config.useMiniDFS()) {
      File nn = new File(config.getAccumuloDir(), "nn");
      nn.mkdirs();
      File dn = new File(config.getAccumuloDir(), "dn");
      dn.mkdirs();
      File dfs = new File(config.getAccumuloDir(), "dfs");
      dfs.mkdirs();
      Configuration conf = new Configuration();
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nn.getAbsolutePath());
      conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dn.getAbsolutePath());
      conf.set(DFSConfigKeys.DFS_REPLICATION_KEY, "1");
      conf.set("dfs.support.append", "true");
      conf.set("dfs.datanode.synconclose", "true");
      conf.set("dfs.datanode.data.dir.perm", MiniDFSUtil.computeDatanodeDirectoryPermission());
      String oldTestBuildData = System.setProperty("test.build.data", dfs.getAbsolutePath());
      miniDFS = new MiniDFSCluster(conf, 1, true, null);
      if (oldTestBuildData == null)
        System.clearProperty("test.build.data");
      else
        System.setProperty("test.build.data", oldTestBuildData);
      miniDFS.waitClusterUp();
      InetSocketAddress dfsAddress = miniDFS.getNameNode().getNameNodeAddress();
      dfsUri = "hdfs://" + dfsAddress.getHostName() + ":" + dfsAddress.getPort();
      File coreFile = new File(config.getConfDir(), "core-site.xml");
      writeConfig(coreFile, Collections.singletonMap("fs.default.name", dfsUri).entrySet());
      File hdfsFile = new File(config.getConfDir(), "hdfs-site.xml");
      writeConfig(hdfsFile, conf);

      Map<String,String> siteConfig = config.getSiteConfig();
      siteConfig.put(Property.INSTANCE_DFS_URI.getKey(), dfsUri);
      siteConfig.put(Property.INSTANCE_DFS_DIR.getKey(), "/accumulo");
      config.setSiteConfig(siteConfig);
    } else {
      dfsUri = "file://";
    }

    File siteFile = new File(config.getConfDir(), "accumulo-site.xml");
    writeConfig(siteFile, config.getSiteConfig().entrySet());

    FileWriter fileWriter = new FileWriter(siteFile);
    fileWriter.append("<configuration>\n");

    for (Entry<String,String> entry : config.getSiteConfig().entrySet())
      fileWriter.append("<property><name>" + entry.getKey() + "</name><value>" + entry.getValue() + "</value></property>\n");
    fileWriter.append("</configuration>\n");
    fileWriter.close();

    zooCfgFile = new File(config.getConfDir(), "zoo.cfg");
    fileWriter = new FileWriter(zooCfgFile);

    // zookeeper uses Properties to read its config, so use that to write in order to properly escape things like Windows paths
    Properties zooCfg = new Properties();
    zooCfg.setProperty("tickTime", "2000");
    zooCfg.setProperty("initLimit", "10");
    zooCfg.setProperty("syncLimit", "5");
    zooCfg.setProperty("clientPort", config.getZooKeeperPort() + "");
    zooCfg.setProperty("maxClientCnxns", "1000");
    zooCfg.setProperty("dataDir", config.getZooKeeperDir().getAbsolutePath());
    zooCfg.store(fileWriter, null);

    fileWriter.close();

    File nativeMap = new File(config.getLibDir().getAbsolutePath() + "/native/map");
    nativeMap.mkdirs();
    File testRoot = new File(new File(new File(System.getProperty("user.dir")).getParent() + "/server/src/main/c++/nativeMap").getAbsolutePath());

    if (testRoot.exists()) {
      for (String file : testRoot.list()) {
        File src = new File(testRoot, file);
        if (src.isFile() && file.startsWith("libNativeMap"))
          FileUtils.copyFile(src, new File(nativeMap, file));
      }
    }
  }

  private void writeConfig(File file, Iterable<Map.Entry<String,String>> settings) throws IOException {
    FileWriter fileWriter = new FileWriter(file);
    fileWriter.append("<configuration>\n");

    for (Entry<String,String> entry : settings) {
      String value = entry.getValue().replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
      fileWriter.append("<property><name>" + entry.getKey() + "</name><value>" + value + "</value></property>\n");
    }
    fileWriter.append("</configuration>\n");
    fileWriter.close();
  }

  /**
   * Starts Accumulo and Zookeeper processes. Can only be called once.
   * 
   * @throws IllegalStateException
   *           if already started
   */
  public void start() throws IOException, InterruptedException {

    if (!initialized) {

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          try {
            MiniAccumuloCluster.this.stop();
          } catch (IOException e) {
            e.printStackTrace();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      });
    }

    if (zooKeeperProcess == null) {
      zooKeeperProcess = exec(ZooKeeperServerMain.class, ServerType.ZOOKEEPER, zooCfgFile.getAbsolutePath());
    }

    if (!initialized) {
      // sleep a little bit to let zookeeper come up before calling init, seems to work better
      while (true) {
        try {
          Socket s = new Socket("localhost", config.getZooKeeperPort());
          s.getOutputStream().write("ruok\n".getBytes());
          s.getOutputStream().flush();
          byte buffer[] = new byte[100];
          int n = s.getInputStream().read(buffer);
          if (n == 4 && new String(buffer, 0, n).equals("imok"))
            break;
        } catch (Exception e) {
          UtilWaitThread.sleep(250);
        }
      }
      Process initProcess = exec(Initialize.class, "--instance-name", config.getInstanceName(), "--password", config.getRootPassword());
      int ret = initProcess.waitFor();
      if (ret != 0) {
        throw new RuntimeException("Initialize process returned " + ret + ". Check the logs in " + config.getLogDir() + " for errors.");
      }
      initialized = true;
    }
    synchronized (tabletServerProcesses) {
      for (int i = tabletServerProcesses.size(); i < config.getNumTservers(); i++) {
        tabletServerProcesses.add(exec(TabletServer.class, ServerType.TABLET_SERVER));
      }
    }
    int ret = 0;
    for (int i = 0; i < 5; i++) {
      ret = exec(Main.class, SetGoalState.class.getName(), MasterGoalState.NORMAL.toString()).waitFor();
      if (ret == 0)
        break;
      UtilWaitThread.sleep(1000);
    }
    if (ret != 0) {
      throw new RuntimeException("Could not set master goal state, process returned " + ret + ". Check the logs in " + config.getLogDir() + " for errors.");
    }
    if (masterProcess == null) {
      masterProcess = exec(Master.class, ServerType.MASTER);
    }
    if (config.shouldRunGC()) {
      gcProcess = exec(SimpleGarbageCollector.class, ServerType.GARBAGE_COLLECTOR);
    }
  }

  private List<String> buildRemoteDebugParams(int port) {
    return Arrays.asList(new String[] {"-Xdebug", String.format("-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=%d", port)});
  }

  /**
   * @return generated remote debug ports if in debug mode.
   * @since 1.6.0
   */
  public Set<Pair<ServerType,Integer>> getDebugPorts() {
    return debugPorts;
  }

  List<ProcessReference> references(Process... procs) {
    List<ProcessReference> result = new ArrayList<ProcessReference>();
    for (Process proc : procs) {
      result.add(new ProcessReference(proc));
    }
    return result;
  }

  public Map<ServerType,Collection<ProcessReference>> getProcesses() {
    Map<ServerType,Collection<ProcessReference>> result = new HashMap<ServerType,Collection<ProcessReference>>();
    result.put(ServerType.MASTER, references(masterProcess));
    result.put(ServerType.TABLET_SERVER, references(tabletServerProcesses.toArray(new Process[0])));
    result.put(ServerType.ZOOKEEPER, references(zooKeeperProcess));
    if (null != gcProcess) {
      result.put(ServerType.GARBAGE_COLLECTOR, references(gcProcess));
    }
    return result;
  }

  public void killProcess(ServerType type, ProcessReference proc) throws ProcessNotFoundException, InterruptedException {
    boolean found = false;
    switch (type) {
      case MASTER:
        if (proc.equals(masterProcess)) {
          masterProcess.destroy();
          masterProcess = null;
          found = true;
        }
        break;
      case TABLET_SERVER:
        synchronized (tabletServerProcesses) {
          for (Process tserver : tabletServerProcesses) {
            if (proc.equals(tserver)) {
              tabletServerProcesses.remove(tserver);
              tserver.destroy();
              found = true;
              break;
            }
          }
        }
        break;
      case ZOOKEEPER:
        if (proc.equals(zooKeeperProcess)) {
          zooKeeperProcess.destroy();
          zooKeeperProcess = null;
          found = true;
        }
        break;
      case GARBAGE_COLLECTOR:
        if (proc.equals(gcProcess)) {
          gcProcess.destroy();
          gcProcess = null;
          found = true;
        }
        break;
    }
    if (!found)
      throw new ProcessNotFoundException();
  }

  /**
   * @return Accumulo instance name
   */
  public String getInstanceName() {
    return config.getInstanceName();
  }

  /**
   * @return zookeeper connection string
   */
  public String getZooKeepers() {
    return config.getZooKeepers();
  }

  /**
   * Stops Accumulo and Zookeeper processes. If stop is not called, there is a shutdown hook that is setup to kill the processes. However its probably best to
   * call stop in a finally block as soon as possible.
   */
  public void stop() throws IOException, InterruptedException {
    for (LogWriter lw : logWriters) {
      lw.flush();
    }

    if (zooKeeperProcess != null) {
      zooKeeperProcess.destroy();
    }
    if (masterProcess != null) {
      masterProcess.destroy();
    }
    synchronized (tabletServerProcesses) {
      if (tabletServerProcesses != null) {
        for (Process tserver : tabletServerProcesses) {
          tserver.destroy();
        }
      }
    }
    if (gcProcess != null) {
      gcProcess.destroy();
    }

    zooKeeperProcess = null;
    masterProcess = null;
    gcProcess = null;
    tabletServerProcesses.clear();
    if (config.useMiniDFS() && miniDFS != null)
      miniDFS.shutdown();
    for (Process p : cleanup)
      p.destroy();
    miniDFS = null;
  }

  /**
   * @since 1.6.0
   */
  public MiniAccumuloConfig getConfig() {
    return config;
  }

  /**
   * Utility method to get a connector to the MAC.
   * 
   * @since 1.6.0
   */
  public Connector getConnector(String user, String passwd) throws AccumuloException, AccumuloSecurityException {
    Instance instance = new ZooKeeperInstance(this.getInstanceName(), this.getZooKeepers());
    return instance.getConnector(user, new PasswordToken(passwd));
  }

  public FileSystem getFileSystem() {
    try {
      return FileSystem.get(new URI(dfsUri), new Configuration());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
