/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.zaxxer.hikari.pool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.SQLExceptionOverride;
import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException;
import com.zaxxer.hikari.util.DriverDataSource;
import com.zaxxer.hikari.util.PropertyElf;
import com.zaxxer.hikari.util.UtilityElf;
import com.zaxxer.hikari.util.UtilityElf.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

import static com.zaxxer.hikari.pool.ProxyConnection.*;
import static com.zaxxer.hikari.util.ClockSource.*;
import static com.zaxxer.hikari.util.UtilityElf.createInstance;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * 连接池抽象基类
 */
abstract class PoolBase
{
   private final Logger logger = LoggerFactory.getLogger(PoolBase.class);
   /**
    * 连接池配置
    */
   public final HikariConfig config;
   IMetricsTrackerDelegate metricsTracker;

   protected final String poolName;

   volatile String catalog;
   /**
    * 用来记录上次连接失败的异常信息
    */
   final AtomicReference<Exception> lastConnectionFailure;
   /**
    * 连接超时时间
    */
   long connectionTimeout;
   /**
    * 检测超时时间
    */
   long validationTimeout;

   SQLExceptionOverride exceptionOverride;

   private static final String[] RESET_STATES = {"readOnly", "autoCommit", "isolation", "catalog", "netTimeout", "schema"};
   private static final int UNINITIALIZED = -1;
   private static final int TRUE = 1;
   private static final int FALSE = 0;
   // 网络连接超时时间
   private int networkTimeout;
   /**
    * 是否支持网络超时设置
    */
   private int isNetworkTimeoutSupported;
   /**
    * 是否支持查询超时
    */
   private int isQueryTimeoutSupported;
   /**
    * more事务隔离级别
    */
   private int defaultTransactionIsolation;
   /**
    * 事务隔离级别
    */
   private int transactionIsolation;
   /**
    * 网络超时检查执行器
    */
   private Executor netTimeoutExecutor;
   /**
    * 数据源
    */
   private DataSource dataSource;

   private final String schema;
   /**
    * 是否只读模式
    */
   private final boolean isReadOnly;
   /**
    * 是否自动提交事务
    */
   private final boolean isAutoCommit;
   /**
    * 是否使用JDBC检测连接有效性
    */
   private final boolean isUseJdbc4Validation;
   /**
    * 是否隔离内部查询
    */
   private final boolean isIsolateInternalQueries;

   private volatile boolean isValidChecked;

   PoolBase(final HikariConfig config)
   {
      this.config = config;
      // 网络连接超时时间默认初始值 -1
      this.networkTimeout = UNINITIALIZED;
      this.catalog = config.getCatalog();
      this.schema = config.getSchema();
      this.isReadOnly = config.isReadOnly();
      this.isAutoCommit = config.isAutoCommit();
      this.exceptionOverride = UtilityElf.createInstance(config.getExceptionOverrideClassName(), SQLExceptionOverride.class);
      this.transactionIsolation = UtilityElf.getTransactionIsolation(config.getTransactionIsolation());

      this.isQueryTimeoutSupported = UNINITIALIZED;
      this.isNetworkTimeoutSupported = UNINITIALIZED;
      //如果设置了连接检测connection-test-query这个参数，则表示使用不使用JDBC方式valid检测,如果配置了检测sql，则直接通过直线检测sql的方式检测
      this.isUseJdbc4Validation = config.getConnectionTestQuery() == null;
      this.isIsolateInternalQueries = config.isIsolateInternalQueries();

      this.poolName = config.getPoolName();
      this.connectionTimeout = config.getConnectionTimeout();
      this.validationTimeout = config.getValidationTimeout();
      this.lastConnectionFailure = new AtomicReference<>();

      initializeDataSource();
   }

   /** {@inheritDoc} */
   @Override
   public String toString()
   {
      return poolName;
   }

   /**
    * 归还封装的连接池实体对象，具体逻辑有子类实现
    * @param poolEntry
    */
   abstract void recycle(final PoolEntry poolEntry);

   // ***********************************************************************
   //                           JDBC methods
   // ***********************************************************************

   /**
    * 偷偷的关闭JDBC链接
    * @param connection
    * @param closureReason
    */
   void quietlyCloseConnection(final Connection connection, final String closureReason)
   {
      if (connection != null) {
         try {
            logger.debug("{} - Closing connection {}: {}", poolName, connection, closureReason);

            try {
               setNetworkTimeout(connection, SECONDS.toMillis(15));
            }
            catch (SQLException e) {
               // ignore
            }
            finally {
               connection.close(); // continue with the close even if setNetworkTimeout() throws
            }
         }
         catch (Exception e) {
            logger.debug("{} - Closing connection {} failed", poolName, connection, e);
         }
      }
   }

   /**
    * 检测连接是否存活，存活的连接返回true
    * @param connection
    * @return
    */
   boolean isConnectionAlive(final Connection connection)
   {
      try {
         try {
            setNetworkTimeout(connection, validationTimeout);
            //可以同配置修改检测超时时间，但是最小值不能超过1秒，否则不予采纳
            final int validationSeconds = (int) Math.max(1000L, validationTimeout) / 1000;

            if (isUseJdbc4Validation) {
               return connection.isValid(validationSeconds);
            }

            try (Statement statement = connection.createStatement()) {
               if (isNetworkTimeoutSupported != TRUE) {
                  setQueryTimeout(statement, validationSeconds);
               }

               statement.execute(config.getConnectionTestQuery());
            }
         }
         finally {
            setNetworkTimeout(connection, networkTimeout);

            if (isIsolateInternalQueries && !isAutoCommit) {
               connection.rollback();
            }
         }

         return true;
      }
      catch (Exception e) {
         lastConnectionFailure.set(e);
         logger.warn("{} - Failed to validate connection {} ({}). Possibly consider using a shorter maxLifetime value.",
                     poolName, connection, e.getMessage());
         return false;
      }
   }

   Exception getLastConnectionFailure()
   {
      return lastConnectionFailure.get();
   }

   public DataSource getUnwrappedDataSource()
   {
      return dataSource;
   }

   // ***********************************************************************
   //                         PoolEntry methods
   // ***********************************************************************

   /**
    * 创建一个新的连接封装对象
    * @return
    * @throws Exception
    */
   PoolEntry newPoolEntry() throws Exception
   {
      return new PoolEntry(newConnection(), this, isReadOnly, isAutoCommit);
   }

   void resetConnectionState(final Connection connection, final ProxyConnection proxyConnection, final int dirtyBits) throws SQLException
   {
      int resetBits = 0;

      if ((dirtyBits & DIRTY_BIT_READONLY) != 0 && proxyConnection.getReadOnlyState() != isReadOnly) {
         connection.setReadOnly(isReadOnly);
         resetBits |= DIRTY_BIT_READONLY;
      }

      if ((dirtyBits & DIRTY_BIT_AUTOCOMMIT) != 0 && proxyConnection.getAutoCommitState() != isAutoCommit) {
         connection.setAutoCommit(isAutoCommit);
         resetBits |= DIRTY_BIT_AUTOCOMMIT;
      }

      if ((dirtyBits & DIRTY_BIT_ISOLATION) != 0 && proxyConnection.getTransactionIsolationState() != transactionIsolation) {
         connection.setTransactionIsolation(transactionIsolation);
         resetBits |= DIRTY_BIT_ISOLATION;
      }

      if ((dirtyBits & DIRTY_BIT_CATALOG) != 0 && catalog != null && !catalog.equals(proxyConnection.getCatalogState())) {
         connection.setCatalog(catalog);
         resetBits |= DIRTY_BIT_CATALOG;
      }

      if ((dirtyBits & DIRTY_BIT_NETTIMEOUT) != 0 && proxyConnection.getNetworkTimeoutState() != networkTimeout) {
         setNetworkTimeout(connection, networkTimeout);
         resetBits |= DIRTY_BIT_NETTIMEOUT;
      }

      if ((dirtyBits & DIRTY_BIT_SCHEMA) != 0 && schema != null && !schema.equals(proxyConnection.getSchemaState())) {
         connection.setSchema(schema);
         resetBits |= DIRTY_BIT_SCHEMA;
      }

      if (resetBits != 0 && logger.isDebugEnabled()) {
         logger.debug("{} - Reset ({}) on connection {}", poolName, stringFromResetBits(resetBits), connection);
      }
   }

   void shutdownNetworkTimeoutExecutor()
   {
      if (netTimeoutExecutor instanceof ThreadPoolExecutor) {
         ((ThreadPoolExecutor) netTimeoutExecutor).shutdownNow();
      }
   }

   long getLoginTimeout()
   {
      try {
         return (dataSource != null) ? dataSource.getLoginTimeout() : SECONDS.toSeconds(5);
      } catch (SQLException e) {
         return SECONDS.toSeconds(5);
      }
   }

   // ***********************************************************************
   //                       JMX methods
   // ***********************************************************************

   /**
    * Register MBeans for HikariConfig and HikariPool.
    * 进行Mbeans的注册处理，可以使用使用jconsole控制台动态修连接池配置 和连接池
    * @param hikariPool a HikariPool instance
    */
   void handleMBeans(final HikariPool hikariPool, final boolean register)
   {
      if (!config.isRegisterMbeans()) {
         return;
      }

      try {
         final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

         final ObjectName beanConfigName = new ObjectName("com.zaxxer.hikari:type=PoolConfig (" + poolName + ")");
         final ObjectName beanPoolName = new ObjectName("com.zaxxer.hikari:type=Pool (" + poolName + ")");
         if (register) {
            if (!mBeanServer.isRegistered(beanConfigName)) {
               mBeanServer.registerMBean(config, beanConfigName);
               mBeanServer.registerMBean(hikariPool, beanPoolName);
            } else {
               logger.error("{} - JMX name ({}) is already registered.", poolName, poolName);
            }
         }
         else if (mBeanServer.isRegistered(beanConfigName)) {
            mBeanServer.unregisterMBean(beanConfigName);
            mBeanServer.unregisterMBean(beanPoolName);
         }
      }
      catch (Exception e) {
         logger.warn("{} - Failed to {} management beans.", poolName, (register ? "register" : "unregister"), e);
      }
   }

   // ***********************************************************************
   //                          Private methods
   // ***********************************************************************

   /**
    * Create/initialize the underlying DataSource.
    * 初始化数据源
    */
   private void initializeDataSource()
   {
      final String jdbcUrl = config.getJdbcUrl();
      final String username = config.getUsername();
      final String password = config.getPassword();
      final String dsClassName = config.getDataSourceClassName();
      final String driverClassName = config.getDriverClassName();
      final String dataSourceJNDI = config.getDataSourceJNDI();
      final Properties dataSourceProperties = config.getDataSourceProperties();

      DataSource ds = config.getDataSource();
      if (dsClassName != null && ds == null) {
         ds = createInstance(dsClassName, DataSource.class);
         PropertyElf.setTargetFromProperties(ds, dataSourceProperties);
      }
      else if (jdbcUrl != null && ds == null) {
         // 初始化数据库驱动数据源
         ds = new DriverDataSource(jdbcUrl, driverClassName, dataSourceProperties, username, password);
      }
      else if (dataSourceJNDI != null && ds == null) {
         try {
            InitialContext ic = new InitialContext();
            ds = (DataSource) ic.lookup(dataSourceJNDI);
         } catch (NamingException e) {
            throw new PoolInitializationException(e);
         }
      }

      if (ds != null) {
         setLoginTimeout(ds);
         createNetworkTimeoutExecutor(ds, dsClassName, jdbcUrl);
      }

      this.dataSource = ds;
   }

   /**
    * Obtain connection from data source.
    * 根据数据源创建一个新的连接
    *
    * @return a Connection connection
    */
   private Connection newConnection() throws Exception
   {
      final long start = currentTime();

      Connection connection = null;
      try {
         String username = config.getUsername();
         String password = config.getPassword();
         // 如果配置了用户名，则使用带认证的方式获取数据库连接
         connection = (username == null) ? dataSource.getConnection() : dataSource.getConnection(username, password);
         if (connection == null) {
            throw new SQLTransientConnectionException("DataSource returned null unexpectedly");
         }
         // 方法内部包含连接有效性检测，如果检测么有通过则抛出异常
         setupConnection(connection);
         lastConnectionFailure.set(null);
         return connection;
      }
      catch (Exception e) {
         if (connection != null) {
            quietlyCloseConnection(connection, "(Failed to create/setup connection)");
         }
         else if (getLastConnectionFailure() == null) {
            logger.debug("{} - Failed to create/setup connection: {}", poolName, e.getMessage());
         }
         //记录连接异常信息
         lastConnectionFailure.set(e);
         throw e;
      }
      finally {
         // tracker will be null during failFast check
         // 跟踪记录获取连接的时间
         if (metricsTracker != null) {
            metricsTracker.recordConnectionCreated(elapsedMillis(start));
         }
      }
   }

   /**
    * Setup a connection initial state.
    *根据连接设置连接参数
    * @param connection a Connection
    * @throws ConnectionSetupException thrown if any exception is encountered
    */
   private void setupConnection(final Connection connection) throws ConnectionSetupException
   {
      try {
         if (networkTimeout == UNINITIALIZED) {
            networkTimeout = getAndSetNetworkTimeout(connection, validationTimeout);
         }
         else {
            setNetworkTimeout(connection, validationTimeout);
         }

         if (connection.isReadOnly() != isReadOnly) {
            connection.setReadOnly(isReadOnly);
         }

         if (connection.getAutoCommit() != isAutoCommit) {
            connection.setAutoCommit(isAutoCommit);
         }

         checkDriverSupport(connection);

         if (transactionIsolation != defaultTransactionIsolation) {
            connection.setTransactionIsolation(transactionIsolation);
         }

         if (catalog != null) {
            connection.setCatalog(catalog);
         }

         if (schema != null) {
            connection.setSchema(schema);
         }
         // 执行初始化连接初始化SQL
         executeSql(connection, config.getConnectionInitSql(), true);
         // 设置网络连接超时时间
         setNetworkTimeout(connection, networkTimeout);
      }
      catch (SQLException e) {
         throw new ConnectionSetupException(e);
      }
   }

   /**
    * Execute isValid() or connection test query.
    *
    * @param connection a Connection to check
    */
   private void checkDriverSupport(final Connection connection) throws SQLException
   {
      if (!isValidChecked) {
         checkValidationSupport(connection);
         checkDefaultIsolation(connection);

         isValidChecked = true;
      }
   }

   /**
    * Check whether Connection.isValid() is supported, or that the user has test query configured.
    * 判断连接是否有效，两种方式
    * @param connection a Connection to check
    * @throws SQLException rethrown from the driver
    */
   private void checkValidationSupport(final Connection connection) throws SQLException
   {
      try {
         if (isUseJdbc4Validation) {
            // 是否使用JDBC方式进行有效性检查，默认设置超时时间1秒
            connection.isValid(1);
         }
         else {
            // 如果配置了检测sql，则执行测试sql执行，并且设置事务不提交，只是判断连接是否可用
            executeSql(connection, config.getConnectionTestQuery(), false);
         }
      }
      catch (Exception | AbstractMethodError e) {
         logger.error("{} - Failed to execute{} connection test query ({}).", poolName, (isUseJdbc4Validation ? " isValid() for connection, configure" : ""), e.getMessage());
         throw e;
      }
   }

   /**
    * Check the default transaction isolation of the Connection.
    * 检测数据库连接的默认事务跟李级别
    *
    * @param connection a Connection to check
    * @throws SQLException rethrown from the driver
    */
   private void checkDefaultIsolation(final Connection connection) throws SQLException
   {
      try {
         defaultTransactionIsolation = connection.getTransactionIsolation();
         if (transactionIsolation == -1) {
            transactionIsolation = defaultTransactionIsolation;
         }
      }
      catch (SQLException e) {
         logger.warn("{} - Default transaction isolation level detection failed ({}).", poolName, e.getMessage());
         if (e.getSQLState() != null && !e.getSQLState().startsWith("08")) {
            throw e;
         }
      }
   }

   /**
    * Set the query timeout, if it is supported by the driver.
    * 设置sql执行的超时时间
    *
    * @param statement a statement to set the query timeout on
    * @param timeoutSec the number of seconds before timeout
    */
   private void setQueryTimeout(final Statement statement, final int timeoutSec)
   {
      if (isQueryTimeoutSupported != FALSE) {
         try {
            statement.setQueryTimeout(timeoutSec);
            isQueryTimeoutSupported = TRUE;
         }
         catch (Exception e) {
            if (isQueryTimeoutSupported == UNINITIALIZED) {
               isQueryTimeoutSupported = FALSE;
               logger.info("{} - Failed to set query timeout for statement. ({})", poolName, e.getMessage());
            }
         }
      }
   }

   /**
    * Set the network timeout, if <code>isUseNetworkTimeout</code> is <code>true</code> and the
    * driver supports it.  Return the pre-existing value of the network timeout.
    *
    * @param connection the connection to set the network timeout on
    * @param timeoutMs the number of milliseconds before timeout
    * @return the pre-existing network timeout value
    */
   private int getAndSetNetworkTimeout(final Connection connection, final long timeoutMs)
   {
      if (isNetworkTimeoutSupported != FALSE) {
         try {
            final int originalTimeout = connection.getNetworkTimeout();
            connection.setNetworkTimeout(netTimeoutExecutor, (int) timeoutMs);
            isNetworkTimeoutSupported = TRUE;
            return originalTimeout;
         }
         catch (Exception | AbstractMethodError e) {
            if (isNetworkTimeoutSupported == UNINITIALIZED) {
               isNetworkTimeoutSupported = FALSE;

               logger.info("{} - Driver does not support get/set network timeout for connections. ({})", poolName, e.getMessage());
               if (validationTimeout < SECONDS.toMillis(1)) {
                  logger.warn("{} - A validationTimeout of less than 1 second cannot be honored on drivers without setNetworkTimeout() support.", poolName);
               }
               else if (validationTimeout % SECONDS.toMillis(1) != 0) {
                  logger.warn("{} - A validationTimeout with fractional second granularity cannot be honored on drivers without setNetworkTimeout() support.", poolName);
               }
            }
         }
      }

      return 0;
   }

   /**
    * Set the network timeout, if <code>isUseNetworkTimeout</code> is <code>true</code> and the
    * driver supports it.
    * 设置网络连接超时时间
    *
    * @param connection the connection to set the network timeout on
    * @param timeoutMs the number of milliseconds before timeout
    * @throws SQLException throw if the connection.setNetworkTimeout() call throws
    */
   private void setNetworkTimeout(final Connection connection, final long timeoutMs) throws SQLException
   {
      if (isNetworkTimeoutSupported == TRUE) {
         connection.setNetworkTimeout(netTimeoutExecutor, (int) timeoutMs);
      }
   }

   /**
    * Execute the user-specified init SQL.
    * 用来执行初始化sql和连接检测sql
    *
    * @param connection the connection to initialize
    * @param sql the SQL to execute
    * @param isCommit whether to commit the SQL after execution or not
    * @throws SQLException throws if the init SQL execution fails
    */
   private void executeSql(final Connection connection, final String sql, final boolean isCommit) throws SQLException
   {
      if (sql != null) {
         try (Statement statement = connection.createStatement()) {
            // connection was created a few milliseconds before, so set query timeout is omitted (we assume it will succeed)
            statement.execute(sql);
         }

         if (isIsolateInternalQueries && !isAutoCommit) {
            if (isCommit) {
               connection.commit();
            }
            else {
               connection.rollback();
            }
         }
      }
   }

   /**
    * 创建网络超时执行线程池
    * @param dataSource
    * @param dsClassName
    * @param jdbcUrl
    */
   private void createNetworkTimeoutExecutor(final DataSource dataSource, final String dsClassName, final String jdbcUrl)
   {
      // Temporary hack for MySQL issue: http://bugs.mysql.com/bug.php?id=75615
      if ((dsClassName != null && dsClassName.contains("Mysql")) ||
          (jdbcUrl != null && jdbcUrl.contains("mysql")) ||
          (dataSource != null && dataSource.getClass().getName().contains("Mysql"))) {
         // 针对mysql驱动，创建了一个同步的执行器，而没有使用异步执行器
         netTimeoutExecutor = new SynchronousExecutor();
      }
      else {
         ThreadFactory threadFactory = config.getThreadFactory();
         threadFactory = threadFactory != null ? threadFactory : new DefaultThreadFactory(poolName + " network timeout executor", true);
         ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool(threadFactory);
         executor.setKeepAliveTime(15, SECONDS);
         executor.allowCoreThreadTimeOut(true);
         netTimeoutExecutor = executor;
      }
   }

   /**
    * Set the loginTimeout on the specified DataSource.
    * 设置数据源的登录超时时间
    *
    * @param dataSource the DataSource
    */
   private void setLoginTimeout(final DataSource dataSource)
   {
      if (connectionTimeout != Integer.MAX_VALUE) {
         try {
            dataSource.setLoginTimeout(Math.max(1, (int) MILLISECONDS.toSeconds(500L + connectionTimeout)));
         }
         catch (Exception e) {
            logger.info("{} - Failed to set login timeout for data source. ({})", poolName, e.getMessage());
         }
      }
   }

   /**
    * This will create a string for debug logging. Given a set of "reset bits", this
    * method will return a concatenated string, for example:
    *
    * Input : 0b00110
    * Output: "autoCommit, isolation"
    *
    * @param bits a set of "reset bits"
    * @return a string of which states were reset
    */
   private String stringFromResetBits(final int bits)
   {
      final StringBuilder sb = new StringBuilder();
      for (int ndx = 0; ndx < RESET_STATES.length; ndx++) {
         if ( (bits & (0b1 << ndx)) != 0) {
            sb.append(RESET_STATES[ndx]).append(", ");
         }
      }

      sb.setLength(sb.length() - 2);  // trim trailing comma
      return sb.toString();
   }

   // ***********************************************************************
   //                      Private Static Classes
   // ***********************************************************************

   static class ConnectionSetupException extends Exception
   {
      private static final long serialVersionUID = 929872118275916521L;

      ConnectionSetupException(Throwable t)
      {
         super(t);
      }
   }

   /**
    * Special executor used only to work around a MySQL issue that has not been addressed.
    * MySQL issue: http://bugs.mysql.com/bug.php?id=75615
    * 同步执行线程池，是个假的线程池, 因为mysql驱动设置超时时间时，只能传入一个异步执行的线程池，由这个线程池在未来某个时间进行异步设置超时时间
    * ，但是如果用户紧接着关闭了连接，就会出现空指针问题
    * 所以这里创建了一个同步执行的线程池，由主线程同步执行设置超时操作，就可以避免空指针问题
    */
   private static class SynchronousExecutor implements Executor
   {
      /** {@inheritDoc} */
      @Override
      public void execute(Runnable command)
      {
         try {
            command.run();
         }
         catch (Exception t) {
            LoggerFactory.getLogger(PoolBase.class).debug("Failed to execute: {}", command, t);
         }
      }
   }

   interface IMetricsTrackerDelegate extends AutoCloseable
   {
      default void recordConnectionUsage(PoolEntry poolEntry) {}

      default void recordConnectionCreated(long connectionCreatedMillis) {}

      default void recordBorrowTimeoutStats(long startTime) {}

      default void recordBorrowStats(final PoolEntry poolEntry, final long startTime) {}

      default void recordConnectionTimeout() {}

      @Override
      default void close() {}
   }

   /**
    * A class that delegates to a MetricsTracker implementation.  The use of a delegate
    * allows us to use the NopMetricsTrackerDelegate when metrics are disabled, which in
    * turn allows the JIT to completely optimize away to callsites to record metrics.
    */
   static class MetricsTrackerDelegate implements IMetricsTrackerDelegate
   {
      final IMetricsTracker tracker;

      MetricsTrackerDelegate(IMetricsTracker tracker)
      {
         this.tracker = tracker;
      }

      @Override
      public void recordConnectionUsage(final PoolEntry poolEntry)
      {
         tracker.recordConnectionUsageMillis(poolEntry.getMillisSinceBorrowed());
      }

      @Override
      public void recordConnectionCreated(long connectionCreatedMillis)
      {
         tracker.recordConnectionCreatedMillis(connectionCreatedMillis);
      }

      @Override
      public void recordBorrowTimeoutStats(long startTime)
      {
         tracker.recordConnectionAcquiredNanos(elapsedNanos(startTime));
      }

      @Override
      public void recordBorrowStats(final PoolEntry poolEntry, final long startTime)
      {
         final long now = currentTime();
         poolEntry.lastBorrowed = now;
         tracker.recordConnectionAcquiredNanos(elapsedNanos(startTime, now));
      }

      @Override
      public void recordConnectionTimeout() {
         tracker.recordConnectionTimeout();
      }

      @Override
      public void close()
      {
         tracker.close();
      }
   }

   /**
    * A no-op implementation of the IMetricsTrackerDelegate that is used when metrics capture is
    * disabled.
    */
   static final class NopMetricsTrackerDelegate implements IMetricsTrackerDelegate {}
}
