/*
 * Copyright (C) 2014 Brett Wooldridge
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

import com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry;
import com.zaxxer.hikari.util.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.zaxxer.hikari.util.ClockSource.*;

/**
 * Entry used in the ConcurrentBag to track Connection instances.
 * 连接池内部封装对象，内部封装了数据库连接对象
 *
 * @author Brett Wooldridge
 */
final class PoolEntry implements IConcurrentBagEntry
{
   private static final Logger LOGGER = LoggerFactory.getLogger(PoolEntry.class);
   /**
    * 这个工具比较有意思，可以实现当前对象某个字段的CAS方式修改，类似于Unfafe的底层调用CAS方法；
    */
   private static final AtomicIntegerFieldUpdater<PoolEntry> stateUpdater;
   /**
    * 当前实体持有的数据库连接
    */
   Connection connection;
   /**
    * 上次访问时间
    */
   long lastAccessed;
   /**
    * 上次借用时间
    */
   long lastBorrowed;

   @SuppressWarnings("FieldCanBeLocal")
   /**
    * 当前实体状态，状态值在实现接口中定义
    *       //未使用状态
    *       int STATE_NOT_IN_USE = 0;
    *       // 使用中状态
    *       int STATE_IN_USE = 1;
    *       // 已删除状态
    *       int STATE_REMOVED = -1;
    *       // 保留中状态
    *       int STATE_RESERVED = -2;
    */
   private volatile int state = 0;
   /**
    * 是否已经被驱逐
    */
   private volatile boolean evict;
   /**
    * 当前实体对象生存倒计时
    */
   private volatile ScheduledFuture<?> endOfLife;
   /**
    * 已经打开的sql语句执行集合
    */
   private final FastList<Statement> openStatements;
   /**
    * 实体所属的连接池对象引用，方便可以直接根据当前对象进行归还操作，不需要在调用连接池的方法
    */
   private final HikariPool hikariPool;
   /**
    * 是否只读模式
    */
   private final boolean isReadOnly;
   /**
    * 是否自动提交模式
    */
   private final boolean isAutoCommit;

   static
   {
      // 提供了一个更新器，可以CAS方式更新PoolEntry对象中的state字段
      stateUpdater = AtomicIntegerFieldUpdater.newUpdater(PoolEntry.class, "state");
   }

   PoolEntry(final Connection connection, final PoolBase pool, final boolean isReadOnly, final boolean isAutoCommit)
   {
      this.connection = connection;
      this.hikariPool = (HikariPool) pool;
      this.isReadOnly = isReadOnly;
      this.isAutoCommit = isAutoCommit;
      this.lastAccessed = currentTime();
      this.openStatements = new FastList<>(Statement.class, 16);
   }

   /**
    * Release this entry back to the pool.
    *
    * @param lastAccessed last access time-stamp
    */
   void recycle(final long lastAccessed)
   {
      if (connection != null) {
         this.lastAccessed = lastAccessed;
         //调用连接池的归还方法
         hikariPool.recycle(this);
      }
   }

   /**
    * Set the end of life {@link ScheduledFuture}.
    *
    * @param endOfLife this PoolEntry/Connection's end of life {@link ScheduledFuture}
    */
   void setFutureEol(final ScheduledFuture<?> endOfLife)
   {
      this.endOfLife = endOfLife;
   }

   /**
    * 获取JDBC连接代理对象
    * @param leakTask
    * @param now
    * @return
    */
   Connection createProxyConnection(final ProxyLeakTask leakTask, final long now)
   {
      return ProxyFactory.getProxyConnection(this, connection, openStatements, leakTask, now, isReadOnly, isAutoCommit);
   }

   void resetConnectionState(final ProxyConnection proxyConnection, final int dirtyBits) throws SQLException
   {
      hikariPool.resetConnectionState(connection, proxyConnection, dirtyBits);
   }

   String getPoolName()
   {
      return hikariPool.toString();
   }

   boolean isMarkedEvicted()
   {
      return evict;
   }

   /**
    * 标记当前实体为驱逐状态
    */
   void markEvicted()
   {
      this.evict = true;
   }

   /**
    * 进行实际的连接驱逐动作，实际上就是指向连接关闭方法
    * @param closureReason
    */
   void evict(final String closureReason)
   {
      hikariPool.closeConnection(this, closureReason);
   }

   /** Returns millis since lastBorrowed */
   long getMillisSinceBorrowed()
   {
      return elapsedMillis(lastBorrowed);
   }

   PoolBase getPoolBase()
   {
      return hikariPool;
   }

   /** {@inheritDoc} */
   @Override
   public String toString()
   {
      final long now = currentTime();
      return connection
         + ", accessed " + elapsedDisplayString(lastAccessed, now) + " ago, "
         + stateToString();
   }

   // ***********************************************************************
   //                      IConcurrentBagEntry methods
   // ***********************************************************************

   /** {@inheritDoc} */
   @Override
   public int getState()
   {
      return stateUpdater.get(this);
   }

   /** {@inheritDoc} */
   @Override
   public boolean compareAndSet(int expect, int update)
   {
      return stateUpdater.compareAndSet(this, expect, update);
   }

   /** {@inheritDoc} */
   @Override
   public void setState(int update)
   {
      stateUpdater.set(this, update);
   }

   /**
    * 关闭封装实体对象中的链接并返回当前关闭的链接
    * @return
    */
   Connection close()
   {
      ScheduledFuture<?> eol = endOfLife;
      if (eol != null && !eol.isDone() && !eol.cancel(false)) {
         LOGGER.warn("{} - maxLifeTime expiration task cancellation unexpectedly returned false for connection {}", getPoolName(), connection);
      }

      Connection con = connection;
      connection = null;
      endOfLife = null;
      return con;
   }

   private String stateToString()
   {
      switch (state) {
      case STATE_IN_USE:
         return "IN_USE";
      case STATE_NOT_IN_USE:
         return "NOT_IN_USE";
      case STATE_REMOVED:
         return "REMOVED";
      case STATE_RESERVED:
         return "RESERVED";
      default:
         return "Invalid";
      }
   }
}
