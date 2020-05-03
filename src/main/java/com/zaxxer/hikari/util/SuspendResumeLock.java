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

package com.zaxxer.hikari.util;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.concurrent.Semaphore;

/**
 * This class implements a lock that can be used to suspend and resume the pool.  It
 * also provides a faux implementation that is used when the feature is disabled that
 * hopefully gets fully "optimized away" by the JIT.
 * 挂起恢复锁
 * @author Brett Wooldridge
 */
public class SuspendResumeLock
{
   /**
    * 假锁实现
    */
   public static final SuspendResumeLock FAUX_LOCK = new SuspendResumeLock(false) {
      @Override
      public void acquire() {}

      @Override
      public void release() {}

      @Override
      public void suspend() {}

      @Override
      public void resume() {}
   };

   private static final int MAX_PERMITS = 10000;
   private final Semaphore acquisitionSemaphore;

   /**
    * Default constructor
    */
   public SuspendResumeLock()
   {
      this(true);
   }

   private SuspendResumeLock(final boolean createSemaphore)
   {
      // 是否要创建信号量
      acquisitionSemaphore = (createSemaphore ? new Semaphore(MAX_PERMITS, true) : null);
   }

   /**
    * 获取一个信号量
    * @throws SQLException
    */
   public void acquire() throws SQLException
   {
      //先尝试获取信号量
      if (acquisitionSemaphore.tryAcquire()) {
         return;
      }
      else if (Boolean.getBoolean("com.zaxxer.hikari.throwIfSuspended")) {
         // 如果挂起直接抛出异常
         throw new SQLTransientException("The pool is currently suspended and configured to throw exceptions upon acquisition");
      }
      // 阻塞获取信号量
      acquisitionSemaphore.acquireUninterruptibly();
   }

   /**
    * 释放一个信号量
    */
   public void release()
   {
      acquisitionSemaphore.release();
   }

   /**
    * 一次性抢占所有信号量
    */
   public void suspend()
   {
      acquisitionSemaphore.acquireUninterruptibly(MAX_PERMITS);
   }

   /**
    * 一次性释放所有信号量
    */
   public void resume()
   {
      acquisitionSemaphore.release(MAX_PERMITS);
   }
}
