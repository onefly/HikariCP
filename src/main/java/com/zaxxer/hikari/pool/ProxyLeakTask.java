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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Runnable that is scheduled in the future to report leaks.  The ScheduledFuture is
 * cancelled if the connection is closed before the leak time expires.
 * JDBC连接泄露检测任务代理对象
 *
 * @author Brett Wooldridge
 */
class ProxyLeakTask implements Runnable
{
   private static final Logger LOGGER = LoggerFactory.getLogger(ProxyLeakTask.class);
   /**
    * 不需要进行内存泄露检测
    */
   static final ProxyLeakTask NO_LEAK;
   /**
    * JDBC连接泄露检测延迟任务引用
    */
   private ScheduledFuture<?> scheduledFuture;
   /**
    * 当前连接名字
    */
   private String connectionName;
   /**
    * 异常信息
    */
   private Exception exception;
   private String threadName;
   private boolean isLeaked;

   static
   {
      NO_LEAK = new ProxyLeakTask() {
         @Override
         void schedule(ScheduledExecutorService executorService, long leakDetectionThreshold) {}

         @Override
         public void run() {}

         @Override
         public void cancel() {}
      };
   }

   ProxyLeakTask(final PoolEntry poolEntry)
   {
      this.exception = new Exception("Apparent connection leak detected");
      this.threadName = Thread.currentThread().getName();
      this.connectionName = poolEntry.connection.toString();
   }

   private ProxyLeakTask()
   {
   }

   /**
    * 根据传入的调度执行器服务和延迟启动的时间，创建一个延迟调度任务，并将该任务的引用交接给当前对象
    * @param executorService
    * @param leakDetectionThreshold
    */
   void schedule(ScheduledExecutorService executorService, long leakDetectionThreshold)
   {
      scheduledFuture = executorService.schedule(this, leakDetectionThreshold, TimeUnit.MILLISECONDS);
   }

   /** {@inheritDoc} */
   /**
    * 到执行时间的时候，如果该任务还没有被取消，可能被泄露了，直接打印连接泄露提示消息
    */
   @Override
   public void run()
   {
      isLeaked = true;

      final StackTraceElement[] stackTrace = exception.getStackTrace();
      final StackTraceElement[] trace = new StackTraceElement[stackTrace.length - 5];
      System.arraycopy(stackTrace, 5, trace, 0, trace.length);

      exception.setStackTrace(trace);
      LOGGER.warn("Connection leak detection triggered for {} on thread {}, stack trace follows", connectionName, threadName, exception);
   }

   /**
    * 取消连接泄露检测任务
    */
   void cancel()
   {
      scheduledFuture.cancel(false);
      if (isLeaked) {
         LOGGER.info("Previously reported leaked connection {} on thread {} was returned to the pool (unleaked)", connectionName, threadName);
      }
   }
}
