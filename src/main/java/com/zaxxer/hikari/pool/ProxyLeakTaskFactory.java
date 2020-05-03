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

/**
 * A factory for {@link ProxyLeakTask} Runnables that are scheduled in the future to report leaks.
 * 创建连接泄露检测任务代理对象的工厂类
 *
 * @author Brett Wooldridge
 * @author Andreas Brenk
 */
class ProxyLeakTaskFactory {
   /**
    * 具体执行连接泄露检测任务的调度服务
    */
   private ScheduledExecutorService executorService;
   private long leakDetectionThreshold;

   ProxyLeakTaskFactory(final long leakDetectionThreshold, final ScheduledExecutorService executorService) {
      this.executorService = executorService;
      this.leakDetectionThreshold = leakDetectionThreshold;
   }

   ProxyLeakTask schedule(final PoolEntry poolEntry) {
      return (leakDetectionThreshold == 0) ? ProxyLeakTask.NO_LEAK : scheduleNewTask(poolEntry);
   }

   /**
    * 更新连接泄露检测阈值，其实就是该延迟任务执行的延迟时间，单位毫秒
    * @param leakDetectionThreshold
    */
   void updateLeakDetectionThreshold(final long leakDetectionThreshold) {
      this.leakDetectionThreshold = leakDetectionThreshold;
   }

   /**
    * 创建一个新的延时执行连接泄露检测任务
    *
    * @param poolEntry
    * @return
    */
   private ProxyLeakTask scheduleNewTask(PoolEntry poolEntry) {
      ProxyLeakTask task = new ProxyLeakTask(poolEntry);
      task.schedule(executorService, leakDetectionThreshold);

      return task;
   }
}
