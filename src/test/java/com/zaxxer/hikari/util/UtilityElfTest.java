/*
 * Copyright (C) 2013, 2019 Brett Wooldridge
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

import org.junit.Test;

import java.util.Locale;

import static org.junit.Assert.assertEquals;

public class UtilityElfTest {
   @Test
   public void shouldReturnValidTransactionIsolationLevel() {
      //Act
      int expectedLevel = UtilityElf.getTransactionIsolation("TRANSACTION_SQL_SERVER_SNAPSHOT_ISOLATION_LEVEL");

      //Assert
      assertEquals(expectedLevel, 4096);
   }

   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowWhenInvalidTransactionNameGiven() {
      //Act
      UtilityElf.getTransactionIsolation("INVALID_TRANSACTION");
   }

   @Test
   public void shouldReturnTransationIsolationLevelFromInteger() {
      int expectedLevel = UtilityElf.getTransactionIsolation("4096");
      assertEquals(expectedLevel, 4096);
   }

   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowWhenInvalidTransactionIntegerGiven() {
      //Act
      UtilityElf.getTransactionIsolation("9999");
   }

   static {
      //System.setProperty("DB", "mysql");//可以作为全局变量，在任何地方使用
   }

   @Test
   public void testEnv() {
      System.out.println(System.getProperty("os.version"));
      System.out.println(System.getProperty("java.library.path"));
      System.out.println(System.getProperty("DB"));
      String jdbc = System.getProperty("jdbc.drivers");
      System.out.println(jdbc);
      for (int i = 0; i < 10; i++) {
         if((i & 0xff) == 0xff) {
            System.out.println(i);
            break;
         }
      }
      System.out.println(Integer.toHexString(0xff));
      String propName = "data";
      String methodName = "set" + propName.substring(0, 1).toUpperCase(Locale.ENGLISH) + propName.substring(1);
      System.out.println(methodName);
   }

   @Test
   public void testLocal() {
      ThreadLocal local = new ThreadLocal();
      local.set("当前线程名称：" + Thread.currentThread().getName());//将ThreadLocal作为key放入threadLocals.Entry中
      Thread t = Thread.currentThread();//注意断点看此时的threadLocals.Entry数组刚设置的referent是指向Local的，referent就是Entry中的key只是被WeakReference包装了一下
      local = null;//断开强引用，即断开local与referent的关联，但Entry中此时的referent还是指向Local的，为什么会这样，当引用传递设置为null时无法影响传递内的结果
      System.gc();//执行GC
      t = Thread.currentThread();//这时Entry中referent是null了，被GC掉了，因为Entry和key的关系是WeakReference，并且在没有其他强引用的情况下就被回收掉了
//如果这里不采用WeakReference，即使local=null，那么也不会回收Entry的key，因为Entry和key是强关联
//但是这里仅能做到回收key不能回收value，如果这个线程运行时间非常长，即使referent GC了，value持续不清空，就有内存溢出的风险
//彻底回收最好调用remove
//即：local.remove();//remove相当于把ThreadLocalMap里的这个元素干掉了，并没有把自己干掉
      System.out.println(local);
   }
}
