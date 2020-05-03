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

import com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.zaxxer.hikari.util.ClockSource.currentTime;
import static com.zaxxer.hikari.util.ClockSource.elapsedNanos;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.*;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

/**
 * This is a specialized concurrent bag that achieves superior performance
 * to LinkedBlockingQueue and LinkedTransferQueue for the purposes of a
 * connection pool.  It uses ThreadLocal storage when possible to avoid
 * locks, but resorts to scanning a common collection if there are no
 * available items in the ThreadLocal list.  Not-in-use items in the
 * ThreadLocal lists can be "stolen" when the borrowing thread has none
 * of its own.  It is a "lock-less" implementation using a specialized
 * AbstractQueuedLongSynchronizer to manage cross-thread signaling.
 *
 * Note that items that are "borrowed" from the bag are not actually
 * removed from any collection, so garbage collection will not occur
 * even if the reference is abandoned.  Thus care must be taken to
 * "requite" borrowed objects otherwise a memory leak will result.  Only
 * the "remove" method can completely remove an object from the bag.
 *
 * @author Brett Wooldridge
 *
 * @param <T> the templated type to store in the bag
 */
public class ConcurrentBag<T extends IConcurrentBagEntry> implements AutoCloseable
{
   private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentBag.class);
   /**
    * 共享连接集合，采用写入时复制集合实现类，该集合类读的时候没有锁，只有在写的时候的加排它锁
    * 适用读多写少的场景，可以提高读的效率
    */
   private final CopyOnWriteArrayList<T> sharedList;
   /**
    * 是否使用弱引用本地线程缓存
    */
   private final boolean weakThreadLocals;
   /**
    * 线程本地缓存
    */
   private final ThreadLocal<List<Object>> threadList;
   /**
    * 包状态监听器
    */
   private final IBagStateListener listener;
   /**
    * 等待者线程计数器
    */
   private final AtomicInteger waiters;
   /**
    * 并发包是否关闭
    */
   private volatile boolean closed;
   /**
    * 手把手同步交接队列，如果队列为空则阻塞线程
    */
   private final SynchronousQueue<T> handoffQueue;

   /**
    * 并发包的条目对象状态操作方法
    */
   public interface IConcurrentBagEntry
   {  //未使用状态
      int STATE_NOT_IN_USE = 0;
      // 使用中状态
      int STATE_IN_USE = 1;
      // 已删除状态
      int STATE_REMOVED = -1;
      // 保留中状态
      int STATE_RESERVED = -2;

      /**
       * CAS 方式修改目标状态
       * @param expectState
       * @param newState
       * @return
       */
      boolean compareAndSet(int expectState, int newState);

      /**
       * 设置新状态
       * @param newState
       */
      void setState(int newState);

      /**
       * 获取状态
       * @return
       */
      int getState();
   }

   /**
    * 并发包状态监听器
    */
   public interface IBagStateListener
   {
      /**
       * 根据等待线程的数量，添加新的元素到并发包中 监听器模式，监听
       * @param waiting
       */
      void addBagItem(int waiting);
   }

   /**
    * Construct a ConcurrentBag with the specified listener.
    *
    * @param listener the IBagStateListener to attach to this bag
    */
   public ConcurrentBag(final IBagStateListener listener)
   {
      this.listener = listener;
      this.weakThreadLocals = useWeakThreadLocals();
      // true 表示队列先进先出，否则为先进后出的栈实现
      this.handoffQueue = new SynchronousQueue<>(true);
      this.waiters = new AtomicInteger();
      this.sharedList = new CopyOnWriteArrayList<>();
      if (weakThreadLocals) {
         this.threadList = ThreadLocal.withInitial(() -> new ArrayList<>(16));
      }
      else {
         this.threadList = ThreadLocal.withInitial(() -> new FastList<>(IConcurrentBagEntry.class, 16));
      }
   }

   /**
    * The method will borrow a BagEntry from the bag, blocking for the
    * specified timeout if none are available.
    *
    * @param timeout how long to wait before giving up, in units of unit
    * @param timeUnit a <code>TimeUnit</code> determining how to interpret the timeout parameter
    * @return a borrowed instance from the bag or null if a timeout occurs
    * @throws InterruptedException if interrupted while waiting
    */
   public T borrow(long timeout, final TimeUnit timeUnit) throws InterruptedException
   {
      // Try the thread-local list first
      //优先从线程本地缓存中获取
      final List<Object> list = threadList.get();
      //如果线程缓存有，从最后一个开始倒序遍历,因为刚刚归还的对象都是直接添加到集合后面，从后面遍历可能第一个就是未使用状态的对象
      for (int i = list.size() - 1; i >= 0; i--) {
         //直接移除最后一个对象
         final Object entry = list.remove(i);
         final T bagEntry = weakThreadLocals ? ((WeakReference<T>) entry).get() : (T) entry;
         //CAS方式将对象状态由未使用状态修改为使用状态，如果修改失败，则继续遍历以下一个元素，直到有一个元素修改成功直接返回，
         if (bagEntry != null && bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
            return bagEntry;
         }
      }
      // 线程本地缓冲中没有找到可以使用的元素，则需要去共享的集合中去找，共享集合也没有则直接去手把手交接队列中等待新创建的元素
      // Otherwise, scan the shared list ... then poll the handoff queue
      final int waiting = waiters.incrementAndGet();
      try {
         // 首先遍历共享连接池，如果找到可用连接，直接返回
         for (T bagEntry : sharedList) {
            //遍历所有元素，并CAS方式修改元素未使用状态到使用中状态，如果有修改成功的，则直接返回，共享集合中包括所有的元素对象
            if (bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
               // If we may have stolen another waiter's connection, request another bag add.
               if (waiting > 1) {
                  //如果等待线程超过1个，则通知监听器创建新的元素添加进来
                  listener.addBagItem(waiting - 1);
               }
               return bagEntry;
            }
         }

         // 共享连接池内也没有找到，则直接通知连接池提交创建连接任务，通知的数量就是当前等待获取连接的线程数
         listener.addBagItem(waiting);

         timeout = timeUnit.toNanos(timeout);
         do {
            final long start = currentTime();
            // 从交接队列中直接获取新添加的元素，并并发修改使用状态，如果抢到一个，则直接返回，如果到超时时间都没有抢到，则返回NULL
            final T bagEntry = handoffQueue.poll(timeout, NANOSECONDS);
            if (bagEntry == null || bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
               return bagEntry;
            }

            timeout -= elapsedNanos(start);
         } while (timeout > 10_000);

         return null;
      }
      finally {
         waiters.decrementAndGet();
      }
   }

   /**
    * This method will return a borrowed object to the bag.  Objects
    * that are borrowed from the bag but never "requited" will result
    * in a memory leak.
    *
    * @param bagEntry the value to return to the bag
    * @throws NullPointerException if value is null
    * @throws IllegalStateException if the bagEntry was not borrowed from the bag
    */
   public void requite(final T bagEntry)
   {  //cas方式设置非使用状态
      bagEntry.setState(STATE_NOT_IN_USE);
      //判断是否有等待获取对象的线程等待着，如果有等待线程，则直接等待等待线程修改刚归还对象的使用状态，如果没有修改，可能归还的晚了，则需要手把手交接给等待线程，
      // 直到有等待线程交接了该归还对象
      // 如果没有等待线程，则直接进行后续操作，添加本地线缓冲对象集合中，如果有等待线程，则需要通过同步阻塞队列手把手交接给等等线程
      for (int i = 0; waiters.get() > 0; i++) {
         // 如果对象状态已经是使用状态则直接返回，说明对象已经归还成功并且被其他线程借走了
         // 如果线程状态还是未使用状态，则直接通过手把手交接队列将该对象交接给等待线程成功，则直接退出循环
         if (bagEntry.getState() != STATE_NOT_IN_USE || handoffQueue.offer(bagEntry)) {
            return;
         }
         else if ((i & 0xff) == 0xff) {
            //如果i>=255的时候停车休息10毫秒
            parkNanos(MICROSECONDS.toNanos(10));
         }
         else {
            //如果i<255 则直接让出CPU
            Thread.yield();
         }
      }

      final List<Object> threadLocalList = threadList.get();
      //线程本地缓冲连接对象最不超过50个
      if (threadLocalList.size() < 50) {
         //将归还的线程添加到线程本地缓冲集合中，如果这个对象本来就是从线程缓存中获取的，因为借走的时候直接remove，归还直接add
         threadLocalList.add(weakThreadLocals ? new WeakReference<>(bagEntry) : bagEntry);
      }
   }

   /**
    * Add a new object to the bag for others to borrow.
    *
    *
    * @param bagEntry an object to add to the bag
    */
   public void add(final T bagEntry)
   {
      if (closed) {
         LOGGER.info("ConcurrentBag has been closed, ignoring add()");
         throw new IllegalStateException("ConcurrentBag has been closed, ignoring add()");
      }
      // 共享集合添加一个元素
      sharedList.add(bagEntry);

      // spin until a thread takes it or none are waiting
      // 如果有线程在等待获取元素，并且刚添加的元素状态属于没有被使用，转移队列添加元素失败则一直循环，直到有线程把这个元素拿走结束循环
      // 其实就是如果等待线程等待获取对象，而且这个新进来的对象处于未使用状态，当前添加线程就手把手把这个对象交接给你，如果交接失败了，就让出CPU，进入循环，在来一次交接
      // 直到交接成功，也就是当前新添加的对象变为使用状态并且等待线程数量没有了
      while (waiters.get() > 0 && bagEntry.getState() == STATE_NOT_IN_USE && !handoffQueue.offer(bagEntry)) {
         Thread.yield();
      }
   }

   /**
    * Remove a value from the bag.  This method should only be called
    * with objects obtained by <code>borrow(long, TimeUnit)</code> or <code>reserve(T)</code>
    *
    * @param bagEntry the value to remove
    * @return true if the entry was removed, false otherwise
    * @throws IllegalStateException if an attempt is made to remove an object
    *         from the bag that was not borrowed or reserved first
    */
   public boolean remove(final T bagEntry)
   {
      // 首先将对象状态由使用状态变为删除状态，如果修改成功，则直接返回，进行后续操作；如果修改修改失败，表示对象状态已经不是使用状态，可能是保留状态
      // 那么继续CAS方式由保留状态修改为删除状态，如果修改成功了，则直接返回，进行后续操作，如果还是修改失败，则继续判断关闭状态，如果已经是关闭状态，则直接返回删除失败，
      // 如果不是删除关闭状态，则继续进行后续操作
      if (!bagEntry.compareAndSet(STATE_IN_USE, STATE_REMOVED) && !bagEntry.compareAndSet(STATE_RESERVED, STATE_REMOVED) && !closed) {
         LOGGER.warn("Attempt to remove an object from the bag that was not borrowed or reserved: {}", bagEntry);
         return false;
      }
      //先从共享队列中删除
      final boolean removed = sharedList.remove(bagEntry);
      if (!removed && !closed) {
         LOGGER.warn("Attempt to remove an object from the bag that does not exist: {}", bagEntry);
      }
      //再从线程本地缓冲中删除
      threadList.get().remove(bagEntry);

      return removed;
   }

   /**
    * Close the bag to further adds.
    */
   @Override
   public void close()
   {
      closed = true;
   }

   /**
    * This method provides a "snapshot" in time of the BagEntry
    * items in the bag in the specified state.  It does not "lock"
    * or reserve items in any way.  Call <code>reserve(T)</code>
    * on items in list before performing any action on them.
    *
    * @param state one of the {@link IConcurrentBagEntry} states
    * @return a possibly empty list of objects having the state specified
    */
   public List<T> values(final int state)
   {
      final List<T> list = sharedList.stream().filter(e -> e.getState() == state).collect(Collectors.toList());
      Collections.reverse(list);
      return list;
   }

   /**
    * This method provides a "snapshot" in time of the bag items.  It
    * does not "lock" or reserve items in any way.  Call <code>reserve(T)</code>
    * on items in the list, or understand the concurrency implications of
    * modifying items, before performing any action on them.
    *
    * @return a possibly empty list of (all) bag items
    */
   @SuppressWarnings("unchecked")
   public List<T> values()
   {
      return (List<T>) sharedList.clone();
   }

   /**
    * The method is used to make an item in the bag "unavailable" for
    * borrowing.  It is primarily used when wanting to operate on items
    * returned by the <code>values(int)</code> method.  Items that are
    * reserved can be removed from the bag via <code>remove(T)</code>
    * without the need to unreserve them.  Items that are not removed
    * from the bag can be make available for borrowing again by calling
    * the <code>unreserve(T)</code> method.
    *
    * @param bagEntry the item to reserve
    * @return true if the item was able to be reserved, false otherwise
    */
   public boolean reserve(final T bagEntry)
   {
      return bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_RESERVED);
   }

   /**
    * This method is used to make an item reserved via <code>reserve(T)</code>
    * available again for borrowing.
    *
    * @param bagEntry the item to unreserve
    */
   @SuppressWarnings("SpellCheckingInspection")
   public void unreserve(final T bagEntry)
   {
      if (bagEntry.compareAndSet(STATE_RESERVED, STATE_NOT_IN_USE)) {
         // spin until a thread takes it or none are waiting
         while (waiters.get() > 0 && !handoffQueue.offer(bagEntry)) {
            Thread.yield();
         }
      }
      else {
         LOGGER.warn("Attempt to relinquish an object to the bag that was not reserved: {}", bagEntry);
      }
   }

   /**
    * Get the number of threads pending (waiting) for an item from the
    * bag to become available.
    *
    * @return the number of threads waiting for items from the bag
    */
   public int getWaitingThreadCount()
   {
      return waiters.get();
   }

   /**
    * Get a count of the number of items in the specified state at the time of this call.
    *
    * @param state the state of the items to count
    * @return a count of how many items in the bag are in the specified state
    */
   public int getCount(final int state)
   {
      int count = 0;
      for (IConcurrentBagEntry e : sharedList) {
         if (e.getState() == state) {
            count++;
         }
      }
      return count;
   }

   public int[] getStateCounts()
   {
      final int[] states = new int[6];
      for (IConcurrentBagEntry e : sharedList) {
         ++states[e.getState()];
      }
      states[4] = sharedList.size();
      states[5] = waiters.get();

      return states;
   }

   /**
    * Get the total number of items in the bag.
    *
    * @return the number of items in the bag
    */
   public int size()
   {
      return sharedList.size();
   }

   public void dumpState()
   {
      sharedList.forEach(entry -> LOGGER.info(entry.toString()));
   }

   /**
    * Determine whether to use WeakReferences based on whether there is a
    * custom ClassLoader implementation sitting between this class and the
    * System ClassLoader.
    *
    * @return true if we should use WeakReferences in our ThreadLocals, false otherwise
    */
   private boolean useWeakThreadLocals()
   {
      try {
         // 如果有系统配置，优先获取系统配置
         if (System.getProperty("com.zaxxer.hikari.useWeakReferences") != null) {   // undocumented manual override of WeakReference behavior
            return Boolean.getBoolean("com.zaxxer.hikari.useWeakReferences");
         }
         // 判断当前对象实例的类加载器是否是应用类加载器，如果不是应用类加载器，则表示适用弱引用线程缓存
         return getClass().getClassLoader() != ClassLoader.getSystemClassLoader();
      }
      catch (SecurityException se) {
         return true;
      }
   }
}
