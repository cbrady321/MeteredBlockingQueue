/*
 * MeteredBlockingQueue written by Colin Brady and released under the MIT
 * license as explained at http://opensource.org/licenses/MIT.
 * The MeteredBlockingQueue is a significant rewrite of ArrayBlockingQueue
 * which was written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
package com.meteredblockingqueue;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A bounded, array-backed queue for many producer threads and a single
 * consumer thread. The consumer removes items in batches with
 * {@link #drainTo(Collection, long)}, which returns as soon as the queue
 * holds at least {@code fillLevel} items, when the queue is poisoned, or
 * when the maximum wait time elapses, whichever comes first.
 *
 * <p>Shutdown: after {@link #poison()} the queue rejects new items
 * ({@link #offer} returns {@code false}), so every item that was accepted
 * is still in the queue for the consumer to drain.
 */
public class MeteredBlockingQueue<E> implements java.io.Serializable
{
   private static final long serialVersionUID = -417911632652828426L;
   /** The queued items  */
   private final E[] items;
   /** items index for next take */
   private int takeIndex;
   /** items index for next put */
   private int putIndex;
   /** Number of items in the queue */
   private int count;
   /** How many items must be queued before a waiting drain is released */
   private final int fillLine;
   /** Main lock guarding all access */
   private final ReentrantLock lock;
   /** Condition for the waiting drain: count reached fillLine, or poisoned */
   private final Condition notEmptyEnough;
   /** Condition for waiting puts */
   private final Condition notFull;
   /** Shutdown flag. Written only while holding lock. */
   private final AtomicBoolean poisoned;

   /**
    * Creates a <tt>MeteredBlockingQueue</tt> with the given fill level,
    * (fixed) capacity and access policy.
    *
    * @param fillLevel the number of queued items at which a waiting
    *        {@link #drainTo(Collection, long)} is released; must be
    *        between 1 and <tt>capacity</tt>
    * @param capacity the capacity of this queue
    * @param fair if <tt>true</tt> then queue accesses for threads blocked
    *        on insertion or removal, are processed in FIFO order;
    *        if <tt>false</tt> the access order is unspecified.
    * @throws IllegalArgumentException if <tt>capacity</tt> is less than 1,
    *         or <tt>fillLevel</tt> is less than 1 or greater than
    *         <tt>capacity</tt>
    */
   @SuppressWarnings("unchecked")
   public MeteredBlockingQueue(int fillLevel, int capacity, boolean fair) {
      if (capacity <= 0) {
         throw new IllegalArgumentException("capacity must be at least 1: " + capacity);
      }
      if (fillLevel <= 0 || fillLevel > capacity) {
         throw new IllegalArgumentException(
               "fillLevel must be between 1 and capacity (" + capacity + "): " + fillLevel);
      }
      this.poisoned = new AtomicBoolean(false);
      this.items = (E[]) new Object[capacity];
      this.fillLine = fillLevel;
      lock = new ReentrantLock(fair);
      notEmptyEnough = lock.newCondition();
      notFull = lock.newCondition();
   }

   /**
    * Returns <tt>true</tt> once {@link #poison()} has been called.
    */
   public boolean isPoisoned(){
      return poisoned.get();
   }

   /**
    * Shuts the queue down. From now on {@link #offer} rejects new items,
    * producers blocked in {@link #offer} return <tt>false</tt>, and
    * {@link #drainTo(Collection, long)} drains whatever is queued
    * without waiting. Items already in the queue are kept for the consumer.
    *
    * @throws InterruptedException never thrown; declared for source
    *         compatibility with earlier versions
    */
   public void poison() throws InterruptedException {
      final ReentrantLock lock = this.lock;
      // Not interruptible: shutdown must always take effect.
      lock.lock();
      try{
         // Set under the lock so offer() (which checks it under the lock)
         // can never insert an item after the consumer has seen the flag.
         poisoned.set(true);
         notEmptyEnough.signalAll();
         notFull.signalAll();
      } finally {
         lock.unlock();
      }
   }

   /**
    * Returns the number of items in this queue.
    */
   public int size() {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         return count;
      } finally {
         lock.unlock();
      }
   }

   /**
    * Waits until the queue holds at least <tt>fillLevel</tt> items, the
    * queue is poisoned, or <tt>drainMaxWaitNanos</tt> elapses, then removes
    * every queued item and adds it to <tt>c</tt>. Returns immediately if
    * the fill level is already reached or the queue is poisoned.
    * Intended for a single consumer thread.
    *
    * <p>If <tt>c.add</tt> throws, the items added before the failure are
    * removed from this queue and the rest stay queued.
    *
    * @param c the collection to transfer items into
    * @param drainMaxWaitNanos the longest time to wait for the fill level
    * @return the number of items transferred
    * @throws InterruptedException if interrupted while waiting
    * @throws NullPointerException if <tt>c</tt> is null
    * @throws IllegalArgumentException if <tt>c</tt> is this queue
    */
   public int drainTo(Collection<? super E> c, long drainMaxWaitNanos) throws InterruptedException {
      if (c == null) {
         throw new NullPointerException();
      }
      if (c == this) {
         throw new IllegalArgumentException();
      }
      final E[] items = this.items;
      final ReentrantLock lock = this.lock;
      lock.lockInterruptibly();
      try {
         // Check the condition before waiting: a signal sent while the
         // consumer was not waiting is lost, and awaitNanos can also
         // return spuriously.
         long nanos = drainMaxWaitNanos;
         while (count < fillLine && !poisoned.get() && nanos > 0) {
            nanos = notEmptyEnough.awaitNanos(nanos);
         }

         int i = takeIndex;
         int n = 0;
         final int max = count;
         try {
            while (n < max) {
               c.add(items[i]);
               items[i] = null;
               i = inc(i);
               ++n;
            }
         } finally {
            // Runs even if c.add throws, so the queue state always
            // reflects exactly the items that were transferred.
            if (n > 0) {
               count -= n;
               takeIndex = i;
               if (count == 0) {
                  takeIndex = 0;
                  putIndex = 0;
               }
               notFull.signalAll();
            }
         }
         return n;
      } finally {
         lock.unlock();
      }
   }

   /**
    * Inserts the specified element at the tail of this queue, waiting
    * up to the specified wait time for space to become available if
    * the queue is full.
    *
    * @param e the element to add
    * @param timeoutNanos the longest time to wait for space
    * @return <tt>true</tt> if the element was added, <tt>false</tt> if the
    *         wait time elapsed before space was available or the queue
    *         is poisoned
    * @throws InterruptedException if interrupted while waiting
    * @throws NullPointerException if the specified element is null
    */
   public boolean offer(E e, long timeoutNanos) throws InterruptedException {
      if (e == null) {
         throw new NullPointerException();
      }
      final ReentrantLock lock = this.lock;
      lock.lockInterruptibly();
      try {
         for (;;) {
            if (poisoned.get()) {
               return false;
            }
            if (count != items.length) {
               insert(e);
               return true;
            }
            if (timeoutNanos <= 0) {
               return false;
            }
            timeoutNanos = notFull.awaitNanos(timeoutNanos);
         }
      } finally {
         lock.unlock();
      }
   }

   /**
    * Inserts element at current put position, advances, and signals.
    * Call only when holding lock.
    */
   private void insert(E x) {
      items[putIndex] = x;
      putIndex = inc(putIndex);
      ++count;
      if (count >= fillLine) {
         notEmptyEnough.signal();
      }
   }

   /**
    * Circularly increment i.
    */
   final int inc(int i) {
      return (++i == items.length) ? 0 : i;
   }
}
