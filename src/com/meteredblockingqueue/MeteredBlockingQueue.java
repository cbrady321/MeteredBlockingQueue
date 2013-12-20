/*
 * MeteredBlockingQueue written by Colin Brady and released under the MIT 
 * license as explained at http://opensource.org/licenses/MIT.
 * The MeteredBlockingQueue is a significant rewrite of ArrayBlockingQueue 
 * which was written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
package com.meteredblockingqueue;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.*;

public class MeteredBlockingQueue<E> implements java.io.Serializable
{
   /**
    * Serialization ID. This class relies on default serialization
    * even for the items array, which is default-serialized, even if
    * it is empty. Otherwise it could not be declared final, which is
    * necessary here.
    */
   private static final long serialVersionUID = -417911632652828426L;
   /** The queued items  */
   private final E[] items;
   /** items index for next take, poll or remove */
   private int takeIndex;
   /** items index for next put, offer, or add. */
   private int putIndex;
   /** Number of items in the queue */
   private int count;
   /** how high the q can fill before draining */
   private int fillLine;
   /** Main lock guarding all access */
   private final ReentrantLock lock;
   /** Condition for waiting takes */
   private final Condition notEmptyEnough;
   /** Condition for waiting puts */
   private final Condition notFull;
   /** shutdown support */
   private AtomicBoolean poisoned;   // TODO convert to volatile

   /*
    * Creates an <tt>MeteredBlockingQueue</tt> with the given (fixed)
    * capacity and the specified access policy.
    * @param capacity the capacity of this queue
    * @param fair if <tt>true</tt> then queue accesses for threads blocked
    *        on insertion or removal, are processed in FIFO order;
    *        if <tt>false</tt> the access order is unspecified.
    * @throws IllegalArgumentException if <tt>capacity</tt> is less than 1
    */
   public MeteredBlockingQueue(int fillLevel, int capacity, boolean fair) {
      if (capacity <= 0 || fillLevel <= 0) {
         throw new IllegalArgumentException();
      }
      this.poisoned = new AtomicBoolean(false);
      this.items = (E[]) new Object[capacity];
      this.fillLine = fillLevel;
      lock = new ReentrantLock(fair);
      notEmptyEnough = lock.newCondition();
      notFull = lock.newCondition();
      clear();
   }
   private void clear() {
      final E[] items = this.items;
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         int i = takeIndex;
         int k = count;
         while (k-- > 0) {
            items[i] = null;
            i = inc(i);
         }
         count = 0;
         putIndex = 0;
         takeIndex = 0;
         notFull.signalAll();
      } finally {
         lock.unlock();
      }
   }

   public boolean isPoisoned(){
      return poisoned.get();
   }
   // big hack (?) used to be two lines
   //      poisoned = true;
   //      notEmptyEnough.signal();
   public void poison() throws InterruptedException {
      poisoned.compareAndSet(false, true);
      final ReentrantLock lock = this.lock;
      lock.lockInterruptibly();
      try{
         notEmptyEnough.signal();
      } finally {
         lock.unlock();
      }
   }
   public int size() {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         return count;
      } finally {
         lock.unlock();
      }
   }

   //////////////////////////////////////////////////////////////////////
   // CPB - support multiple conditions  // formerly drainTo
   // expecting only one consumer thread
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
         try {
            if(!poisoned.get()){  // this section assumes one consumer
               long remain = notEmptyEnough.awaitNanos(drainMaxWaitNanos);
               // System.out.println("Draing Queue with " + remain + "nanos remaining.");
            }
         } catch (InterruptedException ie) {
            // @doubt since there is only one consumer this signal seems unlikely to be useful
            notEmptyEnough.signal(); // propagate to non-interrupted thread
            throw ie;
         }

         // System.out.println("Draining " + this.size() + " elements.");
         int i = takeIndex;
         int n = 0;
         int max = count;
         while (n < max) {
            c.add(items[i]);
            items[i] = null;
            i = inc(i);
            ++n;
         }
         if (n > 0) {
            count = 0;
            putIndex = 0;
            takeIndex = 0;
            notFull.signalAll();
         }
         return c.size();
      } finally {
         lock.unlock();
      }
   }

   /**
    * Inserts the specified element at the tail of this queue, waiting
    * up to the specified wait time for space to become available if
    * the queue is full.
    *
    * @throws InterruptedException {@inheritDoc}
    * @throws NullPointerException {@inheritDoc}
    */
   public boolean offer(E e, long timeoutNanos) throws InterruptedException {
      // System.out.println(new Date().getTime() + ",v:" + e + ", t:" + Thread.currentThread().getName() + ", Offer_A");
      if (e == null) {
         throw new NullPointerException();
      }
      // System.out.println(new Date().getTime() + ",v:" + e + ", t:" + Thread.currentThread().getName() + ", Offer_B");
      final ReentrantLock lock = this.lock;
      lock.lockInterruptibly();
      try {
         for (;;) {
            // System.out.println(new Date().getTime() + ",v:" + e + ", t:" + Thread.currentThread().getName() + ", Offer_C");
            if (count != items.length) {
               // System.out.println(new Date().getTime() + ",v:" + e + ", t:" + Thread.currentThread().getName() + ", Offer_C_Insert");
               insert(e);
               return true;
            }
            if (timeoutNanos <= 0) {
               // System.out.println(new Date().getTime() + ",v:" + e + ", t:" + Thread.currentThread().getName() + ", Offer_C_Fail");
               return false;
            }
            try {
               // System.out.println(new Date().getTime() + ",v:" + e + ", t:" + Thread.currentThread().getName() + ", Offer_C_Wait");
               timeoutNanos = notFull.awaitNanos(timeoutNanos);
            } catch (InterruptedException ie) {
               // System.out.println(new Date().getTime() + ",v:" + e + ", t:" + Thread.currentThread().getName() + ", Offer_C_Interrupted");
               notFull.signal(); // propagate to non-interrupted thread
               throw ie;
            }
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