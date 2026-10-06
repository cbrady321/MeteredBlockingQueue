package com.meteredblockingqueue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Self-contained tests (no external dependencies). Run with:
 * <pre>java -cp build/classes:build/test-classes com.meteredblockingqueue.MeteredBlockingQueueTest</pre>
 * Exits with status 1 if any test fails.
 */
public class MeteredBlockingQueueTest {

   private static final long MS = 1_000_000L;
   private static final long SEC = 1_000 * MS;

   private interface Test { void run() throws Exception; }

   private static int passed, failed;

   public static void main(String[] args) throws Exception {
      run("rejects bad constructor arguments", MeteredBlockingQueueTest::constructorValidation);
      run("FIFO order across wraparound", MeteredBlockingQueueTest::fifoAcrossWraparound);
      run("offer times out when full", MeteredBlockingQueueTest::offerTimesOutWhenFull);
      run("drain returns at once when fill line already reached", MeteredBlockingQueueTest::noWaitWhenAlreadyFilled);
      run("drain returns at once when queue is full", MeteredBlockingQueueTest::noWaitWhenFull);
      run("drain waits for fill line, then returns", MeteredBlockingQueueTest::wakesWhenFillLineReached);
      run("drain returns partial batch after timeout", MeteredBlockingQueueTest::partialBatchAfterTimeout);
      run("drain returns number moved, not c.size()", MeteredBlockingQueueTest::returnsNumberMoved);
      run("poison wakes a waiting drain", MeteredBlockingQueueTest::poisonWakesDrain);
      run("offer rejected after poison", MeteredBlockingQueueTest::offerRejectedAfterPoison);
      run("poison releases blocked producers", MeteredBlockingQueueTest::poisonReleasesBlockedProducers);
      run("c.add failure leaves queue consistent", MeteredBlockingQueueTest::collectionFailureKeepsState);
      run("no items lost with README loop and live producers", MeteredBlockingQueueTest::noLossOnShutdown);
      run("sustained throughput is not timeout-bound", MeteredBlockingQueueTest::throughputNotTimeoutBound);
      System.out.printf("%n%d passed, %d failed%n", passed, failed);
      if (failed > 0) System.exit(1);
   }

   private static void run(String name, Test t) {
      try {
         t.run();
         passed++;
         System.out.println("PASS  " + name);
      } catch (Throwable e) {
         failed++;
         System.out.println("FAIL  " + name + ": " + e);
      }
   }

   private static void check(boolean ok, String msg) {
      if (!ok) throw new AssertionError(msg);
   }

   private static long millisSince(long t0) {
      return (System.nanoTime() - t0) / MS;
   }

   // ---------------------------------------------------------------- tests

   static void constructorValidation() {
      int[][] bad = { {0, 5}, {-1, 5}, {1, 0}, {6, 5} };
      for (int[] b : bad) {
         try {
            new MeteredBlockingQueue<Integer>(b[0], b[1], false);
            throw new AssertionError("accepted fillLevel=" + b[0] + " capacity=" + b[1]);
         } catch (IllegalArgumentException expected) { }
      }
      new MeteredBlockingQueue<Integer>(5, 5, false);
      new MeteredBlockingQueue<Integer>(1, 1, true);
   }

   static void fifoAcrossWraparound() throws Exception {
      MeteredBlockingQueue<Integer> q = new MeteredBlockingQueue<>(1, 4, false);
      int next = 0, expect = 0;
      for (int round = 0; round < 10; round++) {
         for (int k = 0; k < 3; k++) check(q.offer(next++, 0), "offer failed");
         List<Integer> out = new ArrayList<>();
         check(q.drainTo(out, 0) == 3, "expected 3 drained");
         for (int v : out) check(v == expect++, "out of order: " + out);
         check(q.size() == 0, "not empty");
      }
      // partial fill then wrap without a full drain in between
      q = new MeteredBlockingQueue<>(1, 4, false);
      for (int v = 0; v < 4; v++) q.offer(v, 0);
      List<Integer> out = new ArrayList<>();
      q.drainTo(out, 0);
      for (int v = 4; v < 7; v++) q.offer(v, 0);
      q.drainTo(out, 0);
      check(out.equals(List.of(0, 1, 2, 3, 4, 5, 6)), "got " + out);
   }

   static void offerTimesOutWhenFull() throws Exception {
      MeteredBlockingQueue<Integer> q = new MeteredBlockingQueue<>(2, 2, false);
      check(q.offer(1, 0) && q.offer(2, 0), "initial offers");
      check(!q.offer(3, 0), "offer into full queue with zero timeout succeeded");
      long t0 = System.nanoTime();
      check(!q.offer(3, 100 * MS), "offer into full queue succeeded");
      check(millisSince(t0) >= 90, "offer did not wait for its timeout");
      try { q.offer(null, 0); throw new AssertionError("null accepted"); }
      catch (NullPointerException expected) { }
   }

   static void noWaitWhenAlreadyFilled() throws Exception {
      MeteredBlockingQueue<Integer> q = new MeteredBlockingQueue<>(3, 10, false);
      for (int i = 0; i < 5; i++) q.offer(i, 0);
      long t0 = System.nanoTime();
      List<Integer> out = new ArrayList<>();
      int n = q.drainTo(out, 2 * SEC);
      check(n == 5, "drained " + n);
      check(millisSince(t0) < 200, "waited " + millisSince(t0) + "ms although fill line was reached");
   }

   static void noWaitWhenFull() throws Exception {
      MeteredBlockingQueue<Integer> q = new MeteredBlockingQueue<>(3, 3, false);
      for (int i = 0; i < 3; i++) q.offer(i, 0);
      long t0 = System.nanoTime();
      check(q.drainTo(new ArrayList<>(), 2 * SEC) == 3, "wrong count");
      check(millisSince(t0) < 200, "waited " + millisSince(t0) + "ms on a full queue");
   }

   static void wakesWhenFillLineReached() throws Exception {
      MeteredBlockingQueue<Integer> q = new MeteredBlockingQueue<>(3, 10, false);
      Thread p = new Thread(() -> {
         try {
            for (int i = 0; i < 3; i++) { Thread.sleep(50); q.offer(i, 0); }
         } catch (InterruptedException ignored) { }
      });
      long t0 = System.nanoTime();
      p.start();
      List<Integer> out = new ArrayList<>();
      int n = q.drainTo(out, 5 * SEC);
      long ms = millisSince(t0);
      p.join();
      check(n == 3, "drained " + n);
      check(ms >= 100 && ms < 1000, "returned after " + ms + "ms");
   }

   static void partialBatchAfterTimeout() throws Exception {
      MeteredBlockingQueue<Integer> q = new MeteredBlockingQueue<>(5, 10, false);
      q.offer(1, 0);
      q.offer(2, 0);
      long t0 = System.nanoTime();
      List<Integer> out = new ArrayList<>();
      int n = q.drainTo(out, 150 * MS);
      long ms = millisSince(t0);
      check(n == 2 && out.equals(List.of(1, 2)), "got " + out);
      check(ms >= 140, "returned early after " + ms + "ms");
   }

   static void returnsNumberMoved() throws Exception {
      MeteredBlockingQueue<Integer> q = new MeteredBlockingQueue<>(1, 10, false);
      q.offer(42, 0);
      List<Integer> c = new ArrayList<>(List.of(1, 2, 3));
      check(q.drainTo(c, 0) == 1, "did not return 1");
      check(c.size() == 4, "collection not appended to");
      q.poison();
      check(q.drainTo(c, 0) == 0, "empty poisoned queue did not return 0");
   }

   static void poisonWakesDrain() throws Exception {
      for (int i = 0; i < 500; i++) {
         MeteredBlockingQueue<Integer> q = new MeteredBlockingQueue<>(10, 20, false);
         long[] took = new long[1];
         Thread c = new Thread(() -> {
            try {
               long t0 = System.nanoTime();
               q.drainTo(new ArrayList<>(), 5 * SEC);
               took[0] = millisSince(t0);
            } catch (InterruptedException ignored) { }
         });
         c.start();
         if (i % 2 == 0) Thread.yield();
         q.poison();
         c.join();
         check(took[0] < 1000, "poisoned drain took " + took[0] + "ms (lost wakeup)");
      }
   }

   static void offerRejectedAfterPoison() throws Exception {
      MeteredBlockingQueue<Integer> q = new MeteredBlockingQueue<>(2, 10, false);
      q.offer(1, 0);
      q.poison();
      check(q.isPoisoned(), "not poisoned");
      check(!q.offer(2, 0), "offer accepted after poison");
      check(q.size() == 1, "queued item was not kept");
      List<Integer> out = new ArrayList<>();
      check(q.drainTo(out, SEC) == 1 && out.equals(List.of(1)), "residue not drained: " + out);
   }

   static void poisonReleasesBlockedProducers() throws Exception {
      MeteredBlockingQueue<Integer> q = new MeteredBlockingQueue<>(1, 1, false);
      q.offer(0, 0);
      boolean[] result = { true };
      long[] took = new long[1];
      Thread p = new Thread(() -> {
         try {
            long t0 = System.nanoTime();
            result[0] = q.offer(1, 5 * SEC);
            took[0] = millisSince(t0);
         } catch (InterruptedException ignored) { }
      });
      p.start();
      Thread.sleep(50);
      q.poison();
      p.join();
      check(!result[0], "blocked offer succeeded after poison");
      check(took[0] < 1000, "blocked producer waited " + took[0] + "ms after poison");
   }

   static void collectionFailureKeepsState() throws Exception {
      MeteredBlockingQueue<Integer> q = new MeteredBlockingQueue<>(1, 4, false);
      for (int i = 1; i <= 3; i++) q.offer(i, 0);
      Collection<Integer> failing = new ArrayList<Integer>() {
         @Override public boolean add(Integer x) {
            if (x == 2) throw new IllegalStateException("collection full");
            return super.add(x);
         }
      };
      try {
         q.drainTo(failing, 0);
         throw new AssertionError("exception from c.add was swallowed");
      } catch (IllegalStateException expected) { }
      check(failing.size() == 1, "item 1 not transferred");
      check(q.size() == 2, "size is " + q.size() + ", expected 2");
      List<Integer> rest = new ArrayList<>();
      check(q.drainTo(rest, 0) == 2, "wrong count");
      check(rest.equals(List.of(2, 3)), "remaining items wrong: " + rest);
      // queue still usable, including wraparound
      for (int i = 10; i < 14; i++) check(q.offer(i, 0), "offer failed after recovery");
      rest.clear();
      q.drainTo(rest, 0);
      check(rest.equals(List.of(10, 11, 12, 13)), "after recovery: " + rest);
   }

   /** The README consumer loop, with poison() called while producers are still offering. */
   static void noLossOnShutdown() throws Exception {
      for (int run = 0; run < 200; run++) {
         MeteredBlockingQueue<Integer> q = new MeteredBlockingQueue<>(5, 1000, false);
         AtomicLong accepted = new AtomicLong();
         AtomicBoolean stop = new AtomicBoolean();
         Thread[] ps = new Thread[3];
         for (int t = 0; t < ps.length; t++) {
            ps[t] = new Thread(() -> {
               try {
                  while (!stop.get()) if (q.offer(1, 0)) accepted.incrementAndGet();
               } catch (InterruptedException ignored) { }
            });
            ps[t].start();
         }
         long[] consumed = { 0 };
         Thread c = new Thread(() -> {
            try {
               while (!q.isPoisoned() || q.size() != 0) {
                  List<Integer> d = new LinkedList<>();
                  if (q.drainTo(d, MS) != 0) consumed[0] += d.size();
               }
            } catch (InterruptedException ignored) { }
         });
         c.start();
         Thread.sleep(2);
         q.poison();
         c.join();
         stop.set(true);
         for (Thread p : ps) p.join();
         check(accepted.get() == consumed[0],
               "run " + run + ": accepted " + accepted.get() + ", consumed " + consumed[0]);
      }
   }

   /** Consumer busy between drains (so signals arrive while nobody waits); must not stall for the timeout. */
   static void throughputNotTimeoutBound() throws Exception {
      MeteredBlockingQueue<Integer> q = new MeteredBlockingQueue<>(50, 100, false);
      AtomicBoolean stop = new AtomicBoolean();
      Thread[] ps = new Thread[4];
      for (int t = 0; t < ps.length; t++) {
         ps[t] = new Thread(() -> {
            try {
               while (!stop.get()) { q.offer(1, SEC); Thread.sleep(0, 200_000); }
            } catch (InterruptedException ignored) { }
         });
         ps[t].start();
      }
      int slow = 0, drains = 0;
      long end = System.nanoTime() + 2 * SEC;
      while (System.nanoTime() < end) {
         long t0 = System.nanoTime();
         q.drainTo(new ArrayList<>(), 500 * MS);
         if (millisSince(t0) >= 450) slow++;
         drains++;
         Thread.sleep(20);
      }
      stop.set(true);
      for (Thread p : ps) p.join();
      check(slow == 0, slow + " of " + drains + " drains waited out the 500ms timeout");
   }
}
