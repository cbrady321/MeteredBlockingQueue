# MeteredBlockingQueue

A modified version of ArrayBlockingQueue that allows one to set a fill threshold 
that determines when the queue will be emptied.

The queue expects multiple threads enqueuing data and a single thread dequeuing data via the `drainTo` function.
`drainTo(c, maxWaitNanos)` returns as soon as the queue holds at least `fillLevel` items (including when it
already did before the call), when the queue is poisoned, or when `maxWaitNanos` elapses, whichever comes first.
It returns the number of items moved into `c`.

## Usage
A sample usage is as follows:

```java
// fillLevel must be between 1 and capacity
MeteredBlockingQueue<YourDataType> mbq = new MeteredBlockingQueue<>(fillLevel, capacity, false);
...

// producers
if (!mbq.offer(item, timeoutNanos)) {
    // timed out, or the queue has been poisoned
}

// main loop for dequeuing data
while (!mbq.isPoisoned() || mbq.size() != 0) {
    List<YourDataType> data = new LinkedList<>();
    if (mbq.drainTo(data, maxWorkCycleTimeNanos) != 0) {
      // Do something with your list of data
      ...
    }
}
```

## Shutdown

Call `poison()` to shut the queue down. After that, `offer` returns `false` (producers blocked in `offer` are
released and also get `false`), and `drainTo` returns immediately with whatever is still queued. Every item
that `offer` accepted stays in the queue, so the consumer loop above drains them all before it exits.

## Build and test

```
ant test   # compile and run the test suite (no external dependencies)
ant jar    # build dist/MeteredBlockingQueue.jar
```

The library compiles for Java 8; the tests need Java 11 or later.

## License
 
MeteredBlockingQueue is licensed under the MIT License. (See LICENSE) 
