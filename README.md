# MeteredBlockingQueue

A modified version of ArrayBlockingQueue that allows one to set a max fill threshold 
that determines when the queue will be emptied.

The queue expects multiple threads enqueuing data and a single dequeuing thread waiting on the drainTo function.  
Other uses may be possible but are not tested.

## Usage
A sample usage is as follows:

```java
// build queue and launch threads
MeteredBlockingQueue<YourDataType> mbq;
...

// main loop for dequeuing data
while (!mbq.isPoisoned() || (mbq.isPoisoned() && mbq.size() != 0)) {
    status = "Waiting";
    List<YourDataType> data = new LinkedList<>();
    if (mbq.drainTo(data, maxWorkCycleTimeNanos) != 0) {
      // Do something with your list of data
      ...
    }
}
```

## License
 
MeteredBlockingQueue is licensed under the is licensed under the MIT License. (See LICENSE) 