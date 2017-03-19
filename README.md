# Producer Consumer Simulation with Multi Buffer

## **Use-case:** 

Java Producer and Consumer implementation can be done creating a ThreadPoolExecutor and BlockingQueue where consumers in the thread pool will poll for items in the BlockingQueue where as the producer keeps writing items to the BlockingQueue. 

The **enqueue** and **dequeue** operations in the **BlockingQueue** are **thread-safe** which introduces latency for acquiring and releasing lock, hence the application throughput is compromised.

### **What is multi-buffer producer/consumer solution?**

To reduce locking, producer and consumers will be assigned with there own pool of buffers. Producer keeps writing to one of the available buffer in its pool and singal's the buffer manager to move the filled buffer to the consumer pool and consumer keeps consuming the items from the available buffer and signal's the buffer manager to move the empty buffer to producer pool. This solution is thread-safe with reduced locking activity.
