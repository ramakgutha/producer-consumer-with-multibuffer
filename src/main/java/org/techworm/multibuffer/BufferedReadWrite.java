package org.techworm.multibuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to copy data from reader to writer using buffers in middle. In case of read error or write error, {@link #join()} method will
 * throw BufferedException wrapping the actual exception.
 * 
 * Thread using this utility is expected to call {@link #start()} (which would start copy process) and then invoke {@link #join()} (which makes current
 * thread wait till copy process is completed). {@link #join()} method invocation ensures the error from read/write is propagated to caller thread.
 * 
 * This utility is written by considering following points:
 * 	1) A sequential read and write (by single thread) will depend on efficiency of read and write. Every read will wait for every write and vice versa.
 * 		Even when one of the operation is faster.
 *  2) Usage of a buffer will make them independent (using reader and writer threads). A faster operation can write/read to buffer at its own speed. 
 *  	But for every read and write, the buffer access needs to be synchronized.
 * 
 * This utility is written by using multiple parallel buffers, so that operations are independent at same time, the synchronization points are minimized. Both
 * read and write buffers will have individual (current) buffer to operate on. When the buffer is full/empty the reader/writer will add current buffer to 
 * writer/reader buffer pool and obtains new buffer. 
 * 
 * @author ramkgutha
 */
public class BufferedReadWrite<T>
{
	private static Logger logger = LoggerFactory.getLogger(BufferedReadWrite.class);
	
	public static final int MIN_BUFFER_COUNT = 4;
	public static final int MIN_BUFFER_SIZE = 3;
	public static final int MIN_WRITER_COUNT = 1;
	
	/**
	 * Reader for reading data from source
	 * @author ramkgutha
	 */
	public static interface Reader<T>
	{
		/**
		 * Reads the next record from source. If end is reached, this method should return null.
		 * @param buffered
		 * @return Next record, if available. Else null
		 */
		public T readNext(BufferedReadWrite<T> buffered) throws Exception;
		
		/**
		 * Should close underlying resources if any
		 */
		public void close() throws Exception;
	}
	
	/**
	 * Writer for writing data to destination
	 * @author ramkgutha
	 */
	public static interface Writer<T>
	{
		/**
		 * Writes "data" to destination.
		 * @param buffered
		 * @param data Data to be writter
		 */
		public void write(BufferedReadWrite<T> buffered, T data) throws Exception;
		
		/**
		 * Writes entire dataSet to destination.
		 * @param buffered
		 * @param data Data to be writer
		 */
		public void writeAll(BufferedReadWrite<T> buffered, List<T> data) throws Exception;
		
		/**
		 * Should close underlying resources if any
		 */
		public void close(boolean errored) throws Exception;
	}
	
	/**
	 * Number of parallel buffers to be used. At a time, a reader will be using one buffer and
	 * a writer will be using different buffer. This will avoid synchronizations (locks) to the extent possible.
	 * 
	 * Too many parallel buffers will create buffers which will never be used simultaneously
	 */
	private int buffersCount = 4;
	
	/**
	 * Size of each parallel buffer. Too long of this size will make reader/writer (during edge condition) to wait for
	 * each other. Too small size of this value, will create multiple synchronization points between reader/writer
	 */
	private int bufferSize = 50;
	
	/**
	 * A wrapper over list. Mainly created for debugging purpose
	 * @author ramkgutha
	 */
	class Buffer
	{
		private ArrayList<T> data;
		
		public Buffer(int size)
		{
			data = new ArrayList<T>(bufferSize);
		}
		
		public void add(T element)
		{
			data.add(element);
		}
		
		public T next()
		{
			//by removing the last element in array list
			//	no internal adjustment will be done in array list thus
			//	improving the performance
			return data.remove(data.size() - 1);
		}
		
		@SuppressWarnings("unchecked")
		public List<T> all()
		{
			// Copy the data into new list
			List<T> clonedData = (List<T>) data.clone();
			
			// Clear the existing list
			data.clear();
			
			return clonedData;
		}
		
		public boolean isEmpty()
		{
			return data.isEmpty();
		}
		
		public int size()
		{
			return data.size();
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 * /
		@Override
		public String toString()
		{
			StringBuilder builder = new StringBuilder(super.toString());
			builder.append(data.toString());

			return builder.toString();
		}
		*/
	}
	
	/**
	 * Manager for managing parallel buffers
	 */
	class BufferManager
	{
		/**
		 * Parallel buffers that can be used by writer (that is buffers already filled by reader)
		 */
		ArrayList<Buffer> bufferForWriteLst;
		
		/**
		 * Lock used while getting one of the parallel write buffer. And also used by reader to add
		 * filled buffers to {@link #bufferForWriteLst}
		 */
		ReentrantLock writeBufferLock = new ReentrantLock();
		
		/**
		 * Condition to be used when writer is waiting for filled buffer to be added to {@link #bufferForWriteLst}
		 */
		Condition waitingForWriteBuffer = writeBufferLock.newCondition();
		
		
		/**
		 * Parallel buffers that can be used by reader (that is buffers already emptied by writer or empty from starting)
		 */
		ArrayList<Buffer> bufferForReadLst;
		
		/**
		 * Lock used while getting one of the parallel read buffer. And also used by write to add
		 * emptied buffers to {@link #bufferForWriteLst}
		 */
		ReentrantLock readBufferLock = new ReentrantLock();
		
		/**
		 * Condition to be used when reader is waiting for emptied buffer to be added to {@link #bufferForReadLst}
		 */
		Condition waitingForReadBuffer = readBufferLock.newCondition();
		
		/**
		 * Current active buffer used for buffer writing (by reader thread)
		 */
		Buffer currentBufferForWrite;
		
		/**
		 * Current active buffers (one per thread) used for buffer reading (by writer thread)
		 */
		ThreadLocal<Buffer> currentBufferForReadThLocal = new ThreadLocal<Buffer>();
		
		/**
		 * Flag indicating writing process (write to this buffer by source reader) is completed
		 */
		boolean writeToBufferCompleted = false;
		
		public BufferManager()
		{
			logger.debug("Creating buffer manager with [Buffer size: "+bufferSize+", Parallel Buffer Count: "+buffersCount+"]");
			
			//create buffer containers
			bufferForWriteLst = new ArrayList<Buffer>();
			bufferForReadLst = new ArrayList<Buffer>();
			
			//create and add buffers ready to accept writes
			for(int i = 0; i < buffersCount; i++)
			{
				bufferForWriteLst.add(new Buffer(bufferSize));
			}
		}
		
		/**
		 * Gets an empty buffer (empty by default or which is drained by reader) and sets
		 * it to {@link #bufferForWriteLst}. Call to this method will obtain write lock
		 */
		void getBufferForWrite() throws InterruptedException
		{
			//obtain the write lock
			writeBufferLock.lock();
			
			try
			{
				//wait till buffers are available for write
				while(bufferForWriteLst.isEmpty())
				{
					//while waiting if error occurs simply set null to "bufferForWrite" which will end 
					if(isErrored())
					{
						logger.trace("Returning empty write buffer because of error");
						
						currentBufferForWrite = null;
						return;
					}
					
					//wait for condition till writer buffer is available
					logger.trace("Waiting for write buffer");
					waitingForWriteBuffer.await();
					
					//After coming from wait, bufferForWriteLst can be empty. This mainly happens in case of error
					if(!bufferForWriteLst.isEmpty())
					{
						break;
					}
				}
			
				//remove the first available for write
				currentBufferForWrite = bufferForWriteLst.remove(0);
			}finally
			{
				writeBufferLock.unlock();
			}
		}
		
		/**
		 * Gets a filled buffer (fully written by writer) and sets
		 * it to {@link #currentBufferForRead}. Call to this method will obtain read lock
		 */
		void getBufferForRead() throws InterruptedException
		{
			//obtain read lock
			readBufferLock.lock();
			
			try
			{
				//wait till buffer is available for read
				while(bufferForReadLst.isEmpty())
				{
					//if write is completed or error occurred while waiting, set null to "bufferForRead" which will end reading thread
					if(writeToBufferCompleted || isErrored())
					{
						//logger.trace("Returning empty read buffer because of error/write complete [Write Completed: {}, Errored: {}]", writeToBufferCompleted, isErrored());
						
						currentBufferForReadThLocal.set(null);
						return;
					}

					//wait for read buffer condition
					logger.trace("Waiting for read buffer");
					waitingForReadBuffer.await();
					
					//After waiting for condition also, the read buffers can be empty. 
					// 	This will happen when error occurs or when write to buffer is completed
					if(!bufferForReadLst.isEmpty())
					{
						//logger.trace("Got next buffer for read");
						break;
					}
				}
				
				//set the buffer available for read
				currentBufferForReadThLocal.set(bufferForReadLst.remove(0));			
			}finally
			{
				readBufferLock.unlock();
			}
		}
		
		/**
		 * Adds the specified value to the buffer
		 * @param value
		 * @throws InterruptedException 
		 */
		public void add(T value) throws InterruptedException
		{
			//if writing is already marked as completed, throw exception
			if(writeToBufferCompleted)
			{
				//this should never happen
				throw new IllegalStateException("Write-to-buffer is already completed.");
			}
			
			//if no current-buffer is not available for write
			if(currentBufferForWrite == null)
			{
				//obtain one of the buffer ready to write
				getBufferForWrite();
				
				//if no buffer is available simple return. This will happen during error
				if(currentBufferForWrite == null)
				{
					return;
				}
			}
			
			//add the value to current buffer
			currentBufferForWrite.add(value);
			
			//if the current buffer is full
			if(currentBufferForWrite.size() >= bufferSize)
			{
				//obtain lock on read buffers
				readBufferLock.lock();
				
				logger.info(Thread.currentThread().getName() + " has acquired lock to acquire buffer to write !!!!");
				
				try
				{
					//add the current-write buffer to read-parallel buffers
					bufferForReadLst.add(currentBufferForWrite);
					
					//make current-write buffer as null, so that next write will obtain new buffer
					currentBufferForWrite = null;
					
					//indicate reader, a buffer is ready for read
					waitingForReadBuffer.signal();
				}finally
				{
					readBufferLock.unlock();
				}
			}
		}

		/**
		 * Removes next available from buffer and returns the same
		 * @return
		 * @throws InterruptedException 
		 */
		public T drain() throws InterruptedException
		{
			//logger.trace("Drain invoked");
			Buffer currentBufferForRead = currentBufferForReadThLocal.get();
			
			//if no current-read buffer is present
			if(currentBufferForRead == null)
			{
				//obtain read buffer from parallel read buffers
				getBufferForRead();
				
				currentBufferForRead = currentBufferForReadThLocal.get();
				//logger.trace("Got buffer: " + currentBufferForRead);
			}
			
			//if failed to obtain read buffer or read buffer is empty, which happens when an error occurs
			// or when writer is completed
			if(currentBufferForRead == null)
			{
				return null;
			}
			
			if(currentBufferForRead.isEmpty())
			{
				return null;
			}

			//remove the first available value
			T res = currentBufferForRead.next();
			
			if(res == null)
			{
				//this should never happen
				throw new NullPointerException("Null value encountered.");
			}
			
			//if the current-read buffer is completed
			if(currentBufferForRead.isEmpty())
			{
				//if writing is in progress
				if(!writeToBufferCompleted)
				{
					//obtain writer buffers lock
					writeBufferLock.lock();
					
					logger.info(Thread.currentThread().getName() + " has acquired lock to acquire to read !!!!");
					
					try
					{
						//add empty buffer to write list for refill
						bufferForWriteLst.add(currentBufferForRead);
						
						//mark current-read buffer as null, so that new buffer is obtained during next read
						currentBufferForReadThLocal.set(null);
						
						//signal to writer, that empty buffer is available
						waitingForWriteBuffer.signal();
					}finally
					{
						writeBufferLock.unlock();
					}
				}
				else
				{
					//if writing is completed
					
					//mark current-read buffer as null, so that new buffer is obtained during next read
					currentBufferForReadThLocal.set(null);
				}
			}
			
			return res;
		}
		
		/**
		 * Removes next available from buffer and returns the same
		 * @return
		 * @throws InterruptedException 
		 */
		public List<T> drainAll() throws InterruptedException
		{
			//logger.trace("Drain invoked");
			Buffer currentBufferForRead = currentBufferForReadThLocal.get();
			
			//if no current-read buffer is present
			if(currentBufferForRead == null)
			{
				//obtain read buffer from parallel read buffers
				getBufferForRead();
				
				currentBufferForRead = currentBufferForReadThLocal.get();
				//logger.trace("Got buffer: " + currentBufferForRead);
			}
			
			//if failed to obtain read buffer or read buffer is empty, which happens when an error occurs
			// or when writer is completed
			if(currentBufferForRead == null)
			{
				return null;
			}
			
			if(currentBufferForRead.isEmpty())
			{
				return null;
			}

			// Get and clear buffer
			List<T> allRes = currentBufferForRead.all();
			if(allRes == null || allRes.isEmpty())
			{
				//this should never happen
				return null;
			}
			
			//if the current-read buffer is completed
			if(currentBufferForRead.isEmpty())
			{
				//if writing is in progress
				if(!writeToBufferCompleted)
				{
					//obtain writer buffers lock
					writeBufferLock.lock();
					
					logger.info(Thread.currentThread().getName() + " has acquired lock to acquire writeBuffer !!!!");
					
					try
					{
						//add empty buffer to write list for refill
						bufferForWriteLst.add(currentBufferForRead);
						
						//mark current-read buffer as null, so that new buffer is obtained during next read
						currentBufferForReadThLocal.set(null);
						
						//signal to writer, that empty buffer is available
						waitingForWriteBuffer.signal();
					}finally
					{
						writeBufferLock.unlock();
					}
				}
				else
				{
					//if writing is completed
					
					//mark current-read buffer as null, so that new buffer is obtained during next read
					currentBufferForReadThLocal.set(null);
				}
			}
			
			return allRes;
		}
		
		boolean isNotEmpty(Buffer buffer)
		{
			if(buffer == null)
			{
				return false;
			}
			
			if(buffer.isEmpty())
			{
				return false;
			}
			
			return true;
		}
		
		/**
		 * Indicates the buffers that writes to buffer (by source reader) is completed.
		 */
		public void writeToBufferCompleted()
		{
			logger.trace("Invoking writeToBufferCompleted(). Write complete was notified");
			
			//obtain read buffer lock
			readBufferLock.lock();
			
			try
			{
				if(isNotEmpty(currentBufferForWrite))
				{
					logger.trace("Adding left over buffer with size: " + currentBufferForWrite.size());
					
					//add current-write buffer to parallel-buffers to read
					bufferForReadLst.add(currentBufferForWrite);
				}
				
				//mark current-write buffer as null so that no more writes are done on this buffer
				//currentBufferForWrite = null;
			
				//writeToBufferCompleted = true;
				
				//indicate reader data is available for read
				waitingForReadBuffer.signalAll();
			}finally
			{
				readBufferLock.unlock();
			}
		}
		
		/**
		 * Indicates an error occurred, which eventually will stop reader and writer
		 */
		public void errorOccurred()
		{
			logger.trace("Invocking errorOccurred(). Error occurred was notified");
			
			//obtain the read lock and signal reader to come out of wait (which in turn will check for error)
			readBufferLock.lock();
			
			try
			{
				waitingForReadBuffer.signalAll();
			}finally
			{
				readBufferLock.unlock();
			}
			
			//obtain the writer lock and signal writer to come out of wait (which in turn will check for error)
			writeBufferLock.lock();
			
			try
			{
				waitingForWriteBuffer.signalAll();
			}finally
			{
				writeBufferLock.unlock();
			}
		}
	}
	
	private BufferManager bufferManager;
	
	private Reader<T> reader;
	private Writer<T> writer;
	
	/**
	 * Flag indicating if the copy process is started
	 */
	private boolean started = false;
	
	Exception readError = null, writeError = null;
	
	private int writerThreadCount = 1;
	
	private Thread readerThread = null;
	private List<Thread> writerThreads = new ArrayList<Thread>();
	
	private int completedWriterCount = 0;
	
	private boolean keepProcessing;
	
	public BufferedReadWrite(Reader<T> reader, Writer<T> writer)
	{
		this.reader = reader;
		this.writer = writer;
	}
	
	public BufferedReadWrite(Reader<T> reader, Writer<T> writer, int bufferSize, int bufferCount, int threadCount, boolean keepProcessing)
	{
		this.reader = reader;
		this.writer = writer;
		this.keepProcessing = keepProcessing;
		setBufferSize(bufferSize);
		setBuffersCount(bufferCount);
		setWriterThreadCount(threadCount);
	}
	
	/**
	 * Sets value for Buffer-size. This represents size of each parallel buffer. 
	 * Too long of this size will make reader/writer (during edge condition) to wait for
	 * each other. Too small size of this value, will create multiple synchronization points between reader/writer.
	 * 
	 * Minimum size for buffer-size is 2.
	 * 
	 * @param bufferSize the bufferSize to set
	 */
	public void setBufferSize(int bufferSize)
	{
		if(bufferSize < MIN_BUFFER_SIZE)
		{
			throw new IllegalArgumentException("Invalid buffer size specified: " + bufferSize + ". Minimum value for buffer size expected is - " + MIN_BUFFER_SIZE);
		}
			
		this.bufferSize = bufferSize;
	}
	
	/**
	 * Gets value of bufferSize 
	 * @return the bufferSize
	 */
	public int getBufferSize()
	{
		return bufferSize;
	}
	
	/**
	 * Sets value for bUFFERS_COUNT. Number of parallel buffers to be used. At a time, a reader will be using one buffer and
	 * a writer will be using different buffer. This will avoid synchronizations (locks) to the extent possible.
	 * 
	 * Too many parallel buffers will create buffers which will never be used simultaneously.
	 * 
	 * Minimum buffer-count is 3.
	 * 
	 * @param bufferCount the bufferCount to set
	 */
	public void setBuffersCount(int bufferCount)
	{
		if(bufferCount < MIN_BUFFER_COUNT)
		{
			throw new IllegalArgumentException("Invalid buffer count specified: " + bufferCount + ". Minimum value for buffer count expected is - " + MIN_BUFFER_COUNT);
		}
		
		this.buffersCount = bufferCount;
	}

	/**
	 * Gets value of buffersCount 
	 * @return the buffersCount
	 */
	public int getBuffersCount()
	{
		return buffersCount;
	}
	
	/**
	 * Sets value for writerThreadCount - Number of writer threads to be created. Minimum value is 1.
	 * @param writerThreadCount the writerThreadCount to set
	 */
	public void setWriterThreadCount(int writerThreadCount)
	{
		if(writerThreadCount < MIN_WRITER_COUNT)
		{
			throw new IllegalArgumentException("Invalid number of writer threads specified: " + writerThreadCount + ". Minimum vlue for weiter thread count expected is - " + MIN_WRITER_COUNT);
		}
		
		this.writerThreadCount = writerThreadCount;
	}
	
	/**
	 * Gets value of writerThreadCount 
	 * @return the writerThreadCount
	 */
	public int getWriterThreadCount()
	{
		return writerThreadCount;
	}
	
	/**
	 * Set keepProcessing
	 * 
	 * This method will be invoked to stop the executing threads
	 * 
	 * @param keepProcessing
	 */
	public void setKeepProcessing(boolean keepProcessing) {
		this.keepProcessing = keepProcessing;
	}
	
	private void writerThreadCompleted()
	{
		completedWriterCount++;
		
		if(completedWriterCount >= writerThreadCount)
		{
			try
			{
				logger.trace("All writer threads completed. Closing writer...");
				
				//close the writer
				writer.close(isErrored());
			}catch(Exception ex)
			{
				//ignore write close error
				logger.warn("An error occurred while closing writer", ex);

				//the first write error should be considered
				if(writeError == null)
				{
					writeError = ex;
				}
				
				bufferManager.errorOccurred();
			}

		}
	}
	
	/**
	 * Starts the copy process. This method should be called only once. From second time this method invocation
	 * throws exception
	 */
	public synchronized void start()
	{
		//if already started throw error
		if(started)
		{
			throw new IllegalStateException("This buffer-read-write is already started");
		}
		
		started = true;
		
		bufferManager = new BufferManager();
		
		//create the reader thread - which reads data from source and writes to buffer-manager
		readerThread = new Thread()
		{
			public void run()
			{
				T data = null;
				
				while(keepProcessing)
				{
					//if an error occurred by reader/writer stop reading
					if(isErrored())
					{
						break;
					}
					
					try
					{
						//read next data
						data = reader.readNext(BufferedReadWrite.this);

						//if data is null, assume read process is completed
						if(data == null)
						{
							logger.info("No data found in MQ ==> "+System.nanoTime());
							
							//inform buffer manager, write to buffer is completed
							bufferManager.writeToBufferCompleted();
							TimeUnit.MILLISECONDS.sleep(1500);
							continue;
						}
						
						//add data to buffer
						bufferManager.add(data);
					}catch(Exception ex)
					{
						logger.error("An error occurred while reading next data", ex);
						
						//on error, halt reading after informing buffer-manager about error
						readError = ex;
						bufferManager.errorOccurred();
						break;
					}
				}
				
				//close the reader
				try
				{
					reader.close();
					logger.trace("Source reader completed...");
				}catch(Exception ex)
				{
					//ignore close error
					logger.warn("An error occurred while closing reader", ex);

					//on error, halt reading after informing buffer-manager about error
					readError = ex;
					bufferManager.errorOccurred();
				}
			}
		};
		
		Runnable writerRunnable = new Runnable()
		{
			public void run()
			{
				List<T> data = null;
				
				while(keepProcessing)
				{
					//if an error occurred during read/writer halt writing 
					if(isErrored())
					{
						break;
					}
					
					try
					{
						//obtain next value from buffer
						data = bufferManager.drainAll();
						
						//if no data is available, stop writer thread
						if(data == null || data.isEmpty())
						{
							logger.info("Data Not found on ==> "+System.nanoTime());
							
							TimeUnit.MILLISECONDS.sleep(2000);
							continue;
						}

						//write data to destination
						writer.writeAll(BufferedReadWrite.this, data);
					}catch(Exception ex)
					{
						logger.error("An error occurred while writing data", ex);
						
						//on error, halt the copy process and set the write error for future reference
						writeError = ex;
						
						bufferManager.errorOccurred();
						break;
					}
				}

				logger.trace("Writer thread completed...");
				
				//writerThreadCompleted();
			}
		};
		
		//start reader and writers
		readerThread.start();
		
		//create and start writer threads
		Thread writerThread = null;
		
		logger.debug("Starting "+writerThreadCount+" writer threads");
		
		for(int i = 0; i < writerThreadCount; i++)
		{
			writerThread = new Thread(writerRunnable, "Buffered_Writer_" + i);
			writerThread.start();
			
			this.writerThreads.add(writerThread);
		}
	}
	
	/**
	 * @return True if an error occurred while reading/writing
	 */
	private boolean isErrored()
	{
		return (readError != null || writeError != null);
	}

	/**
	 * Makes the caller thread till writer thread(s) is/are completed (which happens only after reader is completed).
	 * 
	 * In case of errors during read or write, {@link BufferedException} is thrown encapsulating the actual exception.
	 */
	public void join() throws InterruptedException
	{
		readerThread.join();

		for(Thread writerThread: this.writerThreads)
		{
			writerThread.join();
		}
		
		if(readError != null)
		{
			throw new BufferedException("An error occurred in read thread", readError, true);
		}
		
		if(writeError != null)
		{
			throw new BufferedException("An error occurred in write thread", writeError, false);
		}
	}
	
}
