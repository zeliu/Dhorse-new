package cn.wanda.dataserv.storage.ram;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import cn.wanda.dataserv.core.Line;
import lombok.extern.log4j.Log4j;

import cn.wanda.dataserv.storage.Storage;

/**
 * Represents a region with two swap spaces, one for storing data which from
 * data source, the other one for storing data which will be transferred to data
 * destination.
 * <br>
 * A classical DoubleCachedQueue, In beginning, space A and space B both
 * empty, then loading task begin to load data to space A, when A is almost
 * full, let the data from data source being loaded to space B, then dumping
 * task begin to dump data from space A to data source. When space A is empty,
 * switch the two spaces for load and dump task. Repeat the above operation.
 */
@Log4j
public class DoubleCachedQueue extends AbstractQueue<Line> implements
        BlockingQueue<Line>, java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private static int default_line_limit = 1000;

    private static long max_cache_size = 67108864L;

    private int lineLimit;

    private long cacheSize;

    private Line[] itemsA;

    private Line[] itemsB;

    private ReentrantLock readLock, writeLock;

    private Condition notFull;

    private Condition awake;

    /**
     * writeArray : in reader's eyes, reader get data from data source and write
     * data to this line array. readArray : in writer's eyes, writer put data to
     * data destination from this line array.
     * <p/>
     * Because of this is doubleQueue mechanism, the two line will exchange when
     * time is suitable.
     */
    private Line[] writeArray, readArray;

    private volatile int writeCount, readCount;

    private int writeArrayTP, readArrayHP;

    private boolean closed = false;

    private int spillSize = 0;

    private long lineRx = 0;

    private long lineTx = 0;

    /**
     * Get info of line number in {@link DoubleCachedQueue} space.
     *
     * @return Information of line number.
     */
    public String info() {
        return String.format("Write Array: %s/%s; Read Array: %s/%s", writeCount, writeArray.length, readCount, readArray.length);
    }

    /**
     * Use the two parameters to construct a {@link DoubleCachedQueue} which hold the
     * swap areas.
     *
     * @param lineLimit Limit of the line number the {@link DoubleCachedQueue} can hold.
     * @param byteLimit Limit of the bytes the {@link DoubleCachedQueue} can hold.
     */
    public DoubleCachedQueue(int lineLimit) {
        if (lineLimit <= 0) {
            this.lineLimit = default_line_limit;
        } else {
            this.lineLimit = lineLimit;
        }
        itemsA = new Line[lineLimit];
        itemsB = new Line[lineLimit];

        readLock = new ReentrantLock();
        writeLock = new ReentrantLock();

        notFull = writeLock.newCondition();
        awake = writeLock.newCondition();

        readArray = itemsA;
        writeArray = itemsB;
        spillSize = lineLimit * 8 / 10;
    }

    public DoubleCachedQueue(long cacheSize) {
        if (cacheSize <= 0) {
            throw new IllegalArgumentException(
                    "Queue initial capacity can't less than 0!");
        }
        this.cacheSize = cacheSize > max_cache_size ? max_cache_size : cacheSize;

        readLock = new ReentrantLock();
        writeLock = new ReentrantLock();

        notFull = writeLock.newCondition();
        awake = writeLock.newCondition();

        readArray = itemsA;
        writeArray = itemsB;
        spillSize = lineLimit * 8 / 10;
    }

    /**
     * Get line number of the {@link DoubleCachedQueue}
     *
     * @return lineLimit Limit of the line number the {@link DoubleCachedQueue} can
     * hold.
     */
    public int getLineLimit() {
        return lineLimit;
    }

    /**
     * Set line number of the {@link DoubleCachedQueue}.
     *
     * @param capacity Limit of the line number the {@link DoubleCachedQueue} can hold.
     */
    public void setLineLimit(int capacity) {
        this.lineLimit = capacity;
    }

    /**
     * Insert one line of record to a apace which buffers the swap data.
     *
     * @param line The inserted line.
     */
    private void insert(Line line) {
        writeArray[writeArrayTP] = line;
        ++writeArrayTP;
        ++writeCount;
        ++lineRx;
    }

    /**
     * Insert a line array(appointed the limit of array size) of data to a apace
     * which buffers the swap data.
     *
     * @param lines Inserted line array.
     * @param size  Limit of inserted size of the line array.
     */
    private void insert(Line[] lines, int size) {
        if (size > 0) {
            System.arraycopy(lines, 0, writeArray, writeArrayTP, size);
            writeArrayTP = writeArrayTP + size;
            writeCount = writeCount + size;
            lineRx = lineRx + size;
        }
//		for (int i = 0; i < size; ++i) {
//			writeArray[writeArrayTP] = lines[i];
//			++writeArrayTP;
//			++writeCount;
//			++lineRx;
//			if(lines[i] != null && lines[i].getLine() != null){
//				byteRx += lines[i].getLine().length();
//			}
//		}
    }

    /**
     * Extract one line of record from the space which contains current data.
     *
     * @return line A line of data.
     */
    private Line extract() {
        Line e = readArray[readArrayHP];
        readArray[readArrayHP] = null;
        ++readArrayHP;
        --readCount;
        ++lineTx;
        return e;
    }

    /**
     * Extract a line array of data from the space which contains current data.
     *
     * @param ea
     * @return Extracted line number of data.
     */
    private int extract(Line[] ea) {
        int readsize = Math.min(ea.length, readCount);
        if (readsize > 0) {
            readCount = readCount - readsize;
            lineTx = lineTx + readsize;
            System.arraycopy(readArray, readArrayHP, ea, 0, readsize);
            readArrayHP = readArrayHP + readsize;
        }
//		for (int i = 0; i < readsize; ++i) {
//			ea[i] = readArray[readArrayHP];
//			readArray[readArrayHP] = null;
//			++readArrayHP;
//			--readCount;
//			++lineTx;
//		}
        return readsize;
    }

    /**
     * switch condition: read queue is empty && write queue is not empty.
     * Notice:This function can only be invoked after readLock is grabbed,or may
     * cause dead lock.
     *
     * @param timeout
     * @param isInfinite whether need to wait forever until some other thread awake it.
     * @return
     * @throws InterruptedException
     */

    private long queueSwitch(long timeout, boolean isInfinite)
            throws InterruptedException {
        writeLock.lock();
        try {
            if (writeCount <= 0) {
                if (closed) {
                    return -2;
                }
                try {
                    if (isInfinite && timeout <= 0) {
                        awake.await();
                        return -1;
                    } else {
                        return awake.awaitNanos(timeout);
                    }
                } catch (InterruptedException ie) {
                    awake.signal();
                    throw ie;
                }
            } else {
                Line[] tmpArray = readArray;
                readArray = writeArray;
                writeArray = tmpArray;

                readCount = writeCount;
                readArrayHP = 0;

                writeCount = 0;
                writeArrayTP = 0;

                notFull.signal();
                // logger.debug("Queue switch successfully!");
                return -1;
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * If exists write space, it will return true, and write one line to the
     * space. otherwise, it will try to do that in a appointed time,when time is
     * out if still failed, return false.
     *
     * @param line    a Line.
     * @param timeout appointed limit time
     * @param unit    time unit
     * @return True if success,False if failed.
     */
    public boolean offer(Line line, long timeout, TimeUnit unit)
            throws InterruptedException {
        if (line == null) {
            throw new NullPointerException();
        }
        long nanoTime = unit.toNanos(timeout);
        writeLock.lockInterruptibly();
        if (itemsA == null || itemsB == null) {
            initArray(line);
        }
        try {
            for (; ; ) {
                if (writeCount < writeArray.length) {
                    insert(line);
                    if (writeCount == 1) {
                        awake.signal();
                    }
                    return true;
                }

                // Time out
                if (nanoTime <= 0) {
                    return false;
                }
                // keep waiting
                try {
                    nanoTime = notFull.awaitNanos(nanoTime);
                } catch (InterruptedException ie) {
                    notFull.signal();
                    throw ie;
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void initArray(Line line) {

        long recordLength = line.getLine().length() * 2;
        long size = cacheSize / recordLength;
        if (size <= 0) {
            size = default_line_limit;
        }
        lineLimit = (int) size;
        itemsA = new Line[(int) size];
        itemsB = new Line[(int) size];
        readArray = itemsA;
        writeArray = itemsB;

    }

    /**
     * If exists write space, it will return true, and write a line array to the
     * space.<br>
     * otherwise, it will try to do that in a appointed time,when time out if
     * still failed, return false.
     *
     * @param lines   line array contains lines of data
     * @param size    Line number needs to write to the space.
     * @param timeout appointed limit time
     * @param unit    time unit
     * @return status of this operation, true or false.
     * @throws InterruptedException if being interrupted during the try limit time.
     */
    public boolean offer(Line[] lines, int size, long timeout, TimeUnit unit)
            throws InterruptedException {
        if (lines == null || lines.length == 0) {
            throw new NullPointerException();
        }
        long nanoTime = unit.toNanos(timeout);
        writeLock.lockInterruptibly();
        if (itemsA == null || itemsB == null) {
            initArray(lines[0]);
        }
        try {
            for (; ; ) {
                if (writeCount + size <= writeArray.length) {
                    insert(lines, size);
                    if (writeCount >= spillSize) {
                        awake.signalAll();
                    }
                    return true;
                }

                // Time out
                if (nanoTime <= 0) {
                    return false;
                }
                // keep waiting
                try {
                    nanoTime = notFull.awaitNanos(nanoTime);
                } catch (InterruptedException ie) {
                    notFull.signal();
                    throw ie;
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Close the synchronized lock and one inner state.
     */
    public void close() {
        writeLock.lock();
        try {
            closed = true;
            awake.signalAll();
        } finally {
            writeLock.unlock();
        }
    }

    public boolean isClosed() {
        return closed;
    }

    /**
     * @param timeout appointed limit time
     * @param unit    time unit
     */
    public Line poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanoTime = unit.toNanos(timeout);
        readLock.lockInterruptibly();

        try {
            for (; ; ) {
                if (readCount > 0) {
                    return extract();
                }

                if (nanoTime <= 0) {
                    return null;
                }
                nanoTime = queueSwitch(nanoTime, true);
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * @param ea      line buffer
     * @param timeout a appointed limit time
     * @param unit    a time unit
     * @return line number of data.if less or equal than 0, means fail.
     * @throws InterruptedException if being interrupted during the try limit time.
     */
    public int poll(Line[] ea, long timeout, TimeUnit unit)
            throws InterruptedException {
        long nanoTime = unit.toNanos(timeout);
        readLock.lockInterruptibly();

        try {
            for (; ; ) {
                if (readCount > 0) {
                    return extract(ea);
                }

                if (nanoTime == -2) {
                    return -1;
                }

                if (nanoTime <= 0) {
                    return 0;
                }
                nanoTime = queueSwitch(nanoTime, false);
            }
        } finally {
            readLock.unlock();
        }
    }

    public Iterator<Line> iterator() {
        return null;
    }

    /**
     * Get size of {@link Storage} in bytes.
     *
     * @return Storage size.
     */
    @Override
    public int size() {
        return (writeCount + readCount);
    }

    @Override
    public int drainTo(Collection<? super Line> c) {
        return 0;
    }

    @Override
    public int drainTo(Collection<? super Line> c, int maxElements) {
        return 0;
    }

    /**
     * If exists write space, it will return true, and write one line to the
     * space.<br>
     * otherwise, it will try to do that in a appointed time(20
     * milliseconds),when time out if still failed, return false.
     *
     * @param line a Line.
     * @see DoubleCachedQueue#offer(Line, long, TimeUnit)
     */
    @Override
    public boolean offer(Line line) {
        try {
            return offer(line, 20, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e1) {
            log.debug(e1.getMessage(), e1);
        }
        return false;
    }

    @Override
    public void put(Line e) throws InterruptedException {
    }

    @Override
    public int remainingCapacity() {
        return 0;
    }

    @Override
    public Line take() throws InterruptedException {
        return null;
    }

    @Override
    public Line peek() {
        return null;
    }

    @Override
    public Line poll() {
        try {
            return poll(20, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.debug(e.getMessage(), e);
        }
        return null;
    }

}
