
package cn.wanda.dataserv.storage.ram;

import java.io.Writer;
import java.util.concurrent.TimeUnit;

import cn.wanda.dataserv.core.Line;
import lombok.extern.log4j.Log4j;

import cn.wanda.dataserv.config.RuntimeConfig;
import cn.wanda.dataserv.storage.AbstractStorage;
import cn.wanda.dataserv.storage.Storage;


/**
 * A concrete storage which use RAM memory space to store the swap spaces.
 * It provides a high-speed, safe way to realize data exchange.
 *
 * @author songqian
 * @see {@link Storage}
 * @see {@link DoubleCachedQueue}
 */
@Log4j
public class FastStorage extends AbstractStorage {

    public FastStorage() {
    }

    private DoubleCachedQueue queue = null;

    private final int waitTime = 3000;

    public boolean init(RuntimeConfig rc, String id) {
        super.init(rc, id);
        int lineLimit = rc.getLineLimit();
        long cacheSize = rc.getByteLimit();
        if (cacheSize > 1000) {
            this.queue = new DoubleCachedQueue(cacheSize);
            return true;
        }
        if (lineLimit <= 0) {
            lineLimit = 1000;
        }

        this.queue = new DoubleCachedQueue(lineLimit);
        return true;
    }

    /**
     * Push one line into {@link Storage}, used by {@link cn.wanda.dataserv.storage.StorageManager}
     *
     * @param line One line of record which will push into storage, see {@link Line}
     * @return true for OK, false for failure.
     */
    @Override
    public boolean putLine(Line line) {
        if (this.putCompleted)
            return false;
        try {
            while (queue.offer(line, waitTime, TimeUnit.MILLISECONDS) == false) {
                log.debug("input was refused by Storage.");
            }
        } catch (InterruptedException e) {
            log.debug(e.getMessage(), e);
            log.warn("thread interrupted.");
            return false;
        }

        return true;
    }

    /**
     * Push multiple lines into {@link Storage}
     *
     * @param lines multiple lines of records which will push into storage, see {@link Line}
     * @param size  limit of line number to be pushed.
     * @return true for OK, false for failure.
     */
    @Override
    public boolean putLine(Line[] lines, int size) {
        if (this.putCompleted) {
            return false;
        }

        try {
            while (this.queue.offer(lines, size, waitTime, TimeUnit.MILLISECONDS) == false) {
                log.debug("input was refused by Storage.");
            }
        } catch (InterruptedException e) {
            return false;
        }

        return true;
    }

    /**
     * Pull one line from {@link Storage}, used by {@link Writer}
     *
     * @return one {@link Line} of record.
     */
    @Override
    public Line getLine() {
        Line line = null;
        try {
            while ((line = queue.poll(waitTime, TimeUnit.MILLISECONDS)) == null) {
                if (queue.isClosed()) {
                    line = Line.EOF;
                    break;
                }
                log.debug("output was refused by Storage.");
            }
        } catch (InterruptedException e) {
            log.debug(e.getMessage(), e);
            return null;
        }
        return line;
    }

    /**
     * Pull multiple lines from {@link Storage}, used by {@link Writer}
     *
     * @return number of lines pulled
     * @param    lines an empty array which will be filled with multiple {@link Line} as the result.
     */
    @Override
    public int getLine(Line[] lines) {
        int readNum = 0;
        try {
            while ((readNum = queue.poll(lines, waitTime, TimeUnit.MILLISECONDS)) <= 0) {
                if (queue.isClosed()) {
                    lines[0] = Line.EOF;
                    return 1;
                }
                log.debug("output was refused by Storage.");
            }
        } catch (InterruptedException e) {
            return 0;
        }
        if (readNum == -1) {
            return 0;
        }
        return readNum;
    }

    /**
     * Get the used byte size of {@link Storage}.
     *
     * @return Used byte size of storage.
     */
    private int size() {
        return queue.size();
    }

    /**
     * Check whether the storage space is empty or not.
     *
     * @return true if empty, false if not empty.
     */
    @Override
    public boolean isEmpty() {
        return (size() <= 0);
    }

    /**
     * Get line number of the {@link Storage}
     *
     * @return Limit of the line number the {@link Storage} can hold.
     */
    @Override
    public int getLineLimit() {
        return queue.getLineLimit();
    }

    /**
     * print stat info periodicity
     */
    public void print() {
        log.info(String.format("cache info : %s", queue.info()));
    }

    /**
     * Set push state closed.
     *
     * @param close A boolean value represents the wanted state of push.
     */
    public void setPutCompleted(boolean close) {
        super.setPutCompleted(close);
        queue.close();
    }
}
