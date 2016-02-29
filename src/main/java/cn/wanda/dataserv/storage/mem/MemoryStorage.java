package cn.wanda.dataserv.storage.mem;

import java.util.concurrent.ArrayBlockingQueue;

import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.storage.AbstractStorage;

/**
 * for test only
 *
 * @author songqian
 */
public class MemoryStorage extends AbstractStorage {
    //TODO refine implemention
    private static int BUFFER_SIZE = 4096;
    @SuppressWarnings("unused")
    private String name;
    private ArrayBlockingQueue<Line> queue = new ArrayBlockingQueue<Line>(BUFFER_SIZE);

    public MemoryStorage() {
    }


    public boolean putLine(Line line) {
        if (putCompleted) {
            return false;
        }
        return queue.offer(line);
    }

    public Line getLine() {
        while (true) {
            try {
                return queue.take();
            } catch (InterruptedException e) {
                //pass
            }
        }
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }


    @Override
    public boolean putLine(Line[] lines, int size) {
        // TODO Auto-generated method stub
        return false;
    }


    @Override
    public int getLine(Line[] lines) {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public int getLineLimit() {
        return 100;
    }


    @Override
    public void print() {
        // TODO Auto-generated method stub

    }

}
