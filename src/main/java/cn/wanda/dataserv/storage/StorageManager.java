package cn.wanda.dataserv.storage;

import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.process.LineSchema;
import cn.wanda.dataserv.utils.Printable;

/**
 * Storage Manager
 *
 * @author songqian
 */
public interface StorageManager extends Printable {

    Line getLine(String id);

    int getLine(String id, Line[] lines);

    boolean putLine(String id, Line line);

    boolean putLine(String id, Line[] lines, int size);

    void add(String id, Storage storage);

    void remove(String id);

    Storage get(String inputId);

    boolean isEmpty(String id);

    void closeInput(String id);

    void closeInput();

    void setInputLineSchema(LineSchema lineSchema);

    void print();

    void printTotalInfo();

}
