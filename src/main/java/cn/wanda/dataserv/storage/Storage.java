package cn.wanda.dataserv.storage;

import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.config.RuntimeConfig;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.utils.Printable;

/**
 * input与output之间的cache模块, 用于解耦input与output
 *
 * @author songqian
 */
public interface Storage extends Printable {

    /**
     * @param rc
     * @param id storage id, 用于指定input与output之间的拓扑关系
     * @return
     */
    boolean init(RuntimeConfig rc, String id);

    boolean putLine(Line line);

    boolean putLine(Line[] lines, int size);

    Line getLine();

    int getLine(Line[] lines);

    boolean isEmpty();

    void setPutCompleted(boolean close);

    int getLineLimit();
}
