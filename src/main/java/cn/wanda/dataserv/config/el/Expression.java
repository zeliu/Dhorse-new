package cn.wanda.dataserv.config.el;

import java.util.List;

import cn.wanda.dataserv.config.Resolvable;
import cn.wanda.dataserv.process.LineWrapper;
import cn.wanda.dataserv.config.Resolvable;
import cn.wanda.dataserv.config.Splittable;
import cn.wanda.dataserv.process.LineWrapper;

/**
 * 表达式
 *
 * @author haobowei
 */
public interface Expression extends Resolvable, Splittable {

    /**
     * @return 表达式的值
     */
    public List value();

    /**
     * 动态执行表达式
     *
     * @param line 文件中的每一行数据
     * @return
     */
    public Object eval(LineWrapper line);

    /**
     * @return 是否已经resolve
     */
    public boolean resolved();


}
