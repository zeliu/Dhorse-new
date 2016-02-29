package cn.wanda.dataserv.config;

import cn.wanda.dataserv.core.Context;

/**
 * 可以Resolve对象的接口
 *
 * @author haobowei
 */
public interface Resolvable {

    /**
     * 根据上下文Resolve表达式
     *
     * @param context
     * @return
     */
    public abstract boolean resolve(Context context);
}
