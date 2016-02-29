package cn.wanda.dataserv.config;

import java.util.List;

/**
 * 可分裂对象BO
 *
 * @author haobowei
 */
public interface Splittable {
    /**
     * 分裂
     *
     * @return 返回分裂后的对象
     */
    public List<Object> split();
}
