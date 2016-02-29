package cn.wanda.dataserv.config;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.el.ElParseException;
import cn.wanda.dataserv.core.AssertUtils;
import cn.wanda.dataserv.core.ConfigParseException;
import lombok.Data;

import lombok.extern.log4j.Log4j;
import org.apache.commons.beanutils.BeanUtils;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.el.ElParseException;
import cn.wanda.dataserv.config.location.FtpInputLocation;
import cn.wanda.dataserv.core.AssertUtils;
import cn.wanda.dataserv.core.ConfigParseException;

/**
 * 位置配置BO
 *
 * @author haobowei
 */
@Data
@Log4j
public abstract class LocationConfig implements Splittable {

    @Override
    public final List<Object> split() {
        List<Object> list = splitRec(this);
        List<Object> result = new ArrayList<Object>();
        for (Object o : list) {
            LocationConfig fil = (LocationConfig) o;
            try {
                result.addAll(fil.doSplit());
            } catch (Exception e) {
                throw new ConfigParseException("DPump split failed!", e);
            }
        }
        return result;
    }

    protected List<Object> doSplit() {
        List<Object> result = new ArrayList<Object>();
        result.add(this);
        return result;
    }

    protected List<Object> splitRec(Object c) {
        try {
            LinkedList<Object> queue = new LinkedList<Object>();
            Field[] fs = c.getClass().getDeclaredFields();
            // for every fields
            queue.add(c);
            for (Field f : fs) {
                Config confAnno = f.getAnnotation(Config.class);
                if (confAnno == null) {
                    continue;
                }
                f.setAccessible(true);
                Object val = f.get(c);
                if (val == null || val instanceof String || val instanceof Number || val instanceof Collection) {
                    continue;
                } else if (val instanceof Splittable) {
                    Splittable sp = (Splittable) val;
                    List<Object> valFork = sp.split();
                    splitByCartesianProduct(queue, f, val, valFork);
                } else {
                    List<Object> valFork = splitRec(val);
                    splitByCartesianProduct(queue, f, val, valFork);
                }

            }

            return queue;
        } catch (Exception e) {
            throw new ElParseException("resolve failed", e);
        }
    }

    private void splitByCartesianProduct(LinkedList<Object> queue, Field f,
                                         Object val, List<Object> valFork) throws IllegalArgumentException,
            IllegalAccessException {
        int qSize = queue.size();
        for (int i = 0; i < qSize; i++) {
            AssertUtils.assertTrue(valFork.size() > 0,
                    "split error, at least one result should return");
            Object o = queue.pop();
            if (val instanceof Collection) {
                f.set(o, valFork);
                queue.add(o);
            } else {
                for (Object v : valFork) {
                    Object co = cloneObject(o);
                    f.set(co, v);
                    queue.add(co);
                }
            }
        }
    }

    private Object cloneObject(Object c) {
        try {
            return BeanUtils.cloneBean(c);
        } catch (Exception e) {
            throw new ElParseException(e);
        }
    }
}
