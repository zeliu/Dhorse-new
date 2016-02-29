package cn.wanda.dataserv.core;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import cn.wanda.dataserv.config.DPumpConfig;
import cn.wanda.dataserv.config.Resolvable;
import cn.wanda.dataserv.config.Splittable;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.el.ElParseException;
import cn.wanda.dataserv.config.el.ElParser;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.el.MacroParser;
import cn.wanda.dataserv.config.parse.ConfigArray;
import cn.wanda.dataserv.config.parse.ConfigElement;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;

import cn.wanda.dataserv.config.ConfigObjectFactory;
import cn.wanda.dataserv.config.DPumpConfig;
import cn.wanda.dataserv.config.Resolvable;
import cn.wanda.dataserv.config.Splittable;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.annotation.TypedConfig;
import cn.wanda.dataserv.config.annotation.TypedConfigMapping;
import cn.wanda.dataserv.config.el.ElParseException;
import cn.wanda.dataserv.config.el.ElParser;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.el.MacroParser;
import cn.wanda.dataserv.config.el.StringConstant;
import cn.wanda.dataserv.config.parse.ConfigArray;
import cn.wanda.dataserv.config.parse.ConfigElement;

public class ConfigParser {
    // Map<Class, ConfigPopulator> populatorMap = new HashMap<Class,
    // ConfigPopulator>();
    ElParser parser = new ElParser();
    MacroParser mParser = new MacroParser();
    Context context = new Context();

    public ConfigParser() {
    }

    public void setContext(Context ctx) {
        this.context.put(ctx.get());
    }

    public void clearContext() {
        this.context = new Context();
    }

    public DPumpConfig parse(ConfigElement ce) {
        Map contextMap = (Map) ce.getAsConfigObject().get("context").getObj();
        context.put(contextMap);
        DPumpConfig c = new DPumpConfig();
        populate(ce, c);
        return c;
    }

    private void populate(ConfigElement ce, Object c) {
        try {
            Field[] fs = c.getClass().getDeclaredFields();
            for (Field f : fs) {
                Class pClass = f.getType();
                String pName = f.getName();
                Config confAnno = f.getAnnotation(Config.class);
                if (confAnno == null)
                    continue;
                pName = ("".equals(confAnno.name()) ? pName : confAnno.name());
                if (confAnno.require() == RequiredType.OPTIONAL
                        && ce.getAsConfigObject().get(pName).isNull())
                    continue;
                if (confAnno.require() == RequiredType.REQUIRED
                        && ce.getAsConfigObject().get(pName).isNull()) {
                    throw new IllegalArgumentException(c.getClass().getName() + "." + pName + " is Required, "
                            + ce);
                }
                ConfigElement propElem = ce.getAsConfigObject().get(pName);
                propElem = mParser.parse(propElem, context);
                Class<? extends ConfigObjectFactory> factoryClass = confAnno.factory();
                ConfigObjectFactory factory = factoryClass.newInstance();
                Object propValue = convertPropValue(pClass, f.getGenericType(),
                        factory, propElem);
                f.setAccessible(true);
                f.set(c, propValue);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private Object convertMacroValue() {
        return null;
    }

    private Object convertPropValue(Class pClass, Type genericType,
                                    ConfigObjectFactory factory, ConfigElement propElem)
            throws Exception {
        Object propValue = null;
        if (Integer.class.equals(pClass)) {
            propValue = propElem.getAsInt();
        } else if (String.class.equals(pClass)) {
            propValue = propElem.getAsString();
        } else if (pClass.isEnum()) {
            final Enum<?>[] values;
            try {
                values = (Enum[]) pClass.getMethod("values").invoke(null);
            } catch (Exception e) {
                throw new IllegalStateException("World seems to be broken, unable to access <EnumType>.values() static method", e);
            }
            String tmp = propElem.getAsString();
            if (tmp == null) {
                propValue = null;
            } else {
                for (Object o : values) {
                    if (tmp.equalsIgnoreCase(o.toString())) {
                        propValue = o;
                    }
                }
            }
        } else if (Expression.class.equals(pClass)) {
            if (StringUtils.isNotBlank(propElem.getAsString())) {
                propValue = parser.parse(propElem.getAsString());
            }
        } else if (List.class.equals(pClass)) {
            List propList = new ArrayList();
            ConfigArray configArr = propElem.getAsConfigArray();
            if (!(genericType instanceof ParameterizedType)) {
                // no List<List<T>> allowed
                throw new IllegalArgumentException("generic type needed");
            }
            Class listItemClass = (Class) ((ParameterizedType) genericType)
                    .getActualTypeArguments()[0];
            for (int i = 0; i < configArr.size(); i++) {
                ConfigElement listItemCE = configArr.get(i);
                propList.add(convertPropValue(listItemClass, null, factory,
                        listItemCE));
            }
            propValue = propList;
        } else {
            Object pObject = factory.create(propElem, pClass);
            ConfigElement propCe = propElem.getAsConfigObject();
            populate(propCe, pObject);
            propValue = pObject;
        }
        return propValue;
    }

    /**
     * resolve all the field in the Config
     *
     * @param c
     */
    private void resolveRec(Object c) {
        try {
            Field[] fs = c.getClass().getDeclaredFields();
            for (Field f : fs) {
                Config confAnno = f.getAnnotation(Config.class);
                if (confAnno == null)
                    continue;
                f.setAccessible(true);
                Object val = f.get(c);
                if (val == null) {
                    continue;
                }
                resolve(val);
            }
        } catch (Exception e) {
            throw new ElParseException("resolve failed", e);
        }
    }

    public void resolve(Object c) {
        try {
            if (c instanceof Resolvable) {
                ((Resolvable) c).resolve(context);
            } else if (c instanceof Collection) {
                Iterator i = ((Collection) c).iterator();
                while (i.hasNext()) {
                    Object o = i.next();
                    resolve(o);
                }
            } else {
                resolveRec(c);
            }
        } catch (Exception e) {
            throw new ElParseException("resolve failed", e);
        }
    }

    private List<Object> splitRec(Object c) {
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
                //TODO 一些jdk自带类型需要直接跳过，目前处理方式比较简单粗暴
                if (val == null || val instanceof String || val instanceof Number)
                    continue;
                List<Object> valFork = split(val);
                int qSize = queue.size();
                for (int i = 0; i < qSize; i++) {
                    AssertUtils.assertTrue(valFork.size() > 0,
                            "split error, at least one result should return");
                    //TODO 为什么弹出来不要了呢？
                    //TODO 如果变量是Collection类型需要特殊处理
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
            return queue;
        } catch (Exception e) {
            throw new ElParseException("resolve failed", e);
        }
    }

    private Object cloneObject(Object c) {
        try {
            //TODO clone this不明白，应该是clone c么？
            return BeanUtils.cloneBean(c);
        } catch (Exception e) {
            throw new ElParseException(e);
        }
    }

    public List<Object> split(Object val) {
        if (val instanceof Splittable) {
            Splittable sp = (Splittable) val;
            return sp.split();
        } else if (val instanceof Collection) {
            Collection<Object> ori = (Collection<Object>) val;
            List<Object> ret = new ArrayList<Object>();
            for (Object o : ori) {
                ret.addAll(split(o));
            }
            return ret;
        } else {
            return splitRec(val);
        }
    }

}
