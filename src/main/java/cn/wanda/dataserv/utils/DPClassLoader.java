package cn.wanda.dataserv.utils;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Set;

import lombok.extern.log4j.Log4j;

@Log4j
public class DPClassLoader extends URLClassLoader {

    private ClassLoader parent;

    private Set<String> excludeclz = new HashSet<String>();

    public DPClassLoader(ClassLoader parent) {
        this(new URL[0], parent);
    }

    public DPClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, null);
        this.parent = parent;
    }

    public void addExcludeclz(Class<?> clz) {
        if (clz != null) {
            this.excludeclz.add(clz.getName());
        }
    }

    @Override
    protected Class<?> findClass(final String name) throws ClassNotFoundException {
        Class<?> clz = null;
        if (!excludeclz.contains(name)) {
            try {
                clz = super.findClass(name);
            } catch (Throwable e) {
                log.debug(e.getMessage(), e);
            }
        }
        if (clz == null) {
            clz = parent.loadClass(name);
        }

        return clz;
    }

}