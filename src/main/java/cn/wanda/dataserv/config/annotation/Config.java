package cn.wanda.dataserv.config.annotation;

import cn.wanda.dataserv.config.ConfigObjectFactory;
import cn.wanda.dataserv.config.DefaultConfigObjectFacotry;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Config annotation 根据此对配置文件进行解析并验证
 *
 * @author haobowei
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Config {
    /**
     * 是否必须输入
     *
     * @return
     */
    RequiredType require() default RequiredType.REQUIRED;

    /**
     * 配置参数名
     *
     * @return
     */
    String name() default "";

    /**
     * 配置参数的工厂类
     *
     * @return
     */
    Class<? extends ConfigObjectFactory> factory() default DefaultConfigObjectFacotry.class;
}
