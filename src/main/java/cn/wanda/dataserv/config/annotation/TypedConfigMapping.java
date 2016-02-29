package cn.wanda.dataserv.config.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface TypedConfigMapping {
    TypedConfig[] value();
}
