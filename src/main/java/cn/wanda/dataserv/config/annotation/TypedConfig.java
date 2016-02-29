package cn.wanda.dataserv.config.annotation;

public @interface TypedConfig {
    String type();

    Class className();
}
