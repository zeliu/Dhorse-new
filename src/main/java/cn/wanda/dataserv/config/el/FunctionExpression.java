package cn.wanda.dataserv.config.el;


public interface FunctionExpression extends Expression {
    String getName();

    void setArgs(String[] args);
}
