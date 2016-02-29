package cn.wanda.dataserv.config.el.macro;

import cn.wanda.dataserv.config.parse.ConfigElement;
import cn.wanda.dataserv.core.Context;
import cn.wanda.dataserv.config.parse.ConfigElement;
import cn.wanda.dataserv.core.Context;

public interface MacroExpression {

    ConfigElement macroReplace(Context context);

    void setArgs(String[] args);
}