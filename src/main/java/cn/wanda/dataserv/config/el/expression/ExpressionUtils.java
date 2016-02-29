package cn.wanda.dataserv.config.el.expression;

import java.util.List;

import org.apache.commons.lang.StringUtils;

import cn.wanda.dataserv.config.el.Expression;

public class ExpressionUtils {

    /**
     * @param e
     * @return empty string if expression is null or expression.value() is null or empty
     */
    public static String getOneValue(Expression e) {
        String result = "";
        if (e != null && e.resolved()) {
            List list = e.value();
            if (list != null && list.size() > 0) {
                result = (String) list.get(0);
                if (StringUtils.isBlank(result)) {
                    result = "";
                }
            }
        }
        return result;
    }
}