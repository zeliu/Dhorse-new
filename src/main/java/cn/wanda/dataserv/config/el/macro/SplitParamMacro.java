package cn.wanda.dataserv.config.el.macro;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cn.wanda.dataserv.config.parse.ConfigElement;
import cn.wanda.dataserv.core.Context;
import org.apache.commons.lang.StringUtils;

import cn.wanda.dataserv.config.parse.ConfigElement;
import cn.wanda.dataserv.config.parse.adaptor.CollectionConfigAdaptor;
import cn.wanda.dataserv.core.Context;

public class SplitParamMacro implements MacroExpression {

    private static String runtime_key = "runtime";
    private static String split_param_key = "N";

    private static final String DIFF_PATTERN = "([+|-]?)(\\d+)";

    private String diff;

    @Override
    public ConfigElement macroReplace(Context context) {
        // if expression is "resource"
        if (context.get(runtime_key) == null || ((Map) context.get(runtime_key)).get(split_param_key) == null) {
            throw new IllegalArgumentException("context param: " + split_param_key + " doesn't exist.");
        }
        ConfigElement runtimeValue = new CollectionConfigAdaptor(context.get()).getAsConfigObject().get(runtime_key);
        String value = ((Map) runtimeValue.getObj()).get(split_param_key).toString();
        Integer intValue = new Double(Double.parseDouble(value)).intValue();
        ConfigElement result = null;
        if (StringUtils.isNotBlank(diff)) {
            result = new CollectionConfigAdaptor(this.calcSplitParam(intValue));
        } else {
            result = new CollectionConfigAdaptor(intValue);
        }
        return result;
    }

    private Integer calcSplitParam(Integer splitParam) {
        if (StringUtils.isBlank(this.diff)) {
            return splitParam;
        }
        Pattern p = Pattern.compile(DIFF_PATTERN);
        Matcher m = p.matcher(this.diff);
        while (m.find()) {
            String op = m.group(1);
            String num = m.group(2);

            if ("-".equals(op)) {
                return splitParam - Integer.parseInt(num);
            } else {
                return splitParam + Integer.parseInt(num);
            }
        }
        return splitParam;
    }

    @Override
    public void setArgs(String[] args) {
        if (args.length > 1 && StringUtils.isNotBlank(args[1])) {
            this.diff = args[1];
        } else {
            this.diff = "";
        }
    }

}