package cn.wanda.dataserv.config.el.macro;

import java.util.Map;

import cn.wanda.dataserv.config.parse.ConfigElement;
import cn.wanda.dataserv.core.Context;
import org.apache.commons.lang.StringUtils;

import cn.wanda.dataserv.config.parse.ConfigElement;
import cn.wanda.dataserv.config.parse.adaptor.CollectionConfigAdaptor;
import cn.wanda.dataserv.core.Context;

public class ResourceMacro implements MacroExpression {

    private static String resource_key = "sand";

    private String resourceNames;

    @Override
    public ConfigElement macroReplace(Context context) {
        // if expression is "resource"
        if (context.get(resource_key) == null || ((Map) context.get(resource_key)).get(this.resourceNames) == null) {
            throw new IllegalArgumentException("resource: " + this.resourceNames + " doesn't exist.");
        }
        ConfigElement resourceValue = new CollectionConfigAdaptor(context.get()).getAsConfigObject().get(resource_key);
        resourceValue = resourceValue.getAsConfigObject().get(this.resourceNames);
        if (!resourceValue.isNull()) {
            return resourceValue;
        }
        return null;
    }

    @Override
    public void setArgs(String[] args) {
        if (args.length > 0 && StringUtils.isNotBlank(args[0])) {
            this.resourceNames = args[0];
        }
    }

}