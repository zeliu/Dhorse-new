package cn.wanda.dataserv.config.el;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import cn.wanda.dataserv.config.el.macro.MacroExpression;
import cn.wanda.dataserv.config.parse.ConfigElement;
import cn.wanda.dataserv.core.Context;
import lombok.extern.log4j.Log4j;

import cn.wanda.dataserv.config.el.macro.MacroExpression;
import cn.wanda.dataserv.config.el.macro.ResourceMacro;
import cn.wanda.dataserv.config.el.macro.SplitParamMacro;
import cn.wanda.dataserv.config.parse.ConfigElement;
import cn.wanda.dataserv.config.parse.adaptor.CollectionConfigAdaptor;
import cn.wanda.dataserv.core.Context;

/**
 * 宏解析器
 *
 * @author songqian
 */
@Log4j
public class MacroParser {

    private static final Map<String, Class<? extends MacroExpression>> macroPool = new HashMap<String, Class<? extends MacroExpression>>();

    static {
        macroPool.put("sand", ResourceMacro.class);
        macroPool.put("seq", SplitParamMacro.class);
    }

    public MacroParser() {
    }

    public ConfigElement parse(ConfigElement propElem, Context context) {
        ConfigElement result = propElem;
        Queue<ConfigElement> elementQueue = new LinkedList<ConfigElement>();
        try {
            String string = propElem.getAsString();
            ElTokenizer tokenizer = new ElTokenizer(string);
            Token t = null;
            while ((t = tokenizer.nextToken()).getType() != TokenType.EOF) {
                String s = t.getText();
                int firstColon = s.indexOf(":");
                String macroName = null;
                String[] args = null;
                if (firstColon < 0) {
                    macroName = s.trim();
                } else {
                    macroName = s.substring(0, firstColon).trim();
                    String[] argsRaw = s.substring(firstColon + 1).split(",");
                    args = new String[argsRaw.length];
                    for (int i = 0; i < args.length; i++) {
                        args[i] = argsRaw[i].trim();
                    }
                }
                Class<? extends MacroExpression> macroClass = macroPool.get(macroName);
                if (macroClass != null) {
                    MacroExpression macro;
                    try {
                        macro = (MacroExpression) macroClass.newInstance();
                    } catch (Exception e) {
                        throw new ElParseException();
                    }
                    macro.setArgs(args);
                    elementQueue.offer(macro.macroReplace(context));
                } else {
                    elementQueue.offer(new CollectionConfigAdaptor(t.getFullText()));
                }
            }
            if (elementQueue.size() == 1) {
                result = elementQueue.poll();
            } else {
                ConfigElement e = null;
                StringBuilder sb = new StringBuilder();
                while ((e = elementQueue.poll()) != null) {
                    try {
                        sb.append(e.getAsString());
                    } catch (IllegalStateException e1) {
                        log.debug(e1.getMessage());
                    }
                }
                result = new CollectionConfigAdaptor(sb.toString());
            }
        } catch (IllegalStateException e) {
            log.debug(e.getMessage());
        } catch (ElParseException e) {
            log.debug(e.getMessage());
        }
        return result;
    }

}