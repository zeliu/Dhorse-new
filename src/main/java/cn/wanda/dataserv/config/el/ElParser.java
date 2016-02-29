package cn.wanda.dataserv.config.el;

import java.util.HashMap;
import java.util.Map;

import cn.wanda.dataserv.config.el.expression.EnumExpression;
import cn.wanda.dataserv.config.el.expression.RangeExpression;
import cn.wanda.dataserv.config.el.expression.TimeExpression;

/**
 * 表达式解析类
 *
 * @author haobowei
 */
public class ElParser {
    private static final Map<String, Class<? extends Expression>> funcPool = new HashMap<String, Class<? extends Expression>>();

    static {
        //TODO: 配置化
        funcPool.put("time", TimeExpression.class);
        funcPool.put("range", RangeExpression.class);
        funcPool.put("enum", EnumExpression.class);
    }

    public Expression parse(String text) {
        ElTokenizer t = new ElTokenizer(text);
        Token token = null;
        CompositeExpression cExpr = new CompositeExpression();
        token = t.nextToken();
        while (token.getType() != TokenType.EOF) {
            String s = token.getText();
            if (token.getType() == TokenType.FUNCTION) {
                String funcName = null;
                String[] args = null;
                int firstColon = s.indexOf(":");
                if (firstColon < 0) {
                    funcName = s.trim();
                } else {
                    funcName = s.substring(0, firstColon).trim();
                    String[] argsRaw = s.substring(firstColon + 1).split(",");
                    args = new String[argsRaw.length];
                    for (int i = 0; i < args.length; i++) {
                        args[i] = argsRaw[i].trim();
                    }
                }
                Class<? extends Expression> fClass = funcPool.get(funcName);
                FunctionExpression func;
                try {
                    func = (FunctionExpression) fClass.newInstance();
                } catch (Exception e) {
                    throw new ElParseException();
                }
                func.setArgs(args);
                cExpr.add(func);

            }

            if (token.getType() == TokenType.STRING) {
                cExpr.add(new StringConstant(token.getText()));
            }


            token = t.nextToken();
        }

        if (!cExpr.isMulti()) {
            return cExpr.one();
        } else {
            return cExpr;
        }
    }
}
