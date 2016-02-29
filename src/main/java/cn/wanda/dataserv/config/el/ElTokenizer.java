package cn.wanda.dataserv.config.el;

import java.io.IOException;
import java.io.StringReader;

/**
 * 表达式解析器
 *
 * @author haobowei
 */
public class ElTokenizer {
    // expression string
    private String el;
    private StringReader sr;
    private int lastChar;
    private boolean inExpr = false;

    /**
     * @param el表达式字符串
     */
    public ElTokenizer(String el) {
        sr = new StringReader(el);
    }

    /**
     * 迭代返回token
     *
     * @return
     */
    public Token nextToken() {
        try {
            StringBuilder s = new StringBuilder();

            int c = sr.read();

            while (true) {
                if (inExpr) {
                    if (c == '}') {
                        inExpr = false;
                        return new Token(TokenType.FUNCTION, s.toString());
                    }
                    if (c == -1) {
                        // unfinished expression
                        throw new ElParseException("the expression: " + s
                                + " is not valid, missing }");
                    } else {
                        s.append((char) c);
                    }

                } else {
                    // meet the $, but not \$
                    if (c == '$' && lastChar != '\\') {
                        inExpr = true;
                        //start with $
                        //read through { and check
                        c = sr.read();
                        if (c != '{') {
                            throw new ElParseException("the expression: " + s
                                    + " is not valid, missing {");
                        }
                        if (s.length() != 0) {
                            return new Token(TokenType.STRING, s.toString());
                        }
                    } else if (c == -1) {
                        if (s.length() == 0) {
                            return new Token(TokenType.EOF, null);
                        } else {
                            return new Token(TokenType.STRING, s.toString());
                        }
                    } else {
                        s.append((char) c);
                    }
                }
                lastChar = c;
                c = sr.read();
            }
        } catch (IOException e) {
            throw new ElParseException("unknown parse error", e);
        }

    }

}
