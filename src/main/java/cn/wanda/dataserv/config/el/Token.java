package cn.wanda.dataserv.config.el;

import lombok.Data;

@Data
public class Token {

    private String text;
    private TokenType type;

    public Token(TokenType type, String text) {
        this.type = type;
        this.text = text;
    }

    public String getFullText() {
        if (type == TokenType.FUNCTION) {
            return "${" + text + "}";
        } else {
            return text;
        }
    }
}
