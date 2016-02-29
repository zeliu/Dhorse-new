package cn.wanda.dataserv.config.el;

/**
 * 表达式解析异常
 *
 * @author haobowei
 */
public class ElParseException extends RuntimeException {

    /**
     *
     */
    private static final long serialVersionUID = -5725647694431311377L;

    public ElParseException() {
    }

    public ElParseException(String message) {
        super(message);
    }

    public ElParseException(Throwable cause) {
        super(cause);
    }

    public ElParseException(String message, Throwable cause) {
        super(message, cause);
    }

}
