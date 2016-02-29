package cn.wanda.dataserv.output;

public class FileExistException extends RuntimeException {

    /**
     *
     */
    private static final long serialVersionUID = 2736687190026338105L;

    public FileExistException() {
    }

    public FileExistException(String message) {
        super(message);
    }

    public FileExistException(Throwable cause) {
        super(cause);
    }

    public FileExistException(String message, Throwable cause) {
        super(message, cause);
    }


}