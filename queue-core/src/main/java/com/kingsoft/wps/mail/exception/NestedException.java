package com.kingsoft.wps.mail.exception;

/**
 * Created by 刘春龙 on 2017/6/6.
 */
public class NestedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private ErrorMessage errorMessage;

    public NestedException() {
        super();
    }

    public NestedException(String message) {
        super(message);
    }

    public NestedException(String message, Throwable cause) {
        super(message, cause);
    }

    public NestedException(Throwable cause) {
        super(cause);
    }

    public NestedException(ErrorMessage errorMessage) {
        super(errorMessage.getErrorMsg());
        this.errorMessage = errorMessage;
    }

    public NestedException(ErrorMessage errorMessage, String message) {
        super(message);
        this.errorMessage = errorMessage;
    }

    public NestedException(ErrorMessage errorMessage, String message, Throwable cause) {
        super(message, cause);
        this.errorMessage = errorMessage;
    }

    public NestedException(ErrorMessage errorMessage, Throwable cause) {
        super(cause);
        this.errorMessage = errorMessage;
    }

    public ErrorMessage getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(ErrorMessage errorMessage) {
        this.errorMessage = errorMessage;
    }

    public Throwable getRootCause() {
        Throwable t = this;
        while (true) {
            Throwable cause = t.getCause();
            if (cause != null) {
                t = cause;
            } else {
                break;
            }
        }
        return t;
    }
}
