package com.github.akoshchiy.datafusion;

public class DatafusionException extends RuntimeException {

    public DatafusionException() {
    }

    public DatafusionException(String message) {
        super(message);
    }

    public DatafusionException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatafusionException(Throwable cause) {
        super(cause);
    }
}
