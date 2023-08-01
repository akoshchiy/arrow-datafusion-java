package com.github.akoshchiy.datafusion;

public interface Runtime extends AutoCloseable {

    static Runtime create() {
        return JniRuntime.create();
    }
}
