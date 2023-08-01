package com.github.akoshchiy.datafusion;

public interface SessionContext extends AutoCloseable {

    static SessionContext create(Runtime rt) {
        return JniSessionContext.create();
    }
}
