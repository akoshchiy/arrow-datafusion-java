package com.github.akoshchiy.datafusion;

import java.util.concurrent.CompletableFuture;

public interface SessionContext extends AutoCloseable {

    static SessionContext create(Runtime rt) {
        return JniSessionContext.create((JniRuntime) rt);
    }

    CompletableFuture<DataFrame> sql(String sql);

    CompletableFuture<Void> registerParquet(String name, String path);
}
