package com.github.akoshchiy.datafusion;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

class JniSessionContext extends JniObject implements SessionContext {

    private static native long nativeCreate();

    private static native void nativeDestroy(long ctx);

    private static native void nativeSql(long runtime, long ctx, String sql, JniPointerCallback callback);

    private static native void nativeRegisterParquet(long runtime, long ctx, String name,
                                                     String path, JniVoidCallback callback);

    private final JniRuntime runtime;

    static JniSessionContext create(JniRuntime runtime) {
        return new JniSessionContext(nativeCreate(), runtime);
    }

    JniSessionContext(long pointer, JniRuntime runtime) {
        super(pointer);
        this.runtime = runtime;
    }

    @Override
    public void close() {
        nativeDestroy(getPointer());
    }

    @Override
    public CompletableFuture<DataFrame> sql(String sql) {
        var future = new CompletableFuture<DataFrame>();
        nativeSql(
                runtime.getPointer(),
                getPointer(),
                sql,
                (pointer, error) -> {
                    if (!handleCallbackError(error, future)) {
                        future.complete(new JniDataFrame(pointer, runtime));
                    }
                }
        );
        return future;
    }

    @Override
    public CompletableFuture<Void> registerParquet(String name, String path) {
        var future = new CompletableFuture<Void>();
        nativeRegisterParquet(
                runtime.getPointer(),
                getPointer(),
                name,
                path,
                (error) -> {
                    if (!handleCallbackError(error, future)) {
                        future.complete(null);
                    }
                }
        );
        return future;
    }

    private boolean handleCallbackError(String error, CompletableFuture<?> future) {
        if (error != null && !error.isEmpty()) {
            future.completeExceptionally(new DatafusionException(error));
            return true;
        }
        return false;
    }
}
