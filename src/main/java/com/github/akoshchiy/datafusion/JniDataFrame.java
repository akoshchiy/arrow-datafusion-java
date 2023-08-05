package com.github.akoshchiy.datafusion;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.reactivestreams.Publisher;

import java.util.concurrent.CompletableFuture;

class JniDataFrame extends JniObject implements DataFrame {

    private static native void nativeCollect(long runtime, long dataFrame, JniPointerCallback callback);

    private final JniRuntime runtime;

    JniDataFrame(long pointer, JniRuntime runtime) {
        super(pointer);
        this.runtime = runtime;
    }

    @Override
    public CompletableFuture<ArrowReader> collect(BufferAllocator allocator) {
        var future = new CompletableFuture<ArrowReader>();
        nativeCollect(
                runtime.getPointer(),
                getPointer(),
                (pointer, error) -> {
                    if (!handleCallbackError(error, future)) {
                        future.complete(Data.importArrayStream(allocator, ArrowArrayStream.wrap(pointer)));
                    }
                }
        );
        return future;
    }

    @Override
    public Publisher<ArrowReader> executeStream(BufferAllocator allocator) {
        throw new UnsupportedOperationException();
    }

    private boolean handleCallbackError(String error, CompletableFuture<?> future) {
        if (error != null && !error.isEmpty()) {
            future.completeExceptionally(new DatafusionException(error));
            return true;
        }
        return false;
    }
}
