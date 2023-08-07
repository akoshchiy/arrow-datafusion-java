package com.github.akoshchiy.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.reactivestreams.Publisher;

import java.util.concurrent.CompletableFuture;

public interface DataFrame {

    CompletableFuture<ArrowReader> collect(BufferAllocator allocator);

    Publisher<ArrowReader> executeStream(BufferAllocator allocator);
}
