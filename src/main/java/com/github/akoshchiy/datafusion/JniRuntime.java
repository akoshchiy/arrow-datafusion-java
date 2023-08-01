package com.github.akoshchiy.datafusion;

import io.questdb.jar.jni.JarJniLoader;

class JniRuntime implements Runtime {

    private static native long createRuntime();

    private static native void destroyRuntime(long pointer);

    static {
        JarJniLoader.loadLib(
                JniRuntime.class,
                "/jni",
                "arrow_datafusion_jni"
        );
    }

    private final long pointer;

    public static JniRuntime create() {
        return new JniRuntime(createRuntime());
    }

    JniRuntime(long pointer) {
        this.pointer = pointer;
    }

    @Override
    public void close() throws Exception {
        destroyRuntime(pointer);
    }
}
