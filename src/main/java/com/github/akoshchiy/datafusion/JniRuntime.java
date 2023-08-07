package com.github.akoshchiy.datafusion;

import io.questdb.jar.jni.JarJniLoader;

class JniRuntime extends JniObject implements Runtime {

    private static native long nativeCreate();

    private static native void nativeDestroy(long pointer);

    static {
        JarJniLoader.loadLib(
                JniRuntime.class,
                "/jni",
                "arrow_datafusion_jni"
        );
    }

    public static JniRuntime create() {
        return new JniRuntime(nativeCreate());
    }

    JniRuntime(long pointer) {
        super(pointer);
    }

    @Override
    public void close() {
        nativeDestroy(getPointer());
    }
}
