package com.github.akoshchiy.datafusion;

class JniSessionContext implements SessionContext {

    private static native long createSessionContext();

    private static native long destroySessionContext(long pointer);

    private final long pointer;

    static JniSessionContext create() {
        return new JniSessionContext(createSessionContext());
    }

    JniSessionContext(long pointer) {
        this.pointer = pointer;
    }

    @Override
    public void close() {
        destroySessionContext(pointer);
    }
}
