package com.github.akoshchiy.datafusion;

abstract class JniObject {
    private final long pointer;

    protected JniObject(long pointer) {
        this.pointer = pointer;
    }

    protected long getPointer() {
        return pointer;
    }
}
