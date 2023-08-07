package com.github.akoshchiy.datafusion;

interface JniPointerCallback {
    void accept(long pointer, String error);
}
