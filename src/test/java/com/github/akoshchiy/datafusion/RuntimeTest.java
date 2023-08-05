package com.github.akoshchiy.datafusion;

import org.junit.jupiter.api.Test;

public class RuntimeTest {

    @Test
    public void testCreateAndDestroy() throws Exception {
        var runtime = Runtime.create();
        runtime.close();
    }
}
