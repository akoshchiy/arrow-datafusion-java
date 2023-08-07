package com.github.akoshchiy.datafusion;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SessionContextTest {

    @Test
    public void testCreateAndDestroy() throws Exception {
        var ctx = SessionContext.create(Runtime.create());
        ctx.close();
    }

    @Test
    public void testSql() {
        var ctx = SessionContext.create(Runtime.create());
        var result = ctx.sql("SELECT 1, 2, 3").join();
        assertNotNull(result);
    }
}
