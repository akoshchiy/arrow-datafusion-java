package com.github.akoshchiy.datafusion;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataFrameTest {

    @Test
    public void testCollect() throws Exception {
        var allocator = new RootAllocator();
        var ctx = SessionContext.create(Runtime.create());
        var frame = ctx.sql("SELECT 1 as col1, 2 as col2, 3 as col3").join();
        var reader = frame.collect(allocator).join();

        assertTrue(reader.loadNextBatch());

        var batch = reader.getVectorSchemaRoot();
        Schema schema = batch.getSchema();

        assertEquals(1, batch.getRowCount());

        schema.findField("col1");
        schema.findField("col2");
        schema.findField("col3");

        var col1 = (BigIntVector) batch.getVector("col1");
        var col2 = (BigIntVector) batch.getVector("col2");
        var col3 = (BigIntVector) batch.getVector("col3");

        assertEquals(1, col1.get(0));
        assertEquals(2, col2.get(0));
        assertEquals(3, col3.get(0));
    }
}
