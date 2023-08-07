package com.github.akoshchiy.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.ArrayList;

public class VectorSchemaRootImporter {

    public static VectorSchemaRoot importNative(long schemaPointer, long[] arrayPointers, BufferAllocator allocator) {
        try (var provider = new CDataDictionaryProvider()) {
            var vectors = new ArrayList<FieldVector>();
            var schema = Data.importSchema(allocator, ArrowSchema.wrap(schemaPointer), provider);
            for (int i = 0; i < arrayPointers.length; i++) {
                var vector = schema.getFields().get(i).createVector(allocator);
                Data.importIntoVector(allocator, ArrowArray.wrap(arrayPointers[i]), vector, provider);
                vectors.add(vector);
            }

            var rowsCount = vectors.isEmpty() ? 0 : vectors.get(0).getValueCount();

            return new VectorSchemaRoot(schema, vectors, rowsCount);
        }
    }

    public static VectorSchemaRoot importNative(long schemaPointer, long arrayPointer, BufferAllocator allocator) {
        try (var provider = new CDataDictionaryProvider()) {
            return Data.importVectorSchemaRoot(
                    allocator,
                    ArrowArray.wrap(arrayPointer),
                    ArrowSchema.wrap(schemaPointer),
                    provider
            );
        }
    }
}
