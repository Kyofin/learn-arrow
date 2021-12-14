package com.data.arrow;

import com.google.common.collect.ImmutableList;
import com.sun.rowset.internal.Row;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: learn-arrow
 * @author: huzekang
 * @create: 2021-12-14 09:31
 **/
public class ArrowDemo {
    public static void main(String[] args) throws IOException {
        // 模拟be存储的数据
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        // schema
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null));
        childrenBuilder.add(new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null));
        childrenBuilder.add(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null));
        childrenBuilder.add(new Field("isDel", FieldType.nullable(new ArrowType.Bool()), null));

        final Schema schema = new Schema(childrenBuilder.build(), null);
        VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(
                schema,
                new RootAllocator(Integer.MAX_VALUE));

        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                schemaRoot,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        // 设置每个字段，存3行
        schemaRoot.setRowCount(3);

        FieldVector vector = schemaRoot.getVector("id");
        BigIntVector bigIntVector = (BigIntVector) vector;
        bigIntVector.setInitialCapacity(3);
        bigIntVector.allocateNew(3);
        bigIntVector.setSafe(0, 101);
        bigIntVector.setSafe(1, 102);
        bigIntVector.setSafe(3, 103);
        vector.setValueCount(3);

        vector = schemaRoot.getVector("age");
        IntVector intVector = (IntVector) vector;
        intVector.setInitialCapacity(3);
        intVector.allocateNew(3);
        intVector.setSafe(0, 22);
        intVector.setNull(1);
        intVector.setSafe(2, 45);
        vector.setValueCount(3);

        vector = schemaRoot.getVector("name");
        VarCharVector charVector = (VarCharVector) vector;
        charVector.setInitialCapacity(3);
        charVector.allocateNew();
        charVector.setIndexDefined(0);
        charVector.setValueLengthSafe(0, 5);
        charVector.setSafe(0, "ppp".getBytes());
        charVector.setIndexDefined(1);
        charVector.setValueLengthSafe(1, 5);
        charVector.setSafe(1, "ccc".getBytes());
        charVector.setIndexDefined(2);
        charVector.setValueLengthSafe(2, 5);
        charVector.setSafe(2, "peterpoker".getBytes());
        vector.setValueCount(3);

        vector = schemaRoot.getVector("isDel");
        BitVector bitVector = (BitVector) vector;
        bitVector.setInitialCapacity(3);
        bitVector.allocateNew(3);
        bitVector.setSafe(0, 1);
        bitVector.setSafe(1, 0);
        bitVector.setSafe(2, 1);
        vector.setValueCount(3);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        // 读取数据解析
        final RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        ArrowStreamReader arrowStreamReader = new ArrowStreamReader(
                new ByteArrayInputStream(outputStream.toByteArray()),
                rootAllocator
        );


        VectorSchemaRoot root = arrowStreamReader.getVectorSchemaRoot();
        while (arrowStreamReader.loadNextBatch()) {
            List<FieldVector> fieldVectors = root.getFieldVectors();

            if (fieldVectors.size() == 0 || root.getRowCount() == 0) {
                System.out.println("One batch in arrow has no data.");
                continue;
            }
           fieldVectors.forEach(v->{
               System.out.println(v.toString());
           });

        }

    }
}
