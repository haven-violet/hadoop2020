package com.qf.hive.function;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class ParseMapUDTFNumber extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        List<String> structFieldNames = new ArrayList<>();
        structFieldNames.add("key");

        List<ObjectInspector> structFieldObjectInspectors = new ArrayList<>();
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        String s = objects[0].toString();
        String[] split = s.split(",");
        for (String s1 : split) {
            if(Character.isLetter(s1.charAt(0))){
                s1 = s1 + s1 + s1;
                String[] arr = new String[]{s1};
                forward(arr);
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }

}