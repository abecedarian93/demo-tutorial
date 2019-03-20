package com.abecedarian.demo.hive;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractDeserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by abecedarian on 2019/3/20
 * <p>
 * Hive serde demo
 */
public class SerdeDemo extends AbstractDeserializer {

    private static List<String> structFieldNames = new ArrayList<String>();
    private static List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();

    static {
        structFieldNames.add("col1");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        structFieldNames.add("col2");
        structFieldObjectInspectors.add(ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector));
    }

    @Override
    public void initialize(Configuration configuration, Properties properties) throws SerDeException {

    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        /** 获得每条数据 **/
        String line = writable.toString();

        List<Object> result = new ArrayList<Object>();

        /**这里可自定义解析line数据**/
        String col1 = "col1";
        Map<String, String> col2 = Maps.newHashMap();
        col2.put("col2", "col2-value");

        result.add(col1);
        result.add(col2);
        return result;
    }

    @Override
    public ObjectInspector getObjectInspector() {

        return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }
}
