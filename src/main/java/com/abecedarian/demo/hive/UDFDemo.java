package com.abecedarian.demo.hive;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by abecedarian on 2019/2/22
 *
 * hive-UDF demo
 */
public class UDFDemo extends GenericUDF {

    Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();

    private StringObjectInspector str;
    private DateObjectInspector bt;
    private IntObjectInspector it;


    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 参数个数校验
        if (arguments.length != 3) {
            throw new UDFArgumentLengthException(
                    "The function 'FeatureTransform' only accepts 3 argument , but got " + arguments.length);
        }
        ObjectInspector arg1 = arguments[0];
        // 参数类型校验
        if (!(arg1 instanceof StringObjectInspector)) {
            throw new UDFArgumentException("The first argument of function must be a string");
        }
        ObjectInspector arg2 = arguments[1];
        // 参数类型校验
        if (!(arg2 instanceof StringObjectInspector)) {
            throw new UDFArgumentException("The two argument of function must be a string");
        }
        ObjectInspector arg3 = arguments[2];
        // 参数类型校验
        if (!(arg3 instanceof IntObjectInspector)) {
            throw new UDFArgumentException("The three argument of function must be a integer");
        }
        this.str = (StringObjectInspector) arg1;
        this.bt = (DateObjectInspector) arg2;
        this.it = (IntObjectInspector) arg3;
        // 返回值类型
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String str = this.str.getPrimitiveJavaObject(arguments[0].get());
        Date dt = this.bt.getPrimitiveJavaObject(arguments[1].get());
        int it = (Integer) this.it.getPrimitiveJavaObject(arguments[2].get());


        return show(str, dt, it);
    }

    public String show(String str, Date dt, int it) {

        StringBuilder sb = new StringBuilder();
        sb.append(str).append(",");
        sb.append(new SimpleDateFormat("yyyyMMdd").format(dt)).append(",");
        sb.append(it);

        return sb.toString();
    }


    @Override
    public String getDisplayString(String[] children) {
        return "GsonUDF (json)";
    }


}
