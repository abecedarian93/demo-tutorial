package com.abecedarian.demo.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by abecedarian on 2019/2/25
 *
 * hive-UDF demo
 */
public class UDF2Demo extends UDF {

    public Text evaluate(Text value, Text baseDate, Text intNum) {


        return new Text(String.valueOf(show(value.toString(),baseDate.toString(),Integer.parseInt(intNum.toString()))));
    }

    public String show(String str, String dt, int it) {

        StringBuilder sb = new StringBuilder();
        sb.append(str).append(",");
        sb.append(dt).append(",");
        sb.append(it);

        return sb.toString();
    }
}
