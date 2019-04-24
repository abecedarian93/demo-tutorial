package com.abecedarian.demo.java;

import org.openstreetmap.osmosis.core.util.MultiMemberGZIPInputStream;

import java.io.*;

/**
 * Created by abecedarian on 2019/3/20
 */
public class IODemo {

    public static void main(String[] args) throws IOException {

//        readGzFile("your read file path");
//        writeToFile("your write file path");
    }

    //读取压缩文件
    private static void readGzFile(String filePath) throws IOException {

        BufferedReader br = new BufferedReader(new InputStreamReader(new MultiMemberGZIPInputStream(new FileInputStream(filePath)), "utf-8"));
        String line = null;
        while ((line = br.readLine()) != null) {

            //处理line
            System.out.println(line);
        }
    }

    //写入文件
    private static void writeToFile(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            file.createNewFile();
            file.setExecutable(true, false);
            file.setReadable(true, false);
            file.setWritable(true, false);
        }
        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);

        //写入数据
        bw.write("test-demo");
        bw.newLine();

        bw.close();
    }


}
