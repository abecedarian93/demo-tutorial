package com.abecedarian.demo.hive;

import org.apache.log4j.Logger;

import java.sql.*;

/**
 * Created by abecedarian on 2019/5/9
 */
public class Service2Demo {

    private static Connection hiveConnection;
    private static Statement hiveStat;
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    private static final Logger log = Logger.getLogger(Service2Demo.class);

    static {
        try {
            Class.forName(driverName);
            hiveConnection = DriverManager.getConnection("jdbc:hive2://localhost:10000", "wirelessdev", "");
            hiveStat = hiveConnection.createStatement();
            hiveStat.executeQuery("use wirelessdata");
            hiveStat.executeQuery("set mapred.job.queue.name = wirelessdev");
            hiveStat.executeQuery("set mapred.job.name = wirelessdev_job_tianle.li");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {

        try {
            hiveStat.executeQuery("show create table tmp_ads_click");

        } catch (SQLException e) {
            e.printStackTrace();
            log.error("Connection error!", e);
            System.exit(1);
        } finally {
            try {
                if (hiveConnection != null) {
                    hiveConnection.close();
                    hiveConnection = null;
                }
                if (hiveStat != null) {
                    hiveStat.close();
                    hiveStat = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


}
