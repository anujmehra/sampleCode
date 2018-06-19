package com.hbase.poc.factory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;

/**
 * A factory for creating Connection objects.
 */
public class ConnectionFactory {

    /** The hbase connections. */
    private static List<Connection> hbaseConnections;

    /**
     * Instantiates a new connection factory.
     */
    private ConnectionFactory() {

        // private constructor
    }

    /** The counter. */
    private static volatile int counter = 0;

    /** The total connections. */
    private static volatile int totalConnections = 5;

    /** The Constant conf. */
    private static final Configuration conf;
    
    static {
        conf = HBaseConfiguration.create();
        
        conf.set("hbase.zookeeper.property.clientPort", "5181");
        conf.set("hbase.client.keyvalue.maxsize", "204857600");
        hbaseConnections = new ArrayList<>();
        try {
            for (int i = 0; i < totalConnections; i++) {
                hbaseConnections.add(org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(conf));
            }
        } catch (final IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Gets the.
     *
     * @return the connection
     */
    public static Connection get() {
        int currentCounter = 0;
        synchronized (ConnectionFactory.class) {
            currentCounter = (counter) % (totalConnections - 1);
            counter = ++currentCounter;
        }
        return hbaseConnections.get(currentCounter);
    }
}