package com.adm.sparkserver;

import com.adm.sparkserver.netty.NettyServer;
import com.codahale.metrics.MetricRegistry;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdmSparkServer {
    private final static Logger LOGGER = LoggerFactory.getLogger(AdmSparkServer.class);

    public static MetricRegistry metricRegistry = new MetricRegistry();

    public static void main(String[] args) {
        LOGGER.info("Starting Netty Server");
        NettyServer nettyServer = new NettyServer();
        nettyServer.start();
        LOGGER.info("Server Started on port " + ConfigFactory.load().getInt("app.netty.port") +"\n \n");
    }
}
