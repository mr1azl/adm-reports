package com.adm.sparkserver.netty;

import com.adm.sparkserver.resources.AdmQuery;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.jboss.resteasy.plugins.server.netty.NettyJaxrsServer;
import org.jboss.resteasy.spi.ResteasyDeployment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;


public class NettyServer {
    private final static Logger LOGGER = LoggerFactory.getLogger(NettyServer.class);

    private Config config = ConfigFactory.load();

    private NettyJaxrsServer netty = null;

    public NettyServer() {}

    public void start() {
        ResteasyDeployment deployment = new ResteasyDeployment();
        AdmQuery admQueryResource = new AdmQuery();
        //HelloWorld hw = new HelloWorld();
        deployment.setResources(Arrays.asList(admQueryResource));

        netty = new NettyJaxrs();
        netty.setDeployment(deployment);
        netty.setPort(config.getInt("app.netty.port"));
        netty.setRootResourcePath("/");
        netty.start();

    }

    public void stop(){
        netty.stop();
    }

    public Boolean isUp(){
        return netty != null;
    }


}
