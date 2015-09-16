package com.adm.sparkserver.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.jboss.resteasy.plugins.server.netty.NettyHttpRequest;

public class HeadersChannelHandler extends SimpleChannelInboundHandler<NettyHttpRequest> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyHttpRequest request) throws Exception {
        request.getResponse().getOutputHeaders().add("Access-Control-Allow-Origin", "*");
        request.getResponse().getOutputHeaders().add("Access-Control-Allow-Methods", "GET,POST,PUT"); //"GET,POST,PUT,DELETE,OPTIONS"
        request.getResponse().getOutputHeaders().add("Access-Control-Allow-Headers", "X-Requested-With, Content-Type, Content-Length");

        ctx.fireChannelRead(request);
    }
}