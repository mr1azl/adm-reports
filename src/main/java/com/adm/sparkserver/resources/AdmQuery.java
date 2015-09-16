package com.adm.sparkserver.resources;


import com.adm.sparkserver.models.EffectiveQuery;
import com.adm.sparkserver.models.Query;
import com.adm.sparkserver.spark.SparkRunner;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.jboss.resteasy.annotations.Suspend;
import org.jboss.resteasy.spi.AsynchronousResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Path("/adm")
@Singleton
public final class AdmQuery {
    private final static Logger LOGGER = LoggerFactory.getLogger(AdmQuery.class);

    private static final Config config = ConfigFactory.load();

    private static final  ExecutorService threadPool = Executors.newFixedThreadPool(10);

    public AdmQuery(){

        SparkRunner.init();
        
    }

    @POST
    @Path("/adhoc")
    public void runQueryString(final @Suspend(360000) AsynchronousResponse response, String qr) {
        LOGGER.info("Query Received : " + qr );

        threadPool.submit(() -> {
            String results = SparkRunner.queryRunner(qr).toString();
            Response resp = Response.ok(results).type(MediaType.TEXT_PLAIN).build();
            response.setResponse(resp);
        });
    }


    @GET
    @Path("/q")
    public void queryOn(final @Suspend(360000) AsynchronousResponse asyncResp,
                            @QueryParam("zone_latitude") String lat,
                            @QueryParam("zone_longitude") String lon,
                            @QueryParam("radius") String radiuses,
                            @QueryParam("applist") String applist,
                            @QueryParam("timeframe") int timeframe,
                            @QueryParam("schedule") String schedule) {

        threadPool.submit(() -> {
            EffectiveQuery fquery = new EffectiveQuery(new Query(lat, lon, radiuses, applist, timeframe, schedule));

            String results = null;
            try {
                results = SparkRunner.geoStats(fquery).toJson();
            } catch (JsonProcessingException e) {
               LOGGER.error("Json Processing error");
            }
            Response resp = Response.ok(results).type(MediaType.APPLICATION_JSON).build();

            asyncResp.setResponse(resp);
        });
    }

    @POST
    @Path("/qp")
    public void queryOn2(final @Suspend(360000) AsynchronousResponse asyncResp,
                        @QueryParam("zone_latitude") String lat,
                        @QueryParam("zone_longitude") String lon,
                        @QueryParam("radius") String radiuses,
                        @QueryParam("applist") String applist,
                        @QueryParam("timeframe") int timeframe,
                        @QueryParam("schedule") String schedule) throws JsonProcessingException {

        threadPool.submit(() -> {
            EffectiveQuery fquery = new EffectiveQuery(new Query(lat, lon, radiuses, applist, timeframe, schedule));

            String results = null;
            try {
                results = SparkRunner.geoStats(fquery).toJson();
            } catch (JsonProcessingException e) {
                LOGGER.error("Json Processing error");
            }
            Response resp = Response.ok(results).type(MediaType.APPLICATION_JSON).build();

            asyncResp.setResponse(resp);
        });
    }

    @POST
    @Path("/jsonq")
    public void query(final @Suspend(360000) AsynchronousResponse asynResp, Query query) throws JsonProcessingException {

        threadPool.submit(() -> {
            EffectiveQuery fquery = new EffectiveQuery(query);

            String results = null;
            try {
                results = SparkRunner.geoStats(fquery).toJson();
            } catch (JsonProcessingException e) {
                LOGGER.error("Json Processing error");
            }

            Response resp = Response.ok(results).type(MediaType.APPLICATION_JSON).build();

            asynResp.setResponse(resp);
        });
    }

    @GET
    @Path("/sparkconf")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSparkConf() {
        return Response.ok(SparkRunner.sc.getConf().toDebugString()).build();
    }

    @GET
    @Path("/count")
    @Produces (MediaType.APPLICATION_JSON)
    public Response logcount(){
         return Response.ok("Logs count : +" + SparkRunner.logCounter()).build();
    }

}
