package com.adm.sparkserver.spatial;


import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import com.spatial4j.core.shape.impl.PointImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.spatial4j.core.distance.DistanceUtils.KM_TO_DEG;


public class SpatialFilter implements java.io.Serializable{
    private final static Logger LOGGER = LoggerFactory.getLogger(SpatialFilter.class);

    private static final SpatialRelation WITHIN = SpatialRelation.WITHIN;
    private static final SpatialRelation INTERSECTS = SpatialRelation.INTERSECTS;


    public SpatialFilter(){ }


    public static boolean within(Double lat, Double lon, Double radius, double centerLat, Double centerLon){
        SpatialContext ctx = SpatialContext.GEO;

        PointImpl point = new PointImpl(lat, lon, ctx);
        PointImpl center = new PointImpl(centerLat, centerLon, ctx);

        return WITHIN == point.relate(ctx.makeCircle(center, radius * KM_TO_DEG));

    }

    /**
     * Checks if the two circles intersects. Meaning area arre close
     */
    public static boolean isWithinIntersectionCircles(String lat, String lon, double radius, Point center){

        SpatialContext ctx = SpatialContext.GEO;
        Shape circle = ctx.makeCircle(Double.parseDouble(lat), Double.parseDouble(lon), radius);
        return  INTERSECTS == circle.relate(ctx.makeCircle(center, radius * KM_TO_DEG));

    }

    //TODO
    public static boolean isWithinPolygon(String polygon, Point center){
        //Parse polygon String to Points
        //Build Polygon
        //check if Contains = polygon.relate( ....)
        return true;
    }




}
