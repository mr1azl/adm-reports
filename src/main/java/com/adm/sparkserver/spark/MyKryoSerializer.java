package com.adm.sparkserver.spark;

import com.adm.sparkserver.spatial.SpatialFilter;
import com.esotericsoftware.kryo.Kryo;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import com.spatial4j.core.shape.impl.PointImpl;
import org.apache.spark.serializer.KryoRegistrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.lang.reflect.Array;

public class MyKryoSerializer implements KryoRegistrator, Serializable {
    private final static Logger LOGGER = LoggerFactory.getLogger(MyKryoSerializer.class);


    /**
     * register a class indicated by name
     */
    protected void doRegistration(@Nonnull Kryo kryo, @Nonnull String s ) {
        Class c;
        try {
            c = Class.forName(s);
            doRegistration(kryo,  c);
        }
        catch (ClassNotFoundException e) {
            LOGGER.error(e.getMessage());
            return;
        }
    }

    /**
     * register a class
     */
    protected void doRegistration(final Kryo kryo , final Class pC) {
        if (kryo != null) {
            kryo.register(pC);
            // also register arrays of that class
            Class arrayType = Array.newInstance(pC, 0).getClass();
            kryo.register(arrayType);
        }
    }

    /**
     * Real work of registering all classes
     */
    @Override
    public void registerClasses(@Nonnull Kryo kryo) {
        kryo.register(Object[].class);
        kryo.register(scala.Tuple2[].class);
        kryo.register(String.class);
        kryo.register(SpatialFilter.class);
        kryo.register(PointImpl.class);
        kryo.register(Shape.class);
        kryo.register(SpatialRelation.class);
        kryo.register(SpatialContext.class);
        kryo.register(SpatialContext.GEO.getClass());
        doRegistration(kryo, "scala.collection.mutable.WrappedArray$ofRef");
    }
}
