package models;

import java.io.Serializable;
import java.lang.reflect.Array;

import javax.annotation.Nonnull;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;

public class StockKryoRegistrator implements KryoRegistrator, Serializable{

	@Override
	public void registerClasses(Kryo arg0) {
		// TODO Auto-generated method stub
		arg0.register(Object[].class);
		arg0.register(scala.Tuple2[].class);
		
		doRegistration(arg0, "models.PriceData");
	    doRegistration(arg0, "models.StockPrice");
	    doRegistration(arg0, "java.util.HashMap");
	    doRegistration(arg0, "scala.collection.mutable.WrappedArray.ofRef");
	    doRegistration(arg0, "scala.collection.mutable.WrappedArray$ofRef");
	 
	}
	
	
	 /**
     * register a class indicated by name
     *
     * @param kryo
     * @param s       name of a class - might not exist
     * @param handled Set of classes already handles
     */
    protected void doRegistration(@Nonnull Kryo kryo, @Nonnull String s ) {
        Class c;
        try {
            c = Class.forName(s);
            doRegistration(kryo,  c);
        }
        catch (ClassNotFoundException e) {
            return;
        }
     }
    
    
    /**
     * register a class
     *
     * @param kryo
     * @param s       name of a class - might not exist
     * @param handled Set of classes already handles
     */
   protected void doRegistration(final Kryo kryo , final Class pC) {
          if (kryo != null) {
           kryo.register(pC);
              // also register arrays of that class
           Class arrayType = Array.newInstance(pC, 0).getClass();
           kryo.register(arrayType);
       }
   }

}
