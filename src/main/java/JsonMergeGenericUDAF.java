package com.reizist.example;

import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.ql.exec.Description;

@Description(name = "merge_json", value = "_FUNC_(expr) - Returns merged json.")
public class JsonMergeGenericUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }

        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
        
        if (oi.getCategory() != ObjectInspector.Category.MAP){
            throw new UDFArgumentTypeException(0,
                            "Argument must be MAP, but "
                            + oi.getCategory().name()
                            + " was passed.");
        }
        return new JsonMergeEvaluator();
    }

    public static class JsonMergeEvaluator extends GenericUDAFEvaluator {
        ObjectInspector inputOI;
        ObjectInspector outputOI;

        MapObjectInspector mapOI;

        // Called by Hive to initialize an instance of your UDAF evaluator class.
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
        	
            assert (parameters.length == 1);
            super.init(m, parameters);

            inputOI = parameters[0];
            // init output object inspectors
            //outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Map.class,
            //        ObjectInspectorOptions.JAVA);
            outputOI = ObjectInspectorUtils.getStandardObjectInspector(inputOI,
                    ObjectInspectorCopyOption.JAVA);
            return outputOI;
        }

        /**
         * class for storing the current object
         */
        static class MergedJsonAgg implements AggregationBuffer {
            Map<String, String> json = new HashMap<String, String>();
            Map<String, String> merge(Map<String, String> another_json) {
                json.putAll(another_json);
                return json;
            }
        }

        // Return an object that will be used to store temporary aggregation results.
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MergedJsonAgg result = new MergedJsonAgg();
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
        	MergedJsonAgg myagg = new MergedJsonAgg();
        }
        
        // Process a new row of data into the aggregation buffer
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            assert (parameters.length == 1);
            if (parameters[0] != null) {
                MergedJsonAgg myagg = (MergedJsonAgg) agg;
                Map<String, String> p1 = (Map<String, String>) ((MapObjectInspector) inputOI).getMap(parameters[0]);
                myagg.merge(p1);
            }
        }

        // Return the contents of the current aggregation in a persistable way.
        // Here persistable means the return value can only be built up in terms of
        // Java primitives, arrays, primitive wrappers (e.g. Double), Hadoop Writables, Lists, and Maps.
        // Do NOT use your own classes (even if they implement java.io.Serializable),
        // otherwise you may get strange errors or (probably worse) wrong results.
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MergedJsonAgg myagg = (MergedJsonAgg) agg;
            return myagg.json;
        }

        // Merge a partial aggregation returned by terminatePartial into the current aggregation
        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            if (partial != null) {
                MergedJsonAgg myagg1 = (MergedJsonAgg) agg;
                Map<String, String> partialJson = (Map<String, String>) mapOI.getMap(partial);
                myagg1.json.putAll(partialJson);
            }
        }

        // Return the final result of the aggregation to Hive
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MergedJsonAgg myagg = (MergedJsonAgg) agg;
            return myagg.json;
        }
    }
}