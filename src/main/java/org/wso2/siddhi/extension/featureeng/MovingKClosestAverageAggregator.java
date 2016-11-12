package org.wso2.siddhi.extension.featureeng;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

public class MovingKClosestAverageAggregator extends AttributeAggregator {
    private static Attribute.Type type = Attribute.Type.DOUBLE;
    private ArrayList<Double> num_arr;
    private int count;
    private int window_size;
    private int k_closest;

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
         /*
        Input parameters - (window_size, k_closest, data_stream) [INT, INT, DOUBLE]
        Input Conditions - k_closest < window_size
        Output - Moving k closest average [DOUBLE]
         */

         //No of parameter check
        if (attributeExpressionExecutors.length != 3){
            throw new OperationNotSupportedException("2 parameters are required, given "
                    + attributeExpressionExecutors.length + " parameter(s)");
        }

        //Parameter type check
        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.INT) {
            //Window size
            throw new IllegalArgumentException("First parameter should be the window size (type.INT)");
        }
        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.INT){
            //K closest value
            throw new IllegalArgumentException("Number of closest values should be in type.INT");
        }
        if (attributeExpressionExecutors[2].getReturnType() != Attribute.Type.DOUBLE){
            //K closest value
            throw new IllegalArgumentException("Stream data should be in type.DOUBLE");
        }

        //Initialize variables
        this.num_arr = new ArrayList<Double>();
        this.count = 1;
        this.window_size = 0;
    }

    @Override
    public Attribute.Type getReturnType() {
        return type;
    }

    @Override
    public Object processAdd(Object o) {
        throw new OperationNotSupportedException("Moving k-closest average required (window, k_closest, data_stream) parameters");
    }

    @Override
    public Object processAdd(Object[] objects) {
        window_size = (Integer) objects[0];    //Run length window
        k_closest = (Integer) objects[1];      //K number
        num_arr.add((Double) objects[2]);      //Append data into array

        if (k_closest > window_size){
            System.out.println("s");
        }

        if ( count < window_size) {            //Return default value until fill the window
            count++;
            return 0.0;
        }else {                                //If window filled, do the calculation
            double tot = 0.0;
            SortedMap<Double, Double> sortedMap = new TreeMap<Double, Double>();
            for(int i=0; i<window_size; i++){
                double key = Math.abs(num_arr.get(i) - num_arr.get(window_size-1));
                sortedMap.put(key, num_arr.get(i));
            }

            Double[] keyArray = (Double[]) sortedMap.keySet().toArray();
            int i=0;
            while(i < k_closest){
                tot += sortedMap.get(keyArray[i]);

                i++;
            }
            num_arr.remove(0);          //FIFO num_arr
            return tot / k_closest;
        }
    }

    @Override
    public Object processRemove(Object o) {
        return null;
    }

    @Override
    public Object processRemove(Object[] objects) {
        return null;
    }

    @Override
    public Object reset() {
        return null;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] objects) {

    }
}
