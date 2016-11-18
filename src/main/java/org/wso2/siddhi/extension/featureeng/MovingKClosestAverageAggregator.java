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
    private ArrayList<Double> num_arr; //Keep window elements
    private double avg;     //Window average
    private int count;      //Window element counter
    private int window_size;//Run length window
    private int k_closest;  //Number of closest values to the last occurence

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
         /*
        Input parameters - (window_size, k number of closest values to the last occurrence, data_stream) [INT, INT, DOUBLE]
        Input Conditions - k_closest < window_size
        Output - Moving k closest average [DOUBLE]
         */

         //No of parameter check
        if (attributeExpressionExecutors.length != 3){
            throw new OperationNotSupportedException("3 parameters are required, given "
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
        this.avg = 0.0;
        this.count = 1;
        this.window_size = 0;
    }

    @Override
    public Attribute.Type getReturnType() {
        return type;
    }

    @Override
    public Object processAdd(Object o) {
        return null;
    }

    @Override
    public Object processAdd(Object[] objects) {
        window_size = (Integer) objects[0];    //Run length window
        k_closest = (Integer) objects[1];      //K number
        num_arr.add((Double) objects[2]);      //Append window array

        if (k_closest > window_size){
            throw new OperationNotSupportedException("K values should be less than window size");
        }

        if ( count < window_size) {            //Return default value until fill the window
            count++;
        }else {                                //If window filled, do the calculation
            avg = calculate();
        }
        return avg;
    }

    @Override
    public Object processRemove(Object o) {
        return null;
    }

    @Override
    public Object processRemove(Object[] objects) {
        //Remove first element in the queue
        num_arr.remove(0);
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

    private double calculate(){
        double tot = 0.0;

        //Add numbers in sorted order by absolute difference of the number compared to the last number in window
        SortedMap<Double, Double> sortedMap = new TreeMap<Double, Double>();
        for(int i=0; i<window_size; i++){
            double key = Math.abs(num_arr.get(i) - num_arr.get(window_size-1));
            sortedMap.put(key, num_arr.get(i));
        }

        //Convert keys in the sorted map to double
        ArrayList<Double> key_array = new ArrayList<Double>();
        for(double key: sortedMap.keySet()){
            key_array.add(key);
        }

        //Calculate the total of k closest values
        for(int i=0; i<k_closest; i++){
            tot += sortedMap.get(key_array.get(i));
        }

        //Calculate the average
        avg = tot / k_closest;
        return avg;
    }
}
