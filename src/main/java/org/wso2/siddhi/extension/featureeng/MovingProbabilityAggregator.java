package org.wso2.siddhi.extension.featureeng;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;

public class MovingProbabilityAggregator extends AttributeAggregator {
    private static Attribute.Type type = Attribute.Type.DOUBLE;
    private ArrayList<Double> num_arr; //Keep window elements
    private double prob;     //Window probability
    private int count;      //Window element counter
    private int window_size;
    private int nbins;      //Number of discrete levels
    private double val;
    private double binSize;
    private double min;
    private double max;

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
         /*
        Input parameters - (window_size, limit, data_stream) [INT, INT, DOUBLE]
        Input Conditions - NULL
        Output - Moving probability [DOUBLE]
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
            //K closest val
            throw new IllegalArgumentException("Number of closest values should be in type.INT");
        }
        if (attributeExpressionExecutors[2].getReturnType() != Attribute.Type.DOUBLE){
            //K closest val
            throw new IllegalArgumentException("Stream data should be in type.DOUBLE");
        }

        //Initialize variables
        this.num_arr = new ArrayList<Double>();
        this.prob = 0.0;
        this.count = 1;
        this.window_size = 0;
        this.nbins = 0;
        this.val = 0.0;
        this.binSize = 0.0;
        this.max = Double.MIN_VALUE;
        this.min = Double.MAX_VALUE;
    }

    @Override
    public Attribute.Type getReturnType() {
        return null;
    }

    @Override
    public Object processAdd(Object o) {
        return null;
    }

    @Override
    public Object processAdd(Object[] objects) {
        window_size = (Integer) objects[0];    //Run length window
        nbins = (Integer) objects[1];
        val = (Double) objects[2];
        num_arr.add((Double) objects[2]);     //Append window array

        if ( count < window_size) {            //Return default val until fill the window
            count++;
        }else {                                //If window filled, do the calculation
            prob = calculate();
        }
        return prob;
    }

    @Override
    public Object processRemove(Object o) {
        return null;
    }

    @Override
    public Object processRemove(Object[] objects) {
        //Remove first element in the queue
        num_arr.remove(0);

        double removedVal = (Double) objects[2];
        if (removedVal == max){
            max = Double.MIN_VALUE;
        }

        if (removedVal == min){
            min = Double.MAX_VALUE;
        }

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
        if (val < min){
            min = val;
        }

        if (val > max){
            max = val;
        }

        binSize = (max - min) / nbins;
        return prob;
    }
}
