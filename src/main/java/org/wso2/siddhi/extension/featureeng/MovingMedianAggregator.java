package org.wso2.siddhi.extension.featureeng;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;
import java.util.ArrayList;
import java.util.Collections;

public class MovingMedianAggregator extends AttributeAggregator {
    private static Attribute.Type type = Attribute.Type.DOUBLE;
    private ArrayList<Double> num_arr; //Keep window elements
    private double median;  //Window median
    private int count;      //Window element counter
    private int window_size;//Run length window

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
         /*
        Input parameters - (window_size, data_stream) [INT, DOUBLE]
        Input Conditions - NULL
        Output - Moving median [DOUBLE]
         */

        //No of parameter check
        if (attributeExpressionExecutors.length != 2){
            throw new OperationNotSupportedException("2 parameters are required, given "
                    + attributeExpressionExecutors.length + " parameter(s)");
        }

        //Parameter type check
        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.INT) {
            //Window size
            throw new IllegalArgumentException("First parameter should be the window size (type.INT)");
        }
        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.DOUBLE){
            //Stream data
            throw new IllegalArgumentException("Stream data should be in type.DOUBLE");
        }

        //Initialize variables
        this.num_arr = new ArrayList<Double>();
        this.median = 0.0;
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
        num_arr.add((Double) objects[1]);      //Append window array

        if ( count < window_size) {            //Return default value until fill the window
            count++;
        }else {                                //If window filled, do the calculation
            median = calculate();
        }
        return median;
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
        //Create temp list, otherwise list sort will change the original order of the data
        ArrayList<Double> tmp = new ArrayList<Double>(num_arr);

        //Sort values
        Collections.sort(tmp);

        //Get median value
        if(window_size % 2 == 0){
            int index = window_size / 2;
            median = (tmp.get(index) + tmp.get(index-1)) / 2;
        }else{
            median =  tmp.get(window_size / 2);
        }
        return median;
    }
}
