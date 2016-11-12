package org.wso2.siddhi.extension.featureeng;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

public class MovingMedianCenteredAverageAggregator extends AttributeAggregator {
    private static Attribute.Type type = Attribute.Type.DOUBLE;
    private ArrayList<Double> num_arr;
    private int count;
    private int window_size;
    private int limit;

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
         /*
        Input parameters - (window_size, limit, data_stream) [INT, INT, DOUBLE]
        Input Conditions - NULL
        Output - Moving median centered average [DOUBLE]
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
        this.count = 1;
        this.window_size = 0;
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
        limit = (Integer) objects[1];          //Limit

        // Append data into array
        if (objects[2] instanceof Integer){
            num_arr.add((double)(Integer) objects[2]);
        } else if (objects[2] instanceof Double){
            num_arr.add((Double) objects[2]);
        }

        if (2*limit > window_size){
            throw new OperationNotSupportedException("limit should be twice less than window size");
        }

        if ( count < window_size) {            //Return default value until fill the window
            count++;
            return 0.0;
        }else {                                //If window filled, do the calculation
            double tot = 0.0;

            //Create temp list, otherwise list sort will change the original order of the data
            ArrayList<Double> tmp = new ArrayList<Double>(num_arr);

            //Sort values
            Collections.sort(tmp);

            //Remove first element in the queue
            num_arr.remove(0);

            //Remove corner values
            for(int i=0; i<limit; i++){
                tmp.remove(0);
                tmp.remove(tmp.size()-1);
            }

            //Calculate total
            for (Double aTmp : tmp) {
                tot += aTmp;
            }

            return tot / tmp.size();
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
