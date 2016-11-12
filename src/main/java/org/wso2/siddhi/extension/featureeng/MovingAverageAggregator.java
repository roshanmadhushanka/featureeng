package org.wso2.siddhi.extension.featureeng;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;
import java.util.ArrayList;

public class MovingAverageAggregator extends AttributeAggregator{
    private static Attribute.Type type = Attribute.Type.DOUBLE;
    private ArrayList<Double> num_arr;
    private int count;
    private int window_size;

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        /*
        Input parameters - (window_size, data_stream) [INT, DOUBLE]
        Input Conditions - NULL
        Output - Moving average [DOUBLE]
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

        // Append data into array
        if (objects[1] instanceof Integer){
            num_arr.add((double)(Integer) objects[1]);
        } else if (objects[1] instanceof Double){
            num_arr.add((Double) objects[1]);
        }

        if ( count < window_size) {            //Return default value until fill the window
            count++;
            return 0.0;
        }else {                                //If window filled, do the calculation
            double tot = 0.0;

            //Calculate total
            for(double num: num_arr){
                tot += num;
            }

            //Remove first element in the queue
            num_arr.remove(0);

            return tot / window_size;
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
        return null;
    }

    @Override
    public void restoreState(Object[] objects) {

    }
}
