package org.wso2.siddhi.extension.featureeng;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;

public class MovingThresholdAverageAggregator extends AttributeAggregator {
    private static Attribute.Type type = Attribute.Type.DOUBLE;
    private double tot;         //Window total
    private double avg;         //Window average
    private double threshold;   //Threshold value to the original value and last occurence. [Only accept if the difference is under threshold]
    private double val;         //Value received from the stream
    private int count;          //Window element counter
    private int window_size;    //Run length window

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        /*
        Input parameters - (window_size, threshold, data_stream) [INT, DOUBLE, DOUBLE]
        Input Conditions - NULL
        Output - Moving threshold average [DOUBLE]
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
        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.DOUBLE){
            //Threshold value
            throw new IllegalArgumentException("Threshold value should be in type.DOUBLE");
        }
        if (attributeExpressionExecutors[2].getReturnType() != Attribute.Type.DOUBLE){
            //Stream data
            throw new IllegalArgumentException("Stream data should be in type.DOUBLE");
        }

        //Initialize variables
        this.tot = 0.0;
        this.avg = 0.0;
        this.threshold = 0.0;
        this.val = 0.0;
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
        threshold = (Double) objects[1];
        val = (Double) objects[2];

        tot += val;
        if ( count < window_size) {            //Return default value until fill the window
            count++;
        }else {                                //If window filled, do the calculation
            avg = caculate();
        }
        return avg;
    }

    @Override
    public Object processRemove(Object o) {
        return null;
    }

    @Override
    public Object processRemove(Object[] objects) {
        tot -= (Double) objects[2];
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

    private double caculate(){
        avg = tot / window_size;
        if (Math.abs(val - avg) > threshold){
            avg = val;
        }
        return avg;
    }
}
