package org.wso2.siddhi.extension.featureeng;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;

public class MovingStandardDeviationAggregator extends AttributeAggregator{
    private static Attribute.Type type = Attribute.Type.DOUBLE;
    private double tot;         //Window total
    private double avg;         //Window average
    private double std;         //Window standard deviation
    private int count;          //Window element counter
    private int window_size;
    private ArrayList<Double> num_arr; //Keep window elements

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        /*
        Input parameters - (window_size, data_stream) [INT, DOUBLE]
        Input Conditions - NULL
        Output - Moving standard deviation [DOUBLE]
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
        this.tot = 0.0;
        this.avg = 0.0;
        this.std = 0.0;
        this.count = 1;
        this.window_size = 0;
        this.num_arr = new ArrayList<Double>();
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
        double val = (Double) objects[1];
        num_arr.add(val);                      //Append window array

        tot += val;
        if ( count < window_size) {            //Return default value until fill the window
            count++;
        }else {                                //If window filled, do the calculation
            std = calculate();
        }
        return std;
    }

    @Override
    public Object processRemove(Object o) {
        return null;
    }

    @Override
    public Object processRemove(Object[] objects) {
        tot -= (Double) objects[1];
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
        return null;
    }

    @Override
    public void restoreState(Object[] objects) {

    }

    private double calculate(){
        avg = tot / window_size;
        std = 0.0;
        for(double num: num_arr){
            std += Math.pow(num, 2.0);
        }
        std = (std/window_size) - Math.pow(avg, 2.0);
        std = Math.sqrt(std);
        return std;
    }
}
