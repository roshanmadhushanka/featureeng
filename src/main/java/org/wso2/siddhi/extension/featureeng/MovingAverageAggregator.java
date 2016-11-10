package org.wso2.siddhi.extension.featureeng;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by wso2123 on 11/10/16.
 */
public class MovingAverageAggregator extends AttributeAggregator{
    private MovingAverageAggregator movingAverageAggregator;

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        //No of parameter check
        if (attributeExpressionExecutors.length != 1){
            throw new OperationNotSupportedException("Required number of parameters is 1, given "
                    + attributeExpressionExecutors.length + " parameters");
        }

        //Parameter type check
        Attribute.Type type = attributeExpressionExecutors[0].getReturnType();
        if(type == Attribute.Type.DOUBLE){
            movingAverageAggregator = new MovingAverageAggregator().
        }else{
            throw new IllegalArgumentException("Moving average only support for DOUBLE, given " + type);
        }

    }

    @Override
    public Attribute.Type getReturnType() {
        return movingAverageAggregator.getReturnType();
    }

    @Override
    public Object processAdd(Object o) {
        return movingAverageAggregator.processAdd(o);
    }

    @Override
    public Object processAdd(Object[] objects) {
        return new OperationNotSupportedException("Moving average cannot process data array, given " + Arrays.deepToString(objects));
    }

    @Override
    public Object processRemove(Object o) {
        return movingAverageAggregator.processRemove(o);
    }

    @Override
    public Object processRemove(Object[] objects) {
        return new OperationNotSupportedException("Moving average cannot process data array, given " + Arrays.deepToString(objects));
    }

    @Override
    public Object reset() {
        return movingAverageAggregator.reset();
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return movingAverageAggregator.currentState();
    }

    @Override
    public void restoreState(Object[] objects) {

    }

    private class MovingAverage extends MovingAverageAggregator {
        private ArrayList<Double> data = new ArrayList<Double>();
        private int count = 0;

        @Override
        public Attribute.Type getReturnType() {
            return Attribute.Type.DOUBLE;
        }

        @Override
        public Object processAdd(Object o) {

        }

        @Override
        public Object processRemove(Object o) {
            data.remove(0);
            return 0;
        }

        private double calculate(ArrayList<Double> arr){
            double
            for(int i=0; i < arr.size(); i++){

            }
        }
    }
}
