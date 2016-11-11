package org.wso2.siddhi.extension.featureeng;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.function.EvalScript;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.core.util.extension.holder.EternalReferencedHolder;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.Arrays;

public class TestAggregator extends AttributeAggregator {
    private static Attribute.Type type = Attribute.Type.DOUBLE;
    private ArrayList<Double> num_arr;
    private int count = 1;
    private int window_size = 5;

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        //No of parameter check
        if (attributeExpressionExecutors.length != 1){
            throw new OperationNotSupportedException("Required number of parameters is 1, given "
                    + attributeExpressionExecutors.length + " parameters");
        }

        //Parameter type check
        Attribute.Type type = attributeExpressionExecutors[0].getReturnType();
        if(type != Attribute.Type.DOUBLE) {
            throw new IllegalArgumentException("Moving average only support for DOUBLE, given " + type);
        }
        num_arr = new ArrayList<Double>();
    }

    @Override
    public Attribute.Type getReturnType() {
        return null;
    }

    @Override
    public Object processAdd(Object o) {
        num_arr.add((Double) o);
        if(count < window_size){
            count++;
            return 0.0;
        }else{
            double tot = 0.0;
            for(double num: num_arr){
                tot += num;
            }
            return tot / window_size;
        }
    }

    @Override
    public Object processAdd(Object[] objects) {
        return null;
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
