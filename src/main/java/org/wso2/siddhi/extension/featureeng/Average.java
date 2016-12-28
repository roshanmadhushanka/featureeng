/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.extension.featureeng;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;

/**
 * featureeng:avg(windowSize, data_stream); [INT, DOUBLE]
 * Input Condition(s): NULL
 * Return Type(s): DOUBLE
 * <p>
 * Calculate moving average
 * Moving Average = SUM (x) / WINDOW_SIZE; where x is an element inside the window
 */

public class Average extends AttributeAggregator {
    private static Attribute.Type type = Attribute.Type.DOUBLE;
    private double total;               //Window total
    private int count;                  //Window element counter
    private int windowSize = 0;         //Run length window

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors,
                        ExecutionPlanContext executionPlanContext) {
        //No of parameter check
        if (attributeExpressionExecutors.length != 2) {
            throw new OperationNotSupportedException("2 parameters are required, given "
                    + attributeExpressionExecutors.length + " parameter(s)");
        }

        /* Data validation */
        //Window size
        if ((attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) &&
                (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT)) {
            this.windowSize = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue();
        } else {
            throw new IllegalArgumentException("First parameter should be the window size " +
                    "(Constant, type.INT)");
        }

        //Stream data
        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.DOUBLE) {
            throw new IllegalArgumentException("Stream data should be in type.DOUBLE");
        }

        //Initialize variables
        this.total = 0.0;
        this.count = 1;
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
        double val;
        double avg = 0.0;

        //Collect stream data
        val = (Double) objects[1];

        //Process data
        total += val;
        if (count < windowSize) {            //Return default value until fill the window
            count++;
        } else {                              //If window filled, do the calculation
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
        total -= (Double) objects[1];
        return null;
    }

    @Override
    public Object reset() {
        return null;
    }

    @Override
    public void start() {
        //Nothing to start
    }

    @Override
    public void stop() {
        //Nothing to stop
    }

    @Override
    public Object[] currentState() {
        return new Object[]{total, count, windowSize};
    }

    @Override
    public void restoreState(Object[] objects) {
        this.total = (Double) objects[0];
        this.count = (Integer) objects[1];
        this.windowSize = (Integer) objects[2];
    }

    /*
    Calculate average for a given window
     */
    private double calculate() {
        double avg;
        avg = total / windowSize;
        return avg;
    }
}
