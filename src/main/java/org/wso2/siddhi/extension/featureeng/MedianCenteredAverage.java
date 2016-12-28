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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
* featureeng:mcavg(windowSize, boundary, data_stream); [INT, INT, DOUBLE]
* Input Condition(s): 2 * boundary < windowSize
* Return Type(s): DOUBLE
*
* Calculate moving median centered average
* Moving Median Centered average = AVERAGE(REMOVE_BOUNDRY_FROM_BOTH_ENDS(SORT(WINDOW_ELEMENTS)))
*/

public class MedianCenteredAverage extends AttributeAggregator {
    private static Attribute.Type type = Attribute.Type.DOUBLE;
    private List<Double> windowElements;    //Keep window elements
    private int count;                      //Window element counter
    private int windowSize;                 //Run length window
    private int boundary;                   //Number of removing items from each side [front, end]

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        //No of parameter check
        if (attributeExpressionExecutors.length != 3) {
            throw new OperationNotSupportedException("3 parameters are required, given "
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

        //Limit
        if ((attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) &&
                (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT)) {
            this.boundary = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
        } else {
            throw new IllegalArgumentException("Boundary value should be (Constant, type.INT");
        }

        //Data stream
        if (attributeExpressionExecutors[2].getReturnType() != Attribute.Type.DOUBLE) {
            throw new IllegalArgumentException("Stream data should be in type.DOUBLE");
        }

        /* Input condition validation */
        if (2 * boundary > windowSize) {
            throw new OperationNotSupportedException("boundary should be twice less than window size");
        }

        //Initialize variables
        this.windowElements = new ArrayList<Double>();
        this.count = 1;
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
        double avg = 0.0;

        //Collect stream data
        windowElements.add((Double) objects[2]);

        //Process data
        if (count < windowSize) {            //Return default value until fill the window
            count++;
        } else {                                //If window filled, do the calculation
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
        windowElements.remove(0);
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
        return new Object[]{windowElements, count, windowSize, boundary};
    }

    @Override
    public void restoreState(Object[] objects) {
        this.windowElements = (List<Double>) objects[0];
        this.count = (Integer) objects[1];
        this.windowSize = (Integer) objects[2];
        this.boundary = (Integer) objects[3];
    }

    /*
        Calculate moving median centered average for a given window
     */
    private double calculate() {
        double tot = 0.0;
        double avg;

        //Create temp list, otherwise list sort will change the original order of the data
        ArrayList<Double> tmp = new ArrayList<Double>(windowElements);

        //Sort values
        Collections.sort(tmp);

        //Remove corner values
        for (int i = 0; i < boundary; i++) {
            tmp.remove(0);
            tmp.remove(tmp.size() - 1);
        }

        //Calculate total
        for (Double aTmp : tmp) {
            tot += aTmp;
        }

        avg = tot / tmp.size();

        return avg;
    }
}
