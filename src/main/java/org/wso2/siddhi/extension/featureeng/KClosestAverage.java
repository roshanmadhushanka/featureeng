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
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/*
* featureeng:movkavg(windowSize, kClosest, data_stream); [INT, INT, DOUBLE]
* Input Condition(s): k_closet < windowSize
* Return Type(s): DOUBLE
*
* Calculate k closest average
* Moving K Closest Average = Select K number of closest values to the last occurrence including
*                            itself and calculate the average
*/

public class KClosestAverage extends AttributeAggregator {
    private static Attribute.Type type = Attribute.Type.DOUBLE;
    private List<Double> windowElements;    //Keep window elements
    private int count;                      //Window element counter
    private int windowSize;                 //Run length window
    private int kClosest;                   //Number of closest values to the last occurence

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors,
                        ExecutionPlanContext executionPlanContext) {
        //No of parameter check
        if (attributeExpressionExecutors.length != 3) {
            throw new OperationNotSupportedException("3 parameters are required, given "
                    + attributeExpressionExecutors.length + " parameter(s)");
        }

        /* Data validation */
        //Window size
        if ((attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) &&
                (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT)) {
            this.windowSize = (Integer) attributeExpressionExecutors[0].execute(null);
        } else {
            throw new IllegalArgumentException("First parameter should be the window size " +
                    "(Constant, type.INT)");
        }

        //K closest value
        if ((attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) &&
                (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT)) {
            this.kClosest = (Integer) attributeExpressionExecutors[1].execute(null);
        } else {
            throw new IllegalArgumentException("Number of closest values should be " +
                    "(Constant, type.INT)");
        }

        //Data stream
        if (attributeExpressionExecutors[2].getReturnType() != Attribute.Type.DOUBLE) {
            //K closest value
            throw new IllegalArgumentException("Stream data should be in type.DOUBLE");
        }

        /* Input condition validation */
        if (kClosest > windowSize) {
            throw new OperationNotSupportedException("K value should be less than window size");
        }

        //Initialize variables
        this.windowElements = new ArrayList<Double>();
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
        double avg = 0.0;

        //Collect stream data
        windowElements.add((Double) objects[2]);

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
        return new Object[] {windowElements, count, windowSize, kClosest};
    }

    @Override
    public void restoreState(Object[] objects) {
        this.windowElements = (List<Double>) objects[0];
        this.count = (Integer) objects[1];
        this.windowSize = (Integer) objects[2];
        this.kClosest = (Integer) objects[3];
    }

    /*
        Calculate k closest average for a given window
     */
    private double calculate() {
        double tot = 0.0;
        double avg;

        /* Add numbers in sorted order by absolute difference of the number compared to the last
        number in window */
        SortedMap<Double, Double> sortedMap = new TreeMap<Double, Double>();
        for (int i = 0; i < windowSize; i++) {
            double key = Math.abs(windowElements.get(i) - windowElements.get(windowSize - 1));
            sortedMap.put(key, windowElements.get(i));
        }

        //Convert keys in the sorted map to double
        ArrayList<Double> key_array = new ArrayList<Double>();
        for (double key : sortedMap.keySet()) {
            key_array.add(key);
        }

        //Calculate the total of k closest values
        for (int i = 0; i < kClosest; i++) {
            tot += sortedMap.get(key_array.get(i));
        }

        //Calculate the average
        avg = tot / kClosest;
        return avg;
    }
}
