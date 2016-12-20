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

/*
* featureeng:movprob(windowSize, no_of_bins, data_stream); [INT, INT, DOUBLE]
* Input Condition(s): no_of_bins < windowSize
* Return Type(s): DOUBLE
*
* Calculate moving average
*/

public class Probability extends AttributeAggregator {
    private static Attribute.Type type = Attribute.Type.DOUBLE;
    private List<Double> windowElements;    //Keep window elements
    private int count;                      //Window element counter
    private int windowSize;                 //Run length window
    private int nbins;                      //Number of discrete levels
    private double currentValue;            //Current value
    private double binSize;                 //Gap between consecutive discrete levels
    private double min;                     //Local minimum for given window
    private double max;                     //Local maximum for given window

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

        //Number of bins
        if ((attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) &&
                (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT)) {
            this.nbins = (Integer) attributeExpressionExecutors[1].execute(null);
        } else {
            throw new IllegalArgumentException("Number of bins should be (Constant, type.INT)");
        }

        //Data stream
        if (attributeExpressionExecutors[2].getReturnType() != Attribute.Type.DOUBLE) {
            throw new IllegalArgumentException("Stream data should be in type.DOUBLE");
        }

        /* Input condition validation */
        if (nbins >= windowSize) {
            throw new OperationNotSupportedException("nbins value should be less than window size");
        }

        //Initialize variables
        this.windowElements = new ArrayList<Double>();
        this.count = 1;
        this.currentValue = 0.0;
        this.binSize = 0.0;
        this.max = Double.MIN_VALUE;
        this.min = Double.MAX_VALUE;
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
        double prob = 0.0;

        //Collect stream data
        currentValue = (Double) objects[2];
        windowElements.add((Double) objects[2]);

        //Process data
        if (count < windowSize) {            //Return default currentValue until fill the window
            count++;
        } else {                                //If window filled, do the calculation
            prob = calculate();
        }

        return prob;
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
        return new Object[] {windowElements, count, windowSize, nbins, currentValue, binSize, min, max};
    }

    @Override
    public void restoreState(Object[] objects) {
        this.windowElements = (List<Double>) objects[0];
        this.count = (Integer) objects[1];
        this.windowSize = (Integer) objects[2];
        this.nbins = (Integer) objects[3];
        this.currentValue = (Double) objects[4];
        this.binSize = (Double) objects[5];
        this.min = (Double) objects[6];
        this.max = (Double) objects[7];
    }

    /*
        Calculate moving probability for the recent value in a given window
     */
    private double calculate() {
        double prob;

        //Initialize variables
        max = Double.MIN_VALUE;
        min = Double.MAX_VALUE;

        //Find local minimum and maximum
        for (double num : windowElements) {
            if (num < min)
                min = num;
            if (num > max)
                max = num;
        }

        //Calculate width of the class
        binSize = (max - min) / nbins;

        //Generate histogram
        int[] result = new int[nbins];
        int binIndex;
        for (double num : windowElements) {
            binIndex = (int) ((num - min) / binSize);
            if (binIndex < 0)
                result[0] += 1;
            else if (binIndex >= nbins)
                result[nbins - 1] += 1;
            else
                result[binIndex] += 1;
        }

        //Calculate relevant bin index for the given value
        binIndex = (int) ((currentValue - min) / binSize);

        //Calculate probability
        if (binIndex < 0)
            prob = (double) result[0] / windowSize;
        else if (binIndex >= nbins)
            prob = (double) result[nbins - 1] / windowSize;
        else
            prob = (double) result[binIndex] / windowSize;

        return prob;
    }
}
