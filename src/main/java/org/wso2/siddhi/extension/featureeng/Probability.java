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

//Change namespace
package org.wso2.siddhi.extension.featureeng;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.UnsupportedAttributeTypeException;

import java.util.ArrayList;
import java.util.List;

/*
* featureeng:prob(windowSize, noOfBins, data_stream); [INT, INT, DOUBLE]
* Input Condition(s): no_of_bins < windowSize
* Return Type(s): DOUBLE
*
* Calculate probability
*/

public class Probability extends AttributeAggregator {
    private static Attribute.Type type = Attribute.Type.DOUBLE;
    private List<Double> windowElements;    //Keep window elements
    private int count;                      //Window element counter
    private int[] histogram;                //Histogram
    private boolean histogramUpdate;        //Histogram Update flag
    private boolean isWindowFull;           //Window flag
    private int windowSize;                 //Run length window
    private int numberOfBins;               //Number of discrete levels *
    private double binSize;                 //Gap between consecutive discrete levels *
    private double min;                     //Local minimum for given window
    private double max;                     //Local maximum for given window
    private double prob;                    //Probability * event expired event

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors,
                        ExecutionPlanContext executionPlanContext) {

        validate();

        //Initialize variables
        this.windowElements = new ArrayList<Double>();
        this.count = 1;
        this.max = Double.MIN_VALUE;
        this.min = Double.MAX_VALUE;
        this.histogram = new int[numberOfBins];
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
        double prob = 0.0;

        //Collect stream data
        Double currentValue = new Double(objects[2].toString());
        windowElements.add(currentValue);

        // Check for local minimum and maximum
        if (currentValue < min) {
            min = currentValue;
            histogramUpdate = true;
        } else if (currentValue > max) {
            max = currentValue;
            histogramUpdate = true;
        }

        if (!isWindowFull) {
            if (count < windowSize) {
                count++;
            } else {
                isWindowFull = true;
            }
        }

        if (isWindowFull) {
            if (histogramUpdate) {
                // Update entire histogram
                updateHistogram();
                histogramUpdate = false;
            } else {
                // Update only entered value
                int binIndex = (int) ((currentValue - min) / binSize);
                if (binIndex < 0)
                    histogram[0] += 1;
                else if (binIndex >= numberOfBins)
                    histogram[numberOfBins - 1] += 1;
                else
                    histogram[binIndex] += 1;
            }

            prob = calculate(currentValue);
        }

        return prob;
    }

    @Override
    public Object processRemove(Object o) {
        return null;
    }

    @Override
    public Object processRemove(Object[] objects) {
        Double removingValue = new Double(objects[2].toString());

        // Remove element from histogram
        int binIndex = (int) ((removingValue - min) / binSize);
        if (binIndex < 0)
            histogram[0] -= 1;
        else if (binIndex >= numberOfBins)
            histogram[numberOfBins - 1] -= 1;
        else
            histogram[binIndex] -= 1;

        // If removing value is minimum or maximum update histogram
        if (removingValue == max) {
            histogramUpdate = true;
        } else if (removingValue == min) {
            histogramUpdate = true;
        }

        // Remove element from window
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
        return new Object[]{windowElements, isWindowFull};
    }

    @Override
    public void restoreState(Object[] objects) {
        this.windowElements = (List<Double>) objects[0];
        this.isWindowFull = (Boolean) objects[1];
        updateHistogram();
    }

    private void validate() {
        //Validate initialisation parameters

        //No of parameter check
        if (attributeExpressionExecutors.length != 3) {
            throw new OperationNotSupportedException("3 parameters are required, given "
                    + attributeExpressionExecutors.length + " parameter(s)");
        }

        /* Data validation */
        //Window size
        if ((attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) &&
                (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT)) {
            // Constant -> get value
            this.windowSize = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue();
        } else {
            throw new IllegalArgumentException("First parameter should be the window size " +
                    "(Constant, type.INT)");
        }

        //Number of bins
        if ((attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) &&
                (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT)) {
            this.numberOfBins = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
        } else {
            throw new IllegalArgumentException("Number of bins should be (Constant, type.INT)");
        }

        //Data stream
        Attribute.Type dataType = attributeExpressionExecutors[2].getReturnType();
        if (dataType != Attribute.Type.FLOAT && dataType != Attribute.Type.DOUBLE &&
                dataType != Attribute.Type.INT && dataType != Attribute.Type.LONG) {

            throw new UnsupportedAttributeTypeException("Stream data should be in type [FLOAT, DOUBLE, INT, LONG]");
        }

        /* Input condition validation */
        if (numberOfBins >= windowSize) {
            throw new OperationNotSupportedException("numberOfBins value should be less than window size");
        }
    }

    private void updateHistogram() {
        //Update histogram

        //Find local min and max
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
        binSize = (max - min) / numberOfBins;

        //Generate histogram
        histogram = new int[numberOfBins];
        int binIndex;
        for (double num : windowElements) {
            binIndex = (int) ((num - min) / binSize);
            if (binIndex < 0)
                histogram[0] += 1;
            else if (binIndex >= numberOfBins)
                histogram[numberOfBins - 1] += 1;
            else
                histogram[binIndex] += 1;
        }
    }

    /*
        Calculate moving probability for the recent value in a given window
     */
    private double calculate(double currentValue) {
        //Calculate relevant bin index for the given value
        int binIndex = (int) ((currentValue - min) / binSize);

        //Calculate probability
        double prob;
        if (binIndex < 0)
            prob = (double) histogram[0] / windowSize;
        else if (binIndex >= numberOfBins)
            prob = (double) histogram[numberOfBins - 1] / windowSize;
        else
            prob = (double) histogram[binIndex] / windowSize;

        return prob;
    }
}
