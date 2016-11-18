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
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;
import java.util.ArrayList;

/*
* featureeng:movprob(window_size, no_of_bins, data_stream); [INT, INT, DOUBLE]
* Input Condition(s): no_of_bins < window_size
* Return Type(s): DOUBLE
*
* Calculate moving average
*/

public class MovingProbabilityAggregator extends AttributeAggregator {
    private static Attribute.Type type = Attribute.Type.DOUBLE;
    private ArrayList<Double> num_arr; //Keep window elements
    private double prob;    //Window probability
    private int count;      //Window element counter
    private int window_size;//Run length window
    private int nbins;      //Number of discrete levels
    private double val;     //Current value
    private double binSize; //Gap between consecutive discrete levels
    private double min;     //Local minimum for given window
    private double max;     //Local maximum for given window

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
         /*
        Input parameters - (window_size, number of bins, data_stream) [INT, INT, DOUBLE]
        Input Conditions - NULL
        Output - Moving probability [DOUBLE]
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
        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.INT){
            //K closest val
            throw new IllegalArgumentException("Number of closest values should be in type.INT");
        }
        if (attributeExpressionExecutors[2].getReturnType() != Attribute.Type.DOUBLE){
            //K closest val
            throw new IllegalArgumentException("Stream data should be in type.DOUBLE");
        }

        //Initialize variables
        this.num_arr = new ArrayList<Double>();
        this.prob = 0.0;
        this.count = 1;
        this.window_size = 0;
        this.nbins = 0;
        this.val = 0.0;
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
        window_size = (Integer) objects[0];   //Run length window
        nbins = (Integer) objects[1];         //No of discrete levels
        val = (Double) objects[2];            //Current value
        num_arr.add((Double) objects[2]);     //Append window array

        if ( count < window_size) {            //Return default val until fill the window
            count++;
        }else {                                //If window filled, do the calculation
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
        num_arr.remove(0);
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
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] objects) {
        //No need to maintain state
    }

    private double calculate(){
        //Initialize variables
        max = Double.MIN_VALUE;
        min = Double.MAX_VALUE;

        //Find local minimum and maximum
        for(double num: num_arr){
            if(num < min)
                min = num;
            if(num > max)
                max = num;
        }

        //Calculate width of the class
        binSize = (max - min) / nbins;

        //Generate histogram
        int[] result = new int[nbins];
        int binIndex;
        for(double num: num_arr){
            binIndex = (int)((num - min)/binSize);
            if(binIndex < 0)
                result[0] += 1;
            else if(binIndex >= nbins)
                result[nbins-1] += 1;
            else
                result[binIndex] += 1;
        }

        //Calculate relevant bin index for the given value
        binIndex = (int)((val - min)/binSize);

        //Calculate probability
        if (binIndex < 0)
            prob = (double)result[0] / window_size;
        else if (binIndex >= nbins)
            prob = (double)result[nbins-1] / window_size;
        else
            prob = (double)result[binIndex] / window_size;

        return prob;
    }
}
