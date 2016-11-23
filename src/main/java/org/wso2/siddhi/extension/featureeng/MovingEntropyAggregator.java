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
* featureeng:moventr(window_size, no_of_bins, data_stream); [INT, INT, DOUBLE]
* Input Condition(s): no_of_bins < window_size
* Return Type(s): DOUBLE
*
* Calculate moving entropy sum
* Moving Entropy Sum = SUM ( -PROBABILITY(x) * LOG(PROBABILITY(x))); where x is a defined interval
*/

public class MovingEntropyAggregator extends AttributeAggregator {
    private static Attribute.Type type = Attribute.Type.DOUBLE;
    private List<Double> num_arr;       //Keep window elements
    private double entr;                //Window entropy sum
    private int count;                  //Window element counter
    private int window_size;            //Run length window
    private int nbins;                  //Number of discrete levels
    private double val;                 //Current value
    private double binSize;             //Gap between consecutive discrete levels
    private double min;                 //Local minimum for given window
    private double max;                 //Local maximum for given window

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors,
                        ExecutionPlanContext executionPlanContext) {
        //No of parameter check
        if (attributeExpressionExecutors.length != 3){
            throw new OperationNotSupportedException("3 parameters are required, given "
                    + attributeExpressionExecutors.length + " parameter(s)");
        }

        /* Data validation */
        //Window size
        if ((attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) &&
                (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT)) {
            this.window_size = (Integer) attributeExpressionExecutors[0].execute(null);
        } else {
            throw new IllegalArgumentException("First parameter should be the window size " +
                    "(Constant, type.INT)");
        }

        //Number of bins
        if ((attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) &&
                (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT)){
            this.nbins = (Integer) attributeExpressionExecutors[1].execute(null);
        } else {
            throw new IllegalArgumentException("Number of bins should be (Constant, type.INT)");
        }

        //Stream data
        if (attributeExpressionExecutors[2].getReturnType() != Attribute.Type.DOUBLE){
            throw new IllegalArgumentException("Stream data should be in type.DOUBLE");
        }

        /* Input condition validation */
        if (nbins >= window_size){
            throw new OperationNotSupportedException("nbins value should be less than window size");
        }

        //Initialize variables
        this.num_arr = new ArrayList<Double>();
        this.entr = 0.0;
        this.count = 1;
        this.val = 0.0;
        this.binSize = 0.0;
        this.max = Double.MIN_VALUE;
        this.min = Double.MAX_VALUE;
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
        //Collect stream data
        val = (Double) objects[2];
        num_arr.add((Double) objects[2]);

        //Process data
        if ( count < window_size) {            //Return default val until fill the window
            count++;
        }else {                                //If window filled, do the calculation
            entr = calculate();
        }

        return entr;
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
        return null;
    }

    @Override
    public void restoreState(Object[] objects) {
        //No need to maintain state
    }

    /*
    Calculate entropy sum for a given window
     */
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
        double[] result = new double[nbins];
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

        //Calculate entropy sum
        entr = 0.0;
        for(int i=0; i<nbins; i++){
            val = result[i] / window_size;
            if(val > 0.0)
                val = -1 * val * Math.log(val);
            else if(val == 0.0)
                val = 0.0;
            else
                val = -1 * Double.MAX_VALUE;
            entr += val;
        }
        return entr;
    }
}