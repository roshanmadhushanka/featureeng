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

/*
* featureeng:movtavg(window_size, threshold, data_stream); [INT, DOUBLE, DOUBLE]
* Input Condition(s): NULL
* Return Type(s): DOUBLE
*
* Calculate moving threshold average
* Moving Threshold Average = Calculate moving average if the absolute difference between moving
* average and the most recent value is less than the threshold then returns the moving average else
* return the recen value.
*/

public class MovingThresholdAverageAggregator extends AttributeAggregator {
    private static Attribute.Type type = Attribute.Type.DOUBLE;
    private double tot;         //Window total
    private double avg;         //Window average
    private double threshold;   //Threshold value to the original value and last occurence.
                                // [Only accept if the difference is under threshold]
    private double val;         //Value received from the stream
    private int count;          //Window element counter
    private int window_size;    //Run length window

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
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

        //Threshold value
        if ((attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) &&
                (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.DOUBLE)){
            this.threshold = (Double) attributeExpressionExecutors[1].execute(null);
        } else {
            throw new IllegalArgumentException("Threshold value should be " +
                    "(constatnt, type.DOUBLE)");
        }

        //Stream data
        if (attributeExpressionExecutors[2].getReturnType() != Attribute.Type.DOUBLE){
            throw new IllegalArgumentException("Stream data should be in type.DOUBLE");
        }

        //Initialize variables
        this.tot = 0.0;
        this.avg = 0.0;
        this.val = 0.0;
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
        //Collect stream data
        val = (Double) objects[2];

        //Process data
        tot += val;
        if ( count < window_size) {            //Return default value until fill the window
            count++;
        }else {                                //If window filled, do the calculation
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
        tot -= (Double) objects[2];
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
        Calculate moving threshold average for a given window
     */
    private double calculate(){
        avg = tot / window_size;
        if (Math.abs(val - avg) > threshold){
            avg = val;
        }
        return avg;
    }
}