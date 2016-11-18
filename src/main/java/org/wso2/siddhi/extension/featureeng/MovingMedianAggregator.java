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

/*
* featureeng:movmed(window_size, data_stream); [INT, DOUBLE]
* Input Condition(s): NULL
* Return Type(s): DOUBLE
*
* Calculate moving median
* Moving Median = MIDDLE_ELEMENT(SORT(WINDOW_ELEMENTS)); if LENGTH(WINDOW) is odd
*                 AVERAGE_OF_MIDDLE_2_ELEMENTS(SORT(WINDOW_ELEMENTS)); if LENGTH(WINDOW) is even
*/

public class MovingMedianAggregator extends AttributeAggregator {
    private static Attribute.Type type = Attribute.Type.DOUBLE;
    private ArrayList<Double> num_arr; //Keep window elements
    private double median;  //Window median
    private int count;      //Window element counter
    private int window_size;//Run length window

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        //No of parameter check
        if (attributeExpressionExecutors.length != 2){
            throw new OperationNotSupportedException("2 parameters are required, given "
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

        //Stream data
        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.DOUBLE){
            throw new IllegalArgumentException("Stream data should be in type.DOUBLE");
        }

        //Initialize variables
        this.num_arr = new ArrayList<Double>();
        this.median = 0.0;
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
        num_arr.add((Double) objects[1]);

        //Process data
        if ( count < window_size) {            //Return default value until fill the window
            count++;
        }else {                                //If window filled, do the calculation
            median = calculate();
        }

        return median;
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

    /*
        Calculate median for a given window
     */
    private double calculate(){
        //Create temp list, otherwise list sort will change the original order of the data
        ArrayList<Double> tmp = new ArrayList<Double>(num_arr);

        //Sort values
        Collections.sort(tmp);

        //Get median value
        if(window_size % 2 == 0){
            int index = window_size / 2;
            median = (tmp.get(index) + tmp.get(index-1)) / 2;
        }else{
            median =  tmp.get(window_size / 2);
        }
        return median;
    }
}
