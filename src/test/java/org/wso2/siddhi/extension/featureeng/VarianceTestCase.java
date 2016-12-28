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

import org.junit.Assert;
import org.junit.Before;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.util.concurrent.atomic.AtomicInteger;

public class VarianceTestCase {
    private AtomicInteger count = new AtomicInteger(0);
    private volatile boolean eventArrived;
    private double[] testVal = {
            0.0,
            0.0,
            0.0,
            0.0,
            3.20955184,
            3.67707344,
            4.54018256,
            4.04403856,
            4.0460092,
            4.23357944,
            3.80901904,
            2.1315532,
            1.36233336,
            1.37510136,
            0.32560216,
            0.42577344,
            0.72531184,
            1.28267344,
            0.77759056
    };

    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
    }

    @org.junit.Test
    public void testMovingStandardDeviationCalculation() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (tt double);";
        String query = "@info(name = 'query1') " + "from inputStream#window.length(5) " +
                "select featureeng:var(5, tt) as ans insert into outputStream";

        ExecutionPlanRuntime executionPlanRuntime =
                siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                Assert.assertEquals(testVal[count.getAndIncrement()], (Double) events[0].getData(0), 0.00000001);
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{5.299});
        inputHandler.send(new Object[]{6.982});
        inputHandler.send(new Object[]{5.363});
        inputHandler.send(new Object[]{8.653});
        inputHandler.send(new Object[]{3.321});
        inputHandler.send(new Object[]{7.959});
        inputHandler.send(new Object[]{8.738});
        inputHandler.send(new Object[]{7.563});
        inputHandler.send(new Object[]{5.134});
        inputHandler.send(new Object[]{3.178});
        inputHandler.send(new Object[]{5.374});
        inputHandler.send(new Object[]{6.431});
        inputHandler.send(new Object[]{6.299});
        inputHandler.send(new Object[]{4.982});
        inputHandler.send(new Object[]{5.363});
        inputHandler.send(new Object[]{6.653});
        inputHandler.send(new Object[]{7.321});
        inputHandler.send(new Object[]{7.959});
        inputHandler.send(new Object[]{6.338});

        Thread.sleep(2000);

        executionPlanRuntime.shutdown();
    }
}
