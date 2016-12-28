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
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.util.concurrent.atomic.AtomicInteger;

public class MedianTestCase {
    private AtomicInteger count = new AtomicInteger(0);
    private volatile boolean eventArrived;
    private double[] testVal = {
            0.0,
            0.0,
            0.0,
            0.0,
            5.363,
            6.982,
            7.959,
            7.959,
            7.563,
            7.563,
            5.374,
            5.374,
            5.374,
            5.374,
            5.374,
            6.299,
            6.299,
            6.653,
            6.653
    };

    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
    }

    @Test
    public void testMovingMedianCalculation() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (tt double);";
        String query = "@info(name = 'query1') " + "from inputStream#window.length(5) " +
                "select featureeng:med(5, tt) as ans insert into outputStream";

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
