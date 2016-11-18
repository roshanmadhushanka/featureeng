package org.wso2.siddhi.extension.featureeng;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import java.util.concurrent.atomic.AtomicInteger;

public class MovingMedianAggregatorTestCase {
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
        String query ="@info(name = 'query1') " + "from inputStream#window.length(5) " + "select featureeng:movmed(5, tt) as ans insert into outputStream";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

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
