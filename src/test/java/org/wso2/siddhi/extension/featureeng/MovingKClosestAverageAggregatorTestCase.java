package org.wso2.siddhi.extension.featureeng;

import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import java.util.concurrent.atomic.AtomicInteger;


public class MovingKClosestAverageAggregatorTestCase {
    private AtomicInteger count = new AtomicInteger(0);
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
    }

    @Test
    public void testSplitFunctionExtension() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (tt double);";
        String query ="@info(name = 'query1') " + "from inputStream#window.length(5) " + "select featureeng:movkavg(5, 3, tt) as ans insert into outputStream";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                EventPrinter.print(events);
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{10.2899});
        inputHandler.send(new Object[]{10.982});
        inputHandler.send(new Object[]{11.4363});
        inputHandler.send(new Object[]{11.653});
        inputHandler.send(new Object[]{11.6321});
        inputHandler.send(new Object[]{10.9959});
        inputHandler.send(new Object[]{10.3738});
        inputHandler.send(new Object[]{9.76563});
        inputHandler.send(new Object[]{9.17144});
        inputHandler.send(new Object[]{8.19278});
        inputHandler.send(new Object[]{7.49374});
        inputHandler.send(new Object[]{7.07431});
        inputHandler.send(new Object[]{10.2899});
        inputHandler.send(new Object[]{10.982});
        inputHandler.send(new Object[]{11.4363});
        inputHandler.send(new Object[]{11.653});
        inputHandler.send(new Object[]{11.6321});
        inputHandler.send(new Object[]{10.9959});
        inputHandler.send(new Object[]{10.3738});
        inputHandler.send(new Object[]{9.76563});
        inputHandler.send(new Object[]{9.17144});
        inputHandler.send(new Object[]{8.19278});
        inputHandler.send(new Object[]{7.49374});
        inputHandler.send(new Object[]{7.07431});
        inputHandler.send(new Object[]{10.2899});
        inputHandler.send(new Object[]{10.982});
        inputHandler.send(new Object[]{11.4363});
        inputHandler.send(new Object[]{11.653});
        inputHandler.send(new Object[]{11.6321});
        inputHandler.send(new Object[]{10.9959});
        inputHandler.send(new Object[]{10.3738});
        inputHandler.send(new Object[]{9.76563});
        inputHandler.send(new Object[]{9.17144});
        inputHandler.send(new Object[]{8.19278});
        inputHandler.send(new Object[]{7.49374});
        inputHandler.send(new Object[]{7.07431});
        inputHandler.send(new Object[]{10.2899});
        inputHandler.send(new Object[]{10.982});
        inputHandler.send(new Object[]{11.4363});
        inputHandler.send(new Object[]{11.653});
        inputHandler.send(new Object[]{11.6321});
        inputHandler.send(new Object[]{10.9959});
        inputHandler.send(new Object[]{10.3738});
        inputHandler.send(new Object[]{9.76563});
        inputHandler.send(new Object[]{9.17144});
        inputHandler.send(new Object[]{8.19278});
        inputHandler.send(new Object[]{7.49374});
        inputHandler.send(new Object[]{7.07431});
        inputHandler.send(new Object[]{10.2899});
        inputHandler.send(new Object[]{10.982});
        inputHandler.send(new Object[]{11.4363});
        inputHandler.send(new Object[]{11.653});
        inputHandler.send(new Object[]{11.6321});
        inputHandler.send(new Object[]{10.9959});
        inputHandler.send(new Object[]{10.3738});
        inputHandler.send(new Object[]{9.76563});
        inputHandler.send(new Object[]{9.17144});
        inputHandler.send(new Object[]{8.19278});
        inputHandler.send(new Object[]{7.49374});
        inputHandler.send(new Object[]{7.07431});
        inputHandler.send(new Object[]{10.2899});
        inputHandler.send(new Object[]{10.982});
        inputHandler.send(new Object[]{11.4363});
        inputHandler.send(new Object[]{11.653});
        inputHandler.send(new Object[]{11.6321});
        inputHandler.send(new Object[]{10.9959});
        inputHandler.send(new Object[]{10.3738});
        inputHandler.send(new Object[]{9.76563});
        inputHandler.send(new Object[]{9.17144});
        inputHandler.send(new Object[]{8.19278});
        inputHandler.send(new Object[]{7.49374});
        inputHandler.send(new Object[]{7.07431});
        inputHandler.send(new Object[]{10.2899});
        inputHandler.send(new Object[]{10.982});
        inputHandler.send(new Object[]{11.4363});
        inputHandler.send(new Object[]{11.653});
        inputHandler.send(new Object[]{11.6321});
        inputHandler.send(new Object[]{10.9959});
        inputHandler.send(new Object[]{10.3738});
        inputHandler.send(new Object[]{9.76563});
        inputHandler.send(new Object[]{9.17144});
        inputHandler.send(new Object[]{8.19278});
        inputHandler.send(new Object[]{7.49374});
        inputHandler.send(new Object[]{7.07431});
        inputHandler.send(new Object[]{10.2899});
        inputHandler.send(new Object[]{10.982});
        inputHandler.send(new Object[]{11.4363});
        inputHandler.send(new Object[]{11.653});
        inputHandler.send(new Object[]{11.6321});
        inputHandler.send(new Object[]{10.9959});
        inputHandler.send(new Object[]{10.3738});
        inputHandler.send(new Object[]{9.76563});
        inputHandler.send(new Object[]{9.17144});
        inputHandler.send(new Object[]{8.19278});
        inputHandler.send(new Object[]{7.49374});
        inputHandler.send(new Object[]{7.07431});

        Thread.sleep(2000);

        executionPlanRuntime.shutdown();
    }
}
