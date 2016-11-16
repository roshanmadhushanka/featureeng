package org.wso2.siddhi.extension.featureeng;

import org.junit.Before;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import java.util.concurrent.atomic.AtomicInteger;

public class TestTestCase {
    private AtomicInteger count = new AtomicInteger(0);
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
    }

    @org.junit.Test
    public void testSplitFunctionExtension() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream1 (id int, tt double); define stream inputStream2 (id int, tt double);";
        String outStreamDefinition = "define stream outputStream1 (id int, tt double, ans1 double); define stream outputStream2 (id int, tt double, ans2 double);";
        String query1 = "@info(name = 'query1') from inputStream1#window.length(5) select id, tt, featureeng:test(5, tt) as ans1 insert into outputStream1;";
        String query2 = "@info(name = 'query2') from inputStream2#window.length(6) select id, tt, featureeng:test(6, tt) as ans2 insert into outputStream2;";
        String join = "@info(name = 'join') from outputStream1#window.time(10) as A join outputStream2#window.time(10) as B on A.id == B.id select A.id, A.tt, A.ans1, B.ans2 insert into outputStream;";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + outStreamDefinition + query1 + query2 + join);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                EventPrinter.print(events);
            }
        });

        InputHandler inputHandler1 = executionPlanRuntime.getInputHandler("inputStream1");
        executionPlanRuntime.start();

        inputHandler1.send(new Object[]{1, 10.2899});
        inputHandler1.send(new Object[]{2, 10.982});
        inputHandler1.send(new Object[]{3, 11.4363});
        inputHandler1.send(new Object[]{4, 11.653});
        inputHandler1.send(new Object[]{5, 11.6321});
        inputHandler1.send(new Object[]{6, 10.9959});
        inputHandler1.send(new Object[]{7, 10.3738});
        inputHandler1.send(new Object[]{8, 9.76563});
        inputHandler1.send(new Object[]{9, 9.17144});
        inputHandler1.send(new Object[]{10, 8.19278});

        InputHandler inputHandler2 = executionPlanRuntime.getInputHandler("inputStream2");
        executionPlanRuntime.start();


        inputHandler2.send(new Object[]{1, 7.49374});
        inputHandler2.send(new Object[]{2, 7.07431});
        inputHandler2.send(new Object[]{3, 10.2899});
        inputHandler2.send(new Object[]{4, 10.982});
        inputHandler2.send(new Object[]{5, 11.4363});
        inputHandler2.send(new Object[]{6, 11.653});
        inputHandler2.send(new Object[]{7, 11.6321});
        inputHandler2.send(new Object[]{8, 10.9959});
        inputHandler2.send(new Object[]{9, 10.3738});
        inputHandler2.send(new Object[]{10, 9.76563});


//        inputHandler.send(new Object[]{9.17144});
//        inputHandler.send(new Object[]{8.19278});
//        inputHandler.send(new Object[]{7.49374});
//        inputHandler.send(new Object[]{7.07431});
//        inputHandler.send(new Object[]{10.2899});
//        inputHandler.send(new Object[]{10.982});
//        inputHandler.send(new Object[]{11.4363});
//        inputHandler.send(new Object[]{11.653});
//        inputHandler.send(new Object[]{11.6321});
//        inputHandler.send(new Object[]{10.9959});
//        inputHandler.send(new Object[]{10.3738});
//        inputHandler.send(new Object[]{9.76563});
//        inputHandler.send(new Object[]{9.17144});
//        inputHandler.send(new Object[]{8.19278});
//        inputHandler.send(new Object[]{7.49374});
//        inputHandler.send(new Object[]{7.07431});
//        inputHandler.send(new Object[]{10.2899});
//        inputHandler.send(new Object[]{10.982});
//        inputHandler.send(new Object[]{11.4363});
//        inputHandler.send(new Object[]{11.653});
//        inputHandler.send(new Object[]{11.6321});
//        inputHandler.send(new Object[]{10.9959});
//        inputHandler.send(new Object[]{10.3738});
//        inputHandler.send(new Object[]{9.76563});
//        inputHandler.send(new Object[]{9.17144});
//        inputHandler.send(new Object[]{8.19278});
//        inputHandler.send(new Object[]{7.49374});
//        inputHandler.send(new Object[]{7.07431});
//        inputHandler.send(new Object[]{10.2899});
//        inputHandler.send(new Object[]{10.982});
//        inputHandler.send(new Object[]{11.4363});
//        inputHandler.send(new Object[]{11.653});
//        inputHandler.send(new Object[]{11.6321});
//        inputHandler.send(new Object[]{10.9959});
//        inputHandler.send(new Object[]{10.3738});
//        inputHandler.send(new Object[]{9.76563});
//        inputHandler.send(new Object[]{9.17144});
//        inputHandler.send(new Object[]{8.19278});
//        inputHandler.send(new Object[]{7.49374});
//        inputHandler.send(new Object[]{7.07431});
//        inputHandler.send(new Object[]{10.2899});
//        inputHandler.send(new Object[]{10.982});
//        inputHandler.send(new Object[]{11.4363});
//        inputHandler.send(new Object[]{11.653});
//        inputHandler.send(new Object[]{11.6321});
//        inputHandler.send(new Object[]{10.9959});
//        inputHandler.send(new Object[]{10.3738});
//        inputHandler.send(new Object[]{9.76563});
//        inputHandler.send(new Object[]{9.17144});
//        inputHandler.send(new Object[]{8.19278});
//        inputHandler.send(new Object[]{7.49374});
//        inputHandler.send(new Object[]{7.07431});
//        inputHandler.send(new Object[]{10.2899});
//        inputHandler.send(new Object[]{10.982});
//        inputHandler.send(new Object[]{11.4363});
//        inputHandler.send(new Object[]{11.653});
//        inputHandler.send(new Object[]{11.6321});
//        inputHandler.send(new Object[]{10.9959});
//        inputHandler.send(new Object[]{10.3738});
//        inputHandler.send(new Object[]{9.76563});
//        inputHandler.send(new Object[]{9.17144});
//        inputHandler.send(new Object[]{8.19278});
//        inputHandler.send(new Object[]{7.49374});
//        inputHandler.send(new Object[]{7.07431});
//        inputHandler.send(new Object[]{10.2899});
//        inputHandler.send(new Object[]{10.982});
//        inputHandler.send(new Object[]{11.4363});
//        inputHandler.send(new Object[]{11.653});
//        inputHandler.send(new Object[]{11.6321});
//        inputHandler.send(new Object[]{10.9959});
//        inputHandler.send(new Object[]{10.3738});
//        inputHandler.send(new Object[]{9.76563});
//        inputHandler.send(new Object[]{9.17144});
//        inputHandler.send(new Object[]{8.19278});
//        inputHandler.send(new Object[]{7.49374});
//        inputHandler.send(new Object[]{7.07431});

        Thread.sleep(2000);

        executionPlanRuntime.shutdown();
    }
}
