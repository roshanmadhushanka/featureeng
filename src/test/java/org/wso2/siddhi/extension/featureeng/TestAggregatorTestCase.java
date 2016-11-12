package org.wso2.siddhi.extension.featureeng;

import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class TestAggregatorTestCase{
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

        String inStreamDefinition = "define stream inputStream1 (id1 int, tt double); define stream inputStream2 (id2 int, ss double);";
        String outStreamDefinition = "define stream outputStream1 (id1 int, tt double, ans1 double); define stream outputStream2 (id2 int, ss double, ans2 double);";

        String query1 ="@info(name = 'query1') " + "from inputStream1#window.length(5) select id1, tt, featureeng:test(tt) as ans1 insert into outputStream1;";
        String query2 ="@info(name = 'query2') " + "from inputStream2#window.length(5) select id2, ss, featureeng:test(ss) as ans2 insert into outputStream2;";
        String query3 ="@info(name = 'query3') " + "from outputStream1#window.time(100) as A join outputStream2#window.time(100) as B on A.id1==B.id2 select * insert into outputStream;";


        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query1 + query2 + query3);

//        executionPlanRuntime.addCallback("outputStream1", new StreamCallback() {
//            @Override
//            public void receive(org.wso2.siddhi.core.event.Event[] events) {
//                EventPrinter.print(events);
//            }
//        });
//
//        executionPlanRuntime.addCallback("outputStream2", new StreamCallback() {
//            @Override
//            public void receive(org.wso2.siddhi.core.event.Event[] events) {
//                EventPrinter.print(events);
//            }
//        });

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                EventPrinter.print(events);
            }
        });



        InputHandler inputHandler1 = executionPlanRuntime.getInputHandler("inputStream1");
        InputHandler inputHandler2 = executionPlanRuntime.getInputHandler("inputStream2");
        executionPlanRuntime.start();
//
//        inputHandler1.send(new Object[]{0, 10.2899});
//        inputHandler1.send(new Object[]{1, 10.982});
//        inputHandler1.send(new Object[]{2, 11.4363});
//        inputHandler1.send(new Object[]{3, 11.653});
//        inputHandler1.send(new Object[]{4, 11.6321});
//        inputHandler1.send(new Object[]{5, 10.9959});
//        inputHandler1.send(new Object[]{6, 10.3738});
//        inputHandler1.send(new Object[]{7, 9.76563});
//        inputHandler1.send(new Object[]{8, 9.17144});
//        inputHandler1.send(new Object[]{9, 8.19278});
//        inputHandler1.send(new Object[]{10, 7.49374});
//        inputHandler1.send(new Object[]{11, 7.07431});
//
//        inputHandler2.send(new Object[]{0, 15.2899});
//        inputHandler2.send(new Object[]{1, 12.982});
//        inputHandler2.send(new Object[]{2, 17.4363});
//        inputHandler2.send(new Object[]{3, 2.653});
//        inputHandler2.send(new Object[]{4, 8.6321});
//        inputHandler2.send(new Object[]{5, 9.9959});
//        inputHandler2.send(new Object[]{6, 12.3738});
//        inputHandler2.send(new Object[]{7, 10.76563});
//        inputHandler2.send(new Object[]{8, 13.17144});
//        inputHandler2.send(new Object[]{9, 12.19278});
//        inputHandler2.send(new Object[]{10, 10.49374});
//        inputHandler2.send(new Object[]{11, 14.07431});


        inputHandler1.send(new Object[]{0, 10.2899});
        inputHandler2.send(new Object[]{0, 15.2899});
        Thread.sleep(10);

        inputHandler1.send(new Object[]{1, 10.982});
        inputHandler2.send(new Object[]{1, 12.982});
        Thread.sleep(10);

        inputHandler1.send(new Object[]{2, 11.4363});
        inputHandler2.send(new Object[]{2, 17.4363});
        Thread.sleep(10);

        inputHandler1.send(new Object[]{3, 11.653});
        inputHandler2.send(new Object[]{3, 2.653});
        Thread.sleep(10);

        inputHandler1.send(new Object[]{4, 11.6321});
        inputHandler2.send(new Object[]{4, 8.6321});
        Thread.sleep(10);

        inputHandler1.send(new Object[]{5, 10.9959});
        inputHandler2.send(new Object[]{5, 9.9959});
        Thread.sleep(10);

        inputHandler1.send(new Object[]{6, 10.3738});
        inputHandler2.send(new Object[]{6, 12.3738});
        Thread.sleep(10);

        inputHandler1.send(new Object[]{7, 9.76563});
        inputHandler2.send(new Object[]{7, 10.76563});
        Thread.sleep(10);

        inputHandler1.send(new Object[]{8, 9.17144});
        inputHandler2.send(new Object[]{8, 13.17144});
        Thread.sleep(10);

        inputHandler1.send(new Object[]{9, 8.19278});
        inputHandler2.send(new Object[]{9, 12.19278});
        Thread.sleep(10);

        inputHandler1.send(new Object[]{10, 7.49374});
        inputHandler2.send(new Object[]{10, 10.49374});
        Thread.sleep(10);

        inputHandler1.send(new Object[]{11, 7.07431});
        inputHandler2.send(new Object[]{11, 14.07431});
        Thread.sleep(10);

        Thread.sleep(2000);

        executionPlanRuntime.shutdown();
    }
}
