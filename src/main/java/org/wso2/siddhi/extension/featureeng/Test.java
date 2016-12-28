package org.wso2.siddhi.extension.featureeng;

/**
 * Created by wso2123 on 12/21/16.
 */
public class Test {
    public static void main(String[] args) {
        Probability probability = new Probability();
        Object[] objects = new Object[] {5.299, 6.982, 5.363, 8.653, 3.321, 7.959, 8.738, 7.563, 5.134, 3.178, 5.374, 6.431, 6.299, 4.982, 5.363, 6.653, 7.321, 7.959, 6.338};

        for(Object object: objects){
            probability.processAdd(new Object[]{10.0, 5.0, new Double(object.toString())});
        }
    }
}
