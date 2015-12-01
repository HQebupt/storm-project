package com.alipay.dw.jstorm.example.drpc;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import org.apache.thrift7.TException;

public class TestReachTopology {

    /**
     * @param args
     * @throws DRPCExecutionException
     * @throws TException
     */
    public static void main(String[] args) throws TException, DRPCExecutionException {

        if (args.length < 1) {
            throw new IllegalArgumentException("Invalid parameter");
        }
        //"foo.com/blog/1" "engineering.twitter.com/blog/5"
        DRPCClient client = new DRPCClient(args[0], 3772);
        String result = client.execute("reach", "tech.backtype.com/blog/123");

        System.out.println("\n!!! Drpc result:" + result);
    }

}
