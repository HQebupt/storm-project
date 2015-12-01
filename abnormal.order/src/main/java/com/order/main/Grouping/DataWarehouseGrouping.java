package com.order.main.Grouping;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by LiMingji on 2015/6/17.
 */
public class DataWarehouseGrouping implements CustomStreamGrouping, Serializable {
    int numTasks = 0;

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList();
        if (values.size() > 0) {
            String msisdn = values.get(0).toString();
            String channelCode = values.get(4).toString();
            String orderType = values.get(7).toString();
            String key = msisdn + channelCode + orderType;
            boltIds.add(key.hashCode() % numTasks);
        }
        return boltIds;
    }

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        numTasks = targetTasks.size();
    }
}
