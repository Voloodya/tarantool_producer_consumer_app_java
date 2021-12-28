package org.tarantool.instances;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.tuple.TarantoolTuple;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Producer {
    public static void run(TarantoolClient client, TarantoolTuple data) {
        while(true) {
            try {
                List<Object> tasks = Arrays.asList(client.call("queue.put", data).get().toArray());
                for (Object task: tasks) {
                    System.out.println(task);
                }
            }
            catch (ExecutionException | InterruptedException e) {
                System.out.println("Failed to put");
            }
        }

        //System.out.println("Failed to deliver message");
    }
}