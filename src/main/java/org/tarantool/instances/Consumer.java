package org.tarantool.instances;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Consumer {
    public static void run(TarantoolClient client, TarantoolTupleFactory tupleFactory){

        int timeout = 1;
        int count = 0;
        long last = System.currentTimeMillis();

        while(true) {
            try {
                List<Object> tasks = Arrays.asList(client.call("queue.take", timeout).get().toArray());

                if(tasks.size() == 1) {
                    List<Object> tuple = (ArrayList<Object>)tasks.get(0);

                    client.call("queue.ack", tupleFactory.create((String)tuple.get(0)));
                    count++;

                    if( count % 5000 == 0) {
                        System.out.printf("Processed %s in %.4fs\n", count, (double)(System.currentTimeMillis() - last) / 1000);
                        last = System.currentTimeMillis();
                    }
                } else {
                    System.out.println("Not tasks from queue");
                }
            }
            catch (ExecutionException | InterruptedException e) {
                System.out.println("Failed to take and ack");
            }
        }
    }
}