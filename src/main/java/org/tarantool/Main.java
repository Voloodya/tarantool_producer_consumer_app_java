package org.tarantool;

    import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
    import io.tarantool.driver.api.TarantoolClient;
    import io.tarantool.driver.api.tuple.TarantoolTuple;
    import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
    import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
    import org.tarantool.config.TarantoolConfig;
    import org.tarantool.instances.Consumer;
    import org.tarantool.instances.Producer;

    public class Main {

        private static final TarantoolTupleFactory tupleFactory =
                new DefaultTarantoolTupleFactory(
                        DefaultMessagePackMapperFactory
                                .getInstance()
                                .defaultComplexTypesMapper()
                );

        public static void main(String[] args) {
            if(args.length < 1) {
                System.out.println("Too few arguments");
                return;
            }

            String instance = args[0];
            TarantoolTuple data;
            TarantoolClient client  = TarantoolConfig.tarantoolClient(
                    "localhost:3301,localhost:3302",
                    "guest",
                    ""
            );
            if(instance.equals("producer") && args.length == 2) {
                data = tupleFactory.create(args[1]);
                Producer.run(client, data);
            } else if(instance.equals("consumer")) {
                Consumer.run(client, tupleFactory);
            } else {
                System.out.println("Too few arguments");
            }
        }
    }