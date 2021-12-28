package org.tarantool.config;

    import io.tarantool.driver.api.TarantoolClient;
    import io.tarantool.driver.api.TarantoolClientConfig;
    import io.tarantool.driver.api.TarantoolClusterAddressProvider;
    import io.tarantool.driver.api.TarantoolServerAddress;
    import io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies;
    import io.tarantool.driver.auth.SimpleTarantoolCredentials;
    import io.tarantool.driver.core.ClusterTarantoolTupleClient;
    import io.tarantool.driver.core.RetryingTarantoolTupleClient;

    import java.util.ArrayList;

    public class TarantoolConfig {
        public static TarantoolClient tarantoolClient(
                String nodes,
                String username,
                String password) {

            SimpleTarantoolCredentials credentials = new SimpleTarantoolCredentials(username, password);

            TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                    .withCredentials(credentials)
                    .withRequestTimeout(1000*5)
                    .build();

            TarantoolClusterAddressProvider provider = () -> {
                ArrayList<TarantoolServerAddress> addresses = new ArrayList<>();

                for (String node: nodes.split(",")) {
                    String[] address = node.split(":");
                    addresses.add(new TarantoolServerAddress(address[0], Integer.parseInt(address[1])));
                }

                return addresses;
            };

            // Создание кластерного клиента, который будет подключаться к каждому из инстансу
            ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(config, provider);

            return new RetryingTarantoolTupleClient(
                    client,
                    TarantoolRequestRetryPolicies.byNumberOfAttempts(
                            10, e -> e.getMessage().contains("Unsuccessful attempt")
                    ).build());
        }
    }