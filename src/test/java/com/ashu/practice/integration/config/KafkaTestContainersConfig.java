package com.ashu.practice.integration.config;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

public class KafkaTestContainersConfig {

    private static final Network NETWORK = Network.newNetwork();
    private static KafkaContainer kafkaContainer;
    private static GenericContainer<?> schemaRegistryContainer;
    private static GenericContainer<?> zookeeperContainer;

    public static void startContainers() {
        if (kafkaContainer == null || !kafkaContainer.isRunning()) {
            zookeeperContainer = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-zookeeper:7.5.1"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("zookeeper")
                    .withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
                    .withEnv("ZOOKEEPER_TICK_TIME", "2000")
                    .withExposedPorts(2181)
                    .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
            zookeeperContainer.start();

            kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.1"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("kafka")
                    .withExternalZookeeper("zookeeper:2181")
                    .withExposedPorts(9092, 9093)
                    .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
            kafkaContainer.start();

            schemaRegistryContainer = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.5.1"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("schema-registry")
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9092")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                    .withExposedPorts(8081)
                    .dependsOn(kafkaContainer)
                    .waitingFor(Wait.forHttp("/subjects").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(2)));
            schemaRegistryContainer.start();
        }
    }

    public static void stopContainers() {
        if (schemaRegistryContainer != null && schemaRegistryContainer.isRunning()) {
            schemaRegistryContainer.stop();
        }
        if (kafkaContainer != null && kafkaContainer.isRunning()) {
            kafkaContainer.stop();
        }
        if (zookeeperContainer != null && zookeeperContainer.isRunning()) {
            zookeeperContainer.stop();
        }
    }

    public static String getKafkaBootstrapServers() {
        return kafkaContainer.getBootstrapServers();
    }

    public static String getSchemaRegistryUrl() {
        return String.format("http://%s:%d", schemaRegistryContainer.getHost(), schemaRegistryContainer.getMappedPort(8081));
    }

    public static KafkaContainer getKafkaContainer() {
        return kafkaContainer;
    }

    public static GenericContainer<?> getSchemaRegistryContainer() {
        return schemaRegistryContainer;
    }

    public static boolean isRunning() {
        return kafkaContainer != null && kafkaContainer.isRunning() &&
               schemaRegistryContainer != null && schemaRegistryContainer.isRunning();
    }
}
