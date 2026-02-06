package com.ashu.practice.integration.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class IntegrationTestProperties {

    private static final Properties properties = new Properties();
    private static boolean loaded = false;

    static {
        loadProperties();
    }

    private static void loadProperties() {
        if (loaded) return;
        
        try (InputStream input = IntegrationTestProperties.class.getClassLoader()
                .getResourceAsStream("application-test.yml")) {
            if (input != null) {
                loadYamlProperties(input);
            }
        } catch (IOException e) {
            System.err.println("Could not load application-test.yml: " + e.getMessage());
        }
        
        try (InputStream input = IntegrationTestProperties.class.getClassLoader()
                .getResourceAsStream("integration-test.properties")) {
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException e) {
            System.err.println("Could not load integration-test.properties: " + e.getMessage());
        }
        
        loaded = true;
    }

    private static void loadYamlProperties(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line;
        int[] indentStack = new int[20];
        String[] pathStack = new String[20];
        int stackDepth = 0;

        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty() || line.trim().startsWith("#")) {
                continue;
            }

            int indent = 0;
            while (indent < line.length() && line.charAt(indent) == ' ') {
                indent++;
            }

            String content = line.trim();
            if (content.contains(":")) {
                int colonIndex = content.indexOf(':');
                String key = content.substring(0, colonIndex).trim();
                String value = colonIndex < content.length() - 1 ? content.substring(colonIndex + 1).trim() : "";

                while (stackDepth > 0 && indent <= indentStack[stackDepth - 1]) {
                    stackDepth--;
                }

                if (!value.isEmpty()) {
                    StringBuilder fullKey = new StringBuilder();
                    for (int i = 0; i < stackDepth; i++) {
                        fullKey.append(pathStack[i]).append(".");
                    }
                    fullKey.append(key);
                    properties.setProperty(fullKey.toString(), value);
                } else {
                    indentStack[stackDepth] = indent;
                    pathStack[stackDepth] = key;
                    stackDepth++;
                }
            }
        }
    }

    public static boolean useTestcontainers() {
        String value = getProperty("integration.use-testcontainers", "true");
        return Boolean.parseBoolean(value);
    }

    public static String getKafkaBootstrapServers() {
        return getProperty("integration.kafka.bootstrap-servers", "localhost:9092");
    }

    public static String getSchemaRegistryUrl() {
        return getProperty("integration.kafka.schema-registry-url", "http://localhost:8081");
    }

    public static String getOrderServiceUrl() {
        return getProperty("integration.services.order-service-url", "http://localhost:8080");
    }

    public static int getPaymentServicePort() {
        return Integer.parseInt(getProperty("integration.services.payment-service-port", "8081"));
    }

    public static int getStockServicePort() {
        return Integer.parseInt(getProperty("integration.services.stock-service-port", "8082"));
    }

    private static String getProperty(String key, String defaultValue) {
        String envKey = key.toUpperCase().replace(".", "_").replace("-", "_");
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isEmpty()) {
            return envValue;
        }
        
        String sysValue = System.getProperty(key);
        if (sysValue != null && !sysValue.isEmpty()) {
            return sysValue;
        }
        
        return properties.getProperty(key, defaultValue);
    }
}
