package io.descoped.dc.server;

import io.descoped.config.DynamicConfiguration;
import io.descoped.config.StoreBasedDynamicConfiguration;
import io.descoped.dc.api.util.CommonUtils;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class IntegrationTestProfile {

    static String[] loadApplicationPropertiesFromConsumerSpecificationProfile(String profile, String... replacePropertyKeyValueWithFullPath) {
        Path currentPath = CommonUtils.currentPath();
        while (!currentPath.resolve("data-collection-consumer-specifications").toFile().exists()) {
            currentPath = currentPath.getParent();
            if (currentPath == null) {
                throw new IllegalStateException("Unable to resolve 'data-collection-consumer-specifications' as parent to: " + CommonUtils.currentPath().toAbsolutePath());
            }
        }
        Path applicationPropertiesPath = currentPath.resolve("data-collection-consumer-specifications").resolve("profile").resolve(profile).resolve("conf").resolve("application.properties");
        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .propertiesResource(applicationPropertiesPath.toAbsolutePath().toString())
                .build();
        List<String> keyValuePairs = new ArrayList<>();
        for (Map.Entry<String, String> entry : configuration.asMap().entrySet()) {
            Path consumerSpecificationProjectPath = currentPath.resolve("data-collection-consumer-specifications");
            List<String> prefixFullPathForPropertyNames = List.of(replacePropertyKeyValueWithFullPath);
            if (prefixFullPathForPropertyNames.stream().anyMatch(name -> name.equals(entry.getKey()))) {
                keyValuePairs.add(entry.getKey());
                keyValuePairs.add(consumerSpecificationProjectPath.toAbsolutePath().normalize().toString() + entry.getValue());
            } else {
                keyValuePairs.add(entry.getKey());
                keyValuePairs.add(entry.getValue());
            }
        }
        return keyValuePairs.toArray(new String[0]);
    }
}
