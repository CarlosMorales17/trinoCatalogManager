/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.customcatalogmanager;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.spi.catalog.CatalogManagerSpi;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorName;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import static java.util.Objects.requireNonNull;

/**
 * Simple test catalog manager that demonstrates the SPI interface.
 * This implementation shows how to use Trino's existing CatalogStore infrastructure
 * with a custom storage backend.
 *
 * This is the RECOMMENDED approach - leverage existing CatalogStore interface
 * while providing your own storage implementation.
 *
 * In a real implementation, you would create a DatabaseCatalogStore, RestApiCatalogStore,
 * or similar that connects to your external system.
 */
public final class TestCatalogManager
        implements CatalogManagerSpi
{
    private static final Logger log = Logger.get(TestCatalogManager.class);
    private final long refreshInterval;
    private final TestCatalogStore catalogStore;

    public TestCatalogManager(Map<String, String> config)
    {
        requireNonNull(config, "config is null");
        log.info("TestCatalogManager initialized successfully!");
        log.info("Configuration received: %s", config);

        this.catalogStore = new TestCatalogStore(config);
        this.refreshInterval = 0;

        // Create some test catalogs in the store
        createTestCatalog("memory_catalog", "memory", ImmutableMap.of(
                "memory.max-data-per-node", "128MB",
                "memory.max-rows-per-page", "10000"));

        createTestCatalog("test_catalog", "memory", ImmutableMap.of(
                "memory.max-data-per-node", "256MB"));
    }

    @Override
    public long getRefreshInterval()
    {
        return refreshInterval;
    }

    @Override
    public void connect()
    {
        log.info("Connect() called");
    }

    @Override
    public void disconnect()
    {
        log.info("Disconnect() called");
    }

    @Override
    public CatalogStore getCatalogStore()
    {
        return catalogStore;
    }

    @Override
    public CatalogStore.StoredCatalog getStoredCatalog(CatalogName catalogName)
    {
        return catalogStore.getCatalog(catalogName);
    }

    @Override
    public void ensureCatalogsLoaded(List<CatalogProperties> catalogsList)
    {
        requireNonNull(catalogsList, "catalogs is null");

        log.info("ensureCatalogsLoaded() called:");
        log.info("Catalogs to ensure: %d", catalogsList.size());
        for (CatalogProperties catalog : catalogsList) {
            log.info("%s (%s)", catalog.catalogHandle().getCatalogName(), catalog.connectorName());
        }
    }

    private void createTestCatalog(String name, String connector, Map<String, String> properties)
    {
        CatalogName catalogName = new CatalogName(name);
        CatalogProperties catalogProperties = catalogStore.createCatalogProperties(
                catalogName,
                new ConnectorName(connector),
                properties);
        catalogStore.addOrReplaceCatalog(catalogProperties);
        log.info("Created catalog: %s", name);
    }

    /**
     * Simple test implementation of CatalogStore.
     * In a real plugin, this would be DatabaseCatalogStore, RestApiCatalogStore, etc.
     * that connects to your external system.
     */
    private class TestCatalogStore
            implements CatalogStore
    {
        private static final Logger log = Logger.get(TestCatalogStore.class);

        private final Map<String, String> config;
        private final Map<CatalogName, CatalogProperties> storage = new ConcurrentHashMap<>();
        private volatile long versionCounter = System.currentTimeMillis();

        public TestCatalogStore(Map<String, String> config)
        {
            this.config = requireNonNull(config, "config is null");
            log.info("🏪 TestCatalogStore initialized with config: %s", config);
        }

        public StoredCatalog getCatalog(CatalogName catalogName)
        {
            CatalogProperties catalogProperties = storage.get(catalogName);
            if (catalogProperties == null) {
                throw new IllegalArgumentException("Catalog not found: " + catalogName);
            }
            return new TestStoredCatalog(catalogProperties);
        }

        @Override
        public Collection<StoredCatalog> getCatalogs()
        {
            log.info("📋 TestCatalogStore: Loading all catalogs from storage");
            return storage.values().stream()
                    .map(TestStoredCatalog::new)
                    .map(StoredCatalog.class::cast)
                    .toList();
        }

        @Override
        public CatalogProperties createCatalogProperties(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties)
        {
            log.info("🏗️ TestCatalogStore: Creating catalog properties for %s", catalogName);
            String version = String.valueOf(versionCounter++);
            CatalogHandle catalogHandle = createRootCatalogHandle(catalogName, new CatalogHandle.CatalogVersion(version));
            return new CatalogProperties(catalogHandle, connectorName, properties);
        }

        @Override
        public void addOrReplaceCatalog(CatalogProperties catalogProperties)
        {
            log.info("💾 TestCatalogStore: Storing catalog %s", catalogProperties.catalogHandle().getCatalogName());
            storage.put(catalogProperties.catalogHandle().getCatalogName(), catalogProperties);
        }

        @Override
        public void removeCatalog(CatalogName catalogName)
        {
            log.info("🗑️ TestCatalogStore: Removing catalog %s", catalogName);
            storage.remove(catalogName);
        }

        private CatalogProperties getCatalogProperties(CatalogName catalogName)
        {
            return storage.get(catalogName);
        }

        /**
         * Wrapper for CatalogProperties to implement StoredCatalog interface.
         */
        private static class TestStoredCatalog
                implements StoredCatalog
        {
            private final CatalogProperties catalogProperties;

            public TestStoredCatalog(CatalogProperties catalogProperties)
            {
                this.catalogProperties = requireNonNull(catalogProperties, "catalogProperties is null");
            }

            @Override
            public CatalogName name()
            {
                return catalogProperties.catalogHandle().getCatalogName();
            }

            @Override
            public CatalogProperties loadProperties()
            {
                return catalogProperties;
            }
        }
    }
}
