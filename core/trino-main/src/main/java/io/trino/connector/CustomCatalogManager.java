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
package io.trino.connector;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.Catalog;
import io.trino.server.ForStartup;
import io.trino.spi.catalog.CatalogManagerFactory;
import io.trino.spi.catalog.CatalogManagerSpi;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorName;
import jakarta.annotation.PreDestroy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

@ThreadSafe
public class CustomCatalogManager
        extends DynamicCatalogManagerBase
{
    private static final Logger log = Logger.get(CustomCatalogManager.class);
    private static final File CATALOG_MANAGER_CONFIGURATION = new File("etc/catalog-manager.properties");

    private final Map<String, CatalogManagerFactory> catalogManagerFactories = new ConcurrentHashMap<>();
    private final AtomicReference<Optional<CatalogManagerSpi>> configuredCatalogManager = new AtomicReference<>(Optional.empty());
    private final SecretsResolver secretsResolver;
    private final String catalogManagerKind;

    private volatile CatalogManagerSpi catalogManagerSpi;
    private volatile Thread refreshThread;

    @Inject
    public CustomCatalogManager(SecretsResolver secretsResolver, CustomCatalogManagerConfig catalogManagerConfig,
                                CatalogFactory catalogFactory, @ForStartup Executor executor)
    {
        super(catalogFactory, executor);
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
        this.catalogManagerKind = requireNonNull(catalogManagerConfig.getCatalogManagerName(), "catalogManagerKind is null");
        this.removeCatalogsInstantly = false;

        super.setLogger(log);
    }

    private void ensureCatalogManagerLoaded()
    {
        if (catalogManagerSpi == null) {
            synchronized (this) {
                if (catalogManagerSpi == null) {
                    loadConfiguredCatalogManager(catalogManagerKind);
                    catalogManagerSpi.connect();
                    super.setCatalogStore(catalogManagerSpi.getCatalogStore());
                }
            }
        }
    }

    @VisibleForTesting
    void loadConfiguredCatalogManager(String catalogManagerName)
    {
        if (configuredCatalogManager.get().isPresent()) {
            return;
        }
        Map<String, String> properties = new HashMap<>();
        if (CATALOG_MANAGER_CONFIGURATION.exists()) {
            try {
                properties = new HashMap<>(loadPropertiesFrom(CATALOG_MANAGER_CONFIGURATION.getPath()));
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to read configuration file: " + CATALOG_MANAGER_CONFIGURATION, e);
            }
        }
        setConfiguredCatalogManager(catalogManagerName, properties);
    }

    @VisibleForTesting
    protected void setConfiguredCatalogManager(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        log.info("-- Loading catalog manager %s --", name);

        CatalogManagerFactory factory = catalogManagerFactories.get(name);
        checkState(factory != null, "Catalog manager factory '%s' is not registered", name);

        CatalogManagerSpi catalogManagerSpi;
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            catalogManagerSpi = factory.create(ImmutableMap.copyOf(secretsResolver.getResolvedConfiguration(properties)));
        }

        checkState(configuredCatalogManager.compareAndSet(Optional.empty(), Optional.of(catalogManagerSpi)),
                "catalogManager is already set");
        this.catalogManagerSpi = catalogManagerSpi;
        log.info("Plugin catalog manager configured successfully");
    }

    public void addCatalogManagerFactory(CatalogManagerFactory catalogManagerFactory)
    {
        requireNonNull(catalogManagerFactory, "catalogManagerFactory is null");

        if (catalogManagerFactories.putIfAbsent(catalogManagerFactory.getName(), catalogManagerFactory) != null) {
            throw new IllegalArgumentException("Catalog manager factory '%s' is already registered".formatted(catalogManagerFactory.getName()));
        }
    }

    @PreDestroy
    public void stop()
    {
        super.stop();

        if (refreshThread != null) {
            refreshThread.interrupt();
            refreshThread = null;
        }
        catalogManagerSpi.disconnect();
    }

    @Override
    public Optional<Catalog> getCatalog(CatalogName catalogName)
    {
        ensureCatalogManagerLoaded();
        Optional<Catalog> catalog = super.getCatalog(catalogName);
        if (catalog.isPresent()) {
            return catalog;
        }

        // Fall back to allCatalogs
        log.debug("Attempting to load catalog '{}' from all catalogs cache", catalogName);
        List<Map.Entry<CatalogHandle, CatalogConnector>> matchingCatalogs = allCatalogs.entrySet().stream()
                .filter(entry -> entry.getKey().getCatalogName().equals(catalogName))
                .collect(toImmutableList());

        if (!matchingCatalogs.isEmpty()) {
            CatalogConnector catalogConnector;
            if (matchingCatalogs.size() == 1) {
                // Single version - use it directly
                catalogConnector = matchingCatalogs.getFirst().getValue();
                log.debug("Found single version of catalog %s' in allCatalogs", catalogName);
            }
            else {
                // Multiple versions - for now, pick first one
                catalogConnector = matchingCatalogs.getFirst().getValue();
                log.debug("Found %s versions of catalog %s in allCatalogs, using first available version",
                         matchingCatalogs.size(), catalogName);
            }
            return Optional.of(catalogConnector.getCatalog());
        }

        //Try to load from Catalog Store
        log.debug("Attempting to load catalog '{}' from catalog store", catalogName);
        CatalogStore.StoredCatalog storedCatalog = catalogManagerSpi.getStoredCatalog(catalogName);
        if (storedCatalog != null) {
            addStoredCatalogToManagerState(storedCatalog);
        }
        else {
            log.debug("Catalog '{}' not found in catalog store", catalogName);
        }

        return Optional.ofNullable(activeCatalogs.get(catalogName));
    }

    @Override
    public void ensureCatalogsLoaded(Session session, List<CatalogProperties> catalogs)
    {
        ensureCatalogManagerLoaded();
        List<CatalogProperties> missingCatalogs = catalogs.stream()
                .filter(catalog -> !allCatalogs.containsKey(catalog.catalogHandle()))
                .collect(toImmutableList());

        if (!missingCatalogs.isEmpty()) {
            //We Can try to load the missing catalogs from the catalog store
            missingCatalogs.forEach(catalog -> {
                CatalogStore.StoredCatalog storedCatalog = catalogManagerSpi.getStoredCatalog(catalog.catalogHandle().getCatalogName());
                if (storedCatalog != null) {
                    addStoredCatalogToManagerState(storedCatalog);
                }
            });
        }
        super.ensureCatalogsLoaded(session, catalogs);
    }

    @Override
    public void loadInitialCatalogs()
    {
        ensureCatalogManagerLoaded();
        super.loadInitialCatalogs();
    }

    @Override
    public void createCatalog(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties, boolean notExists)
    {
        ensureCatalogManagerLoaded();
        super.createCatalog(catalogName, connectorName, properties, notExists);
    }

    @Override
    public void dropCatalog(CatalogName catalogName, boolean exists)
    {
        ensureCatalogManagerLoaded();
        super.dropCatalog(catalogName, exists);
    }

    @Override
    public void doLoadInitialCatalogs()
    {
        log.info("Initial Active Catalogs: %s", activeCatalogs);
        log.info("Initial All Catalogs: %s", allCatalogs);

        // Start the catalog synchronization thread
        synchronizeCatalogsWithCatalogStore();
    }

    private void synchronizeCatalogsWithCatalogStore()
    {
        long refreshInterval = catalogManagerSpi.getRefreshInterval();
        if (refreshInterval != 0 && refreshThread == null) {
            refreshThread = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Thread.sleep(refreshInterval);
                        catalogsUpdateLock.lock();
                        try {
                            if (state == State.STOPPED) {
                                return;
                            }
                            updateCatalogs();
                        }
                        finally {
                            catalogsUpdateLock.unlock();
                        }
                    }
                    catch (InterruptedException e) {
                        log.debug("Catalog refresh thread interrupted");
                        Thread.currentThread().interrupt();
                        break;
                    }
                    catch (Throwable e) {
                        log.warn(e, "Error refreshing catalogs");
                    }
                }
            });
            refreshThread.setName("Catalog Refresh Thread");
            refreshThread.setDaemon(true);
            refreshThread.start();
            log.info("Started catalog refresh thread with interval: %s ms", refreshInterval);
        }
    }

    private void updateCatalogs()
    {
        //Get System Catalog (this will never be pruned)
        CatalogName systemCatalogName = new CatalogName(GlobalSystemConnector.NAME);
        Catalog systemCatalog = activeCatalogs.get(systemCatalogName);
        Set<CatalogHandle> catalogsInUse = this.activeCatalogs.values().stream().map(Catalog::getCatalogHandle).collect(toSet());
        catalogsInUse.add(systemCatalog.getCatalogHandle());

        log.info("Active Catalogs in use: %s", catalogsInUse);
        log.info("All Catalogs: %s", allCatalogs);

        // Prune Old Catalogs
        pruneCatalogs(catalogsInUse);

        // Add New Catalogs From Catalog Store
        Collection<CatalogStore.StoredCatalog> storeCatalogs = catalogStore.getCatalogs();

        for (CatalogStore.StoredCatalog catalog : storeCatalogs) {
            addStoredCatalogToManagerState(catalog);
        }

        // Deactivate Catalogs not in Catalog Store (excluding system catalog)
        Set<CatalogName> storeCatalogNames = storeCatalogs.stream().map(CatalogStore.StoredCatalog::name).collect(toImmutableSet());
        List<CatalogName> deactiveCatalogs = activeCatalogs.keySet().stream().filter(
                catalogName -> !catalogName.equals(systemCatalogName) &&
                        !storeCatalogNames.contains(catalogName)).toList();

        // Remove from activeCatalogs only - keep in allCatalogs for existing connections
        for (CatalogName catalogName : deactiveCatalogs) {
            Catalog removedCatalog = activeCatalogs.remove(catalogName);
            if (removedCatalog != null) {
                log.info("Deactivated catalog from activeCatalogs: %s (keeping in allCatalogs for existing connections)", catalogName);
            }
        }
    }

    private void addStoredCatalogToManagerState(CatalogStore.StoredCatalog storedCatalog)
    {
        if (storedCatalog == null) {
            log.debug("Stored catalog is null, skipping");
            return;
        }

        try {
            CatalogProperties properties = storedCatalog.loadProperties();
            CatalogName catalogName = properties.catalogHandle().getCatalogName();
            verify(catalogName.equals(storedCatalog.name()), "Catalog name does not match catalog handle");

            // Check if Catalog Already Exists within Active Catalogs
            Catalog existingCatalog = activeCatalogs.get(catalogName);
            if (existingCatalog != null) {
                // Check if the catalog version has changed
                if (existingCatalog.getCatalogHandle().getVersion().equals(properties.catalogHandle().getVersion())) {
                    return;
                }
                log.info("-- Updating catalog %s using connector %s from version %s to %s --",
                        storedCatalog.name(), properties.connectorName(),
                        existingCatalog.getCatalogHandle().getVersion(), properties.catalogHandle().getVersion());
            }

            CatalogConnector newCatalog = catalogFactory.createCatalog(properties);
            Catalog previousCatalog = activeCatalogs.put(storedCatalog.name(), newCatalog.getCatalog());
            if (previousCatalog == null) {
                log.info("-- Added catalog %s using connector %s --", storedCatalog.name(), properties.connectorName());
            }
            allCatalogs.put(properties.catalogHandle(), newCatalog);
        }
        catch (Exception e) {
            log.error(e, "Failed to add stored catalog %s to manager state", storedCatalog.name());
        }
    }
}
