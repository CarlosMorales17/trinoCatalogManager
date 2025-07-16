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
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.server.ForStartup;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogManagerFactory;
import io.trino.spi.catalog.CatalogManagerSpi;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.spi.connector.ConnectorName;
import jakarta.annotation.PreDestroy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.metadata.Catalog.failedCatalog;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_AVAILABLE;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.util.Executors.executeUntilFailure;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

@ThreadSafe
public class CustomCatalogManager
        implements CatalogManager, ConnectorServicesProvider
{
    private static final Logger log = Logger.get(CustomCatalogManager.class);
    private static final File CATALOG_MANAGER_CONFIGURATION = new File("etc/catalog-manager.properties");

    private enum State { CREATED, INITIALIZED, STOPPED }

    private final Map<String, CatalogManagerFactory> catalogManagerFactories = new ConcurrentHashMap<>();
    private final AtomicReference<Optional<CatalogManagerSpi>> configuredCatalogManager = new AtomicReference<>(Optional.empty());
    private final SecretsResolver secretsResolver;
    private final String catalogManagerKind;
    private final CatalogFactory catalogFactory;
    private volatile CatalogManagerSpi catalogManagerSpi;
    private final Executor executor;
    private volatile Thread refreshThread;

    // Catalog management state similar to CoordinatorDynamicCatalogManager
    private final Lock catalogsUpdateLock = new ReentrantLock();

    /**
     * Active catalogs that have been created and not dropped.
     */
    private final ConcurrentMap<CatalogName, Catalog> activeCatalogs = new ConcurrentHashMap<>();

    /**
     * All catalogs including those that have been dropped. Should this always be updated from the external DC SPI?
     */
    private final ConcurrentMap<CatalogHandle, CatalogConnector> allCatalogs = new ConcurrentHashMap<>();

    @GuardedBy("catalogsUpdateLock")
    private State state = State.CREATED;

    @Inject
    public CustomCatalogManager(SecretsResolver secretsResolver, CustomCatalogManagerConfig catalogManagerConfig,
                                CatalogFactory catalogFactory, @ForStartup Executor executor)
    {
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
        this.catalogManagerKind = requireNonNull(catalogManagerConfig.getCatalogManagerName(), "catalogManagerKind is null");
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    @PreDestroy
    public void stop()
    {
        List<CatalogConnector> catalogs;

        catalogsUpdateLock.lock();
        try {
            if (state == State.STOPPED) {
                return;
            }
            state = State.STOPPED;

            catalogs = new ArrayList<>(allCatalogs.values());
            allCatalogs.clear();
            activeCatalogs.clear();

            // Stop the refresh thread
            if (refreshThread != null) {
                refreshThread.interrupt();
                refreshThread = null;
            }

            // Clear the Connection
            catalogManagerSpi.disconnect();
        }
        finally {
            catalogsUpdateLock.unlock();
        }

        for (CatalogConnector connector : catalogs) {
            try {
                connector.shutdown();
            }
            catch (Throwable e) {
                log.error(e, "Error shutting down catalog: %s", connector.getCatalogHandle());
            }
        }
    }

    public void addCatalogManagerFactory(CatalogManagerFactory catalogManagerFactory)
    {
        requireNonNull(catalogManagerFactory, "catalogManagerFactory is null");

        if (catalogManagerFactories.putIfAbsent(catalogManagerFactory.getName(), catalogManagerFactory) != null) {
            throw new IllegalArgumentException("Catalog manager factory '%s' is already registered".formatted(catalogManagerFactory.getName()));
        }
    }

    @VisibleForTesting
    void loadConfiguredCatalogManager(String catalogManagerName, File catalogManagerFile)
    {
        if (configuredCatalogManager.get().isPresent()) {
            return;
        }
        Map<String, String> properties = new HashMap<>();
        if (catalogManagerFile.exists()) {
            try {
                properties = new HashMap<>(loadPropertiesFrom(catalogManagerFile.getPath()));
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to read configuration file: " + catalogManagerFile, e);
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

        // Initialize the Connection Lazily
        catalogManagerSpi.connect();
        log.info("Plugin catalog manager configured successfully");
    }

    @VisibleForTesting
    public CatalogManagerSpi getCatalogManager()
    {
        return configuredCatalogManager.get().orElseThrow(() -> new IllegalStateException("Catalog manager is not configured"));
    }

    public void registerGlobalSystemConnector(GlobalSystemConnector connector)
    {
        requireNonNull(connector, "connector is null");

        catalogsUpdateLock.lock();
        try {
            if (state == State.STOPPED) {
                return;
            }

            CatalogConnector catalog = catalogFactory.createCatalog(
                    GlobalSystemConnector.CATALOG_HANDLE,
                    new ConnectorName(GlobalSystemConnector.NAME),
                    connector);

            if (activeCatalogs.putIfAbsent(new CatalogName(GlobalSystemConnector.NAME), catalog.getCatalog()) != null) {
                throw new IllegalStateException("Global system catalog already registered");
            }
            allCatalogs.put(GlobalSystemConnector.CATALOG_HANDLE, catalog);
            log.info("Registered system catalog");
        }
        finally {
            catalogsUpdateLock.unlock();
        }
    }

    private void ensureCatalogManagerLoaded()
    {
        if (catalogManagerSpi == null) {
            synchronized (this) {
                if (catalogManagerSpi == null) {
                    loadConfiguredCatalogManager(catalogManagerKind, CATALOG_MANAGER_CONFIGURATION);
                }
            }
        }
    }

    // CatalogManager interface methods
    @Override
    public Set<CatalogName> getCatalogNames()
    {
        ensureCatalogManagerLoaded();
        return ImmutableSet.copyOf(activeCatalogs.keySet());
    }

    @Override
    public Optional<Catalog> getCatalog(CatalogName catalogName)
    {
        ensureCatalogManagerLoaded();
        Catalog catalog = activeCatalogs.get(catalogName);
        if (catalog != null) {
            return Optional.of(catalog);
        }

        //Try to load from Catalog Store
        CatalogStore.StoredCatalog storedCatalog = catalogManagerSpi.getStoredCatalog(catalogName);
        addStoredCatalogToManagerState(storedCatalog);

        return Optional.ofNullable(activeCatalogs.get(catalogName));
    }

    @Override
    public Optional<CatalogProperties> getCatalogProperties(CatalogHandle catalogHandle)
    {
        ensureCatalogManagerLoaded();
        return Optional.ofNullable(allCatalogs.get(catalogHandle.getRootCatalogHandle()))
                .flatMap(CatalogConnector::getCatalogProperties);
    }

    @Override
    public Set<CatalogHandle> getActiveCatalogs()
    {
        return activeCatalogs.values().stream()
                .map(Catalog::getCatalogHandle)
                .collect(toImmutableSet());
    }

    @Override
    public void createCatalog(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties, boolean notExists)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(connectorName, "connectorName is null");
        requireNonNull(properties, "properties is null");

        ensureCatalogManagerLoaded();

        catalogsUpdateLock.lock();
        try {
            checkState(state != State.STOPPED, "CatalogManager is stopped");

            if (activeCatalogs.containsKey(catalogName)) {
                if (notExists) {
                    return;
                }
                throw new TrinoException(io.trino.spi.StandardErrorCode.ALREADY_EXISTS, "Catalog '%s' already exists".formatted(catalogName));
            }

            CatalogProperties catalogProperties = catalogManagerSpi.getCatalogStore()
                    .createCatalogProperties(catalogName, connectorName, properties);

            // get or create catalog for the handle
            CatalogConnector catalog = allCatalogs.computeIfAbsent(
                    catalogProperties.catalogHandle(),
                    handle -> catalogFactory.createCatalog(catalogProperties));

            // Delegate to the Catalog Store
            catalogManagerSpi.getCatalogStore().addOrReplaceCatalog(catalogProperties);
            activeCatalogs.put(catalogName, catalog.getCatalog());

            log.debug("Added catalog: %s", catalog.getCatalogHandle());
        }
        finally {
            catalogsUpdateLock.unlock();
        }
    }

    @Override
    public void dropCatalog(CatalogName catalogName, boolean exists)
    {
        ensureCatalogManagerLoaded();
        requireNonNull(catalogName, "catalogName is null");

        boolean removed;
        catalogsUpdateLock.lock();
        try {
            checkState(state != State.STOPPED, "CatalogManager is stopped");

            // Remove from SPI
            catalogManagerSpi.getCatalogStore().removeCatalog(catalogName);

            // Remove from active catalogs
            removed = activeCatalogs.remove(catalogName) != null;
        }
        finally {
            catalogsUpdateLock.unlock();
        }

        if (!removed && !exists) {
            throw new TrinoException(CATALOG_NOT_FOUND, "Catalog '%s' not found".formatted(catalogName));
        }

        log.info("Dropped catalog: %s", catalogName);
    }

    // ConnectorServicesProvider interface methods
    @Override
    public void loadInitialCatalogs()
    {
        catalogsUpdateLock.lock();
        try {
            if (state == State.INITIALIZED) {
                return;
            }
            checkState(state != State.STOPPED, "CatalogManager is stopped");
            state = State.INITIALIZED;

            ensureCatalogManagerLoaded();
            executeUntilFailure(
                    executor,
                    catalogManagerSpi.getCatalogStore().getCatalogs().stream()
                            .map(storedCatalog -> (Callable<?>) () -> {
                                CatalogProperties catalog = null;
                                try {
                                    catalog = storedCatalog.loadProperties();
                                    verify(catalog.catalogHandle().getCatalogName().equals(storedCatalog.name()), "Catalog name does not match catalog handle");
                                    CatalogConnector newCatalog = catalogFactory.createCatalog(catalog);
                                    activeCatalogs.put(storedCatalog.name(), newCatalog.getCatalog());
                                    allCatalogs.put(catalog.catalogHandle(), newCatalog);
                                    log.debug("-- Added catalog %s using connector %s --", storedCatalog.name(), catalog.connectorName());
                                }
                                catch (Throwable e) {
                                    CatalogHandle catalogHandle = catalog != null ? catalog.catalogHandle() : createRootCatalogHandle(storedCatalog.name(), new CatalogVersion("failed"));
                                    ConnectorName connectorName = catalog != null ? catalog.connectorName() : new ConnectorName("unknown");
                                    activeCatalogs.put(storedCatalog.name(), failedCatalog(storedCatalog.name(), catalogHandle, connectorName));
                                    log.error(e, "-- Failed to load catalog %s using connector %s --", storedCatalog.name(), connectorName);
                                }
                                return null;
                            })
                            .collect(toImmutableList()));

            log.info("Loaded initial catalogs successfully");

            log.info("Initial Active Catalogs: %s", activeCatalogs);
            log.info("Initial All Catalogs: %s", allCatalogs);

            // Start the catalog synchronization thread
            synchronizeCatalogsWithCatalogStore();
        }
        finally {
            catalogsUpdateLock.unlock();
        }
    }

    @Override
    public void ensureCatalogsLoaded(Session session, List<CatalogProperties> catalogs)
    {
        ensureCatalogManagerLoaded();

        // Check for missing catalogs
        List<CatalogProperties> missingCatalogs = catalogs.stream()
                .filter(catalog -> !allCatalogs.containsKey(catalog.catalogHandle()))
                .collect(toImmutableList());

        if (!missingCatalogs.isEmpty()) {
            //We Can try to load the missing catalogs from the catalog store
            missingCatalogs.forEach(catalog -> {
                CatalogStore.StoredCatalog storedCatalog = catalogManagerSpi.getStoredCatalog(catalog.catalogHandle().getCatalogName());
                addStoredCatalogToManagerState(storedCatalog);
            });
            if (missingCatalogs.isEmpty()) {
                return;
            }
            throw new TrinoException(CATALOG_NOT_AVAILABLE, "Missing catalogs: " + missingCatalogs);
        }

        // Example of how we can leave extensibilty for the user to extend thier logic, Delegate to SPI for any additional logic
        catalogManagerSpi.ensureCatalogsLoaded(catalogs);
    }

    @Override
    public void pruneCatalogs(Set<CatalogHandle> catalogsInUse)
    {
        ensureCatalogManagerLoaded();

        List<CatalogConnector> removedCatalogs = new ArrayList<>();
        catalogsUpdateLock.lock();
        try {
            if (state == State.STOPPED) {
                return;
            }

            Iterator<Entry<CatalogHandle, CatalogConnector>> iterator = allCatalogs.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<CatalogHandle, CatalogConnector> entry = iterator.next();
                CatalogHandle catalogHandle = entry.getKey();

                // Check if catalog is still active
                Catalog activeCatalog = activeCatalogs.get(catalogHandle.getCatalogName());
                if (activeCatalog != null && activeCatalog.getCatalogHandle().equals(catalogHandle)) {
                    // catalog is registered with a name, and therefore is available for new queries
                    continue;
                }

                // Remove if not in use
                if (!catalogsInUse.contains(catalogHandle)) {
                    iterator.remove();
                    removedCatalogs.add(entry.getValue());
                }
            }
        }
        finally {
            catalogsUpdateLock.unlock();
        }

        // Shutdown removed catalogs
        for (CatalogConnector removedCatalog : removedCatalogs) {
            try {
                removedCatalog.shutdown();
            }
            catch (Throwable e) {
                log.error(e, "Error shutting down catalog: %s", removedCatalog.getCatalogHandle());
            }
        }

        if (!removedCatalogs.isEmpty()) {
            List<String> sortedHandles = removedCatalogs.stream()
                    .map(connector -> connector.getCatalogHandle().toString())
                    .sorted()
                    .toList();
            log.debug("Pruned catalogs: %s", sortedHandles);
        }
    }

    @Override
    public ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
    {
        CatalogConnector catalogConnector = allCatalogs.get(catalogHandle.getRootCatalogHandle());
        checkArgument(catalogConnector != null, "No catalog '%s'", catalogHandle.getCatalogName());
        return catalogConnector.getMaterializedConnector(catalogHandle.getType());
    }

    private void synchronizeCatalogsWithCatalogStore()
    {
        long refreshInterval = catalogManagerSpi.getRefreshInterval();
        if (refreshInterval != 0 && refreshThread == null) {
            refreshThread = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Thread.sleep(refreshInterval);
                        triggerUpdateCatalogs();
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
            log.info("Started catalog refresh thread with interval: {}ms", refreshInterval);
        }
    }

    private void triggerUpdateCatalogs()
    {
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

    private void updateCatalogs()
    {
        //Get System Catalog (this will never be pruned)
        CatalogName systemCatalogName = new CatalogName(GlobalSystemConnector.NAME);
        Catalog systemCatalog = activeCatalogs.get(systemCatalogName);
        Set<CatalogHandle> catalogsInUse = this.activeCatalogs.values().stream().map(Catalog::getCatalogHandle).collect(toSet());
        catalogsInUse.add(systemCatalog.getCatalogHandle());

        log.debug("Catalogs in use: %s", catalogsInUse);

        // Prune Old Catalogs
        pruneCatalogs(catalogsInUse);

        // Add New Catalogs From Catalog Store
        Collection<CatalogStore.StoredCatalog> storeCatalogs = catalogManagerSpi.getCatalogStore().getCatalogs();

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
            // Version changed, need to update
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
}
