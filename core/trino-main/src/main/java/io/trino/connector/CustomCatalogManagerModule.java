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

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.CatalogManager;
import io.trino.server.ServerConfig;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class CustomCatalogManagerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        // Configure CustomCatalogManagerConfig
        configBinder(binder).bindConfig(CustomCatalogManagerConfig.class);

        if (buildConfigObject(ServerConfig.class).isCoordinator()) {
            // Coordinator: bind both CatalogManager and ConnectorServicesProvider
            binder.bind(CustomCatalogManager.class).in(Scopes.SINGLETON);
            binder.bind(ConnectorServicesProvider.class).to(CustomCatalogManager.class).in(Scopes.SINGLETON);
            binder.bind(CatalogManager.class).to(CustomCatalogManager.class).in(Scopes.SINGLETON);
            binder.bind(CoordinatorLazyRegister.class).asEagerSingleton();
        }
        else {
            // Worker: only bind ConnectorServicesProvider (no CatalogManager on workers)
            binder.bind(CustomCatalogManager.class).in(Scopes.SINGLETON);
            binder.bind(ConnectorServicesProvider.class).to(CustomCatalogManager.class).in(Scopes.SINGLETON);
            binder.bind(WorkerLazyRegister.class).asEagerSingleton();
        }
    }

    private static class CoordinatorLazyRegister
    {
        @Inject
        public CoordinatorLazyRegister(
                DefaultCatalogFactory defaultCatalogFactory,
                LazyCatalogFactory lazyCatalogFactory,
                CustomCatalogManager catalogManager,
                GlobalSystemConnector globalSystemConnector)
        {
            lazyCatalogFactory.setCatalogFactory(defaultCatalogFactory);
            catalogManager.registerGlobalSystemConnector(globalSystemConnector);
        }
    }

    private static class WorkerLazyRegister
    {
        @Inject
        public WorkerLazyRegister(
                DefaultCatalogFactory defaultCatalogFactory,
                LazyCatalogFactory lazyCatalogFactory,
                CustomCatalogManager catalogManager,
                GlobalSystemConnector globalSystemConnector)
        {
            lazyCatalogFactory.setCatalogFactory(defaultCatalogFactory);
            catalogManager.registerGlobalSystemConnector(globalSystemConnector);
        }
    }
}
