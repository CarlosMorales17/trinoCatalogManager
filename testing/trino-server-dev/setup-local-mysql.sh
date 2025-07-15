#!/bin/bash

# Quick setup script for local MySQL development

echo "🚀 Setting up local MySQL for Trino Catalog Manager..."

# Check if MySQL is running
if ! command -v mysql &> /dev/null; then
    echo "❌ MySQL is not installed or not in PATH"
    echo "   Install MySQL: brew install mysql (on macOS)"
    exit 1
fi

echo "📋 Current Configuration:"
echo "   Database: trino_catalogs"
echo "   User: root"
echo "   Password: (empty)"
echo "   URL: jdbc:mysql://localhost:3306/trino_catalogs"

echo ""
echo "🗄️  Creating database (you may need to enter your MySQL root password)..."
mysql -u root -p -e "
CREATE DATABASE IF NOT EXISTS trino_catalogs;
GRANT ALL PRIVILEGES ON trino_catalogs.* TO 'root'@'localhost';
FLUSH PRIVILEGES;
SELECT 'Database trino_catalogs created successfully!' as Result;
"

if [ $? -eq 0 ]; then
    echo "✅ Database setup complete!"
    echo ""
    echo "📖 You can now start the Trino server with:"
    echo "   cd testing/trino-server-dev"
    echo "   ./bin/launcher run"
    echo ""
    echo "🔍 Test catalog creation with:"
    echo "   CREATE CATALOG my_tpch USING tpch;"
    echo "   CREATE CATALOG my_memory USING memory;"
else
    echo "❌ Database setup failed. Please check your MySQL installation."
fi 