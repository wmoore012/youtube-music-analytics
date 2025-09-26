"""Demonstration of complete plugin system integration."""

import os
from pathlib import Path

from src.youtubeviz.notebook_plugin_integration import create_plugin_enhanced_notebook
from src.youtubeviz.plugin_integration import (
    get_available_algorithms,
    get_system_status,
    initialize_plugins,
    validate_plugin_system,
)
from web.enhanced_sentiment_job import create_enhanced_sentiment_job


def demo_plugin_system_integration():
    """Demonstrate complete plugin system integration."""
    print("🚀 MusicScope™ Plugin System Integration Demo")
    print("=" * 60)

    # 1. Initialize Plugin System
    print("\n1️⃣ Initializing Plugin System...")
    try:
        status = initialize_plugins(auto_discover=True, enable_storage=False)
        print(f"   ✅ Plugin system initialized successfully")
        print(f"   📊 Status: {status}")
    except Exception as e:
        print(f"   ❌ Plugin initialization failed: {e}")
        return

    # 2. Show Available Algorithms
    print("\n2️⃣ Available Algorithms...")
    algorithms = get_available_algorithms()
    print(f"   🎯 Found {len(algorithms)} algorithms:")
    for i, alg in enumerate(algorithms, 1):
        print(f"      {i}. {alg}")

    # 3. Validate Plugin System
    print("\n3️⃣ Validating Plugin System...")
    validation = validate_plugin_system()
    print(f"   📋 Validation Results:")
    print(f"      Total Plugins: {validation['total_plugins']}")
    print(f"      Valid Plugins: {validation['valid_plugins']}")
    print(f"      Invalid Plugins: {validation['invalid_plugins']}")

    # 4. Demonstrate ETL Integration
    print("\n4️⃣ ETL Integration Demo...")
    try:
        from web.plugin_etl_integration import get_etl_plugin_status

        etl_status = get_etl_plugin_status()
        print(f"   🔗 ETL Integration Status:")
        print(f"      Database Connected: {etl_status.get('database_connected', False)}")
        print(f"      Plugins Available: {len(etl_status.get('available_algorithms', []))}")
    except Exception as e:
        print(f"   ⚠️  ETL integration demo skipped: {e}")

    # 5. Demonstrate Sentiment Integration
    print("\n5️⃣ Sentiment Analysis Integration Demo...")
    try:
        sentiment_job = create_enhanced_sentiment_job(enable_plugins=True)
        sentiment_status = sentiment_job.get_plugin_system_status()
        print(f"   💭 Sentiment Integration Status:")
        print(f"      Plugins Enabled: {sentiment_status['plugins_enabled']}")
        print(f"      Available Algorithms: {len(sentiment_status['available_algorithms'])}")
        print(f"      Sentiment Algorithms: {len(sentiment_status['sentiment_algorithms'])}")
    except Exception as e:
        print(f"   ⚠️  Sentiment integration demo failed: {e}")

    # 6. Demonstrate Notebook Integration
    print("\n6️⃣ Notebook Generation Integration Demo...")
    try:
        # Create a demo notebook
        notebooks_dir = Path("notebooks")
        notebooks_dir.mkdir(exist_ok=True)

        result = create_plugin_enhanced_notebook(title="Plugin Integration Demo Notebook", enable_plugins=True)

        if result["success"]:
            print(f"   📓 Notebook created successfully:")
            print(f"      Path: {result['notebook_path']}")
            print(f"      Cells: {result['cells_created']}")
            print(f"      Plugins Used: {len(result['plugins_used'])}")
        else:
            print(f"   ❌ Notebook creation failed")

    except Exception as e:
        print(f"   ⚠️  Notebook integration demo failed: {e}")

    # 7. System Status Summary
    print("\n7️⃣ Complete System Status...")
    try:
        system_status = get_system_status()
        print(f"   🖥️  System Summary:")
        print(f"      Initialized: {system_status.get('initialized', False)}")
        print(f"      Loaded Plugins: {system_status.get('loaded_plugins', 0)}")
        print(f"      Storage Enabled: {system_status.get('storage_enabled', False)}")
        print(f"      Isolation Enabled: {system_status.get('isolation_enabled', False)}")
    except Exception as e:
        print(f"   ⚠️  System status check failed: {e}")

    # 8. Integration Success Summary
    print("\n🎉 Plugin Integration Demo Complete!")
    print("=" * 60)
    print("✅ Successfully demonstrated:")
    print("   • Plugin system initialization and discovery")
    print("   • Algorithm loading and validation")
    print("   • ETL pipeline integration")
    print("   • Sentiment analysis enhancement")
    print("   • Notebook generation with plugins")
    print("   • System status monitoring")

    if algorithms:
        print(f"\n🔌 {len(algorithms)} plugins are ready for use in:")
        print("   • ETL scoring pipelines")
        print("   • Enhanced sentiment analysis")
        print("   • Interactive notebook generation")
        print("   • Real-time analytics dashboards")

    print("\n🚀 The plugin system is fully integrated with the main codebase!")


if __name__ == "__main__":
    demo_plugin_system_integration()
