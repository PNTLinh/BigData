#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script để kiểm tra Airflow Lab setup
"""

import requests
import subprocess
import sys
import time

# Configuration
AIRFLOW_URL = "http://localhost:8080"
AIRFLOW_USERNAME = "airflow"
AIRFLOW_PASSWORD = "airflow"

def test_docker_services():
    """Kiểm tra Docker services"""
    print("🐳 Testing Docker services...")
    
    try:
        result = subprocess.run(
            ["docker", "compose", "ps"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            print("✅ Docker Compose is accessible")
            
            # Check if services are running
            output = result.stdout
            if "airflow-webserver" in output and "Up" in output:
                print("✅ Airflow webserver is running")
            else:
                print("⚠️  Airflow webserver might not be running")
                return False
                
            if "airflow-scheduler" in output and "Up" in output:
                print("✅ Airflow scheduler is running")
            else:
                print("⚠️  Airflow scheduler might not be running")
                return False
                
            if "postgres" in output and "Up" in output:
                print("✅ PostgreSQL is running")
            else:
                print("⚠️  PostgreSQL might not be running")
                return False
                
            return True
        else:
            print("❌ Docker Compose check failed")
            print(result.stderr)
            return False
            
    except Exception as e:
        print(f"❌ Docker test failed: {e}")
        return False

def test_airflow_ui():
    """Kiểm tra Airflow Web UI"""
    print("\n🌐 Testing Airflow Web UI...")
    
    try:
        # Test health endpoint
        response = requests.get(
            f"{AIRFLOW_URL}/health",
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            timeout=10
        )
        
        if response.status_code == 200:
            print("✅ Airflow UI is accessible")
            health_data = response.json()
            print(f"   Health: {health_data}")
            return True
        else:
            print(f"⚠️  Airflow UI returned status code: {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to Airflow UI")
        print("💡 Make sure Airflow is running: docker compose up -d")
        return False
    except Exception as e:
        print(f"❌ Airflow UI test failed: {e}")
        return False

def test_airflow_api():
    """Kiểm tra Airflow REST API"""
    print("\n🔌 Testing Airflow REST API...")
    
    try:
        # Test DAGs endpoint
        response = requests.get(
            f"{AIRFLOW_URL}/api/v1/dags",
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            timeout=10
        )
        
        if response.status_code == 200:
            print("✅ Airflow REST API is accessible")
            data = response.json()
            dag_count = len(data.get("dags", []))
            print(f"   Found {dag_count} DAG(s)")
            return True
        else:
            print(f"⚠️  API returned status code: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Airflow API test failed: {e}")
        return False

def test_airflow_cli():
    """Kiểm tra Airflow CLI"""
    print("\n💻 Testing Airflow CLI...")
    
    try:
        result = subprocess.run(
            ["docker", "compose", "exec", "-T", "airflow-webserver", "airflow", "version"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            print("✅ Airflow CLI is accessible")
            print(f"   {result.stdout.strip()}")
            return True
        else:
            print("⚠️  Airflow CLI check failed")
            print(result.stderr)
            return False
            
    except Exception as e:
        print(f"❌ Airflow CLI test failed: {e}")
        return False

def test_dags():
    """Kiểm tra DAGs"""
    print("\n📋 Testing DAGs...")
    
    try:
        result = subprocess.run(
            ["docker", "compose", "exec", "-T", "airflow-webserver", "airflow", "dags", "list"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            print("✅ DAGs list command works")
            output = result.stdout
            
            # Count DAGs
            lines = output.strip().split('\n')
            dag_count = len([l for l in lines[2:] if l.strip()])
            
            if dag_count > 0:
                print(f"   Found {dag_count} DAG(s)")
                # Show first few DAGs
                for line in lines[2:6]:
                    if line.strip():
                        print(f"   - {line.split()[0]}")
            else:
                print("⚠️  No DAGs found")
                print("💡 Make sure DAGs are in the dags/ directory")
            
            return True
        else:
            print("⚠️  DAGs list command failed")
            print(result.stderr)
            return False
            
    except Exception as e:
        print(f"❌ DAGs test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("=" * 60)
    print("🧪 Airflow Lab Test Suite")
    print("=" * 60)
    
    tests = [
        ("Docker Services", test_docker_services),
        ("Airflow UI", test_airflow_ui),
        ("Airflow API", test_airflow_api),
        ("Airflow CLI", test_airflow_cli),
        ("DAGs", test_dags),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} test crashed: {e}")
            results.append((test_name, False))
        
        time.sleep(1)  # Small delay between tests
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 Test Summary")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Airflow Lab is ready to use.")
        print("\n📋 Next steps:")
        print("1. Access Airflow UI: http://localhost:8080")
        print("2. Start Jupyter Lab: jupyter lab")
        print("3. Open notebooks/01_airflow_basics.ipynb")
        return 0
    else:
        print("\n⚠️  Some tests failed. Please check the errors above.")
        print("\n💡 Troubleshooting:")
        print("1. Make sure Docker is running")
        print("2. Start Airflow: docker compose up -d")
        print("3. Wait for services to be ready (30-60 seconds)")
        print("4. Check logs: docker compose logs")
        return 1

if __name__ == "__main__":
    sys.exit(main())

