#!/usr/bin/env python3
"""
Test script để verify dbt lab setup
"""

import subprocess
import sys
from pathlib import Path

def run_command(cmd, description):
    """Run command và return success status"""
    print(f"\n{'='*60}")
    print(f"Testing: {description}")
    print(f"Command: {' '.join(cmd)}")
    print('='*60)
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            print("✅ SUCCESS")
            if result.stdout:
                print(result.stdout)
            return True
        else:
            print("❌ FAILED")
            print(result.stderr)
            return False
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

def main():
    """Main test function"""
    project_root = Path(__file__).parent.resolve()
    
    print("🧪 Testing dbt Lab Setup")
    print("="*60)
    
    tests = [
        (['dbt', '--version'], 'dbt version'),
        (['dbt', 'debug', '--profiles-dir', '.', '--project-dir', '.'], 'dbt connection'),
        (['dbt', 'list', '--profiles-dir', '.', '--project-dir', '.'], 'dbt list resources'),
    ]
    
    results = []
    for cmd, desc in tests:
        success = run_command(cmd, desc)
        results.append((desc, success))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for desc, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status}: {desc}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! dbt Lab is ready to use.")
        return 0
    else:
        print("\n⚠️  Some tests failed. Please check the errors above.")
        return 1

if __name__ == '__main__':
    sys.exit(main())

