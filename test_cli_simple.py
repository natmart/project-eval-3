#!/usr/bin/env python
"""Simple test to verify CLI can be imported."""
import sys
sys.path.insert(0, '.')

try:
    print("Attempting to import CLI...")
    from pytaskq.cli import create_parser, main
    print("✓ CLI imported successfully")
    
    parser = create_parser()
    print("✓ Parser created successfully")
    
    # Test parsing a simple command
    args = parser.parse_args(['--version'])
    print(f"✓ Parsed --version: {args.version}")
    
    args = parser.parse_args(['status'])
    print(f"✓ Parsed status command: {args.command}")
    
    print("\nAll CLI tests passed!")
    sys.exit(0)
    
except ImportError as e:
    print(f"✗ Import error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)