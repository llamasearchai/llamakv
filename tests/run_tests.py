#!/usr/bin/env python
"""
Test runner for LlamaKV.

This script discovers and runs all tests in the tests directory.
"""

import unittest
import sys
import os


def run_tests():
    """Discover and run all tests."""
    # Get the directory of this script
    test_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Discover tests
    loader = unittest.TestLoader()
    suite = loader.discover(test_dir)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Return exit code based on test result
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(run_tests()) 