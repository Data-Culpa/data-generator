#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
                 8888888b.           888              .d8888b.           888                   
                 888  "Y88b          888             d88P  Y88b          888                   
                 888    888          888             888    888          888                   
                 888    888  8888b.  888888  8888b.  888        888  888 888 88888b.   8888b.  
                 888    888     "88b 888        "88b 888        888  888 888 888 "88b     "88b 
                 888    888 .d888888 888    .d888888 888    888 888  888 888 888  888 .d888888 
                 888  .d88P 888  888 Y88b.  888  888 Y88b  d88P Y88b 888 888 888 d88P 888  888 
                 8888888P"  "Y888888  "Y888 "Y888888  "Y8888P"   "Y88888 888 88888P"  "Y888888 
                                                                             888               
                                                                             888               
                                                                             888               

Copyright © 2019-2025 Data Culpa, Inc. All Rights Reserved.

Basic test suite for the improved data generator.
"""

import unittest
import tempfile
import os
import shutil
import json
from pathlib import Path

from config import GeneratorConfig, ColumnConfig, DataType, DistributionType, TransitionType
from data_generator import DataGenerator
from column_space import ColumnSpace
from output_writers import OutputWriterFactory
from exceptions import DataGenerationError, ConfigurationError


class TestConfig(unittest.TestCase):
    """Test configuration functionality"""
    
    def test_default_config(self):
        """Test default configuration creation"""
        from config import get_default_config
        config = get_default_config()
        
        self.assertEqual(config.num_days, 10)
        self.assertEqual(config.approx_rows_per_day, 500)
        self.assertTrue(len(config.columns) > 0)
    
    def test_column_config_validation(self):
        """Test column configuration validation"""
        # Valid config
        col_config = ColumnConfig("test", DataType.INTEGER, 0.5, 0, 0)
        self.assertEqual(col_config.name, "test")
        
        # Invalid transition percentage
        with self.assertRaises(ValueError):
            ColumnConfig("test", DataType.INTEGER, 1.5, 0, 0)  # > 1.0
        
        with self.assertRaises(ValueError):
            ColumnConfig("test", DataType.INTEGER, -0.1, 0, 0)  # < 0.0
    
    def test_generator_config_validation(self):
        """Test generator configuration validation"""
        # Valid config
        config = GeneratorConfig(num_days=5, approx_rows_per_day=100)
        self.assertEqual(config.num_days, 5)
        
        # Invalid values
        with self.assertRaises(ValueError):
            GeneratorConfig(num_days=0)  # Must be positive
        
        with self.assertRaises(ValueError):
            GeneratorConfig(approx_rows_per_day=-1)  # Must be positive
        
        with self.assertRaises(ValueError):
            GeneratorConfig(output_format="invalid")  # Must be valid format


class TestColumnSpace(unittest.TestCase):
    """Test column space functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()  # Store original working directory
        os.chdir(self.temp_dir)
    
    def tearDown(self):
        """Clean up test fixtures"""
        os.chdir(self.original_cwd)  # Restore original working directory first
        shutil.rmtree(self.temp_dir)
    
    def test_integer_column_creation(self):
        """Test creating an integer column"""
        config = ColumnConfig("test_int", DataType.INTEGER, 0.0, 0, DistributionType.UNIFORM)
        column = ColumnSpace(config)
        
        self.assertEqual(column.field_name, "test_int")
        self.assertTrue(column.has_distribution(DistributionType.UNIFORM))
    
    def test_string_column_creation(self):
        """Test creating a string column"""
        config = ColumnConfig("test_str", DataType.STRING_CATEGORY, 0.0, 0, 0)
        column = ColumnSpace(config)
        
        self.assertEqual(column.field_name, "test_str")
        self.assertIsNotNone(column.word_gen)
    
    def test_data_generation(self):
        """Test basic data generation"""
        config = ColumnConfig("test", DataType.INTEGER, 0.0, 0, DistributionType.UNIFORM)
        column = ColumnSpace(config)
        
        # Generate some data
        column.generate_day_data(0, 100, 5, 50)  # day 0, 100 values, 5 total days, batch size 50
        
        # Check cache file was created
        cache_path = column._get_cache_path(0)
        self.assertTrue(os.path.exists(cache_path))
        
        # Clean up
        column.cleanup_cache(0)
        self.assertFalse(os.path.exists(cache_path))


class TestOutputWriters(unittest.TestCase):
    """Test output writer functionality"""
    
    def test_factory_creation(self):
        """Test output writer factory"""
        csv_writer = OutputWriterFactory.create_writer('csv')
        self.assertEqual(csv_writer.get_file_extension(), 'csv')
        
        json_writer = OutputWriterFactory.create_writer('json')
        self.assertEqual(json_writer.get_file_extension(), 'json')
        
        jsonl_writer = OutputWriterFactory.create_writer('jsonl')
        self.assertEqual(jsonl_writer.get_file_extension(), 'jsonl')
    
    def test_unsupported_format(self):
        """Test unsupported format handling"""
        from exceptions import OutputError
        with self.assertRaises(OutputError):
            OutputWriterFactory.create_writer('xml')
    
    def test_supported_formats(self):
        """Test getting supported formats"""
        formats = OutputWriterFactory.get_supported_formats()
        self.assertIn('csv', formats)
        self.assertIn('json', formats)
        self.assertIn('jsonl', formats)


class TestDataGenerator(unittest.TestCase):
    """Test main data generator functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()  # Store original working directory
        
        # Simple test configuration
        self.config = GeneratorConfig(
            num_days=2,
            approx_rows_per_day=10,
            output_dir=os.path.join(self.temp_dir, "output"),
            output_format="csv",
            max_workers=1,  # Single-threaded for testing
            progress_bars=False,
            log_level="ERROR"  # Reduce noise in tests
        )
        
        # Add a simple column
        self.config.columns = [
            ColumnConfig("id", DataType.INTEGER, 0.0, 0, DistributionType.INCREMENT),
            ColumnConfig("value", DataType.FLOAT, 0.0, 0, DistributionType.UNIFORM)
        ]
    
    def tearDown(self):
        """Clean up test fixtures"""
        os.chdir(self.original_cwd)  # Restore original working directory
        shutil.rmtree(self.temp_dir)
    
    def test_basic_generation(self):
        """Test basic data generation"""
        generator = DataGenerator(self.config)
        metrics = generator.generate()
        
        # Check metrics
        self.assertGreater(metrics.total_rows_generated, 0)
        self.assertEqual(metrics.total_columns, 2)
        self.assertEqual(metrics.files_written, 2)  # 2 days
        self.assertEqual(metrics.errors_encountered, 0)
        
        # Check output files exist
        output_dir = self.config.output_dir
        self.assertTrue(os.path.exists(output_dir))
        self.assertTrue(os.path.exists(os.path.join(output_dir, "0.csv")))
        self.assertTrue(os.path.exists(os.path.join(output_dir, "1.csv")))
        
        # Check generation profile was created
        profile_path = os.path.join(output_dir, "generation_profile.json")
        self.assertTrue(os.path.exists(profile_path))
        
        with open(profile_path) as f:
            profile = json.load(f)
            self.assertIn('config', profile)
            self.assertIn('metrics', profile)
    
    def test_data_validation(self):
        """Test data validation functionality"""
        generator = DataGenerator(self.config)
        generator.generate()
        
        issues = generator.validate_generated_data()
        self.assertEqual(len(issues), 0)  # Should have no issues
    
    def test_json_output(self):
        """Test JSON output format"""
        self.config.output_format = "json"
        generator = DataGenerator(self.config)
        generator.generate()
        
        # Check JSON files exist and are valid
        output_dir = self.config.output_dir
        json_file = os.path.join(output_dir, "0.json")
        self.assertTrue(os.path.exists(json_file))
        
        with open(json_file) as f:
            data = json.load(f)
            self.assertIsInstance(data, list)
            if len(data) > 0:
                self.assertIn('id', data[0])
                self.assertIn('value', data[0])


def run_integration_test():
    """Run a simple integration test"""
    print("Running integration test...")
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        config = GeneratorConfig(
            num_days=3,
            approx_rows_per_day=50,
            output_dir=os.path.join(temp_dir, "test_output"),
            output_format="csv",
            max_workers=1,
            progress_bars=False,
            cleanup_cache=True
        )
        
        # Simple columns with transitions
        config.columns = [
            ColumnConfig("id", DataType.INTEGER, 0.0, 0, DistributionType.INCREMENT),
            ColumnConfig("normal_data", DataType.FLOAT, 0.0, 0, DistributionType.NORMAL),
            ColumnConfig("scaled_later", DataType.FLOAT, 0.5, TransitionType.VALUES_SCALE, DistributionType.UNIFORM),
            ColumnConfig("nulls_later", DataType.INTEGER, 0.7, TransitionType.NULLS_HIGH, DistributionType.UNIFORM),
        ]
        
        generator = DataGenerator(config)
        metrics = generator.generate()
        
        print(f"✓ Generated {metrics.total_rows_generated} rows in {metrics.generation_time_seconds:.2f}s")
        print(f"✓ Created {metrics.files_written} files")
        print(f"✓ No errors: {metrics.errors_encountered == 0}")
        
        # Validate
        issues = generator.validate_generated_data()
        print(f"✓ Validation: {'PASSED' if len(issues) == 0 else f'FAILED ({len(issues)} issues)'}")
        
        print("Integration test completed successfully!")


if __name__ == "__main__":
    # Run unit tests
    print("Running unit tests...")
    unittest.main(verbosity=2, exit=False)
    
    print("\n" + "="*50)
    
    # Run integration test
    run_integration_test() 