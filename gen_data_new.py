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

Improved data generator with modular architecture, configuration files, and better CLI.

This is the new version of gen-data.py with all the recommended improvements:
- Modular architecture
- Configuration files (YAML/JSON)
- Type hints and better error handling
- Progress tracking and metrics
- Extensible output formats
- Proper logging
- Data validation
"""

import argparse
import sys
import os
import json
import logging
from pathlib import Path
from typing import Optional

from config import GeneratorConfig, get_default_config, ColumnConfig, DataType, TransitionType, DistributionType
from data_generator import DataGenerator, GenerationMetrics
from output_writers import OutputWriterFactory
from exceptions import DataGenerationError, ConfigurationError

logger = logging.getLogger(__name__)


def validate_args(args: argparse.Namespace) -> None:
    """Validate command line arguments"""
    if args.rows is not None and args.rows <= 0:
        raise ValueError("Number of rows must be positive")
    
    if args.days is not None and args.days <= 0:
        raise ValueError("Number of days must be positive")
    
    if args.format and args.format not in OutputWriterFactory.get_supported_formats():
        raise ValueError(f"Unsupported format: {args.format}. Supported: {OutputWriterFactory.get_supported_formats()}")
    
    if args.config and not os.path.isfile(args.config):
        raise ValueError(f"Configuration file not found: {args.config}")
    
    if args.workers is not None and args.workers < 1:
        raise ValueError("Number of workers must be at least 1")


def load_config_from_args(args: argparse.Namespace) -> GeneratorConfig:
    """Load configuration from command line arguments and config files"""
    
    # Start with config file if provided
    if args.config:
        try:
            if args.config.endswith('.yaml') or args.config.endswith('.yml'):
                config = GeneratorConfig.from_yaml(args.config)
            elif args.config.endswith('.json'):
                with open(args.config, 'r') as f:
                    data = json.load(f)
                config = GeneratorConfig.from_dict(data)
            else:
                raise ConfigurationError(f"Unsupported config file format: {args.config}")
        except Exception as e:
            raise ConfigurationError(f"Failed to load config file {args.config}: {e}")
    else:
        # Use default configuration
        config = get_default_config()
    
    # Override with command line arguments
    if args.out:
        config.output_dir = args.out
    if args.rows:
        config.approx_rows_per_day = args.rows
    if args.days:
        config.num_days = args.days
    if args.format:
        config.output_format = args.format
    if args.workers is not None:
        config.max_workers = args.workers
    if args.batch_size:
        config.cache_batch_size = args.batch_size
    if args.log_level:
        config.log_level = args.log_level
    if args.no_progress:
        config.progress_bars = False
    if args.no_cleanup:
        config.cleanup_cache = False
    
    return config


def print_config_example() -> None:
    """Print an example configuration"""
    print("Example YAML configuration:")
    print("""
# data_generator_config.yaml
num_days: 10
approx_rows_per_day: 1000
output_dir: "data"
output_format: "csv"
max_workers: null
log_level: "INFO"

columns:
  - name: "id"
    data_type: "INTEGER"
    transition_percentage: 0.0
    transition_type: 0
    distribution_flags: 1  # INCREMENT
    
  - name: "measurements"
    data_type: "FLOAT"
    transition_percentage: 0.3
    transition_type: 2  # VALUES_SCALE
    distribution_flags: 4  # NORMAL
""")


def print_generation_summary(metrics: GenerationMetrics, config: GeneratorConfig) -> None:
    """Print a summary of the generation process"""
    print("\n" + "="*60)
    print("GENERATION SUMMARY")
    print("="*60)
    print(f"Status: {'SUCCESS' if metrics.errors_encountered == 0 else 'COMPLETED WITH ERRORS'}")
    print(f"Total time: {metrics.generation_time_seconds:.2f} seconds")
    print(f"Rows generated: {metrics.total_rows_generated:,}")
    print(f"Columns: {metrics.total_columns}")
    print(f"Files written: {metrics.files_written}")
    print(f"Output directory: {config.output_dir}")
    print(f"Output format: {config.output_format}")
    if metrics.errors_encountered > 0:
        print(f"Errors encountered: {metrics.errors_encountered}")
    print("="*60)


def cleanup_cache_files() -> None:
    """Clean up all cache files in current directory"""
    import glob
    cache_files = glob.glob("*.cache")
    
    if not cache_files:
        print("No cache files found to clean up.")
        return
    
    print(f"Cleaning up {len(cache_files)} cache files...")
    for cache_file in cache_files:
        try:
            os.unlink(cache_file)
            print(f"  Removed: {cache_file}")
        except OSError as e:
            print(f"  Failed to remove {cache_file}: {e}")
    
    print("Cache cleanup completed.")


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser with all options"""
    parser = argparse.ArgumentParser(
        description="Generate synthetic time-series data with configurable anomalies and transitions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate with defaults
  python gen_data_new.py
  
  # Generate 1000 rows per day for 30 days
  python gen_data_new.py --rows 1000 --days 30
  
  # Use JSON output format
  python gen_data_new.py --format json --out json_data
  
  # Use configuration file
  python gen_data_new.py --config my_config.yaml
  
  # Single-threaded generation with debug logging
  python gen_data_new.py --workers 1 --log-level DEBUG
  
  # Clean up cache files
  python gen_data_new.py --clean-cache
        """
    )
    
    parser.add_argument(
        "-c", "--config",
        help="Configuration file (YAML or JSON)"
    )
    
    parser.add_argument(
        "-o", "--out",
        help="Output directory (default: data)"
    )
    
    parser.add_argument(
        "-r", "--rows",
        type=int,
        help="Approximate number of rows per day"
    )
    
    parser.add_argument(
        "--days",
        type=int,
        help="Number of days of data to generate"
    )
    
    parser.add_argument(
        "--format",
        choices=OutputWriterFactory.get_supported_formats(),
        help="Output format"
    )
    
    parser.add_argument(
        "--workers",
        type=int,
        help="Number of worker processes (default: auto-detect)"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Batch size for cache operations"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bars"
    )
    
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Don't clean up cache files after generation"
    )
    
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate generated data after creation"
    )
    
    parser.add_argument(
        "--clean-cache",
        action="store_true",
        help="Clean up cache files and exit"
    )
    
    parser.add_argument(
        "--example-config",
        action="store_true",
        help="Print example configuration and exit"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="Data Generator 2.0 - Improved and Modular"
    )
    
    return parser


def main() -> int:
    """Main entry point"""
    parser = create_parser()
    args = parser.parse_args()
    
    # Handle special commands first
    if args.example_config:
        print_config_example()
        return 0
    
    if args.clean_cache:
        cleanup_cache_files()
        return 0
    
    try:
        # Validate arguments
        validate_args(args)
        
        # Load configuration
        config = load_config_from_args(args)
        
        # Set up basic logging before generator does more detailed setup
        logging.basicConfig(level=logging.INFO)
        
        logger.info("Starting data generation...")
        logger.info(f"Configuration: {config.num_days} days, {config.approx_rows_per_day} rows/day, {len(config.columns)} columns")
        
        # Create and run generator
        generator = DataGenerator(config)
        metrics = generator.generate()
        
        # Validate generated data if requested
        if args.validate:
            logger.info("Validating generated data...")
            issues = generator.validate_generated_data()
            if issues:
                logger.warning(f"Data validation found {len(issues)} issues:")
                for issue in issues:
                    logger.warning(f"  - {issue}")
            else:
                logger.info("Data validation passed - no issues found")
        
        # Print summary
        print_generation_summary(metrics, config)
        
        return 0 if metrics.errors_encountered == 0 else 1
        
    except KeyboardInterrupt:
        logger.info("Generation interrupted by user")
        return 130
        
    except (DataGenerationError, ConfigurationError, ValueError) as e:
        logger.error(f"Generation failed: {e}")
        return 1
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.debug("Full traceback:", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main()) 