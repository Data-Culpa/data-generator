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

                Main data generator orchestrating the entire generation process.
"""

import os
import random
import time
import logging
import sqlite3
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Generator
import json

from config import GeneratorConfig, ColumnConfig
from column_space import ColumnSpace
from output_writers import OutputWriterFactory
from exceptions import DataGenerationError, CacheError, OutputError

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

logger = logging.getLogger(__name__)


@dataclass
class GenerationMetrics:
    """Metrics collected during generation"""
    total_rows_generated: int = 0
    total_columns: int = 0
    generation_time_seconds: float = 0.0
    cache_operations: int = 0
    files_written: int = 0
    errors_encountered: int = 0
    
    def to_dict(self) -> Dict[str, any]:
        return asdict(self)


def generate_column_worker(column_config: ColumnConfig, rows_per_day_map: Dict[int, int], 
                          total_days: int, batch_size: int) -> str:
    """Worker function for multiprocessing column data generation"""
    try:
        column_space = ColumnSpace(column_config)
        
        for day_index in range(total_days):
            num_values = rows_per_day_map[day_index]
            column_space.generate_day_data(day_index, num_values, total_days, batch_size)
        
        return f"Successfully generated column: {column_config.name}"
    except Exception as e:
        logger.error(f"Failed to generate column {column_config.name}: {e}")
        raise


class DataGenerator:
    """Main data generator class"""
    
    def __init__(self, config: GeneratorConfig):
        self.config = config
        self.metrics = GenerationMetrics()
        self.column_spaces: List[ColumnSpace] = []
        self.rows_per_day_map: Dict[int, int] = {}
        
        # Set up logging
        self._setup_logging()
        
        # Initialize column spaces
        for col_config in config.columns:
            self.column_spaces.append(ColumnSpace(col_config))
        
        self.metrics.total_columns = len(self.column_spaces)
    
    def _setup_logging(self) -> None:
        """Set up logging configuration"""
        log_level = getattr(logging, self.config.log_level.upper(), logging.INFO)
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def _generate_rows_per_day_map(self) -> Dict[int, int]:
        """Generate the number of rows for each day with variation"""
        rows_map = {}
        base_rows = self.config.approx_rows_per_day
        variation = self.config.row_variation_percentage
        
        for day in range(self.config.num_days):
            # Add random variation
            variation_factor = 1 + (random.random() - 0.5) * 2 * variation
            num_rows = int(base_rows * variation_factor)
            
            # Randomly reduce some days significantly
            if random.random() > self.config.random_day_reduction_probability:
                num_rows = int(num_rows * self.config.random_day_reduction_factor)
            
            rows_map[day] = max(1, num_rows)  # Ensure at least 1 row
        
        return rows_map
    
    def _create_output_directory(self) -> None:
        """Create output directory if it doesn't exist"""
        if not os.path.exists(self.config.output_dir):
            os.makedirs(self.config.output_dir)
            logger.info(f"Created output directory: {self.config.output_dir}")
    
    def _generate_column_data(self) -> None:
        """Generate data for all columns using multiprocessing"""
        logger.info(f"Generating data for {len(self.column_spaces)} columns across {self.config.num_days} days")
        
        if self.config.max_workers == 1:
            # Single-threaded generation
            progress_iter = tqdm(self.column_spaces, desc="Generating columns") if HAS_TQDM and self.config.progress_bars else self.column_spaces
            
            for column_space in progress_iter:
                for day_index in range(self.config.num_days):
                    num_values = self.rows_per_day_map[day_index]
                    column_space.generate_day_data(day_index, num_values, self.config.num_days, self.config.cache_batch_size)
                    self.metrics.cache_operations += 1
        else:
            # Multi-process generation
            max_workers = self.config.max_workers or os.cpu_count()
            
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                # Submit tasks
                future_to_column = {
                    executor.submit(
                        generate_column_worker,
                        col.config,
                        self.rows_per_day_map,
                        self.config.num_days,
                        self.config.cache_batch_size
                    ): col.config.name
                    for col in self.column_spaces
                }
                
                # Process results with progress bar
                progress_iter = tqdm(as_completed(future_to_column), total=len(future_to_column), desc="Generating columns") if HAS_TQDM and self.config.progress_bars else as_completed(future_to_column)
                
                for future in progress_iter:
                    column_name = future_to_column[future]
                    try:
                        result = future.result()
                        logger.debug(result)
                        self.metrics.cache_operations += self.config.num_days
                    except Exception as e:
                        logger.error(f"Column {column_name} generation failed: {e}")
                        self.metrics.errors_encountered += 1
                        raise DataGenerationError(f"Failed to generate column {column_name}: {e}")
    
    @contextmanager
    def _open_cache_connections(self, day_index: int) -> Generator[Dict[str, sqlite3.Connection], None, None]:
        """Context manager for opening cache connections for a day"""
        connections = {}
        try:
            for column_space in self.column_spaces:
                cache_path = column_space._get_cache_path(day_index)
                if not os.path.exists(cache_path):
                    raise CacheError(f"Cache file not found: {cache_path}")
                
                connections[column_space.config.name] = sqlite3.connect(cache_path)
            
            yield connections
        
        finally:
            for conn in connections.values():
                conn.close()
    
    def _write_output_data(self) -> None:
        """Write output data in the specified format"""
        logger.info(f"Writing output data in {self.config.output_format} format")
        
        writer = OutputWriterFactory.create_writer(self.config.output_format)
        extension = writer.get_file_extension()
        
        progress_iter = tqdm(range(self.config.num_days), desc="Writing output files") if HAS_TQDM and self.config.progress_bars else range(self.config.num_days)
        
        for day_index in progress_iter:
            try:
                # Get field names for this day (may change due to schema transitions)
                field_names = []
                for column_space in self.column_spaces:
                    field_name = column_space.get_field_name(day_index, self.config.num_days)
                    field_names.append(field_name)
                
                # Open cache connections for this day
                with self._open_cache_connections(day_index) as cache_handles:
                    # Write the day's data
                    output_path = os.path.join(self.config.output_dir, f"{day_index}.{extension}")
                    rows_for_day = self.rows_per_day_map[day_index]
                    
                    writer.write_day(day_index, field_names, cache_handles, rows_for_day, output_path)
                    writer.set_file_timestamp(output_path, day_index, self.config.num_days)
                    
                    self.metrics.files_written += 1
                    self.metrics.total_rows_generated += rows_for_day
                    
            except Exception as e:
                logger.error(f"Failed to write output for day {day_index}: {e}")
                self.metrics.errors_encountered += 1
                raise OutputError(f"Failed to write output for day {day_index}: {e}")
    
    def _cleanup_cache_files(self) -> None:
        """Clean up cache files if requested"""
        if not self.config.cleanup_cache:
            return
        
        logger.info("Cleaning up cache files")
        for column_space in self.column_spaces:
            column_space.cleanup_all_cache()
    
    def _write_generation_profile(self) -> None:
        """Write generation profile and metrics"""
        profile = {
            'config': self.config.to_dict(),
            'metrics': self.metrics.to_dict(),
            'rows_per_day': self.rows_per_day_map,
            'generation_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        }
        
        profile_path = os.path.join(self.config.output_dir, 'generation_profile.json')
        with open(profile_path, 'w') as f:
            json.dump(profile, f, indent=2)
        
        logger.info(f"Generation profile written to: {profile_path}")
    
    def generate(self) -> GenerationMetrics:
        """Main generation method"""
        start_time = time.time()
        
        try:
            logger.info(f"Starting data generation with {self.config.approx_rows_per_day} rows/day for {self.config.num_days} days")
            
            # Generate rows per day mapping
            self.rows_per_day_map = self._generate_rows_per_day_map()
            total_expected_rows = sum(self.rows_per_day_map.values())
            logger.info(f"Will generate approximately {total_expected_rows} total rows")
            
            # Create output directory
            self._create_output_directory()
            
            # Generate column data
            self._generate_column_data()
            
            # Write output files
            self._write_output_data()
            
            # Write generation profile
            self._write_generation_profile()
            
            # Cleanup if requested
            self._cleanup_cache_files()
            
            self.metrics.generation_time_seconds = time.time() - start_time
            
            logger.info(f"Generation completed successfully in {self.metrics.generation_time_seconds:.2f} seconds")
            logger.info(f"Generated {self.metrics.total_rows_generated} rows across {self.metrics.total_columns} columns")
            logger.info(f"Wrote {self.metrics.files_written} files to {self.config.output_dir}")
            
            return self.metrics
            
        except Exception as e:
            self.metrics.generation_time_seconds = time.time() - start_time
            self.metrics.errors_encountered += 1
            logger.error(f"Generation failed after {self.metrics.generation_time_seconds:.2f} seconds: {e}")
            raise DataGenerationError(f"Data generation failed: {e}")
    
    def validate_generated_data(self) -> List[str]:
        """Validate the generated data and return list of issues"""
        issues = []
        
        # Check if output directory exists and has files
        if not os.path.exists(self.config.output_dir):
            issues.append("Output directory does not exist")
            return issues
        
        # Check if expected number of files were created
        extension = OutputWriterFactory.create_writer(self.config.output_format).get_file_extension()
        expected_files = [f"{i}.{extension}" for i in range(self.config.num_days)]
        
        for expected_file in expected_files:
            file_path = os.path.join(self.config.output_dir, expected_file)
            if not os.path.exists(file_path):
                issues.append(f"Missing expected file: {expected_file}")
            elif os.path.getsize(file_path) == 0:
                issues.append(f"Empty file: {expected_file}")
        
        # Validate metrics
        if self.metrics.total_rows_generated == 0:
            issues.append("No rows were generated")
        
        if self.metrics.files_written != self.config.num_days:
            issues.append(f"Expected {self.config.num_days} files, but wrote {self.metrics.files_written}")
        
        return issues 