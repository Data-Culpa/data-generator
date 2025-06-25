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

                Column space module for generating data for individual columns.
"""

import os
import random
import sqlite3
from contextlib import contextmanager
from typing import List, Any, Optional, Dict, Generator
import numpy as np
import logging

from GenWords import ValueGenerator
from config import ColumnConfig, DataType, TransitionType, DistributionType
from exceptions import CacheError, DataGenerationError

logger = logging.getLogger(__name__)


class ColumnSpace:
    """Generates data for a single column with transitions and caching"""
    
    def __init__(self, config: ColumnConfig):
        self.config = config
        self.word_gen: Optional[ValueGenerator] = None
        if config.data_type in (DataType.STRING_LONG, DataType.STRING_CATEGORY):
            self.word_gen = ValueGenerator()
    
    @property
    def field_name(self) -> str:
        """Get the field name"""
        return self.config.name
    
    def has_distribution(self, mask: int) -> bool:
        """Check if column has a specific distribution flag"""
        return (self.config.distribution_flags & mask) != 0
    
    def has_transition(self, day_index: int, total_days: int, mask: int) -> bool:
        """Check if a transition should be applied on a given day"""
        transition_day = int(total_days * self.config.transition_percentage)
        if day_index < transition_day:
            return False
        return (self.config.transition_type & mask) != 0
    
    def get_field_name(self, day_index: int, total_days: int) -> str:
        """Get field name, potentially modified by transitions"""
        if self.has_transition(day_index, total_days, TransitionType.SCHEMA_NAME):
            return f"new-{self.config.name}"
        return self.config.name
    
    def _generate_word(self, day_index: int, total_days: int) -> str:
        """Generate a word based on column type and transitions"""
        if not self.word_gen:
            raise DataGenerationError(f"Word generator not initialized for column {self.config.name}")
        
        if self.config.data_type == DataType.STRING_LONG:
            if self.has_transition(day_index, total_days, TransitionType.STRING_LONG_TO_SMALL):
                return self.word_gen.rand_cat_str()
            return self.word_gen.rand_words(20)
        
        elif self.config.data_type == DataType.STRING_CATEGORY:
            if self.has_transition(day_index, total_days, TransitionType.STRING_SMALL_TO_LONG):
                return self.word_gen.rand_words(5)
            return self.word_gen.rand_cat_str()
        
        else:
            # Fallback for numeric types that become strings
            return self.word_gen.rand_cat_str()
    
    def _generate_numeric_distribution(self, num_values: int) -> np.ndarray:
        """Generate numeric values based on distribution flags"""
        if self.has_distribution(DistributionType.INCREMENT):
            return np.arange(0, num_values, 1.0)
        
        if self.has_distribution(DistributionType.UNIFORM):
            return np.random.rand(num_values)
        
        if self.has_distribution(DistributionType.NORMAL):
            return np.random.normal(0, 100, num_values)
        
        # Default to uniform if no distribution specified
        return np.random.rand(num_values)
    
    def _apply_transitions(self, values: List[Any], day_index: int, total_days: int) -> List[Any]:
        """Apply transitions to generated values"""
        if self.has_transition(day_index, total_days, TransitionType.VALUES_SCALE):
            values = [v * 40 if isinstance(v, (int, float)) else v for v in values]
        
        if self.has_transition(day_index, total_days, TransitionType.ZEROS_HIGH):
            if random.random() >= 0.5:
                values = self._replace_half(values, 0)
        
        if self.has_transition(day_index, total_days, TransitionType.NULLS_HIGH):
            if random.random() >= 0.5:
                values = self._replace_half(values, "")
        
        if self.has_transition(day_index, total_days, TransitionType.VALUES_SOME_STRINGS):
            values = [
                f"sometimes-{v}" if random.choice([True, False]) else v
                for v in values
            ]
        elif self.has_transition(day_index, total_days, TransitionType.VALUES_ALL_STRINGS):
            values = [f"all-{v}" for v in values]
        
        return values
    
    def _replace_half(self, values: List[Any], new_value: Any) -> List[Any]:
        """Replace roughly half of the values with a new value"""
        return [new_value if random.random() > 0.5 else v for v in values]
    
    def _convert_types(self, values: List[Any]) -> List[Any]:
        """Convert values to appropriate types"""
        if self.config.data_type == DataType.INTEGER:
            return [int(v * 100) if isinstance(v, (int, float)) and v != 0 and v != "" else v for v in values]
        return values
    
    def _get_cache_path(self, day_index: int) -> str:
        """Get cache file path for a given day"""
        return f".{self.config.name}-{day_index}.cache"
    
    @contextmanager
    def _cache_connection(self, day_index: int) -> Generator[sqlite3.Connection, None, None]:
        """Context manager for cache database connection"""
        cache_path = self._get_cache_path(day_index)
        if os.path.exists(cache_path):
            os.unlink(cache_path)
        
        conn = None
        try:
            conn = sqlite3.connect(cache_path)
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS data (val)")
            yield conn
        except Exception as e:
            raise CacheError(f"Cache operation failed for {self.config.name}: {e}")
        finally:
            if conn:
                conn.close()
    
    def generate_day_data(self, day_index: int, num_values: int, total_days: int, batch_size: int = 10000) -> None:
        """Generate data for a single day and cache it"""
        logger.debug(f"Generating {num_values} values for column {self.config.name}, day {day_index}")
        
        with self._cache_connection(day_index) as conn:
            cursor = conn.cursor()
            values_generated = 0
            
            while values_generated < num_values:
                batch_size_actual = min(batch_size, num_values - values_generated)
                
                # Generate batch of data
                if self.config.data_type in (DataType.STRING_LONG, DataType.STRING_CATEGORY):
                    batch_values = [
                        self._generate_word(day_index, total_days)
                        for _ in range(batch_size_actual)
                    ]
                else:
                    batch_values = self._generate_numeric_distribution(batch_size_actual).tolist()
                    batch_values = self._convert_types(batch_values)
                
                # Apply transitions
                batch_values = self._apply_transitions(batch_values, day_index, total_days)
                
                # Insert into cache
                cursor.executemany("INSERT INTO data VALUES (?)", [(v,) for v in batch_values])
                values_generated += len(batch_values)
            
            conn.commit()
        
        logger.debug(f"Generated {values_generated} values for column {self.config.name}, day {day_index}")
    
    def cleanup_cache(self, day_index: int) -> None:
        """Clean up cache files for a specific day"""
        cache_path = self._get_cache_path(day_index)
        if os.path.exists(cache_path):
            try:
                os.unlink(cache_path)
                logger.debug(f"Cleaned up cache file: {cache_path}")
            except OSError as e:
                logger.warning(f"Failed to remove cache file {cache_path}: {e}")
    
    def cleanup_all_cache(self) -> None:
        """Clean up all cache files for this column"""
        cache_pattern = f".{self.config.name}-*.cache"
        import glob
        for cache_file in glob.glob(cache_pattern):
            try:
                os.unlink(cache_file)
                logger.debug(f"Cleaned up cache file: {cache_file}")
            except OSError as e:
                logger.warning(f"Failed to remove cache file {cache_file}: {e}") 