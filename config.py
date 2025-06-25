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

                Configuration module for the data generator.
                Contains all constants, settings, and configuration dataclasses.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import IntEnum, IntFlag
import os


class DataType(IntEnum):
    """Data types for columns"""
    INTEGER = 0
    FLOAT = 1
    STRING_LONG = 2
    STRING_CATEGORY = 3


class TransitionType(IntFlag):
    """Types of transitions that can occur in data"""
    VALUES_SCALE = 1 << 1
    VALUES_ALL_STRINGS = 1 << 2
    VALUES_SOME_STRINGS = 1 << 3
    SCHEMA_NAME = 1 << 4
    STRING_LONG_TO_SMALL = 1 << 5
    STRING_SMALL_TO_LONG = 1 << 6
    NULLS_HIGH = 1 << 7
    ZEROS_HIGH = 1 << 8


class DistributionType(IntFlag):
    """Types of data distributions"""
    INCREMENT = 1 << 0
    UNIFORM = 1 << 1
    NORMAL = 1 << 2


@dataclass
class ColumnConfig:
    """Configuration for a single column"""
    name: str
    data_type: DataType
    transition_percentage: float = 0.5
    transition_type: int = 0
    distribution_flags: int = 0
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if not 0 <= self.transition_percentage <= 1:
            raise ValueError(f"transition_percentage must be between 0 and 1, got {self.transition_percentage}")


@dataclass
class GeneratorConfig:
    """Main configuration for data generation"""
    # Basic settings
    num_days: int = 10
    approx_rows_per_day: int = 500
    output_dir: str = "data"
    
    # Output formats
    output_format: str = "csv"  # csv, json, jsonl
    
    # Performance settings
    cache_batch_size: int = 10_000
    max_workers: int = None  # None means use all available CPUs
    
    # Data variation settings
    row_variation_percentage: float = 0.2  # +/- variation in rows per day
    random_day_reduction_probability: float = 0.8  # Probability of reducing a day's data
    random_day_reduction_factor: float = 0.1  # Factor to reduce by
    
    # File settings
    cleanup_cache: bool = True
    resume_generation: bool = False
    
    # Logging
    log_level: str = "INFO"
    progress_bars: bool = True
    
    # Columns configuration
    columns: List[ColumnConfig] = field(default_factory=list)
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if self.num_days <= 0:
            raise ValueError("num_days must be positive")
        if self.approx_rows_per_day <= 0:
            raise ValueError("approx_rows_per_day must be positive")
        if self.output_format not in ["csv", "json", "jsonl"]:
            raise ValueError("output_format must be one of: csv, json, jsonl")
        if not 0 <= self.row_variation_percentage <= 1:
            raise ValueError("row_variation_percentage must be between 0 and 1")
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> 'GeneratorConfig':
        """Load configuration from YAML file"""
        try:
            import yaml
            with open(yaml_path, 'r') as f:
                data = yaml.safe_load(f)
            return cls.from_dict(data)
        except ImportError:
            raise ImportError("PyYAML is required for YAML configuration files")
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'GeneratorConfig':
        """Create config from dictionary"""
        columns_data = data.pop('columns', [])
        columns = []
        
        for col_data in columns_data:
            # Convert string enum values to enum instances
            if isinstance(col_data.get('data_type'), str):
                col_data['data_type'] = DataType[col_data['data_type'].upper()]
            
            columns.append(ColumnConfig(**col_data))
        
        return cls(columns=columns, **data)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary"""
        result = {}
        for key, value in self.__dict__.items():
            if key == 'columns':
                result[key] = [col.__dict__ for col in value]
            else:
                result[key] = value
        return result


def get_default_columns() -> List[ColumnConfig]:
    """Get the default column configuration"""
    return [
        ColumnConfig("id", DataType.INTEGER, 0.0, 0, DistributionType.INCREMENT),
        ColumnConfig("uniform1", DataType.INTEGER, 0.0, 0, DistributionType.UNIFORM),
        ColumnConfig("zeroes1", DataType.INTEGER, 0.6, TransitionType.ZEROS_HIGH, DistributionType.UNIFORM),
        ColumnConfig("nulls1", DataType.INTEGER, 0.4, TransitionType.NULLS_HIGH, DistributionType.UNIFORM),
        ColumnConfig("normal1", DataType.FLOAT, 0.2, TransitionType.SCHEMA_NAME, DistributionType.NORMAL),
        ColumnConfig("uniform2", DataType.FLOAT, 0.3, TransitionType.VALUES_SCALE, DistributionType.UNIFORM),
        ColumnConfig("normal2", DataType.INTEGER, 0.7, TransitionType.VALUES_SCALE | TransitionType.VALUES_SOME_STRINGS, DistributionType.NORMAL),
        ColumnConfig("normal3", DataType.INTEGER, 0.7, TransitionType.VALUES_SCALE, DistributionType.NORMAL),
        ColumnConfig("uniform3", DataType.FLOAT, 0.9, TransitionType.VALUES_ALL_STRINGS, DistributionType.UNIFORM),
        ColumnConfig("catStr1", DataType.STRING_CATEGORY, 0.5, TransitionType.STRING_SMALL_TO_LONG, 0),
        ColumnConfig("catStrSteady2", DataType.STRING_CATEGORY, 0, 0, 0),
        ColumnConfig("descStr1", DataType.STRING_LONG, 0.5, TransitionType.STRING_LONG_TO_SMALL, 0),
    ]


def get_default_config() -> GeneratorConfig:
    """Get default configuration with standard columns"""
    config = GeneratorConfig()
    config.columns = get_default_columns()
    return config 