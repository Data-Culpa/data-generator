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

Output writers for different data formats.
Uses strategy pattern for extensible output formats.
"""

import json
import csv
import os
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import sqlite3
import logging

from exceptions import OutputError

logger = logging.getLogger(__name__)


class OutputWriter(ABC):
    """Abstract base class for output writers"""
    
    @abstractmethod
    def write_day(self, day_index: int, field_names: List[str], 
                  cache_handles: Dict[str, sqlite3.Connection], 
                  rows_per_day: int, output_path: str) -> None:
        """Write data for a single day"""
        pass
    
    @abstractmethod
    def get_file_extension(self) -> str:
        """Get file extension for this format"""
        pass
    
    def set_file_timestamp(self, file_path: str, day_index: int, total_days: int) -> None:
        """Set file modification time to simulate historical data"""
        time_now = time.time()
        day_time = time_now - ((total_days - day_index - 1) * 86400)
        os.utime(file_path, (day_time, day_time))


class CSVWriter(OutputWriter):
    """Writer for CSV format"""
    
    def write_day(self, day_index: int, field_names: List[str], 
                  cache_handles: Dict[str, sqlite3.Connection], 
                  rows_per_day: int, output_path: str) -> None:
        """Write CSV data for a single day"""
        try:
            with open(output_path, "w", newline='') as fp:
                writer = csv.writer(fp)
                
                # Write header
                writer.writerow(field_names)
                
                # Write data rows
                for row_index in range(1, rows_per_day + 1):
                    row = self._get_data_row(field_names, row_index, cache_handles, none_value="")
                    if row and row != [None]:
                        writer.writerow(row)
                        
        except Exception as e:
            raise OutputError(f"Failed to write CSV file {output_path}: {e}")
    
    def get_file_extension(self) -> str:
        return "csv"
    
    def _get_data_row(self, field_names: List[str], row_index: int, 
                      cache_handles: Dict[str, sqlite3.Connection], 
                      none_value: Any = None) -> List[Any]:
        """Get a single row of data from caches"""
        row = []
        for field_name in field_names:
            cache_conn = cache_handles.get(field_name)
            if cache_conn is None:
                row.append(none_value)
            else:
                try:
                    cursor = cache_conn.cursor()
                    cursor.execute("SELECT val FROM data WHERE rowid=?", (row_index,))
                    result = cursor.fetchone()
                    if result:
                        row.append(str(result[0]))
                    else:
                        row.append(none_value)
                except Exception as e:
                    logger.warning(f"Error reading row {row_index} from {field_name}: {e}")
                    row.append(none_value)
        return row


class JSONWriter(OutputWriter):
    """Writer for JSON format"""
    
    def write_day(self, day_index: int, field_names: List[str], 
                  cache_handles: Dict[str, sqlite3.Connection], 
                  rows_per_day: int, output_path: str) -> None:
        """Write JSON data for a single day"""
        try:
            records = []
            
            for row_index in range(1, rows_per_day + 1):
                row = self._get_data_row(field_names, row_index, cache_handles)
                if row and row != [None]:
                    record = dict(zip(field_names, row))
                    records.append(record)
            
            with open(output_path, "w") as fp:
                json.dump(records, fp, indent=2)
                        
        except Exception as e:
            raise OutputError(f"Failed to write JSON file {output_path}: {e}")
    
    def get_file_extension(self) -> str:
        return "json"
    
    def _get_data_row(self, field_names: List[str], row_index: int, 
                      cache_handles: Dict[str, sqlite3.Connection], 
                      none_value: Any = None) -> List[Any]:
        """Get a single row of data from caches"""
        row = []
        for field_name in field_names:
            cache_conn = cache_handles.get(field_name)
            if cache_conn is None:
                row.append(none_value)
            else:
                try:
                    cursor = cache_conn.cursor()
                    cursor.execute("SELECT val FROM data WHERE rowid=?", (row_index,))
                    result = cursor.fetchone()
                    if result:
                        row.append(result[0])  # Keep original type for JSON
                    else:
                        row.append(none_value)
                except Exception as e:
                    logger.warning(f"Error reading row {row_index} from {field_name}: {e}")
                    row.append(none_value)
        return row


class JSONLinesWriter(OutputWriter):
    """Writer for JSON Lines format"""
    
    def write_day(self, day_index: int, field_names: List[str], 
                  cache_handles: Dict[str, sqlite3.Connection], 
                  rows_per_day: int, output_path: str) -> None:
        """Write JSON Lines data for a single day"""
        try:
            with open(output_path, "w") as fp:
                for row_index in range(1, rows_per_day + 1):
                    row = self._get_data_row(field_names, row_index, cache_handles)
                    if row and row != [None]:
                        record = dict(zip(field_names, row))
                        fp.write(json.dumps(record))
                        fp.write('\n')
                        
        except Exception as e:
            raise OutputError(f"Failed to write JSONL file {output_path}: {e}")
    
    def get_file_extension(self) -> str:
        return "jsonl"
    
    def _get_data_row(self, field_names: List[str], row_index: int, 
                      cache_handles: Dict[str, sqlite3.Connection], 
                      none_value: Any = None) -> List[Any]:
        """Get a single row of data from caches"""
        row = []
        for field_name in field_names:
            cache_conn = cache_handles.get(field_name)
            if cache_conn is None:
                row.append(none_value)
            else:
                try:
                    cursor = cache_conn.cursor()
                    cursor.execute("SELECT val FROM data WHERE rowid=?", (row_index,))
                    result = cursor.fetchone()
                    if result:
                        row.append(result[0])  # Keep original type for JSON
                    else:
                        row.append(none_value)
                except Exception as e:
                    logger.warning(f"Error reading row {row_index} from {field_name}: {e}")
                    row.append(none_value)
        return row


class OutputWriterFactory:
    """Factory for creating output writers"""
    
    _writers = {
        'csv': CSVWriter,
        'json': JSONWriter,
        'jsonl': JSONLinesWriter,
    }
    
    @classmethod
    def create_writer(cls, format_name: str) -> OutputWriter:
        """Create an output writer for the specified format"""
        if format_name not in cls._writers:
            raise OutputError(f"Unsupported output format: {format_name}")
        
        return cls._writers[format_name]()
    
    @classmethod
    def get_supported_formats(cls) -> List[str]:
        """Get list of supported output formats"""
        return list(cls._writers.keys()) 