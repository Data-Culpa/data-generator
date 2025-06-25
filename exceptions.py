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

Custom exceptions for the data generator.
"""


class DataGenerationError(Exception):
    """Base exception for data generation errors"""
    pass


class ConfigurationError(DataGenerationError):
    """Raised when there's an issue with configuration"""
    pass


class CacheError(DataGenerationError):
    """Raised when there's an issue with cache operations"""
    pass


class OutputError(DataGenerationError):
    """Raised when there's an issue with output operations"""
    pass


class ValidationError(DataGenerationError):
    """Raised when data validation fails"""
    pass 