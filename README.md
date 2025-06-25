# Improved Data Generator 2.0

A complete rewrite of the original `gen-data.py` with modern architecture, configuration files, and enhanced functionality for generating synthetic time-series data with configurable anomalies and transitions.

## 🚀 Key Improvements

### ✅ **Modular Architecture**
- Split into logical modules: `config.py`, `column_space.py`, `data_generator.py`, `output_writers.py`, `exceptions.py`
- Clean separation of concerns and responsibilities
- Extensible design for adding new features

### ✅ **Configuration Management**
- YAML and JSON configuration file support
- Command-line argument validation
- Default configurations with sensible defaults
- Type-safe configuration with validation

### ✅ **Better Performance & Reliability** 
- Proper multiprocessing with configurable workers
- Context managers for resource management
- Comprehensive error handling and logging
- Progress bars and metrics collection

### ✅ **Enhanced User Experience**
- Rich command-line interface with help and examples
- Data validation and quality checks
- Generation summaries and profiling
- Easy-to-use configuration examples

### ✅ **Code Quality**
- Full type hints throughout
- Comprehensive test suite
- Proper exception hierarchy
- PEP 8 compliant code

## 📋 Requirements

```bash
# Core requirements
numpy
pandas  # For data operations

# Optional but recommended
PyYAML  # For YAML configuration files
tqdm    # For progress bars
```

## 🏃 Quick Start

### Basic Usage

```bash
# Generate with defaults (10 days, 500 rows/day, CSV format)
python gen_data_new.py

# Generate more data with JSON output
python gen_data_new.py --rows 1000 --days 30 --format json

# Use configuration file
python gen_data_new.py --config example_config.yaml
```

### Configuration File Example

```yaml
# my_config.yaml
num_days: 15
approx_rows_per_day: 2000
output_dir: "my_data"
output_format: "csv"
max_workers: 4

columns:
  - name: "id"
    data_type: "INTEGER" 
    transition_percentage: 0.0
    transition_type: 0
    distribution_flags: 1  # INCREMENT
    
  - name: "sensor_reading"
    data_type: "FLOAT"
    transition_percentage: 0.4  # Values scale at 40% through timeline
    transition_type: 2  # VALUES_SCALE
    distribution_flags: 4  # NORMAL distribution
```

## 🔧 Command Line Options

```
python gen_data_new.py [options]

Configuration:
  -c, --config FILE          Configuration file (YAML or JSON)
  
Data Generation:
  -r, --rows INT            Approximate rows per day
  --days INT                Number of days to generate
  --format {csv,json,jsonl} Output format
  -o, --out DIR             Output directory
  
Performance:
  --workers INT             Number of worker processes
  --batch-size INT          Batch size for cache operations
  
Control:
  --log-level LEVEL         Logging level (DEBUG, INFO, WARNING, ERROR)
  --no-progress             Disable progress bars
  --no-cleanup              Don't clean up cache files
  --validate                Validate generated data
  
Utilities:
  --clean-cache             Clean up cache files and exit
  --example-config          Print example configuration
  --version                 Show version information
```

## 📊 Output Formats

### CSV Format
```csv
id,uniform_numbers,sensor_reading
0,0.123,45.67
1,0.456,48.23
```

### JSON Format
```json
[
  {"id": 0, "uniform_numbers": 0.123, "sensor_reading": 45.67},
  {"id": 1, "uniform_numbers": 0.456, "sensor_reading": 48.23}
]
```

### JSON Lines Format
```jsonl
{"id": 0, "uniform_numbers": 0.123, "sensor_reading": 45.67}
{"id": 1, "uniform_numbers": 0.456, "sensor_reading": 48.23}
```

## 🎛️ Data Types & Transitions

### Data Types
- `INTEGER` - Integer values
- `FLOAT` - Floating point values  
- `STRING_LONG` - Long descriptive strings
- `STRING_CATEGORY` - Short categorical strings

### Distribution Types
- `INCREMENT` (1) - Sequential incrementing values
- `UNIFORM` (2) - Uniform random distribution
- `NORMAL` (4) - Normal (Gaussian) distribution

### Transition Types
- `VALUES_SCALE` (2) - Scale values by factor
- `VALUES_ALL_STRINGS` (4) - Convert all values to strings
- `VALUES_SOME_STRINGS` (8) - Convert some values to strings
- `SCHEMA_NAME` (16) - Change field names
- `STRING_LONG_TO_SMALL` (32) - Long strings become short
- `STRING_SMALL_TO_LONG` (64) - Short strings become long
- `NULLS_HIGH` (128) - Increase null rate
- `ZEROS_HIGH` (256) - Increase zero rate

## 🏗️ Architecture

```
gen_data_new.py          # Main CLI entry point
├── config.py            # Configuration classes and defaults
├── data_generator.py    # Main orchestration class
├── column_space.py      # Individual column data generation
├── output_writers.py    # Pluggable output format writers
├── exceptions.py        # Custom exception hierarchy
└── GenWords.py          # Word generation utilities
```

## 🧪 Testing

```bash
# Run the test suite
python test_generator.py

# Just run integration test
python -c "from test_generator import run_integration_test; run_integration_test()"
```

## 📈 Performance & Monitoring

### Metrics Collected
- Total rows generated
- Generation time
- Cache operations
- Files written
- Errors encountered

### Generation Profile
The system automatically creates a `generation_profile.json` file with:
- Complete configuration used
- Performance metrics
- Rows per day breakdown
- Generation timestamp

### Example Profile
```json
{
  "config": {
    "num_days": 10,
    "approx_rows_per_day": 1000,
    "output_format": "csv"
  },
  "metrics": {
    "total_rows_generated": 9847,
    "generation_time_seconds": 12.34,
    "files_written": 10,
    "errors_encountered": 0
  },
  "rows_per_day": {
    "0": 1023,
    "1": 987,
    "2": 1105
  }
}
```

## 🔌 Extensibility

### Adding New Output Formats

```python
# In output_writers.py
class XMLWriter(OutputWriter):
    def write_day(self, day_index, field_names, cache_handles, rows_per_day, output_path):
        # Implementation here
        pass
        
    def get_file_extension(self):
        return "xml"

# Register in factory
OutputWriterFactory._writers['xml'] = XMLWriter
```

### Adding New Data Types

```python
# In config.py
class DataType(IntEnum):
    INTEGER = 0
    FLOAT = 1
    STRING_LONG = 2
    STRING_CATEGORY = 3
    DATETIME = 4  # New type
```

## 🐛 Troubleshooting

### Common Issues

**Import Errors**
```bash
# Make sure all modules are in the same directory
ls -la *.py
```

**Memory Issues with Large Datasets**
```yaml
# Reduce batch size in config
cache_batch_size: 1000
max_workers: 1
```

**Progress Bar Not Showing**
```bash
# Install tqdm for progress bars
pip install tqdm
```

### Debug Mode
```bash
python gen_data_new.py --log-level DEBUG --workers 1
```

## 📚 Examples

### Minimal Configuration
```yaml
num_days: 5
approx_rows_per_day: 100
columns:
  - name: "id"
    data_type: "INTEGER"
    distribution_flags: 1
```

### Complex Transitions
```yaml
num_days: 20
approx_rows_per_day: 5000
columns:
  - name: "measurements"
    data_type: "FLOAT" 
    transition_percentage: 0.3  # Transitions at 30% through
    transition_type: 10  # SCALE + SOME_STRINGS (2 + 8)
    distribution_flags: 4  # NORMAL
```

### Performance Optimized
```yaml
num_days: 100
approx_rows_per_day: 50000
max_workers: 8
cache_batch_size: 50000
progress_bars: true
cleanup_cache: true
```

## 🤝 Contributing

1. Follow PEP 8 style guidelines
2. Add type hints to all new functions
3. Include tests for new functionality
4. Update documentation for new features

## 📄 License

Same as original - Copyright (c) 2020-2023 Data Culpa, Inc.

---

This improved generator maintains full compatibility with the Data Culpa demonstration pipeline while providing a much more maintainable and extensible codebase. 