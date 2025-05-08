# Looker View Analysis Tool Architecture

This document describes in detail the code architecture, component functionality, and processing flow of the Looker View Analysis Tool. The tool is used to analyze Looker project structure, extract mappings between views and actual database tables, and generate export commands needed for data migration.

## 1. File Structure

```
.
├── main.py                  # Main script entry point
└── looker_utils/            # Tool module directory
    ├── __init__.py          # Package initialization file
    ├── analyzers.py         # Functions for analyzing explore/view relationships
    ├── constants.py         # Global constant definitions
    ├── data_loaders.py      # Data loading related functions
    ├── extractors.py        # Table name and view information extraction functionality
    ├── reporters.py         # Report and export command generation functionality
    └── utils.py             # Common utility functions
```

## 2. Main Module Functions

### 2.1 main.py

The main script is the entry point for the entire tool, responsible for coordinating various modules to complete view usage analysis. Main functions:

- Parse command line arguments, including the new include_source_info parameter
- Configure global project settings (default project name, dataset name, etc.)
- Manage working directory and output directory
- Check Looker project directory structure, including standard and non-standard locations for views and model files
- Load Explore usage frequency data (if provided)
- Call functions from various modules to complete the entire analysis process
- Generate reports and export commands (when GCS bucket parameter is provided)

### 2.2 looker_utils/constants.py

Defines global constants used by the tool, including:

- `DEFAULT_PROJECT`: Default BigQuery project name (default value: 'curated-dwh')
- `SNAPSHOT_PROJECT`: Snapshot table project name (default value: 'curated-dwh-snapshot')

These values can be overridden via command line arguments.

### 2.3 looker_utils/utils.py

Provides common utility functions used throughout the tool, main functionalities:

- Set global project configuration (project name, dataset name, etc.)
- Process Liquid conditional blocks to extract table names
- Extract table names from SQL statements
- Identify views based on explore_source
- Automatically detect related tables (streaming tables, partitioned tables, etc.)

Main functional functions include:
- `set_global_project_settings()`: Set global project configuration
- `extract_tables_from_liquid_block()`: Extract table names from Liquid conditional blocks, supporting extraction of table references from all condition branches
- `extract_tables_from_sql()`: Extract table names from SQL statements, supporting various SQL table reference forms and aliases
- `contains_explore_source()`: Check if content contains an explore_source definition
- `auto_detect_related_tables()`: Automatically detect related tables

### 2.4 looker_utils/data_loaders.py

Responsible for loading data and extracting view information, main functionalities:

- Load Explore usage data
- Extract all views from the project, including views from standard and non-standard locations
- Identify view alias relationships

Main functions:
- `load_explore_usage()`: Load Explore usage frequency data from CSV file
- `extract_all_views()`: Scan Looker project structure and extract all views, while recording view types and view file paths

### 2.5 looker_utils/extractors.py

Responsible for extracting information such as table names from view definitions, main functionalities:

- Extract table names from single view content, handling reference styles for different databases
- Process multiple scenarios including SQL table-based views, derived tables, SQL queries, etc.
- Identify similarities between table names and view names to optimize table name selection

Key functions:
- `extract_tables_from_view_content()`: Extract table names from single view content, supporting processing of SQL table-based views and derived tables
- `extract_actual_table_names()`: Extract actual table names from all view definition files

### 2.6 looker_utils/analyzers.py

Analyze relationships between Explores and views, main functionalities:

- Analyze relationships between Explores and views
- Identify views created through UNNEST
- Process view aliases
- Extract and normalize view data source definitions
- Update table information in the view list
- Calculate actual usage frequency

Main functions:
- `analyze_explores()`: Analyze relationships between Explores and views
- `update_view_table_info()`: Update table information in the view list
- `calculate_actual_usage()`: Calculate actual usage frequency of views
- `analyze_explores_and_extract_tables()`: Unified entry function for analyzing explore relationships and extracting table information
- `extract_view_source_definitions()`: Extract original data source definitions of views
- `normalize_source_definitions()`: Normalize source information definitions, handling syntax differences between different database dialects
- `extract_tables_from_views()`: Extract table names from all view definition files
- `guess_table_info()`: Guess table information based on view name and known information

### 2.7 looker_utils/reporters.py

Generate reports and export commands, main functionalities:

- Generate view analysis reports
- Generate export commands
- Provide view usage statistics
- Support including view source definition information

Main functions:
- `generate_report()`: Generate view analysis report, including view name, explore count, calculated usage frequency, table name, citation type, etc.
- `generate_export_commands()`: Generate export commands, supporting generation of export commands for all tables and active tables
- `generate_view_usage_report()`: Generate view usage report
- `filter_views_by_usage()`: Filter views by usage

## 3. Processing Flow

The processing flow of the entire tool is as follows:

### 3.1 Initialization and Configuration

1. Parse command line arguments (`main.py`)
2. Set global project configuration (`main.py`, `utils.py` in `set_global_project_settings()`)
3. Determine output directory (`main.py`)
4. If Looker path is specified, change to that directory and analyze directory structure (`main.py`)
   - Identify standard and non-standard locations for views and model files
   - Output project structure information

### 3.2 Data Extraction

1. Load Explore usage frequency data (if provided) (`data_loaders.py` in `load_explore_usage()` function)
2. Extract all views (`data_loaders.py` in `extract_all_views()` function)
   - Scan all possible directories for view files
   - Extract view names and file paths

### 3.3 Relationship Analysis and Table Information Extraction

1. Analyze relationships between Explores and views (`analyzers.py` in `analyze_explores()` function, called via `analyze_explores_and_extract_tables()`)
   - Identify reference relationships between views
   - Identify views created through UNNEST
   - Process view aliases and FROM clauses
   - Record which model each explore belongs to

2. Extract original data source definitions of views (`analyzers.py` in `extract_view_source_definitions()` function, called via `analyze_explores_and_extract_tables()`)
   - First remove all comment lines starting with `#` to avoid extracting commented code
   - Extract original sql_table_name or derived_table definition for each view
   - Save data source definitions to view information

3. Normalize source information definitions (`analyzers.py` in `normalize_source_definitions()` function)
   - Handle syntax differences between different database dialects
   - Remove all double quotes to standardize source definition format
   - Retain original definition while adding normalized definition for subsequent table name extraction

4. Extract table information from view data source definitions (`analyzers.py` in `extract_tables_from_views()` function, called via `analyze_explores_and_extract_tables()`)
   - Process SQL table-based views (`sql_table_name: thelook.users;;`)
   - Process SQL query derived tables (`derived_table: { sql: SELECT ... }`)
   - Process native derived tables based on explore_source
   - Process join definitions using sql_on
   - Process join definitions using sql parameter
   - Process nested data joins using UNNEST
   - Process table references in Liquid conditional blocks

5. Process table references in Liquid conditional blocks and SQL text
   - `extract_tables_from_liquid_block()` function extracts table references from all condition branches, regardless of whether the condition is met
   - `extract_tables_from_sql()` function supports multiple table reference forms and alias handling

### 3.4 Table Information Update

1. Update table information in the view list (`analyzers.py` in `update_view_table_info()` function)
   - Handle different reference methods (quote differences, aliases, etc.)
   - Apply default project prefix (if needed)
   - Mark special view types (unnest, derived_explore, etc.)

2. Process default project prefixes and special table types, including:
   - Process three-part table references (BigQuery: project.dataset.table / Snowflake: database.schema.table)
   - Process alias views and UNNEST views
   - Process streaming tables and partitioned tables

### 3.5 Usage Frequency Calculation

1. Calculate actual usage frequency of views based on Explore usage frequency (`analyzers.py` in `calculate_actual_usage()` function)
2. If usage frequency data is not provided, set calculated usage frequency of all views to NULL (`main.py` in main process)

### 3.6 Report Generation

1. Generate view analysis report (`reporters.py` in `generate_report()` function)
   - Organize view information, including view name, usage frequency, table name, citation type, etc.
   - Process additional table references, ensuring only valid complete three-part table names are included
   - Selectively include source definition information (based on include_source_info parameter)
2. Save results to CSV file

### 3.7 Export Command Generation (Optional)

1. If GCS bucket name is provided, generate export commands (`reporters.py` in `generate_export_commands()` function)
   - Generate export commands for all tables
   - If Explore usage frequency data is provided, also generate export commands for only active tables
2. Save export commands to file

## 4. Database Compatibility

The tool is primarily designed for BigQuery backend, but also considers other database systems such as Snowflake. The differences between the two main database systems in terms of table references:

### 4.1 BigQuery Table Reference Format

```
project.dataset.table
```

- **project**: Project ID, similar to a database collection
- **dataset**: Dataset, similar to schema
- **table**: Table name

### 4.2 Snowflake Table Reference Format

```
database.schema.table
```

- **database**: Database name
- **schema**: Schema name
- **table**: Table name

The tool handles these differences through:

1. Normalizing reference formats when extracting table names, removing double quotes
2. Identifying complete three-part references, regardless of which quote format is used
3. Handling syntax differences between different database dialects during source definition normalization

## 5. Usage Examples

Basic usage:
```bash
python main.py --looker_path /path/to/looker/project
```

Advanced usage (analyze Explore usage and generate export commands):
```bash
python main.py --looker_path /path/to/looker/project \
               --explore_usage_file explore_usage.csv \
               --output_dir ./output \
               --export_gs_bucket your-gcs-bucket-name \
               --default_project custom-project \
               --default_dataset custom_dataset
```

Usage including source definition information:
```bash
python main.py --looker_path /path/to/looker/project \
               --include_source_info
```

## 6. Output Files

- `view_analysis.csv`: CSV file containing all view-to-table mapping information, optionally including source definition information
- `export_command.txt`: Export commands for all tables
- `export_command_active.txt`: Export commands for only tables with usage frequency greater than 0 (only generated when Explore usage frequency data is provided)

## 7. Known Issues and Limitations

1. **Table Name Parsing Issues**: Different database systems (such as BigQuery and Snowflake) have different table reference formats, which may lead to table name parsing errors
2. **Quote Handling**: The tool attempts to handle different quoting styles (backticks, double quotes, etc.), but complex quote nesting may lead to parsing errors
3. **Liquid Conditional Block Processing**: The tool extracts table references from all condition branches, regardless of whether the condition is met, which may cause some tables that are not actually used to be identified as dependencies 