# Looker View Analysis Tool Architecture

This document describes in detail the code architecture, component functionality, and processing flow of the Looker View Analysis Tool. The tool is used to analyze Looker project structure, extract mappings between views and actual database tables, and generate export commands needed for data migration.

## 1. File Structure

```
.
├── main.py                  # Main script entry point
└── looker_utils/            # Tool module directory
    ├── __init__.py          # Package initialization file
    ├── analyzers.py         # Functions for analyzing explore/view relationships and table extraction logic
    ├── constants.py         # Global constant definitions (e.g., default project names)
    ├── data_loaders.py      # Data loading (e.g., explore usage) and initial view extraction
    ├── extractors.py        # Detailed table name and view information extraction from view content
    ├── reporters.py         # Report and export command generation functionality
    └── utils.py             # Common utility functions (e.g., SQL parsing, Liquid block processing)
```

## 2. Main Module Functions

### 2.1 main.py

The main script is the entry point for the entire tool, responsible for orchestrating various modules to complete the Looker view usage analysis. Its main functions include:

- **Argument Parsing**: Parses command-line arguments using `argparse` to configure tool behavior. This includes parameters like `--looker_path`, `--model`, `--explore_usage_file`, `--default_project`, `--default_dataset`, `--snapshot_project`, `--snapshot_dataset`, `--output_dir`, and `--include_source_info`.
- **Global Configuration**:
    - Updates global constants (from `looker_utils.constants`) with values provided via command-line arguments (e.g., `constants.DEFAULT_PROJECT`).
    - Calls `set_global_project_settings()` (from `looker_utils.utils`) to establish global project and dataset names used throughout the analysis.
- **Working Directory Management**:
    - Captures the original current working directory (`original_cwd`).
    - Changes the current working directory to the `--looker_path` if provided, allowing the tool to operate relative to the Looker project's root.
    - Resolves the `output_base_dir` for output files, prioritizing user-specified `--output_dir` or defaulting to the `original_cwd`.
- **Project Structure Analysis**:
    - Scans the Looker project for view and model files in standard (`views/`, `models/`) and non-standard locations using `glob`.
    - Prints a summary of found files to inform the user.
- **Data Loading**:
    - Calls `load_explore_usage()` (from `looker_utils.data_loaders`) if `--explore_usage_file` is provided.
    - Calls `extract_all_views()` (from `looker_utils.data_loaders`) to get an initial list of all views and their file paths.
- **Core Analysis Orchestration**:
    - Calls `analyze_explores_and_extract_tables()` (from `looker_utils.analyzers`) to perform the main analysis of explore-view relationships and extract table information.
    - Calls `update_view_table_info()` (from `looker_utils.analyzers`) to refine and consolidate table information for each view.
    - Calls `calculate_actual_usage()` (from `looker_utils.analyzers`) if explore usage data is available.
- **Reporting**:
    - Calls `generate_report()` (from `looker_utils.reporters`) to create the main CSV analysis report.
    - Calls `generate_export_commands()` (from `looker_utils.reporters`) if `--export_gs_bucket` is specified, to create files with export commands.

### 2.2 looker_utils/constants.py

Defines global constants used by the tool. These constants provide default values that can be overridden by command-line arguments passed to `main.py`.

- `DEFAULT_PROJECT`: Default BigQuery project name (e.g., 'company-dwh'). Overridden by `--default_project` argument.
- `SNAPSHOT_PROJECT`: Snapshot table project name (e.g., 'company-dwh-snapshot'). Overridden by `--snapshot_project` argument.
- Note: `DEFAULT_DATASET` and `SNAPSHOT_DATASET` are primarily managed via `set_global_project_settings` in `utils.py` based on arguments to `main.py`.

### 2.3 looker_utils/utils.py

Provides common utility functions used throughout the tool. Key functionalities and their main implementing functions include:

- **Global Project Configuration**:
  - `set_global_project_settings()`: Sets global variables for default and snapshot project/dataset names (e.g., `DEFAULT_PROJECT`, `DEFAULT_DATASET`, `SNAPSHOT_PROJECT`, `SNAPSHOT_DATASET`). These are typically initialized from command-line arguments in `main.py`.
- **Table Name Extraction from SQL/Liquid**:
  - `extract_tables_from_liquid_block()`: Extracts table names from LookML Liquid conditional blocks (e.g., `{% if ... %}`). It aims to extract table references from all condition branches, regardless of runtime evaluation. It preprocesses content by removing double quotes to normalize formats.
  - `extract_tables_from_sql()`: Extracts table names from SQL strings. It handles various SQL table reference forms (including those with aliases, in `FROM`/`JOIN` clauses, `WITH` statements, and `UNNEST` operations). It also identifies and can normalize common table name variations like streaming (`_streaming`) or daily partitioned (`_YYYYMMDD`) tables. Preprocessing includes removing SQL comments and normalizing quotes (replacing `"` with empty string).
- **View Definition Analysis Utilities**:
  - `contains_explore_source()`: Checks if a view's content block (typically from `derived_table`) defines an `explore_source`, which indicates a native derived table referencing another explore.

### 2.4 looker_utils/data_loaders.py

Responsible for loading initial data and performing a first pass at extracting view definitions.

Main functions:
- `load_explore_usage()`: Loads Explore usage frequency data from a specified CSV file (`--explore_usage_file`). If the file is not provided or an error occurs, it returns an empty dictionary, and downstream calculations will reflect NULL usage.
- `extract_all_views()`: Scans the Looker project structure (standard and non-standard directories identified in `main.py`) for `.view.lkml` files and `.model.lkml` (or general `.lkml`) files. It extracts view names and their file paths, and also identifies basic view alias relationships defined with `from:` at the explore or join level. Initializes a preliminary `view_list` dictionary.

### 2.5 looker_utils/extractors.py

Responsible for detailed extraction of table names and citation types from individual view definitions. This module dives deep into the content of each view file.

Key functions:
- `extract_tables_from_view_content()`: This is a core function that takes the content of a single view and attempts to identify the underlying database tables.
    - It first checks for `explore_source` definitions (using `utils.contains_explore_source()`), returning a `derived_explore` citation type if found.
    - It parses `sql_table_name` directives, attempting to match various quoting and structural patterns for table identifiers (e.g., ``` `project.dataset.table` ```, `project.dataset.table`, ``` `project`.`dataset`.`table` ```).
    - For `derived_table` blocks, it extracts the SQL content. It then uses `utils.extract_tables_from_liquid_block()` and `utils.extract_tables_from_sql()` to find table references within the SQL.
    - It includes logic to select the most relevant primary table if multiple tables are found (e.g., based on similarity to the view name).
    - Returns a list of extracted table names and a `citation_type` (e.g., 'native', 'derived_explore').
- `extract_actual_table_names()`: Iterates through all view files, reads their content, and calls `extract_tables_from_view_content()` for each. It aggregates the results into dictionaries mapping view names to their extracted table names and citation types.

### 2.6 looker_utils/analyzers.py

This module focuses on analyzing the relationships between Explores and Views, resolving aliases, processing different view types, and consolidating the extracted table information.

Main functions:
- `analyze_explores()`:
    - Parses model and view files to identify all `explore` definitions and the views they join.
    - Determines `explore_to_views` (mapping explores to the set of views they use) and `explore_to_model` (mapping explores to their parent model).
    - Identifies views created via `UNNEST` operations within join `sql` blocks (if they don't have an explicit `sql_table_name` or `derived_table`).
    - Records view alias relationships (`view_from_alias`) from `from:` clauses in explores and joins.
- `extract_view_source_definitions()`: Reads each view file and extracts the raw source definition block (e.g., the content of `sql_table_name` or `derived_table:{...}`). It preprocesses by removing comment lines starting with `#`.
- `normalize_source_definitions()`: Takes the raw source definitions and normalizes them, primarily by removing double quotes, to aid in consistent parsing later.
- `extract_tables_from_views()`: This function orchestrates the extraction of table names from the *normalized* source definitions. It calls `extractors.extract_tables_from_view_content()` on these definitions. (Note: The primary table extraction from original file content is often done via `extractors.extract_actual_table_names()` which is called within `analyze_explores_and_extract_tables`; this function might handle a second pass or specific cases based on normalized definitions). *Self-correction: The main call path seems to be `analyze_explores_and_extract_tables` which calls `extract_actual_table_names` from `extractors.py` directly for table names, and then `extract_view_source_definitions` and `normalize_source_definitions`. The role of a separate `extract_tables_from_views` here needs to be clarified or reconciled with the flow described in section 3.*
- `update_view_table_info()`: Consolidates information into the main `view_list`. It takes results from table extraction (`actual_table_names`), unnest view identification, citation types, aliases, and source definitions. It applies default project/dataset prefixes where appropriate and standardizes table name information.
- `calculate_actual_usage()`: Calculates the usage frequency for each view by summing the usage of Explores that reference it, based on the loaded `explore_usage` data.
- `analyze_explores_and_extract_tables()`: A wrapper function that calls `analyze_explores()`, `extract_view_source_definitions()`, `normalize_source_definitions()`, and `extractors.extract_actual_table_names()` to gather all necessary information for the subsequent update and reporting steps. This is a key orchestrator in the analysis phase.
- `guess_table_info()`: A utility function (potentially used within `update_view_table_info` or reporting) to infer table information if direct extraction fails, for example, for UNNEST views.

### 2.7 looker_utils/reporters.py

This module is responsible for generating the final output files based on the analysis results.

Main functions:
- `generate_report()`:
    - Generates the main CSV analysis report (`view_analysis.csv`).
    - Columns include view name, explore count, calculated usage, primary table name, citation type, and additional tables.
    - Optionally includes view `source_type` and `source_definition` if the `--include_source_info` flag is set.
    - Sorts views based on usage (if available) and explore count.
    - Cleans and formats `additional_tables` to include only valid, unique, three-part table names.
    - Prints top N used views and citation type statistics to the console.
- `generate_export_commands()`:
    - Generates text files with export commands (e.g., for BigQuery `bq extract`).
    - Creates `export_command.txt` for all tables associated with views.
    - Creates `export_command_active.txt` for tables associated with views that have a calculated usage greater than 0 (only if explore usage data was provided).
    - Commands are formatted to export data to a specified GCS bucket (`--export_gs_bucket`).
- `generate_view_usage_report()`: Appears to be a separate, potentially more detailed view usage report generator. (Based on function signature, its direct call from `main.py` is not apparent, may be for specific use cases or older functionality).
- `filter_views_by_usage()`: A utility function that might be used to filter views based on their activity. (Similar to above, its direct call from `main.py` in the main workflow isn't obvious).

## 3. Processing Flow

The processing flow of the entire tool is as follows:

### 3.1 Initialization and Configuration

1. **Parse Command Line Arguments** (`main.py`):
    The tool starts by parsing command-line arguments (e.g., `--looker_path`, `--explore_usage_file`, `--default_project`) using the `argparse` module. These arguments control the tool's execution path and parameters.
2. **Set Global Project Configuration** (`main.py`, `looker_utils/utils.py`):
    - Command-line arguments for project and dataset names (e.g., `--default_project`, `--default_dataset`) are used to update global constants in `looker_utils.constants` and to call `set_global_project_settings()` from `looker_utils.utils`. This establishes a consistent naming convention for database entities throughout the analysis.
3. **Determine Output and Working Directories** (`main.py`):
    - The output directory for reports is determined (defaulting to the original working directory or as specified by `--output_dir`).
    - If a `--looker_path` is provided, the tool changes its current working directory to this path. This allows file operations (like `glob`) to be relative to the Looker project root.
4. **Analyze Project Directory Structure** (`main.py`):
    - The tool scans the specified Looker project path for `.view.lkml` and model files (`.model.lkml` or `.lkml`).
    - It identifies files in standard locations (e.g., `views/`, `models/`) as well as non-standard locations (e.g., views or models in the project root or other subdirectories).
    - A summary of the found file structure is printed to the console.

### 3.2 Data Extraction

1. **Load Explore Usage Frequency Data** (if provided) (`main.py` calling `looker_utils.data_loaders.load_explore_usage()`):
    - If the `--explore_usage_file` argument is provided, this function is called to read a CSV file containing data on how frequently each Explore is used. This data is used later to calculate view usage.
    - If the file is not found or is invalid, an empty dictionary is returned, and usage will be marked as NULL.
2. **Extract All Views (Initial Pass)** (`main.py` calling `looker_utils.data_loaders.extract_all_views()`):
    - This function performs an initial scan of all located `.view.lkml` and model files.
    - It extracts all declared view names and records their corresponding file paths.
    - It also performs a basic identification of view aliases (views defined using `from:` at the explore or join level).
    - The output is a preliminary `view_list` (a dictionary with basic info) and `view_to_file` (mapping view names to paths).

### 3.3 Relationship Analysis and Table Information Extraction

This phase is primarily orchestrated by `analyze_explores_and_extract_tables()` in `looker_utils/analyzers.py`, which calls several other functions in `analyzers.py` and `extractors.py`.

1. **Analyze Explore-View Relationships** (`looker_utils.analyzers.analyze_explores()`):
    - Parses model and view files to identify all `explore` definitions and the views they join.
    - Determines `explore_to_views` (mapping explores to the set of views they use) and `explore_to_model` (mapping explores to their parent model).
    - Identifies views created via `UNNEST` operations within join `sql` blocks (if they don't have an explicit `sql_table_name` or `derived_table`).
    - Records view alias relationships (`view_from_alias`) from `from:` clauses in explores and joins.

2. **Extract Raw View Source Definitions** (`looker_utils.analyzers.extract_view_source_definitions()`):
    - For each view identified, this function reads its corresponding `.view.lkml` file.
    - It removes comment lines (those starting with `#`).
    - It then extracts the raw text block defining the view's data source, which is typically the content of `sql_table_name: ... ;;` or the entire `derived_table: { ... }` block.
    - These raw source definitions are stored per view, primarily for inclusion in the output report if `--include_source_info` is enabled.

3. **Normalize Raw View Source Definitions** (`looker_utils.analyzers.normalize_source_definitions()`):
    - Takes the raw source definitions extracted in the previous step.
    - Performs normalization, primarily by removing all double quotes (`"`). This helps in standardizing the definitions for more consistent processing or display, especially when dealing with different SQL dialects or quoting styles.

4. **Extract Table Names and Citation Types from View Content** (`looker_utils.extractors.extract_actual_table_names()`, which calls `looker_utils.extractors.extract_tables_from_view_content()` for each view):
    - This is the primary step for determining the underlying database table(s) for each view.
    - `extract_tables_from_view_content()` processes the *original, uncommented* content of each view file:
        - **Native Derived Tables**: Checks for `explore_source:` definitions (using `utils.contains_explore_source()`). If found, the view is typically classified with `citation_type = 'derived_explore'`, and no direct table name is extracted at this stage.
        - **Direct Table Views**: Parses `sql_table_name: ... ;;` directives. It uses regular expressions to match various common table identifier patterns (e.g., ``` `project.dataset.table` ```, `project.dataset.table`, with or without backticks, and handling Snowflake-style double quotes by prior normalization or flexible regex).
        - **SQL Derived Tables**: For `derived_table: { ... }` blocks:
            - It extracts the SQL query from the `sql: ... ;;` parameter.
            - **Liquid Templating**: It first calls `utils.extract_tables_from_liquid_block()` to find any table names referenced within Liquid templating constructs (e.g., `{% if ... %}`). This attempts to capture all potential tables from all branches of the Liquid logic.
            - **SQL Parsing**: Then, it calls `utils.extract_tables_from_sql()` to parse the (potentially Liquid-processed) SQL text and extract table names from `FROM`, `JOIN`, and other clauses. This function handles various SQL syntaxes and also normalizes table names (e.g., stripping `_streaming` suffixes).
        - Returns a list of extracted table names and an initial `citation_type` (e.g., 'native', 'derived_explore'). It also has heuristics to select a primary table if multiple are found.
    - `extract_actual_table_names()` aggregates these results for all views.

### 3.4 Table Information Update

1. **Consolidate and Refine View Information** (`looker_utils.analyzers.update_view_table_info()`):
    - This crucial function takes all the information gathered so far: the initial `view_list`, the `actual_table_names` and `view_citation_types` from `extractors.py`, the `unnest_views` set, `view_from_alias` map, and the (potentially normalized) `view_source_definitions`.
    - It iterates through each view and updates its entry in the `view_list`.
    - **Table Name Finalization**: It assigns the primary table name and any additional table names. It applies default project/dataset prefixes (from global settings like `DEFAULT_PROJECT`, `DEFAULT_DATASET`) to table names that are not already fully qualified, carefully considering BigQuery vs. Snowflake three-part naming conventions (project.dataset.table vs. database.schema.table) based on the structure of the identified table name.
    - **Citation Type Refinement**: It finalizes the `citation_type` (e.g., 'native', 'derived', 'unnest', 'derived_explore', 'derived_from' for aliased views).
    - **Source Information**: If `--include_source_info` is used, the (normalized) source type and definition are also added to the view's record.
    - Special handling for UNNEST views (often have no direct table name) and derived explores.

### 3.5 Usage Frequency Calculation

1. **Calculate Actual View Usage** (`looker_utils.analyzers.calculate_actual_usage()`):
    - This function is called if Explore usage frequency data was successfully loaded (`explore_usage` is populated).
    - It iterates through the `explore_to_views` map (from `analyze_explores()`). For each explore with a known usage count, it distributes that count to all views participating in that explore.
    - The result is an `actual_usage` dictionary mapping each view name to its calculated total usage score.
2. **Handle Missing Usage Data** (`main.py`):
    - If Explore usage data was not provided, `main.py` initializes `actual_usage` so that every view has a usage value of `None` (which will be represented as NULL or empty in the report).

### 3.6 Report Generation

1. **Generate View Analysis CSV Report** (`looker_utils.reporters.generate_report()`):
    - Takes the finalized `view_list`, `actual_usage` data, `unnest_views`, `actual_table_names` (for additional tables), and `explore_to_views` (for explore counts).
    - Sorts the views for reporting (typically by usage frequency if available, then by explore count).
    - Generates a CSV file (e.g., `view_analysis.csv`) with columns like: `view_name`, `explore_count`, `calculated_usage`, `table_name`, `citation_type`, `additional_tables`.
    - If `--include_source_info` is true, `source_type` and `source_definition` columns are added.
    - It performs final cleaning of `additional_tables` to ensure only valid, unique, three-part table names are included.
    - Prints summary statistics (e.g., top 20 views, citation type counts) to the console.

### 3.7 Export Command Generation (Optional)

1. **Generate Table Export Commands** (`looker_utils.reporters.generate_export_commands()`):
    - This function is called if the `--export_gs_bucket` command-line argument is provided.
    - It iterates through the sorted views and their associated table names.
    - Generates export commands (e.g., BigQuery `bq extract ... gs://<bucket-name>/...`) for these tables.
    - Creates `export_command.txt` containing commands for all identified tables.
    - If Explore usage data was available, it also creates `export_command_active.txt` containing commands only for tables associated with views that have a `calculated_usage > 0`.
    - The commands are saved to text files in the specified output directory.

## 4. Database Compatibility

The tool is primarily designed with Google BigQuery as the backend in mind, but it incorporates several mechanisms to improve compatibility with other SQL-based database systems like Snowflake, particularly concerning how table names are referenced in LookML.

### 4.1 BigQuery Table Reference Format

BigQuery typically uses a three-part reference: `project.dataset.table`.

- **project**: The Google Cloud Project ID.
- **dataset**: The BigQuery dataset (analogous to a schema).
- **table**: The table name.

Example in LookML `sql_table_name`:
```lookml
sql_table_name: `my-project.my_dataset.my_table` ;;
```
Or without backticks:
```lookml
sql_table_name: my-project.my_dataset.my_table ;;
```

### 4.2 Snowflake Table Reference Format

Snowflake also uses a three-part reference: `database.schema.table`.

- **database**: The Snowflake database name.
- **schema**: The schema within the database.
- **table**: The table name.

Example in LookML `sql_table_name` (often uses double quotes):
```lookml
sql_table_name: "MY_DATABASE"."MY_SCHEMA"."MY_TABLE" ;;
```
Or without quotes if identifiers are case-insensitive and standard:
```lookml
sql_table_name: MY_DATABASE.MY_SCHEMA.MY_TABLE ;;
```

### 4.3 How the Tool Handles Differences

The tool employs several strategies to accommodate these variations:

1.  **Flexible Regex for Table Name Extraction**:
    - In `looker_utils.extractors.extract_tables_from_view_content()` (for `sql_table_name` and SQL within `derived_table`):
        - Regular expressions are designed to capture table names regardless of whether they are enclosed in BigQuery-style backticks (\`\`) or Snowflake-style double quotes (`"`). The removal of double quotes during pre-processing (see below) aids this.
        - It attempts to identify one, two, or three-part table names.
    - In `looker_utils.utils.extract_tables_from_sql()`:
        - Similarly, regex patterns for `FROM` and `JOIN` clauses are written to be flexible regarding quoting.

2.  **Normalization by Removing Double Quotes**:
    - `looker_utils.analyzers.normalize_source_definitions()`: When extracting raw source definitions for potential inclusion in reports (with `--include_source_info`), this function removes double quotes (`"`) from the source string. This standardizes the appearance and can simplify subsequent parsing if these normalized strings were to be re-parsed (though primary table extraction works on original/lightly preprocessed content).
    - `looker_utils.utils.extract_tables_from_sql()` and `looker_utils.utils.extract_tables_from_liquid_block()` also remove double quotes from the input SQL/Liquid content before attempting to extract table names. This helps treat `"schema"."table"` similarly to `schema.table` for regex matching.

3.  **Intelligent Prefixing and Qualification**:
    - In `looker_utils.analyzers.update_view_table_info()`: When finalizing table names, if a table name is not fully qualified (e.g., only `dataset.table` or just `table`), the tool attempts to prefix it using the `DEFAULT_PROJECT` and `DEFAULT_DATASET` (or `SNAPSHOT_PROJECT`, `SNAPSHOT_DATASET`) values obtained from command-line arguments or defaults.
    - The logic for adding prefixes tries to be mindful of whether it's dealing with a 2-part (e.g. `dataset.table`, common if project is implicit) or 1-part name to correctly construct the assumed 3-part BigQuery full name.
    - While the default prefixing logic is geared towards BigQuery's `project.dataset.table`, the initial extraction tries to capture the full 3-part name as provided, which would be preserved for Snowflake if fully specified (e.g., `MY_DATABASE.MY_SCHEMA.MY_TABLE`).

4.  **Preservation of Original Full Names**: The tool prioritizes using the fully qualified table name if it's found directly in the LookML. The normalization and prefixing are fallbacks or standardization steps.

Despite these measures, the tool's table name resolution is more robust for BigQuery due to its design focus. Complex aliasing or highly customized SQL in other dialects might still pose challenges.

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
               --default_dataset custom_dataset \
               --snapshot_project custom-snapshot-project \
               --snapshot_dataset custom_snapshot_dataset
```

Usage including source definition information:
```bash
python main.py --looker_path /path/to/looker/project \
               --include_source_info
```

## 6. Output Files

- `view_analysis.csv`: CSV file containing all view-to-table mapping information. Columns include `view_name`, `explore_count`, `calculated_usage`, `table_name`, `citation_type`, `additional_tables`. If `--include_source_info` is used, `source_type` and `source_definition` columns are also included.
- `export_command.txt`: Text file with export commands (e.g., BigQuery `bq extract`) for all tables associated with the analyzed views. Generated if `--export_gs_bucket` is provided.
- `export_command_active.txt`: Text file with export commands for tables associated with views that have a `calculated_usage` greater than 0. This file is only generated if `--export_gs_bucket` is provided AND Explore usage frequency data (via `--explore_usage_file`) was available to calculate usage.

## 7. Known Issues and Limitations

1.  **Table Name Parsing Complexity**: While the tool uses sophisticated regex and normalization to extract table names from various LookML constructs (`sql_table_name`, `derived_table` SQL, Liquid templates) and SQL dialects (primarily BigQuery, with some flexibility for Snowflake-like patterns), highly complex or unconventional SQL, deeply nested Liquid logic, or unusual quoting/aliasing can still lead to:
    - Incorrect primary table identification.
    - Missing some table dependencies.
    - Extracting non-table strings as table names.
    The tool is generally biased towards BigQuery SQL syntax.

2.  **Quote Handling**: The primary strategy for handling quotes involves removing double quotes (`"`) during preprocessing steps in `utils.py` (for `extract_tables_from_sql`, `extract_tables_from_liquid_block`) and `analyzers.py` (for `normalize_source_definitions`). Backticks (\`\`) are typically handled by regex patterns. This simplification works well for many common cases in BigQuery and Snowflake but might lose nuance if double quotes have very specific, differing meanings in certain SQL contexts beyond simple identifier quoting.

3.  **Liquid Conditional Block Processing**: As designed, the `extract_tables_from_liquid_block()` function in `looker_utils.utils` extracts table references from *all* condition branches of a Liquid `{% if ... %}` block, regardless of whether the condition would evaluate to true at runtime. This ensures all *potential* dependencies are captured but may identify tables that are not actually used in a specific environment or configuration. The `view_analysis.csv` report does not distinguish these conditional tables.

4.  **View Aliases and `from:` Clause**: The tool identifies view aliases created with the `from:` parameter in `explore` or `join` blocks. The aliased view name is used in the report, and its `citation_type` is often marked as `derived_from`. The underlying "real" view it points to is resolved for relationship analysis, but the final report focuses on the usage of the alias itself.

5.  **UNNEST Complexity**: Identifying the true origin of an UNNESTed view can be ambiguous. The tool flags views involved in `UNNEST` operations (if no other direct table source is defined for the view) but might not always pinpoint the exact original source table with full accuracy if the `UNNEST` SQL is complex.

6.  **Default Dataset/Project Application**: The logic in `update_view_table_info` to apply default project/dataset names to partially qualified tables is heuristic. It assumes a BigQuery-like structure if prefixes are missing and might not always correctly infer the intended full name for other database systems if they follow different conventions for partial names.

7.  **Error Reporting for Individual Files**: While the main script might catch general errors, detailed per-file parsing errors (e.g., a malformed LookML file) might result in that file being skipped or partially processed, with warnings printed to the console, rather than halting the entire analysis. The completeness of the analysis depends on the parsability of all LookML files. 