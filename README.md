# Looker View Usage Analysis Tool

This project is entirely written with Cursor AI agent (more specifically, claude-3.7-sonnet with thinking), but has been carefully validated with the authors' extensive knowledge of the underlying data system to make sure that it produces desired output for a complex production looker project. Users are encouraged to use this as a good starting point but keep using AI tools to implement additional features as needed.

This tool analyzes Looker project structure to extract mappings between views and actual database tables, and generates export commands for data migration.

## Features

- Extracts view and table relationships from Looker projects
- Analyzes view usage patterns
- Identifies different types of views (native, derived, unnest, etc.)
- Generates export commands for BigQuery tables
- Creates comprehensive reports of view-to-table relationships

## Installation

Clone this repository:

```bash
git clone https://github.com/zhanghongkai/Looker-view-analyzer.git
cd Looker-view-analyzer
```

No additional dependencies are required beyond Python 3.6+.

## Usage

### Basic Usage

The most basic way to use this tool is to simply provide the path to your Looker project:

```bash
python main.py --looker_path /path/to/looker/project
```

This will analyze your Looker project and generate the `view_analysis.csv` file containing view-to-table mapping information for all views.

### Advanced Usage

To analyze explore usage and generate export commands, you can use additional parameters:

```bash
python main.py --looker_path /path/to/looker/project --explore_usage_file explore_usage.csv --export_gs_bucket your-gcs-bucket-name
```

For custom project and dataset settings:

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

### Support for Non-Standard Directory Structures

This tool has been enhanced to work with Looker projects that use non-standard directory structures. It can now detect and analyze:

- Model files in the root directory (*.model.lkml)
- View files in any subdirectory of the project
- Views and explores defined in non-standard locations

When you run the tool, it will automatically scan:
1. Standard directories (`views/` and `models/`)
2. The root directory for model files (*.model.lkml)
3. All subdirectories for view files (*.view.lkml)

This ensures that all views and explores are found, regardless of their location in the project structure. The tool will display information about the discovered files in non-standard locations to help you understand how your project is being analyzed.

### Optional Features

#### 1. Explore Usage Analysis (--explore_usage_file)

This feature allows you to analyze the usage frequency of views in your Looker project:

- **Description**: By providing a CSV file containing explore usage data, the tool can calculate the usage frequency of each view
- **Usage**: `python main.py --looker_path /path/to/looker/project --explore_usage_file explore_usage.csv`
- **Input Requirements**: A CSV file with explore usage data containing explore names and usage counts. The file should have at least these columns:
  - `Query Explore`: The name of the explore in Looker
  - `Query Model`: The model to which the explore belongs
  - `History Query Run Count`: The number of times the explore has been queried
  
  You can generate this file from the Looker Admin interface by navigating to the "Explores" section under "Usage" and exporting the data as CSV.
- **Output Results**:
  - The `view_analysis.csv` file will include a `calculated_usage` field showing the usage frequency of each view
  - If the `--export_gs_bucket` parameter is also provided, an additional `export_command_active.txt` file will be generated, containing export commands only for active views
- **Use Cases**: This feature is particularly useful when you need to identify which views are actively used and which ones might be obsolete

#### 2. Export Command Generation (--export_gs_bucket)

This feature allows you to generate BigQuery export commands for data migration:

- **Description**: By providing a GCS bucket name, the tool can generate commands to export data from BigQuery to GCS
- **Usage**: `python main.py --looker_path /path/to/looker/project --export_gs_bucket your-gcs-bucket-name`
- **Input Requirements**: A valid Google Cloud Storage bucket name
- **Output Results**:
  - Generates an `export_command.txt` file containing export commands for all tables
  - If the `--explore_usage_file` parameter is also provided, an additional `export_command_active.txt` file will be generated, containing export commands only for active tables
- **Use Cases**: This feature is particularly useful for data migration, backup, or when moving from one BigQuery project to another

These two features can be used independently or in combination for more comprehensive analysis and export capabilities. When used together, you can export only actively used tables, saving storage space and migration time.

### Command Line Arguments

- `--looker_path`: Path to the Looker project directory (required if the script is not in the Looker project directory)
- `--explore_usage_file`: Path to the CSV file containing explore activity data (optional). If not provided, the `calculated_usage` field will be set to NULL in the output and the `export_command_active.txt` file will not be generated.
- `--include_source_info`: Include additional columns in the `view_analysis.csv` output detailing the source of each view's reference (e.g., `sql_table_name` or the source explore for derived tables) (optional).
- `--output_dir`: Directory where output files will be saved (default: current directory)
- `--export_gs_bucket`: GCS bucket name for export commands (optional). If not provided, the script will analyze the views but will not generate export commands.
- `--default_project`: Default BigQuery project name (default: 'your-company')
- `--default_dataset`: Default dataset name (default: 'analytics_prod')
- `--snapshot_project`: Snapshot table project name (default: 'your-company-snapshot')
- `--snapshot_dataset`: Snapshot table dataset name (default: 'analytics_prod_snapshots')

### Input Files

- `explore_usage.csv`: CSV file containing explore usage data with columns for explore name and usage count (optional). This file should have the following columns:
  - `Query Explore`: The name of the explore in Looker
  - `Query Model`: The model to which the explore belongs
  - `History Query Run Count`: The number of times the explore has been queried
  - `User Count`: The number of users who have used this explore
  
  You can generate this file from the Looker Admin interface by navigating to the "Explores" section under "Usage" and exporting the data as CSV.

### Output Files

The script generates up to three output files:

- `view_analysis.csv`: View-to-table mapping information for all views (always generated)
- `export_command.txt`: Export commands for all tables (only generated if `--export_gs_bucket` is provided)
- `export_command_active.txt`: Export commands for active tables only (tables with usage frequency > 0) (only generated if both `--export_gs_bucket` and `--explore_usage_file` are provided)

## Project Structure

```
.
├── main.py                  # Main script entry point
└── looker_utils/            # Utility modules directory
    ├── __init__.py          # Package initialization
    ├── analyzers.py         # Functions for analyzing explore/view relationships
    ├── data_loaders.py      # Functions for loading data
    ├── extractors.py        # Functions for extracting table names and view information
    ├── reporters.py         # Functions for generating reports and export commands
    └── utils.py             # Common utility functions
```

### Module Descriptions

- **main.py**: Coordinates all modules and provides the command-line interface
- **data_loaders.py**: Contains functions for loading explore usage data and extracting views
- **extractors.py**: Contains functions for extracting table names from view definitions
- **analyzers.py**: Contains functions for analyzing relationships between explores and views
- **reporters.py**: Contains functions for generating reports and export commands
- **utils.py**: Contains common utility functions used across modules

## Citation Types

The tool classifies views into several types:

- **native**: Views directly referencing database tables
- **derived**: Views derived from SQL operations on other tables
- **unnest**: Views created using UNNEST operations
- **derived_explore**: Views based on explores
- **derived_from**: Views derived from other views (aliases)
- **nested**: Nested views within other views

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

MIT License

Copyright (c) 2025 Hongkai Zhang

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Contributing

Contributions to this project are welcome! As this tool was created to solve specific use cases, please consider the following when contributing:

1. **Open an issue first**: Before submitting a pull request, please open an issue to discuss the proposed changes.

2. **Follow the code style**: Try to match the existing code style in your contributions.

3. **Document your changes**: Update the README or add comments to your code when necessary.

4. **Test your changes**: Make sure your changes don't break existing functionality.

To contribute:

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

For questions or suggestions, please contact the sole author and maintainer: [Hongkai Zhang](https://github.com/zhanghongkai) 