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

Run the script using Python 3:

```bash
python main.py --looker_path /path/to/looker/project --activities_file activities.csv --output_dir ./output --export_gs_bucket your-gcs-bucket-name
```

Advanced usage with custom project and dataset settings:

```bash
python main.py --looker_path /path/to/looker/project \
               --activities_file activities.csv \
               --output_dir ./output \
               --export_gs_bucket your-gcs-bucket-name \
               --default_project custom-project \
               --default_dataset custom_dataset \
               --snapshot_project custom-snapshot-project \
               --snapshot_dataset custom_snapshot_dataset
```

### Command Line Arguments

- `--looker_path`: Path to the Looker project directory (required if the script is not in the Looker project directory)
- `--activities_file`: Path to the CSV file containing explore activity data (default: 'activities.csv')
- `--output_dir`: Directory where output files will be saved (default: current directory)
- `--export_gs_bucket`: GCS bucket name for export commands (optional). If not provided, the script will analyze the views but will not generate export commands.
- `--default_project`: Default BigQuery project name (default: 'your-company')
- `--default_dataset`: Default dataset name (default: 'analytics_prod')
- `--snapshot_project`: Snapshot table project name (default: 'your-company-snapshot')
- `--snapshot_dataset`: Snapshot table dataset name (default: 'analytics_prod_snapshots')

### Input Files

- `activities.csv`: CSV file containing explore usage data with columns for explore name and usage count

### Output Files

The script generates up to three output files:

- `updated_table_list.csv`: View-to-table mapping information for all views (always generated)
- `export_command.txt`: Export commands for all tables (only generated if `--export_gs_bucket` is provided)
- `export_command_active.txt`: Export commands for active tables only (tables with usage frequency > 0) (only generated if `--export_gs_bucket` is provided)

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