#!/usr/bin/env python3
"""
Looker View Usage Analysis Tool
This tool analyzes Looker project structure to extract mappings between views and actual database tables,
and generates export commands for data migration.
"""

import os
import sys
import argparse
import glob
from looker_utils.data_loaders import load_explore_usage, extract_all_views
from looker_utils.analyzers import (
    calculate_actual_usage,
    update_view_table_info,
    analyze_explores_and_extract_tables
)
from looker_utils.reporters import generate_report, generate_export_commands
from looker_utils.utils import set_global_project_settings
from looker_utils.constants import DEFAULT_PROJECT, SNAPSHOT_PROJECT
import looker_utils.constants as constants

def main():
    """Main function that coordinates different modules to complete the view usage analysis"""
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Analyze Looker views to determine table usage')
    parser.add_argument('--looker_path', required=True, help='Path to the Looker project')
    parser.add_argument('--model', help='Specific model to analyze (default: all models)')
    parser.add_argument('--max_models', type=int, default=None, help='Maximum number of models to process')
    parser.add_argument('--table_file', help='CSV file with table data to compare (optional)')
    parser.add_argument('--export_gs_bucket', help='GCS bucket name for export commands (e.g., bucket-name)')
    parser.add_argument('--default_project', default='company-dwh', help='Default BigQuery project name (default: company-dwh)')
    parser.add_argument('--default_dataset', default='analytics_prod', help='Default BigQuery dataset name (default: analytics_prod)')
    parser.add_argument('--snapshot_project', default='company-dwh-snapshot', help='Snapshot BigQuery project name (default: company-dwh-snapshot)')
    parser.add_argument('--snapshot_dataset', default='analytics_prod_snapshots', help='Snapshot BigQuery dataset name (default: analytics_prod_snapshots)')
    parser.add_argument('--output_dir', default='.', help='Directory to save output files (default: current directory)')
    parser.add_argument('--explore_usage_file', help='Path to explore usage CSV file (optional, if not provided calculated_usage will be NULL)')
    parser.add_argument('--include_source_info', action='store_true', help='Include source definitions in output (may result in large files)')
    args = parser.parse_args()
    
    # Update constants with command line values
    constants.DEFAULT_PROJECT = args.default_project
    constants.SNAPSHOT_PROJECT = args.snapshot_project
    
    # Set global project settings
    set_global_project_settings(
        default_project=args.default_project, 
        default_dataset=args.default_dataset, 
        snapshot_project=args.snapshot_project, 
        snapshot_dataset=args.snapshot_dataset
    )
    
    # Capture the directory where the script was invoked **before** any potential working
    # directory change. This will be used as the default output location when the user
    # supplies a `--looker_path` but omits `--output_dir`.
    original_cwd = os.getcwd()

    # Resolve the output directory:
    #   1. If the caller did **not** provide --output_dir (i.e. left it as "."),
    #      we keep the files in the original CWD so that they stay alongside the
    #      main script rather than being written inside the Looker project folder.
    #   2. If the caller *did* pass a value, we honour it. For relative paths we
    #      resolve them against the original CWD to obtain an absolute path that
    #      remains valid even after `os.chdir` is executed.
    if args.output_dir == '.':
        output_base_dir = original_cwd
    else:
        # Convert to absolute path relative to the original CWD if necessary
        output_base_dir = args.output_dir
        if not os.path.isabs(output_base_dir):
            output_base_dir = os.path.abspath(os.path.join(original_cwd, output_base_dir))

    # Ensure the output directory exists
    os.makedirs(output_base_dir, exist_ok=True)

    # File path definitions
    INPUT_EXPLORE_USAGE = args.explore_usage_file  # This can now be None
    OUTPUT_TABLE_LIST = os.path.join(output_base_dir, 'view_analysis.csv')
    EXPORT_COMMANDS_FILE = os.path.join(output_base_dir, 'export_command.txt')
    EXPORT_COMMANDS_ACTIVE_FILE = os.path.join(output_base_dir, 'export_command_active.txt') if INPUT_EXPLORE_USAGE else None
    
    # If Looker path is provided, change to that directory
    if args.looker_path:
        if not os.path.isdir(args.looker_path):
            print(f"Error: The specified Looker path '{args.looker_path}' is not a valid directory")
            sys.exit(1)
        os.chdir(args.looker_path)
        print(f"Changed working directory to: {args.looker_path}")
        
        # Show directory structure information for the user
        print("\nAnalyzing directory structure...")
        
        # Check for view and model files in standard directories
        standard_view_files = glob.glob('views/**/*.view.lkml', recursive=True)
        standard_model_files = glob.glob('models/*.lkml')
        
        # Check for view and model files in non-standard locations
        root_model_files = glob.glob('*.model.lkml')
        all_view_files = glob.glob('**/*.view.lkml', recursive=True)
        
        # Count non-standard files
        nonstandard_views = [f for f in all_view_files if f not in standard_view_files]
        nonstandard_models = [f for f in root_model_files if f not in standard_model_files]
        
        print(f"Found {len(standard_view_files)} view files in standard 'views/' directory")
        print(f"Found {len(standard_model_files)} model files in standard 'models/' directory")
        print(f"Found {len(nonstandard_views)} view files in non-standard locations")
        print(f"Found {len(nonstandard_models)} model files in root directory")
        
        if nonstandard_models:
            print("\nNon-standard model files found:")
            for model_file in nonstandard_models[:5]:  # Show up to 5 examples
                print(f"  - {model_file}")
            if len(nonstandard_models) > 5:
                print(f"  - ... and {len(nonstandard_models) - 5} more")
        
        if nonstandard_views:
            print("\nSample non-standard view file locations:")
            nonstandard_dirs = set()
            for view_file in nonstandard_views:
                nonstandard_dirs.add(os.path.dirname(view_file))
            
            # Show up to 10 unique directories
            for i, directory in enumerate(sorted(nonstandard_dirs)):
                if i >= 10:
                    print(f"  - ... and {len(nonstandard_dirs) - 10} more directories")
                    break
                print(f"  - {directory}/")
        
        print("\nAll files will be processed in the analysis.\n")
    
    # Check if explore usage file is provided
    if INPUT_EXPLORE_USAGE:
        print(f"Loading explore usage frequency from {INPUT_EXPLORE_USAGE}...")
        explore_usage = load_explore_usage(INPUT_EXPLORE_USAGE)
        print(f"Loaded usage frequency for {len(explore_usage)} explores")
    else:
        print("No explore usage file provided, calculated_usage will be set to NULL")
        explore_usage = {}  # Use an empty dictionary as explore_usage
    
    print("Extracting all views...")
    view_list, view_to_file = extract_all_views()
    print(f"Extracted {len(view_list)} views")
    
    print("Analyzing explore relationships and extracting table information...")
    explore_to_views, unnest_views, explore_list, explore_to_model, view_from_alias, actual_table_names, view_citation_types, view_source_definitions = analyze_explores_and_extract_tables()
    print(f"Analyzed {len(explore_to_views)} explores")
    print(f"Identified {len(unnest_views)} views created through unnest")
    print(f"Identified {len(view_from_alias)} view alias relationships")
    print(f"Extracted table names for {len(actual_table_names)} views")
    print(f"Extracted data source definitions for {len(view_source_definitions)} views")
    total_tables = sum(len(tables) for tables in actual_table_names.values())
    print(f"Total table references extracted: {total_tables}")
    
    # Update table information in the view list
    view_list = update_view_table_info(
        view_list, 
        actual_table_names, 
        unnest_views, 
        view_citation_types, 
        view_from_alias,
        view_source_definitions,
        default_project=args.default_project,
        default_dataset=args.default_dataset,
        snapshot_project=args.snapshot_project,
        snapshot_dataset=args.snapshot_dataset
    )
    print("Updated table information in the view list")
    
    # Calculate actual usage based on whether explore_usage_file is provided
    if INPUT_EXPLORE_USAGE:
        print("Calculating actual usage frequency...")
        actual_usage = calculate_actual_usage(view_list, explore_usage, explore_to_views)
    else:
        print("Setting calculated_usage to NULL for all views...")
        actual_usage = {view_name: None for view_name in view_list}  # Set actual_usage to None for all views
    
    print("Generating report...")
    sorted_views = generate_report(view_list, actual_usage, unnest_views, actual_table_names, OUTPUT_TABLE_LIST, explore_to_views, include_source_info=args.include_source_info)
    print(f"Done! Results saved to {OUTPUT_TABLE_LIST}")
    
    # Only generate export commands if --export_gs_bucket parameter is provided
    if args.export_gs_bucket:
        print("Generating export commands...")
        generate_export_commands(
            sorted_views, 
            view_list, 
            unnest_views, 
            actual_table_names, 
            EXPORT_COMMANDS_FILE, 
            EXPORT_COMMANDS_ACTIVE_FILE, 
            args.export_gs_bucket,
            default_project=args.default_project,
            snapshot_project=args.snapshot_project
        )
        if INPUT_EXPLORE_USAGE:
            print(f"Export commands saved to {EXPORT_COMMANDS_FILE} and {EXPORT_COMMANDS_ACTIVE_FILE}")
        else:
            print(f"Export commands saved to {EXPORT_COMMANDS_FILE}")
    else:
        print("No GCS bucket specified, skipping export command generation")
    
    print("All tasks completed!")

if __name__ == "__main__":
    main() 