#!/usr/bin/env python3
import csv
from collections import defaultdict
from looker_utils.analyzers import guess_table_info
import re
import os
from looker_utils.utils import DEFAULT_PROJECT, DEFAULT_DATASET, SNAPSHOT_PROJECT, SNAPSHOT_DATASET

# Generate result report
def generate_report(view_list, actual_usage, unnest_views, actual_table_names, output_file, explore_to_views=None, include_source_info=False):
    # Check if calculated usage values are available (whether the user provided an activities_file)
    has_usage_data = all(usage is not None for usage in actual_usage.values())
    
    # Calculate explore count for each view
    view_to_explore_count = defaultdict(int)
    if explore_to_views:
        # Reverse the explore_to_views dictionary, calculate how many explores use each view
        for explore_name, views in explore_to_views.items():
            for view in views:
                view_to_explore_count[view] += 1
    
    # Sort by calculated_usage first, then by explore_count, both in descending order
    if has_usage_data:
        sorted_views = sorted(
            actual_usage.items(),
            key=lambda x: (x[1] if x[1] is not None else 0, view_to_explore_count.get(x[0], 0)),
            reverse=True
        )
    else:
        # If no usage data, sort by explore_count in descending order, then by view name
        sorted_views = sorted(
            actual_usage.items(),
            key=lambda x: (view_to_explore_count.get(x[0], 0), x[0]),
            reverse=True
        )
    
    # Create a statistics counter to record the number of different citation_types
    citation_type_counts = defaultdict(int)
    
    # Write to CSV file
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Decide which columns to include in the header based on include_source_info parameter
        header = ['view_name', 'explore_count', 'calculated_usage', 'table_name', 'citation_type', 'additional_tables']
        # Only add source_type and source_definition columns when include_source_info is True
        if include_source_info:
            header.extend(['source_type', 'source_definition'])
        writer.writerow(header)
        
        for view_name, usage in sorted_views:
            table_name = ""
            citation_type = "native"  # Default type
            additional_tables = []
            source_type = ""  # Data source type
            source_definition = ""  # Data source definition
            
            # First, prioritize getting table references from actual_table_names, as it contains all tables extracted from Liquid blocks
            if view_name in actual_table_names and actual_table_names[view_name]:
                table_names = actual_table_names[view_name]
                if table_names:
                    table_name = table_names[0]  # Main table name
                    if len(table_names) > 1:
                        additional_tables = table_names[1:]  # Additional table names
                        print(f"DEBUG - View {view_name} extracted additional tables from actual_table_names: {additional_tables}")
            
            # Then, extract view information from view_list
            if view_name in view_list:
                citation_type = view_list[view_name]['citation_type']
                # Record the number of views of this type
                citation_type_counts[citation_type] += 1
                
                # If no table was found from actual_table_names, try getting it from view_list
                if not table_name and view_list[view_name]['table_names']:
                    table_names = view_list[view_name]['table_names']
                    if table_names:
                        table_name = table_names[0]  # Main table name
                        if len(table_names) > 1:
                            # If additional_tables already has content, don't overwrite it, merge instead
                            new_additional = table_names[1:]
                            additional_tables.extend([t for t in new_additional if t not in additional_tables])
                            print(f"DEBUG - View {view_name} merged additional tables from view_list: {new_additional}")
                
                # New: If citation_type is derived but actually has a table name, change it to native
                if citation_type == "derived" and table_name:
                    citation_type = "native"
                    print(f"DEBUG - Changed citation_type for {view_name} from 'derived' to 'native' because it has a table_name")
                
                # Get view data source definition
                if include_source_info:
                    source_type = view_list[view_name].get('source_type', '')
                    source_definition = view_list[view_name].get('source_definition', '')
            
            # If it's an unnest view, ensure citation_type is correct
            if view_name in unnest_views:
                citation_type = 'unnest'
                # Reset table name
                if table_name and (DEFAULT_PROJECT in table_name or SNAPSHOT_PROJECT in table_name):
                    table_name = ''
                    additional_tables = []
            
            # Ensure table name is in complete three-part format (if not derived_explore type)
            # Only attempt to complete when the table name starts with DEFAULT_PROJECT or SNAPSHOT_PROJECT
            if citation_type != 'derived_explore' and table_name and (table_name.startswith(DEFAULT_PROJECT) or table_name.startswith(SNAPSHOT_PROJECT)):
                parts = table_name.split('.')
                if len(parts) == 1:  # Only project part
                    table_name = f"{DEFAULT_PROJECT}.{DEFAULT_DATASET}.{view_name}"
                elif len(parts) == 2:  # Missing table part
                    table_name = f"{parts[0]}.{parts[1]}.{view_name}"
            
            # For derived_explore type views, clear the table name
            if citation_type == 'derived_explore':
                table_name = ''
                additional_tables = []
            
            # ----------------------------------------------------------
            # Clean up `additional_tables`:
            #   • Only keep valid complete three-part table names (project.dataset.table format)
            #   • No longer attempt to auto-complete table name paths missing projects
            #   • Remove duplicates and maintain stable sorting
            # ----------------------------------------------------------
            formatted_additional_tables = []
            seen_tables = set()
            normalized_seen = set()  # For tracking normalized table names
            
            for raw_entry in additional_tables:
                # Skip tables that have already been processed to avoid duplicates
                if raw_entry in seen_tables:
                    continue
                
                # If the table name is the same as the main table name, skip it
                if raw_entry == table_name:
                    continue
                    
                # Only process complete three-part table names (project.dataset.table format)
                if isinstance(raw_entry, str) and raw_entry.count('.') == 2:
                    parts = raw_entry.split('.')
                    # Ensure all three parts have values and don't start with . (incomplete reference)
                    if len(parts) == 3 and all(parts) and not raw_entry.startswith('.'):
                        # Check if it's a normalized duplicate table name
                        normalized = raw_entry.lower()
                        if normalized in normalized_seen:
                            print(f"DEBUG - Skipping normalized duplicate: {raw_entry}")
                            continue
                        
                        # MODIFICATION: Filter out tables where the last part starts with '_'
                        table_actual_name = parts[-1]
                        if table_actual_name.startswith('_'):
                            print(f"DEBUG - Skipping table with leading underscore in last part: {raw_entry}")
                            continue
                        
                        formatted_additional_tables.append(raw_entry)
                        seen_tables.add(raw_entry)
                        normalized_seen.add(normalized)
                        continue
                else:
                    # If it's not a three-part table name, process complex strings that may contain multiple table references
                    tokens = re.split(r'[;\s,()]+', str(raw_entry))
                    for token in tokens:
                        if not token:
                            continue
                        token = token.strip('`')  # Remove backticks
                        
                        # Only process complete three-part table names
                        if '.' in token and token.count('.') == 2 and not token.startswith('.'):
                            parts = token.split('.')
                            # Verify all three parts have values
                            if len(parts) == 3 and all(parts):
                                # Check if it's a duplicate table name
                                if token not in seen_tables and token != table_name:
                                    # Check if it's a normalized duplicate
                                    normalized = token.lower()
                                    if normalized in normalized_seen:
                                        print(f"DEBUG - Skipping normalized duplicate token: {token}")
                                        continue
                                    
                                    # MODIFICATION: Filter out tables where the last part starts with '_'
                                    table_actual_name = parts[-1]
                                    if table_actual_name.startswith('_'):
                                        print(f"DEBUG - Skipping table token with leading underscore in last part: {token}")
                                        continue

                                    formatted_additional_tables.append(token)
                                    seen_tables.add(token)
                                    normalized_seen.add(normalized)
            
            # Print debug information
            if formatted_additional_tables:
                print(f"DEBUG - View {view_name} final additional tables: {formatted_additional_tables}")
            else:
                print(f"DEBUG - View {view_name} has no valid additional table references")
            
            # Special handling for any view that still has no table name
            if not table_name and view_name in view_list and 'table_names' in view_list[view_name]:
                print(f"DEBUG - Using table names from view_list for {view_name}: {view_list[view_name]['table_names']}")
                if view_list[view_name]['table_names']:
                    table_name = view_list[view_name]['table_names'][0]
                    # If additional_tables already has content, don't overwrite it, merge instead
                    if len(view_list[view_name]['table_names']) > 1:
                        new_additional = [t for t in view_list[view_name]['table_names'][1:] if t != table_name]
                        for t in new_additional:
                            if t not in seen_tables:
                                formatted_additional_tables.append(t)
                                seen_tables.add(t)
            
            # Log citation type for debugging
            print(f"DEBUG - Citation type for {view_name}: {citation_type}")
            
            # Handle the case where calculated_usage is None, output "NULL"
            calc_usage_value = "NULL" if usage is None else usage
            
            # Get explore count for this view
            explore_count = view_to_explore_count.get(view_name, 0)
            
            # Prepare basic row data
            row_data = [
                view_name, 
                explore_count,
                calc_usage_value, 
                table_name, 
                citation_type, 
                ';'.join(formatted_additional_tables) if formatted_additional_tables else ''
            ]
            
            # Decide whether to add source information columns based on include_source_info parameter
            if include_source_info:
                row_data.extend([source_type, source_definition])
                
            # Write CSV row
            writer.writerow(row_data)
    
    # Only output the top 20 most frequently used views when usage data is available
    if has_usage_data:
        print("Top 20 most used views (sorted by calculated usage frequency):")
        print("View Name,Explore Count,Calculated Usage Frequency")
        for view_name, usage in sorted_views[:20]:
            explore_count = view_to_explore_count.get(view_name, 0)
            print(f"{view_name},{explore_count},{usage}")
    else:
        print("No usage data available (no activities file provided), skipping top views display")
    
    # Print statistics for different view types
    print("\nView citation type statistics:")
    for citation_type, count in citation_type_counts.items():
        print(f"{citation_type}: {count} views")
            
    # Return the sorted view list for generating export commands
    return sorted_views

# Generate export commands to GCP bucket
def generate_export_commands(sorted_views, view_list, unnest_views, actual_table_names, export_all_file, export_active_file, gcs_bucket=None, default_project=None, snapshot_project=None):
    """
    Generates BigQuery export commands for tables identified in the view analysis.
    
    Parameters:
    sorted_views (list): List of views sorted by usage.
    view_list (dict): Dictionary containing view definitions and metadata.
    unnest_views (set): Set of views that are unnested from other tables.
    actual_table_names (dict): Dictionary mapping view names to actual table names.
    export_all_file (str): Path to write export commands for all tables.
    export_active_file (str): Path to write export commands for active tables. If None, active tables export will not be generated.
    gcs_bucket (str): Google Cloud Storage bucket name for export destination.
    default_project (str): Default BigQuery project name (like 'company-dwh').
    snapshot_project (str): Snapshot BigQuery project name (like 'company-dwh-snapshot').
    
    Returns:
    None
    """
    
    # Use provided project names or fall back to global constants
    actual_default_project = default_project or DEFAULT_PROJECT
    actual_snapshot_project = snapshot_project or SNAPSHOT_PROJECT
    
    print(f"DEBUG - Using project names: default_project={actual_default_project}, snapshot_project={actual_snapshot_project}")
    
    # Use sets to record processed table names, avoiding duplicate exports
    processed_tables = set()  # All tables that have been processed
    processed_active_tables = set()  # Tables that are active (have usage > 0)
    skipped_views = set()  # Views that were skipped
    error_tables = set()  # Views that had errors during processing
    
    # If no GCS bucket is provided, skip command generation
    if not gcs_bucket:
        print("No GCS bucket specified, skipping export command generation")
        return
    
    # Debug info: check sample views for table names
    sample_views = list(view_list.keys())[:5] if view_list else []
    for view_name in sample_views:
        print(f"DEBUG - Sample view: {view_name}, table names: {view_list[view_name].get('table_names', [])}")
    
    # Create writer for export_active_file
    f_active = None
    if export_active_file:
        f_active = open(export_active_file, 'w')
    
    # Open all tables file for writing
    with open(export_all_file, 'w') as f_all:
        for view_name, usage in sorted_views:
            # Skip unnest views
            if view_name in unnest_views:
                skipped_views.add(view_name)
                continue
                
            try:
                table_names = []
                
                # Get table names (prioritize actual table names from view definitions)
                if view_name in actual_table_names:
                    table_names = actual_table_names[view_name]
                elif view_name in view_list:
                    table_names = view_list[view_name]['table_names']
                    # If the view is in the original table list but has no table names, skip it
                    if not table_names:
                        skipped_views.add(view_name)
                        continue
                
                # If there are still no table names, try to guess
                if not table_names:
                    table_names_list, citation_type = guess_table_info(view_name, view_list, unnest_views, actual_table_names)
                    # If table names cannot be determined or it's an unnest view, skip it
                    if not table_names_list or citation_type == 'unnest':
                        skipped_views.add(view_name)
                        continue
                    table_names = table_names_list
                
                # Special handling for views without table reference
                if not table_names:
                    print(f"DEBUG - No table names found for {view_name}, attempting to derive from naming convention")
                    # Try to derive table name from view name using naming conventions
                    derived_table_name = f'{actual_default_project}.{DEFAULT_DATASET}.{view_name}'
                    table_names = [derived_table_name]
                
                # Process each table name
                for table_name in table_names:
                    # Clean up all special characters and newlines in the table name
                    table_name = table_name.strip().replace('\n', '').replace('#', '').replace('\r', '')
                    
                    # Parse the table name to extract its components
                    parts = table_name.split('.')
                    
                    # Handle cases where table_name is incomplete
                    if len(parts) < 3:
                        # Only process one-part table names (just table name without project, dataset)
                        # Skip two-part table names (like CUSTOM_SYSTEM.PUBLIC)
                        if len(parts) == 1:  # Only table name without project and dataset
                            table_name = f"{actual_default_project}.{DEFAULT_DATASET}.{parts[0]}"
                        # No longer adding prefix to two-part table names
                        # elif len(parts) == 2:  # Dataset and table, but no project
                        #     table_name = f"{actual_default_project}.{parts[0]}.{parts[1]}"
                    
                    # At this point, table_name should have a three-part structure
                    # Check if it's a valid three-part name and hasn't been processed yet
                    if ('.' in table_name) and (table_name.count('.') == 2) and (table_name not in processed_tables):
                        # Add table name to processed set
                        processed_tables.add(table_name)
                        
                        # Build export command
                        # Extract short table name from full table name (for URI construction)
                        table_parts = table_name.split('.')
                        if len(table_parts) >= 3:
                            project = table_parts[0]  # Project name from table definition
                            dataset = table_parts[1]  # Dataset name from table definition
                            short_table_name = table_parts[2].replace('*', '')  # Remove possible wildcards
                            
                            print(f"DEBUG - Original: project={project}, dataset={dataset}, short_table_name={short_table_name}")
                            print(f"DEBUG - Constants: actual_default_project={actual_default_project}, actual_snapshot_project={actual_snapshot_project}")
                            
                            # Generate SQL export command for this table
                            print(f"DEBUG - Generating export command for {table_name}")
                            
                            # Use project names directly from actual table names
                            if 'snapshot' in project.lower():
                                source_project = actual_snapshot_project
                            else:
                                source_project = actual_default_project
                                
                            print(f"DEBUG - Using source_project={source_project} for export command")
                            
                            export_command = f"""BEGIN
EXPORT DATA
  OPTIONS (
    uri = 'gs://{gcs_bucket}/{source_project}/{dataset}/{short_table_name}/*.parquet',
    format = 'PARQUET',
    compression = "SNAPPY",
    overwrite = true)
AS (
  SELECT *
  FROM `{source_project}.{dataset}.{short_table_name}`
);
EXCEPTION WHEN ERROR THEN
SELECT 1; -- Skip if table does not exist or other issues
END;
"""
                            # Write to all tables file
                            f_all.write(export_command)
                            
                            # Only write to active tables file if f_active exists and usage is not None and > 0
                            if f_active and usage is not None and usage > 0 and table_name not in processed_active_tables:
                                f_active.write(export_command)
                                processed_active_tables.add(table_name)
            except Exception as e:
                # Record views that had exceptions during processing
                error_tables.add(view_name)
                print(f"Error processing view {view_name}: {str(e)}")
    
    # If f_active was opened, close it
    if f_active:
        f_active.close()
        print(f"Export commands for active tables saved to {export_active_file}")
    
    print(f"Export commands for all tables saved to {export_all_file}")
    print(f"Generated export commands for {len(processed_tables)} unique tables")
    if export_active_file:
        print(f"Of these, {len(processed_active_tables)} are active tables (usage frequency > 0)")
    print(f"Skipped {len(skipped_views)} non-actual table views")
    if error_tables:
        print(f"Views with errors during processing: {len(error_tables)}")

    if export_active_file:
        print(f"Export commands saved to {export_all_file} and {export_active_file}")
    else:
        print(f"Export commands saved to {export_all_file}")

# Function to generate a report on Looker view usage
def generate_view_usage_report(view_list, model_uses, explore_uses, active_explore_list, output_path=None, output_filename="view_analysis.csv"):
    """
    Generates a CSV report of Looker view usage.
    
    Parameters:
    view_list (dict): Dictionary containing view information.
    model_uses (dict): Dictionary tracking view usage in models.
    explore_uses (dict): Dictionary tracking view usage in explores.
    active_explore_list (set): Set of active/valid explores.
    output_path (str): Directory to write output file.
    output_filename (str): Name of output file.
    
    Returns:
    str: Path to the generated report file.
    """
    # Set default output path if not provided
    if output_path is None:
        output_path = os.getcwd()
    
    # Ensure output path exists
    os.makedirs(output_path, exist_ok=True)
    
    # Prepare the full output path
    output_file = os.path.join(output_path, output_filename)
    print(f"Generating view usage report: {output_file}")
    
    # Open the CSV file for writing
    with open(output_file, 'w', newline='') as csvfile:
        # Define CSV writer with headers
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow([
            'view_name', 'used_in_models', 'used_in_explores', 'used_in_valid_explores', 
            'total_explore_usages', 'valid_explore_usages', 'table_name', 'table_names', 
            'table_type', 'models', 'explores'
        ])
        
        # Write data for each view
        for view_name, info in sorted(view_list.items()):
            # Count usage metrics
            model_count = len(model_uses.get(view_name, []))
            explore_count = len(explore_uses.get(view_name, []))
            
            # Count valid explore usages
            valid_explores = []
            for explore in explore_uses.get(view_name, []):
                if explore in active_explore_list:
                    valid_explores.append(explore)
            valid_explore_count = len(valid_explores)
            
            # List table names
            table_name = info.get('table_name', '')  # Primary table name
            table_names = info.get('table_names', [])  # All associated table names
            citation_type = info.get('citation_type', 'unknown')  # Type of table citation
            
            # Clean up table names
            if not table_name and table_names:
                table_name = table_names[0]
            
            # Lists of models and explores for this view
            models_list = ','.join(sorted(model_uses.get(view_name, [])))
            explores_list = ','.join(sorted(explore_uses.get(view_name, [])))
            
            # Write row to CSV
            csv_writer.writerow([
                view_name,                  # Name of the view
                model_count,                # Number of models using this view
                explore_count,              # Number of explores using this view
                valid_explore_count,        # Number of valid explores using this view
                explore_uses.get(view_name, {}),  # Total number of explore usages
                valid_explore_count,        # Number of valid explore usages
                table_name,                 # Primary table name
                ','.join(table_names),      # All table names, comma-separated
                citation_type,              # Type of table citation
                models_list,                # List of models using this view
                explores_list               # List of explores using this view
            ])
    
    print(f"View usage report generated: {output_file}")
    return output_file

# Function to filter views based on usage in active explores
def filter_views_by_usage(view_list, explore_uses, active_explore_list):
    """
    Filters views based on their usage in active explores.
    
    Parameters:
    view_list (dict): Dictionary containing view information.
    explore_uses (dict): Dictionary tracking view usage in explores.
    active_explore_list (set): Set of active/valid explores.
    
    Returns:
    dict: Filtered view list containing only views used in active explores.
    """
    filtered_views = {}
    
    for view_name, info in view_list.items():
        # Check if view is used in any active explore
        is_used = False
        for explore in explore_uses.get(view_name, []):
            if explore in active_explore_list:
                is_used = True
                break
        
        # Include view if it's used in active explores
        if is_used:
            filtered_views[view_name] = info
    
    return filtered_views

