#!/usr/bin/env python3
import csv
import re
import os
import glob
from collections import defaultdict

# Load explore usage frequency data
def load_explore_usage(input_file):
    explore_usage = {}
    
    # Check if the file exists, if not return an empty dictionary
    if not input_file or not os.path.exists(input_file):
        print(f"Warning: Activities file '{input_file}' does not exist. Setting all calculated_usage values to NULL.")
        return explore_usage
    
    try:
        with open(input_file, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            for row in reader:
                if len(row) >= 3:
                    explore_name = row[0].strip()
                    usage_count = int(row[2].replace(',', ''))
                    explore_usage[explore_name] = usage_count
    except Exception as e:
        print(f"Error reading activities file: {e}. Setting all calculated_usage values to NULL.")
        return {}
        
    return explore_usage

# Scan all views and models to build a complete view list
def extract_all_views():
    print("Extracting views from all possible directories...")
    
    # Collect all view and model files from standard directories
    view_files = glob.glob('views/**/*.view.lkml', recursive=True)
    model_files = glob.glob('models/*.lkml')
    
    # Add additional view file patterns to search for views in non-standard directories
    view_files += glob.glob('**/*.view.lkml', recursive=True)  # All view files in any subdirectory
    
    # Add all .lkml files in root directory as potential model files
    root_lkml_files = glob.glob('*.lkml')
    
    # Filter out files that are already in the view_files list
    for file in root_lkml_files:
        if file not in model_files and '.view.lkml' not in file:
            model_files.append(file)
    
    # Remove duplicates
    view_files = list(set(view_files))
    model_files = list(set(model_files))
    
    print(f"Found {len(view_files)} view files and {len(model_files)} model files")
    
    view_list = {}
    view_to_file = {}  # Record the file path for each view
    
    # Extract view names from view files
    for file_path in view_files:
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                
                # Extract all view definitions
                view_matches = re.finditer(r'view:\s+(\w+)\s+{', content)
                for match in view_matches:
                    view_name = match.group(1)
                    
                    # If the view name is very long or contains special characters, it might be a false detection, skip it
                    if len(view_name) > 100 or not view_name.isalnum() and '_' not in view_name:
                        continue
                    
                    # Debug log for identified views
                    print(f"DEBUG - Found view: {view_name} in {file_path}")
                    
                    # Initialize view information
                    view_list[view_name] = {
                        'usage': 0,  # Initial usage frequency is 0
                        'table_name': "",  # Initial table name is empty
                        'citation_type': "native",  # Default to native type
                        'table_names': []  # Table name list
                    }
                    view_to_file[view_name] = file_path
        except Exception as e:
            print(f"Error extracting view from {file_path}: {e}")
    
    # Extract view aliases from model files
    for file_path in model_files:
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                
                # Find all explore definition blocks
                explore_matches = re.finditer(r'explore:\s+([a-zA-Z0-9_]+)\s+{', content)
                for explore_match in explore_matches:
                    explore_name = explore_match.group(1)
                    start_pos = explore_match.end()
                    
                    # Find the end position of the explore block
                    bracket_level = 1
                    end_pos = start_pos
                    for i in range(start_pos, len(content)):
                        if content[i] == '{':
                            bracket_level += 1
                        elif content[i] == '}':
                            bracket_level -= 1
                            if bracket_level == 0:
                                end_pos = i
                                break
                    
                    if end_pos <= start_pos:
                        continue  # Can't find closing bracket
                        
                    explore_content = content[start_pos:end_pos]
                    
                    # Check if there's a from statement, indicating this is an alias view
                    from_match = re.search(r'from:\s+([a-zA-Z0-9_]+)', explore_content)
                    if from_match:
                        base_view = from_match.group(1)
                        # If the explore name is different from the base_view, this is a view alias
                        if explore_name != base_view:
                            # Add the alias view to the view list
                            if explore_name not in view_list:
                                view_list[explore_name] = {
                                    'usage': 0,
                                    'table_name': "",
                                    'citation_type': "derived_from",  # Set to derived_from type
                                    'table_names': [],
                                    'derived_from': base_view  # Record which view it's derived from
                                }
                                view_to_file[explore_name] = file_path
                    
                    # Find all join statements - use multiple patterns to match different join block styles
                    join_patterns = [
                        # Match standard format join blocks
                        r'join:\s+([a-zA-Z0-9_]+)\s+{([^{}]*(?:{[^{}]*}[^{}]*)*)}',
                        # Match compact format join blocks
                        r'join:\s+([a-zA-Z0-9_]+)\s+{([^}]+)}'
                    ]
                    
                    # Use all patterns to find join blocks
                    join_blocks = []
                    for pattern in join_patterns:
                        for match in re.finditer(pattern, explore_content, re.DOTALL):
                            join_view = match.group(1)
                            join_content = match.group(2)
                            join_blocks.append((join_view, join_content))
                    
                    # Process all found join blocks
                    for join_view, join_content in join_blocks:
                        # Add the join view to the view list
                        if join_view not in view_list:
                            view_list[join_view] = {
                                'usage': 0,
                                'table_name': "",
                                'citation_type': "native",  # Default to native type
                                'table_names': []
                            }
                            view_to_file[join_view] = file_path
                        
                        # Check if there's a from statement in the join block, indicating this is an alias view
                        from_in_join_match = re.search(r'from:\s+([a-zA-Z0-9_]+)', join_content)
                        if from_in_join_match:
                            from_view = from_in_join_match.group(1)
                            if join_view != from_view:
                                # Update to derived_from type
                                view_list[join_view]['citation_type'] = "derived_from"
                                view_list[join_view]['derived_from'] = from_view
                                print(f"DEBUG - extract_all_views identified alias view: {join_view} from {from_view}")
        except Exception as e:
            print(f"Error extracting views from model {file_path}: {e}")
    
    return view_list, view_to_file 