#!/usr/bin/env python3
import re
import os
import glob
from collections import defaultdict
from looker_utils.utils import auto_detect_related_tables, DEFAULT_PROJECT, DEFAULT_DATASET, SNAPSHOT_PROJECT, SNAPSHOT_DATASET

# Analyze relationships between explores and views
def analyze_explores():
    print("Analyzing all explores from models and included files...")
    explore_to_views = defaultdict(set)
    explore_to_model = {}  # Record which model each explore belongs to
    unnest_views = set()  # Used to record views created through unnest
    explore_list = {}  # Record all explores
    view_from_alias = {}  # Record view alias relationships
    
    # Identify views that already reference a base table (sql_table_name) or use a derived_table.
    # These views should not be considered UNNEST-derived even if UNNEST appears somewhere.
    # The set is built dynamically below instead of being hard-coded.
    non_unnest_views = set()
    
    # Store the set of views with explicit table references
    views_with_table_reference = set()
    
    # First get all view files and check for table references
    view_files = glob.glob('views/**/*.view.lkml', recursive=True)
    # Also include views from any other directory
    view_files += glob.glob('**/*.view.lkml', recursive=True)
    # Remove duplicates
    view_files = list(set(view_files))
    
    for file_path in view_files:
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                
                # Extract view name
                view_match = re.search(r'view:\s+(\w+)\s+{', content)
                if not view_match:
                    continue
                    
                view_name = view_match.group(1)
                
                # Check if there's a sql_table_name definition (direct table reference)
                sql_table_match = re.search(r'sql_table_name:\s+[^;]+;', content)
                
                # Check if there's a derived_table definition (derived table)
                derived_table_match = re.search(r'derived_table:\s*{', content)
                
                # If there's a table reference or derived table definition, record this view
                if sql_table_match or derived_table_match:
                    views_with_table_reference.add(view_name)
        except Exception as e:
            print(f"Error checking table reference in {file_path}: {e}")
    
    # Find all LookML files
    model_files = glob.glob('models/*.lkml')
    # Also include models from root directory
    model_files += glob.glob('*.model.lkml')
    # Include any file containing "model" in its name
    model_files += glob.glob('*model*.lkml')
    # Remove duplicates
    model_files = list(set(model_files))
    
    print(f"Found {len(model_files)} model files to analyze")
    
    view_files = glob.glob('views/**/*.view.lkml', recursive=True)
    # Also include views from other directories
    view_files += glob.glob('**/*.view.lkml', recursive=True)
    # Remove duplicates
    view_files = list(set(view_files))
    
    all_lkml_files = model_files + view_files
    
    for file_path in all_lkml_files:
        try:
            file_basename = os.path.basename(file_path)
            # Extract model name, handling different naming conventions
            model_name = None
            if file_basename.endswith('.model.lkml'):
                model_name = file_basename.replace('.model.lkml', '')
            elif 'models/' in file_path and file_basename.endswith('.lkml'):
                model_name = file_basename.replace('.lkml', '')
                
            with open(file_path, 'r') as f:
                content = f.read()
                
                if 'explore:' in content:
                    # Find all explore definitions
                    explore_matches = re.finditer(r'explore:\s+(\w+)\s+{', content)
                    for explore_match in explore_matches:
                        explore_name = explore_match.group(1)
                        start_pos = explore_match.end()
                        
                        # Use model name from file if possible, otherwise use the directory name
                        if not model_name and 'models/' in file_path:
                            model_name = os.path.basename(os.path.dirname(file_path))
                        elif not model_name:
                            # Fallback for files outside models directory
                            model_name = "unknown_model"
                        
                        # Record explore information
                        explore_list[explore_name] = {
                            'model': model_name,
                            'file_path': file_path
                        }
                        
                        # Record which model this explore belongs to
                        explore_to_model[explore_name] = model_name
                    
                        # Find the corresponding closing bracket
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
                            print(f"Warning: Could not find end of explore block for {explore_name} in {file_path}")
                            continue  # Skip if we can't find the closing bracket
                            
                        explore_block = content[start_pos:end_pos]
                        
                        # The main view usually has the same name as the explore or is specified via from
                        from_match = re.search(r'from:\s+(\w+)', explore_block)
                        if from_match:
                            base_view = from_match.group(1)
                            explore_to_views[explore_name].add(base_view)
                            # If the explore name is different from base_view, record the alias relationship
                            if explore_name != base_view:
                                view_from_alias[explore_name] = base_view
                        else:
                            explore_to_views[explore_name].add(explore_name)
                        
                        # Find all join statements with different patterns
                        # 1. Standard style: join: view_name { ... }
                        standard_joins = re.finditer(r'join:\s+(\w+)\s+{', explore_block)
                        for join_match in standard_joins:
                            join_view = join_match.group(1)
                            join_start = join_match.end()
                            
                            # Find the end position of this join block
                            bracket_level = 1
                            join_end = join_start
                            for i in range(join_start, len(explore_block)):
                                if explore_block[i] == '{':
                                    bracket_level += 1
                                elif explore_block[i] == '}':
                                    bracket_level -= 1
                                    if bracket_level == 0:
                                        join_end = i
                                        break
                            
                            if join_end <= join_start:
                                print(f"Warning: Could not find end of join block for {join_view} in explore {explore_name}")
                                continue  # Skip if we can't find the closing bracket
                                
                            join_block = explore_block[join_start:join_end]
                            
                            # Add the join view to the explore's view list
                            explore_to_views[explore_name].add(join_view)
                            
                            # Look for "from:" statements in the join block, which indicates join_view is an alias view
                            from_in_join_match = re.search(r'from:\s+(\w+)', join_block)
                            if from_in_join_match:
                                from_view = from_in_join_match.group(1)
                                if join_view != from_view:
                                    # Record alias relationship
                                    view_from_alias[join_view] = from_view
                                    print(f"DEBUG - Detected alias view: {join_view} from {from_view}")
                            
                            # Check if unnest operation is used
                            if re.search(r'sql:\s+.*unnest\(', join_block, re.IGNORECASE) and join_view not in non_unnest_views and join_view not in views_with_table_reference:
                                unnest_views.add(join_view)
                                print(f"DEBUG - Detected UNNEST view: {join_view}")
                            
                            # Look for nested join statements in the join block
                            nested_joins = re.finditer(r'join:\s+(\w+)\s+{', join_block)
                            for nested_join in nested_joins:
                                nested_view = nested_join.group(1)
                                explore_to_views[explore_name].add(nested_view)
                                
                                # Get the nested join block
                                nested_start = nested_join.end()
                                nested_bracket_level = 1
                                nested_end = nested_start
                                for i in range(nested_start, len(join_block)):
                                    if join_block[i] == '{':
                                        nested_bracket_level += 1
                                    elif join_block[i] == '}':
                                        nested_bracket_level -= 1
                                        if nested_bracket_level == 0:
                                            nested_end = i
                                            break
                                
                                if nested_end <= nested_start:
                                    continue  # Skip if we can't find the closing bracket
                                    
                                nested_block = join_block[nested_start:nested_end]
                                
                                # Look for "from:" statements in the nested join block
                                nested_from_match = re.search(r'from:\s+(\w+)', nested_block)
                                if nested_from_match:
                                    nested_from_view = nested_from_match.group(1)
                                    if nested_view != nested_from_view:
                                        # Record alias relationship
                                        view_from_alias[nested_view] = nested_from_view
                                        print(f"DEBUG - Detected nested alias view: {nested_view} from {nested_from_view}")
                                
                                # Check if unnest operation is used
                                if re.search(r'sql:\s+.*unnest\(', nested_block, re.IGNORECASE) and nested_view not in non_unnest_views and nested_view not in views_with_table_reference:
                                    unnest_views.add(nested_view)
                                    print(f"DEBUG - Detected nested UNNEST view: {nested_view}")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    print(f"Analyzed {len(explore_to_views)} explores across all models")
    print(f"Identified {len(unnest_views)} views created through unnest")
    print(f"Identified {len(view_from_alias)} alias view relationships")
    return explore_to_views, unnest_views, explore_list, explore_to_model, view_from_alias

# Calculate the actual usage frequency for each view
def calculate_actual_usage(view_list, explore_usage, explore_to_views):
    actual_view_usage = defaultdict(int)
    
    # Initialize to original usage frequency (there's no original usage frequency here, set to 0)
    for view_name in view_list:
        actual_view_usage[view_name] = 0
    
    # Distribute the explore usage frequency to related views
    for explore_name, views in explore_to_views.items():
        if explore_name in explore_usage:
            usage = explore_usage[explore_name]
            for view in views:
                actual_view_usage[view] += usage
    
    return actual_view_usage

# Guess table name and citation type for nested views
def guess_table_info(view_name, view_list, unnest_views, actual_table_names):
    # Check if it's a view created through unnest
    if view_name in unnest_views:
        return [], 'unnest'
    
    # Check if it's in the actual table name mapping
    if view_name in actual_table_names and actual_table_names[view_name]:
        return actual_table_names[view_name], 'native'
    
    # For nested views, extract the parent view name
    if '__' in view_name:
        parent_view = view_name.split('__')[0]
        if parent_view in view_list:
            return view_list[parent_view]['table_names'], 'nested'
    
    # For tables ending with _snapshot, use special database and dataset
    if view_name.endswith('_snapshot'):
        base_name = view_name
        return [f'{SNAPSHOT_PROJECT}.{SNAPSHOT_DATASET}.{base_name}'], 'derived'
    
    # For views with common naming patterns, try to guess table name
    if view_name.startswith('dim_') or view_name.startswith('fact_'):
        base_name = view_name.replace('_v2', '') # Handle v2 versions
        return [f'{DEFAULT_PROJECT}.{DEFAULT_DATASET}.{base_name}'], 'derived'
    
    # For any other view, use standard format
    return [f'{DEFAULT_PROJECT}.{DEFAULT_DATASET}.{view_name}'], 'derived'

# Update table information in the view list
def update_view_table_info(view_list, actual_table_names, unnest_views, view_citation_types=None, view_from_alias=None, 
                           default_project=DEFAULT_PROJECT, default_dataset=DEFAULT_DATASET, 
                           snapshot_project=SNAPSHOT_PROJECT, snapshot_dataset=SNAPSHOT_DATASET):
    print("Updating view table information...")
    
    # If view_citation_types is not provided, initialize to empty dictionary
    if view_citation_types is None:
        view_citation_types = {}
    
    # If view_from_alias is not provided, initialize to empty dictionary
    if view_from_alias is None:
        view_from_alias = {}
    
    # Print the list of alias views for debugging
    print(f"DEBUG - Detected {len(view_from_alias)} alias view relationships in total")
    for alias_view, base_view in view_from_alias.items():
        print(f"DEBUG - Alias view: {alias_view} -> {base_view}")
    
    # Process view aliases - ensure this step is executed first to properly set derived_from type
    for alias_view, base_view in view_from_alias.items():
        if alias_view in view_list:
            # Mark as derived_from type
            view_list[alias_view]['citation_type'] = 'derived_from'
            # Record which view it's derived from
            view_list[alias_view]['derived_from'] = base_view
            
            # If the base view has table names, copy them
            if base_view in view_list and 'table_names' in view_list[base_view] and view_list[base_view]['table_names']:
                view_list[alias_view]['table_name'] = view_list[base_view]['table_name']
                view_list[alias_view]['table_names'] = view_list[base_view]['table_names'][:]
                print(f"DEBUG - Updated alias view: {alias_view} citation_type to derived_from, based on {base_view}, tables: {view_list[base_view]['table_names']}")
            else:
                # If the base view has no table names or is not in the view list, clear table names
                view_list[alias_view]['table_name'] = ""
                view_list[alias_view]['table_names'] = []
                print(f"DEBUG - Updated alias view: {alias_view} citation_type to derived_from, based on {base_view}, but base view has no table names")
    
    # Update table locations with actual table names extracted from view definitions
    for view_name, table_names in actual_table_names.items():
        if view_name in view_list and table_names and view_name not in view_from_alias:
            print(f"DEBUG - Updating {view_name} in view_list: {table_names}")
            
            view_list[view_name]['table_name'] = table_names[0] if table_names else ""
            view_list[view_name]['table_names'] = table_names[:]
            
            # Set citation type, preferring already identified type
            if view_name in view_citation_types:
                view_list[view_name]['citation_type'] = view_citation_types[view_name]
            else:
                # Don't override already set derived_from type
                if 'citation_type' not in view_list[view_name] or view_list[view_name]['citation_type'] != 'derived_from':
                    view_list[view_name]['citation_type'] = 'native'  # Default to native type
    
    # Check view list citation types before generating the report
    if len(view_list) > 0:
        print(f"DEBUG - Sample of view citation types before generating report:")
        for view_name, info in list(view_list.items())[:5]:  # Sample just a few to check
            print(f"DEBUG - {view_name} citation_type: {info.get('citation_type', 'none')}, tables: {info.get('table_names', [])}")
    
    # Update snapshot table locations
    for view_name, info in view_list.items():
        # Set citation_type based on type identified from view definitions
        if view_name in view_citation_types:
            info['citation_type'] = view_citation_types[view_name]
        
        # Only process snapshot tables not updated by actual table names
        if view_name not in actual_table_names and view_name.endswith('_snapshot'):
            # Update to correct path
            new_table_name = f'{snapshot_project}.{snapshot_dataset}.{view_name}'
            info['table_name'] = new_table_name
            info['table_names'] = [new_table_name]
            if 'citation_type' not in info or info['citation_type'] not in ['derived_explore']:
                info['citation_type'] = 'native'  # Set to native type
        
        # If it's a nested table and not defined in actual_table_names, get information from parent table
        if '__' in view_name and not info['table_names'] and view_name not in actual_table_names:
            parent_view = view_name.split('__')[0]
            if parent_view in view_list and view_list[parent_view]['table_names']:
                if 'citation_type' not in info or info['citation_type'] not in ['derived_explore']:
                    info['citation_type'] = 'nested'
                info['table_names'] = view_list[parent_view]['table_names'][:]
                if view_list[parent_view]['table_names']:
                    info['table_name'] = view_list[parent_view]['table_names'][0]
        
        # Update unnest views' citation_type
        if view_name in unnest_views:
            info['citation_type'] = 'unnest'
            # Clear table names, as unnest views are not directly associated with tables
            info['table_name'] = ''
            info['table_names'] = []
        
        # For views that still have no table names, try to guess based on naming rules
        if not info['table_names'] and view_name not in unnest_views and info['citation_type'] not in ['derived_explore']:
            # For views with common naming patterns, try to guess table name
            if view_name.startswith('dim_') or view_name.startswith('fact_'):
                base_name = view_name.replace('_v2', '') # Handle v2 versions
                info['table_name'] = f'{default_project}.{default_dataset}.{base_name}'
                info['table_names'] = [info['table_name']]
                
                # Auto-detect variants
                variants = auto_detect_related_tables(info['table_name'])
                for variant in variants:
                    if variant not in info['table_names']:
                        info['table_names'].append(variant)
                        
                if 'citation_type' not in info or info['citation_type'] not in ['derived_explore']:
                    info['citation_type'] = 'derived'
    
    return view_list 