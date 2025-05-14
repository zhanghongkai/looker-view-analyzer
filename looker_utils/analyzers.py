#!/usr/bin/env python3
import re
import os
import glob
from collections import defaultdict
from looker_utils.utils import (
    DEFAULT_PROJECT, 
    DEFAULT_DATASET, 
    SNAPSHOT_PROJECT, 
    SNAPSHOT_DATASET,
    extract_tables_from_liquid_block,
    extract_tables_from_sql,
    contains_explore_source
)

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
                            if re.search(r'(?i)sql:\s+.*unnest\(', join_block) and join_view not in non_unnest_views and join_view not in views_with_table_reference:
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
                                if re.search(r'(?i)sql:\s+.*unnest\(', nested_block) and nested_view not in non_unnest_views and nested_view not in views_with_table_reference:
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
                           view_source_definitions=None,
                           default_project=DEFAULT_PROJECT, default_dataset=DEFAULT_DATASET, 
                           snapshot_project=SNAPSHOT_PROJECT, snapshot_dataset=SNAPSHOT_DATASET):
    print("Updating view table information...")
    
    # If view_citation_types is not provided, initialize to empty dictionary
    if view_citation_types is None:
        view_citation_types = {}
    
    # If view_from_alias is not provided, initialize to empty dictionary
    if view_from_alias is None:
        view_from_alias = {}
        
    # If view_source_definitions is not provided, initialize to empty dictionary
    if view_source_definitions is None:
        view_source_definitions = {}
    
    # Print the list of alias views for debugging
    print(f"DEBUG - Detected {len(view_from_alias)} alias view relationships in total")
    for alias_view, base_view in view_from_alias.items():
        print(f"DEBUG - Alias view: {alias_view} -> {base_view}")
    
    # Add data source definitions to view information
    for view_name, source_def in view_source_definitions.items():
        if view_name in view_list:
            view_list[view_name]['source_type'] = source_def['type']
            view_list[view_name]['source_definition'] = source_def['definition']
    
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
    
    # Process actual table names - now filters out two-part table names (like CUSTOM_SYSTEM.PUBLIC) without adding default project prefix
    for view_name, table_names in actual_table_names.items():
        if view_name in view_list and table_names and view_name not in view_from_alias:
            print(f"DEBUG - Updating {view_name} in view_list: {table_names}")
            
            # Filter out two-part table names, don't add default project prefix
            filtered_table_names = []
            for table_name in table_names:
                # Count the dots to determine if it's a two-part table name
                dots_count = table_name.count('.')
                
                # If it's a two-part table name, skip adding to the filtered table names list
                if dots_count == 1:  # E.g., CUSTOM_SYSTEM.PUBLIC
                    print(f"DEBUG - Skipping two-part table name: {table_name}")
                    continue
                
                filtered_table_names.append(table_name)
            
            # Update view information with the filtered table names list
            if filtered_table_names:
                view_list[view_name]['table_name'] = filtered_table_names[0] if filtered_table_names else ""
                view_list[view_name]['table_names'] = filtered_table_names[:]
            else:
                # If there are no table names after filtering, clear table name information
                view_list[view_name]['table_name'] = ""
                view_list[view_name]['table_names'] = []
            
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
                
                if 'citation_type' not in info or info['citation_type'] not in ['derived_explore']:
                    info['citation_type'] = 'derived'
        
        # If we already have table names, don't add any variants
        
    return view_list 

# Newly added function that integrates relationship analysis and table information extraction
def analyze_explores_and_extract_tables():
    print("Analyzing explore-view relationships and extracting table information...")
    
    # Step 1: Relationship analysis
    explore_to_views, unnest_views, explore_list, explore_to_model, view_from_alias = analyze_explores()
    print(f"Analyzed {len(explore_to_views)} explores")
    print(f"Identified {len(unnest_views)} views created through unnest")
    print(f"Identified {len(view_from_alias)} alias view relationships")
    
    # Step 2: Extract view data source definitions (for debugging)
    view_source_definitions = extract_view_source_definitions()
    print(f"Extracted data source definitions for {len(view_source_definitions)} views")
    
    # Step 3: Normalize source information definitions
    normalized_view_source_definitions = normalize_source_definitions(view_source_definitions)
    print(f"Normalized data source definitions for {len(normalized_view_source_definitions)} views")
    
    # Step 4: Extract table information from view data source definitions
    actual_table_names, view_citation_types = extract_tables_from_views(normalized_view_source_definitions)
    print(f"Extracted table names for {len(actual_table_names)} views from view definitions")
    print(f"Total number of table references extracted: {sum(len(tables) for tables in actual_table_names.values())}")
    
    return explore_to_views, unnest_views, explore_list, explore_to_model, view_from_alias, actual_table_names, view_citation_types, view_source_definitions

# Normalize source information definitions, handling syntax differences between database dialects
def normalize_source_definitions(view_source_definitions):
    print("Normalizing view data source definitions...")
    normalized_definitions = {}
    
    for view_name, source_info in view_source_definitions.items():
        # Create a copy of the source information
        normalized_info = source_info.copy()
        
        # If it's an SQL type definition, normalization is needed
        if source_info['type'] in ['derived_table_sql', 'sql_table_name']:
            # Remove all double quotes to standardize source definition format
            normalized_definition = source_info['definition'].replace('"', '')
            normalized_info['normalized_definition'] = normalized_definition
        else:
            # Non-SQL type definitions remain unchanged
            normalized_info['normalized_definition'] = source_info['definition']
        
        normalized_definitions[view_name] = normalized_info
    
    return normalized_definitions

# Extract view data source definitions for debugging
def extract_view_source_definitions():
    view_source_definitions = {}
    
    # Find all view files
    view_files = glob.glob('views/**/*.view.lkml', recursive=True)
    # Also look for view files in other directories
    view_files += glob.glob('**/*.view.lkml', recursive=True)
    # Remove duplicates
    view_files = list(set(view_files))
    
    print(f"DEBUG - Searching for view data source definitions in all directories, found {len(view_files)} view files")
    
    # Special focus on fact_purchased_orders view
    fact_purchased_orders_file = None
    for file_path in view_files:
        if 'fact_purchased_orders.view.lkml' in file_path:
            fact_purchased_orders_file = file_path
            print(f"DEBUG - Found fact_purchased_orders view file: {fact_purchased_orders_file}")
            break
    
    # Priority processing for fact_purchased_orders view file
    if fact_purchased_orders_file:
        try:
            print(f"DEBUG - Special processing for fact_purchased_orders view file")
            with open(fact_purchased_orders_file, 'r') as f:
                content = f.read()
                # First try to extract the derived_table block
                dt_match = re.search(r'derived_table\s*:\s*{', content)
                if dt_match:
                    dt_pos = dt_match.start()
                    print(f"DEBUG - Found derived_table start position in fact_purchased_orders: {dt_pos}")
                    
                    # Extract the entire derived_table block, handling nested braces
                    brace_start = content.find('{', dt_pos)
                    if brace_start != -1:
                        brace_level = 1
                        i = brace_start + 1
                        while i < len(content) and brace_level > 0:
                            if content[i] == '{':
                                brace_level += 1
                            elif content[i] == '}':
                                brace_level -= 1
                            i += 1
                        
                        if brace_level == 0:
                            derived_block = content[brace_start+1:i-1].strip()
                            print(f"DEBUG - Successfully extracted derived_table block from fact_purchased_orders, length: {len(derived_block)}")
                            
                            # Extract SQL part - first find sql: marked content to ;;
                            sql_pos = derived_block.find("sql:")
                            if sql_pos != -1:
                                sql_pos += 4  # Skip "sql:"
                                end_pos = derived_block.find(";;", sql_pos)
                                if end_pos != -1:
                                    sql_text = derived_block[sql_pos:end_pos].strip()
                                    # Store extracted SQL
                                    view_source_definitions["fact_purchased_orders"] = {
                                        'type': 'derived_table_sql',
                                        'definition': sql_text
                                    }
                                    print(f"DEBUG - Successfully extracted SQL definition from fact_purchased_orders, length: {len(sql_text)}")
        except Exception as e:
            print(f"Special processing for fact_purchased_orders view file failed: {e}")
    
    # Process all view files
    for file_path in view_files:
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                
                # Preprocess content: split by line
                content_lines = content.split('\n')
                # Filter out comment lines (lines starting with #)
                uncommented_content = '\n'.join([line for line in content_lines if not line.strip().startswith('#')])
                
                # Extract view names using preprocessed content
                view_matches = re.finditer(r'view:\s+(\w+)\s+{', uncommented_content)
                for view_match in view_matches:
                    view_name = view_match.group(1)
                    
                    # Skip if fact_purchased_orders has already been processed
                    if view_name == "fact_purchased_orders" and "fact_purchased_orders" in view_source_definitions:
                        continue
                        
                    view_start_pos = view_match.start()
                    
                    # Find the end position of this view definition (using uncommented_content)
                    bracket_level = 0
                    view_end_pos = None
                    in_view = False
                    
                    for i, char in enumerate(uncommented_content[view_start_pos:]):
                        if char == '{':
                            bracket_level += 1
                            in_view = True
                        elif char == '}':
                            bracket_level -= 1
                            if in_view and bracket_level == 0:
                                view_end_pos = view_start_pos + i + 1
                                break
                    
                    if view_end_pos is None:
                        continue  # Cannot determine the end position of the view
                    
                    # Extract the content of the current view (using uncommented_content)
                    view_content = uncommented_content[view_start_pos:view_end_pos]
                    
                    # Extract sql_table_name definition (using uncommented_content)
                    sql_table_match = re.search(r'sql_table_name:\s+([^;]+);', view_content)
                    if sql_table_match:
                        view_source_definitions[view_name] = {
                            'type': 'sql_table_name',
                            'definition': sql_table_match.group(1).strip()
                        }
                        continue
                    
                    # Extract derived_table definition - enhanced version
                    derived_table_found = False
                    
                    # 1. First find the derived_table block
                    dt_match = re.search(r'derived_table\s*:\s*{', view_content)
                    if dt_match:
                        dt_pos = dt_match.start()
                        if dt_pos != -1:
                            # 2. Extract the entire derived_table block, handling nested braces
                            brace_start = view_content.find('{', dt_pos)
                            if brace_start != -1:
                                brace_level = 1
                                i = brace_start + 1
                                while i < len(view_content) and brace_level > 0:
                                    if view_content[i] == '{':
                                        brace_level += 1
                                    elif view_content[i] == '}':
                                        brace_level -= 1
                                    i += 1
                                
                                if brace_level == 0:
                                    derived_block = view_content[brace_start+1:i-1].strip()
                                    derived_table_found = True
                                    
                                    # 3. Check if there is explore_source
                                    if "explore_source:" in derived_block:
                                        explore_match = re.search(r'explore_source:\s+(\w+)', derived_block)
                                        if explore_match:
                                            explore_name = explore_match.group(1)
                                            view_source_definitions[view_name] = {
                                                'type': 'explore_source',
                                                'definition': f"explore_source: {explore_name}"
                                            }
                                            continue
                                    
                                    # 4. Extract SQL query - handle complex SQL blocks and Liquid templates
                                    if "sql:" in derived_block:
                                        # Find content after sql:
                                        sql_pos = derived_block.find("sql:")
                                        if sql_pos != -1:
                                            sql_pos += 4  # Skip "sql:"
                                            # Find the ending double semicolon
                                            end_pos = derived_block.find(";;", sql_pos)
                                            if end_pos != -1:
                                                sql_text = derived_block[sql_pos:end_pos].strip()
                                                view_source_definitions[view_name] = {
                                                    'type': 'derived_table_sql',
                                                    'definition': sql_text
                                                }
                                                continue
                    
                    # If derived_table was not found through the above method, try a looser match
                    if not derived_table_found and "derived_table" in view_content:
                        # Simply extract the block starting from derived_table
                        dt_pos = view_content.find("derived_table")
                        if dt_pos != -1:
                            # Find the double semicolon mark afterwards
                            dt_end = view_content.find(";;", dt_pos)
                            if dt_end != -1:
                                dt_block = view_content[dt_pos:dt_end+2].strip()
                                # Try to extract the SQL part
                                if "sql:" in dt_block:
                                    sql_start = dt_block.find("sql:") + 4
                                    sql_text = dt_block[sql_start:dt_block.find(";;", sql_start)].strip()
                                    view_source_definitions[view_name] = {
                                        'type': 'derived_table_sql',
                                        'definition': sql_text
                                    }
                                    continue
                    
                    # If no definition was found, record as unknown
                    if view_name not in view_source_definitions:
                        view_source_definitions[view_name] = {
                            'type': 'unknown',
                            'definition': 'No sql_table_name or derived_table found'
                        }
                    
        except Exception as e:
            print(f"Error extracting view data source definition {file_path}: {e}")
    
    print(f"Extracted data source definitions for {len(view_source_definitions)} views")
    
    # Check if fact_purchased_orders is in the results
    if "fact_purchased_orders" in view_source_definitions:
        print(f"Successfully extracted source definition type for fact_purchased_orders: {view_source_definitions['fact_purchased_orders']['type']}")
        print(f"Definition length: {len(view_source_definitions['fact_purchased_orders']['definition'])}")
    else:
        print(f"Warning: Could not extract source definition for fact_purchased_orders")
    
    return view_source_definitions

# Function to extract table information, focusing on extracting table info from view definitions
def extract_tables_from_views(normalized_view_source_definitions=None):
    actual_table_names = {}
    view_citation_types = {}  # Record citation type for each view
    
    # Find all view files
    view_files = glob.glob('views/**/*.view.lkml', recursive=True)
    # Also look for view files in other directories
    view_files += glob.glob('**/*.view.lkml', recursive=True)
    # Remove duplicates
    view_files = list(set(view_files))
    
    print(f"DEBUG - Found {len(view_files)} view files in all directories")
    
    # Create a set to record view files in directories that might contain derived views
    derived_view_files = [f for f in view_files if 'derived_views/' in f or 'derived_tables/' in f or 'custom_views/derived_tables/' in f]
    print(f"DEBUG - Found {len(derived_view_files)} potential derived view files")
    
    # First process views in derived view directories, they might be based on explore_source
    for file_path in derived_view_files:
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                
                # Try to find the explore_source keyword
                if 'explore_source:' in content or 'explore_source :' in content:
                    # Extract view name
                    view_match = re.search(r'view:\s+(\w+)\s+{', content)
                    if view_match:
                        view_name = view_match.group(1)
                        print(f"DEBUG - Found potential explore_source view in derived directory: {view_name}")
                        
                        # Check if it actually contains explore_source
                        has_explore, explore_name = contains_explore_source(content, view_name)
                        if has_explore:
                            view_citation_types[view_name] = 'derived_explore'
                            print(f"DEBUG - Marked {view_name} as derived_explore type")
        except Exception as e:
            print(f"Error processing derived view {file_path}: {e}")
    
    # Process views using normalized source definitions (if provided)
    if normalized_view_source_definitions:
        for view_name, source_info in normalized_view_source_definitions.items():
            if source_info['type'] == 'derived_table_sql':
                # Extract table names using normalized definition
                sql_text = source_info.get('normalized_definition', source_info['definition'])
                tables = extract_tables_from_sql(sql_text)
                if tables:
                    actual_table_names[view_name] = tables
                    view_citation_types[view_name] = 'native'
            elif source_info['type'] == 'sql_table_name':
                # Use normalized sql_table_name
                table_name = source_info.get('normalized_definition', source_info['definition'])
                if table_name:
                    actual_table_names[view_name] = [table_name]
                    view_citation_types[view_name] = 'native'
    
    # Then process all other view files
    for file_path in view_files:
        try:
            # Skip already processed derived views or views already processed using normalized source definitions
            if ((file_path in derived_view_files and 
                any(view_name in view_citation_types for view_name in view_citation_types if view_citation_types[view_name] == 'derived_explore')) or
                (normalized_view_source_definitions and 
                any(view_name in normalized_view_source_definitions and view_name in actual_table_names for view_name in actual_table_names))):
                continue
            
            with open(file_path, 'r') as f:
                content = f.read()
                
                # Extract all view names, not just the first one
                view_matches = re.finditer(r'view:\s+(\w+)\s+{', content)
                for view_match in view_matches:
                    view_name = view_match.group(1)
                    
                    # Skip views that have already been processed
                    if (view_name in view_citation_types and view_citation_types[view_name] == 'derived_explore') or (
                        normalized_view_source_definitions and view_name in normalized_view_source_definitions and view_name in actual_table_names):
                        continue
                    
                    view_start_pos = view_match.start()
                    
                    # Find the end position of this view definition
                    # Calculate the nesting level of curly braces
                    bracket_level = 0
                    view_end_pos = None
                    in_view = False
                    
                    for i, char in enumerate(content[view_start_pos:]):
                        if char == '{':
                            bracket_level += 1
                            in_view = True
                        elif char == '}':
                            bracket_level -= 1
                            if in_view and bracket_level == 0:
                                view_end_pos = view_start_pos + i + 1
                                break
                    
                    if view_end_pos is None:
                        continue  # Cannot determine the end position of the view
                    
                    # Extract the content of the current view
                    view_content = content[view_start_pos:view_end_pos]
                    
                    # Call the function to process a single view
                    tables, citation_type = extract_tables_from_view_content(view_name, view_content)
                    
                    # Record citation type
                    if citation_type:
                        view_citation_types[view_name] = citation_type
                    
                    # Only add to the dictionary if table names are found
                    if tables:
                        actual_table_names[view_name] = tables
                    
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    print(f"Extracted table names for {len(actual_table_names)} views from view definitions")
    print(f"Total number of table references extracted: {sum(len(tables) for tables in actual_table_names.values())}")
    
    return actual_table_names, view_citation_types

# Extract table names from a single view's content
def extract_tables_from_view_content(view_name, view_content):
    tables = []
    citation_type = None
    
    # Try to find sql_table_name definition (excluding comment lines)
    sql_table_pattern = re.compile(r'sql_table_name:\s*(?!//)(.*?)\s*;;', re.DOTALL)
    sql_table_match = sql_table_pattern.search(view_content)
    
    if sql_table_match:
        sql_table_name = sql_table_match.group(1).strip()
        
        # Complex pattern to handle various SQL table reference formats
        table_pattern = re.compile(
            r'`?([^`\s.]+)`?\.`?([^`\s.]+)`?\.`?([^`\s.;]+)`?|'  # Format with optional backticks: `project`.`dataset`.`table`
            r'`([^`]+)`|'  # Format with entire reference in backticks: `project.dataset.table`
            r'([^`\s.]+)\.([^`\s.]+)\.([^`\s.;]+)|'  # Format without quotes: project.dataset.table
            r'([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]+)'  # Simple format after removing double quotes
        )
        
        # Try to find table name
        sql_table_match = table_pattern.search(sql_table_name)
        if sql_table_match:
            citation_type = 'native'
            
            if sql_table_match.group(4):  # Original format: `project.dataset.table`
                tables.append(sql_table_match.group(4))
            elif sql_table_match.group(7):  # Format without quotes or backticks: project.dataset.table
                table_name = f"{sql_table_match.group(5)}.{sql_table_match.group(6)}.{sql_table_match.group(7)}"
                tables.append(table_name)
            elif sql_table_match.group(10):  # Simple format after removing double quotes
                table_name = f"{sql_table_match.group(8)}.{sql_table_match.group(9)}.{sql_table_match.group(10)}"
                tables.append(table_name)
            else:  # New format: `project`.`dataset`.`table` or variations
                table_name = f"{sql_table_match.group(1)}.{sql_table_match.group(2)}.{sql_table_match.group(3)}"
                tables.append(table_name)
        else:
            # If no table pattern match, just record the raw sql_table_name
            tables.append(sql_table_name)
    else:
        # Check for derived_table content (if extracted)
        derived_table_pattern = re.compile(r'derived_table\s*{(.*?)}\s*;;', re.DOTALL)
        derived_table_match = derived_table_pattern.search(view_content)
        
        if derived_table_match:
            derived_table_content = derived_table_match.group(1).strip()
            
            # Check if it has explore_source
            explore_pattern = re.compile(r'explore_source:\s*(\w+)', re.DOTALL)
            explore_match = explore_pattern.search(derived_table_content)
            
            if explore_match:
                # Derived from explore_source
                citation_type = 'derived_explore'
                explore_name = explore_match.group(1)
                tables.append(f"explore:{explore_name}")
            elif 'sql:' in derived_table_content:
                # Derived from SQL
                citation_type = 'derived_sql'
                sql_pattern = re.compile(r'sql:\s*(.*?)(?:;;|$)', re.DOTALL)
                sql_match = sql_pattern.search(derived_table_content)
                
                if sql_match:
                    sql_content = sql_match.group(1).strip()
                    tables = extract_tables_from_sql(sql_content)
    
    return tables, citation_type 