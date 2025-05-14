#!/usr/bin/env python3
import re
import os
import glob
from collections import defaultdict
from looker_utils.utils import (
    extract_tables_from_liquid_block,
    extract_tables_from_sql,
    contains_explore_source
)

# Extract table names from a single view definition
def extract_tables_from_view_content(view_name, content):
    # This function processes the content of a single view and extracts table names
    tables = []
    
    # First check if it contains the explore_source keyword
    has_explore, explore_name = contains_explore_source(content, "")
    if has_explore:
        return [], 'derived_explore'
    
    # Preprocess content: split by lines
    content_lines = content.split('\n')
    # Filter out comment lines (lines starting with #)
    uncommented_content = '\n'.join([line for line in content_lines if not line.strip().startswith('#')])
    
    # Check for explore_source in the preprocessed content
    has_explore, explore_name = contains_explore_source(uncommented_content)
    if has_explore:
        print(f"DEBUG - Detected explore_source view in preprocessed content: {view_name}, explore: {explore_name}")
        return [], 'derived_explore'
    
    # ------- Improved derived_table block extraction -------
    derived_block = None
    dt_pos = content.find('derived_table')
    if dt_pos != -1:
        # Find the first opening brace after the keyword
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
                derived_block = content[brace_start + 1 : i - 1]
    
    if derived_block:
        
        # Check for explore_source in the derived_block
        has_explore, explore_name = contains_explore_source(derived_block)
        if has_explore:
            print(f"DEBUG - Detected explore_source in derived_table: {view_name}, explore: {explore_name}")
            return [], 'derived_explore'
    else:
        derived_block = None  # Ensure variable exists
    
    # Remove double quotes from uncommented_content for better table name matching
    # This helps handle different quoting styles in Looker (double quotes, backticks, or no quotes)
    uncommented_content_no_quotes = uncommented_content.replace('"', '')
    
    # Try to find sql_table_name definition (excluding comment lines)
    # Original regex only matches table names enclosed in backticks
    # sql_table_match = re.search(r'sql_table_name:\s+`([^`]+)`', uncommented_content)
    
    # New regex can handle project name, dataset and table name separately enclosed in backticks
    # Also handles standard format without quotes and with removed double quotes
    sql_table_match = re.search(
        r'sql_table_name:\s+(?:'
        r'`?([^`\s.]+)`?\.`?([^`\s.]+)`?\.`?([^`\s.;]+)`?|'  # Format with potential backticks: `project`.`dataset`.`table`
        r'`([^`]+)`|'  # Format with entire reference in backticks: `project.dataset.table`
        r'([^`\s.]+)\.([^`\s.]+)\.([^`\s.;]+)|'  # Format without quotes: project.dataset.table
        r'([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]+)'  # Simple format after double quotes removal
        r')',
        uncommented_content_no_quotes
    )
    
    if sql_table_match:
        if sql_table_match.group(4):  # Original format: `project.dataset.table`
            table_name = sql_table_match.group(4)
        elif sql_table_match.group(7):  # Format without quotes or backticks: project.dataset.table
            project = sql_table_match.group(5)
            dataset = sql_table_match.group(6)
            table = sql_table_match.group(7)
            table_name = f"{project}.{dataset}.{table}"
        elif sql_table_match.group(10):  # Simple format after double quotes removal
            project = sql_table_match.group(8)
            dataset = sql_table_match.group(9)
            table = sql_table_match.group(10)
            table_name = f"{project}.{dataset}.{table}"
        else:  # New format: `project`.`dataset`.`table` or variants
            project = sql_table_match.group(1)
            dataset = sql_table_match.group(2)
            table = sql_table_match.group(3)
            table_name = f"{project}.{dataset}.{table}"
        
        tables.append(table_name)
        return tables, 'native'
    
    # Check the content of derived_table (if extracted)
    if derived_block:
        # Remove double quotes from derived_block for better SQL parsing
        derived_block_no_quotes = derived_block.replace('"', '')
        
        # Extract different formats of SQL definitions
        sql_match = None
        # 1) explicit block until ;; (LookML standard)
        sql_match = re.search(r'sql\s*:\s*([\s\S]*?);;', derived_block_no_quotes, re.DOTALL)
        if not sql_match:
            # 2) triple quotes / braces fallback
            for pattern in [r'sql:\s*{{{([^}]+)}}}', r'sql:\s*"""([\s\S]+?)"""', r'sql:\s*{([\s\S]+?)}', r'sql:\s*"([^"]+)"']:
                sql_match = re.search(pattern, derived_block_no_quotes, re.DOTALL)
                if sql_match:
                    break
        
        if sql_match:
            sql_text = sql_match.group(1)
            # Remove double quotes from SQL text for better table extraction
            sql_text_no_quotes = sql_text.replace('"', '')
            
            # Improvement: First try to extract table names from all possible Liquid conditional blocks, regardless of conditions
            # This ensures capturing all possible table dependencies
            liquid_tables = extract_tables_from_liquid_block(sql_text_no_quotes, True)
            if liquid_tables:
                print(f"DEBUG - Tables extracted from Liquid blocks in view {view_name}: {liquid_tables}")
                tables.extend(liquid_tables)
            
            # Then use regular SQL parsing to extract the remaining table names
            extracted_tables = extract_tables_from_sql(sql_text_no_quotes, True)
            if extracted_tables:
                print(f"DEBUG - Tables extracted from SQL in view {view_name}: {extracted_tables}")
                for table in extracted_tables:
                    if table not in tables:
                        tables.append(table)
        
        # If no SQL definition is found, search for table references directly in the entire derived_block
        else:
            # Check directly referenced tables (using backticks)
            table_refs = re.finditer(r'`([^`]+\.[^`]+\.[^`]+)`', derived_block_no_quotes)
            for match in table_refs:
                table_name = match.group(1)
                if table_name not in tables:
                    tables.append(table_name)
            
            # Check for regular references without backticks (e.g., "schema.dataset.table")
            table_refs_no_backticks = re.finditer(r'([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]+)', derived_block_no_quotes)
            for match in table_refs_no_backticks:
                table_name = f"{match.group(1)}.{match.group(2)}.{match.group(3)}"
                if table_name not in tables:
                    tables.append(table_name)
            
            # Try to extract table references from Liquid blocks
            liquid_tables = extract_tables_from_liquid_block(derived_block_no_quotes, True)
            if liquid_tables:
                print(f"DEBUG - Tables extracted from Liquid blocks in derived table block for view {view_name}: {liquid_tables}")
                for table in liquid_tables:
                    if table not in tables:
                        tables.append(table)
    
    # Improvement: If multiple table references are found, try to select the most relevant one as the main table
    if len(tables) > 0:
        if len(tables) > 1:
            for i, table in enumerate(tables):
                parts = table.split('.')
                if len(parts) == 3:
                    # Check if table name part is equal to project name part
                    if parts[0] == parts[2]:
                        # This might be an incorrect extraction attempt, try to find a better replacement
                        for other_table in tables:
                            if other_table != table and other_table.endswith(view_name) or view_name in other_table:
                                # Find a more relevant table, set it as the main table
                                tables.remove(other_table)
                                tables.insert(0, other_table)
                                break
                        
                        # Remove incorrect table references
                        tables.remove(table)
                        break
        
        # Try to find table names similar to the view name
        view_base_name = view_name.replace('fact_', '').replace('dim_', '')
        matched_tables = []
        
        for table in tables:
            table_parts = table.split('.')
            if len(table_parts) == 3:
                table_base = table_parts[2]  # Extract table name from full path
            elif len(table_parts) == 2:
                table_base = table_parts[1]
            else:
                table_base = table_parts[0]
                
            # Check if table name is similar to the view name
            table_base = table_base.replace('fact_', '').replace('dim_', '')
            
            if view_base_name in table_base or table_base in view_base_name:
                matched_tables.append(table)
        
        # If matching tables are found, use the first matching table as the main table
        if matched_tables:
            primary_table = matched_tables[0]
            # Move this table to the front of the tables list
            if primary_table in tables:
                tables.remove(primary_table)
            tables.insert(0, primary_table)
            
        # Prioritize tables with shortest actual table name
        if len(tables) > 1:
            # Extract actual table names (last part of full table path)
            table_with_lengths = []
            for table in tables:
                parts = table.split('.')
                actual_table = parts[-1] if len(parts) > 0 else table
                table_with_lengths.append((len(actual_table), table))
            
            # Sort by actual table name length
            table_with_lengths.sort()
            
            if table_with_lengths:
                shortest_table = table_with_lengths[0][1]
                # Move the table with shortest name to the front
                tables.remove(shortest_table)
                tables.insert(0, shortest_table)
    
    # Summary of analysis results
    if tables:
        print(f"DEBUG - View {view_name} analysis results: Found {len(tables)} table references: {tables}")
    else:
        print(f"DEBUG - View {view_name} analysis results: No table references found")
    
    # New: If derived_table is used but actually references a real table, set citation_type to 'native'
    if derived_block and tables:
        return tables, 'native'
    
    return tables, 'native' if tables else ''

# Extract actual table names from all view definition files
def extract_actual_table_names():
    actual_table_names = {}
    view_citation_types = {}  # New: record citation type for each view
    
    # Scan all view files in any directory
    view_files = glob.glob('views/**/*.view.lkml', recursive=True)
    # Also search in other directories for view files
    view_files += glob.glob('**/*.view.lkml', recursive=True)
    # Remove duplicates
    view_files = list(set(view_files))
    
    print(f"DEBUG - Found {len(view_files)} view files across all directories")
    
    # Create a set to record view files in directories that might contain derived views
    derived_view_files = [f for f in view_files if 'derived_views/' in f or 'derived_tables/' in f or 'custom_views/derived_tables/' in f]
    print(f"DEBUG - Found {len(derived_view_files)} potential derived view files")
    
    # First process views in derived_views-like directories, they might be based on explore_source
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
    
    # Then process all other view files
    for file_path in view_files:
        try:
            # Skip already processed derived_views
            if file_path in derived_view_files and any(view_name in view_citation_types for view_name in view_citation_types if view_citation_types[view_name] == 'derived_explore'):
                continue
                
            with open(file_path, 'r') as f:
                content = f.read()
                
                # Extract all view names, not just the first one
                view_matches = re.finditer(r'view:\s+(\w+)\s+{', content)
                for view_match in view_matches:
                    view_name = view_match.group(1)
                    
                    # Skip views that have already been processed
                    if view_name in view_citation_types and view_citation_types[view_name] == 'derived_explore':
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
    print(f"Total table references extracted: {sum(len(tables) for tables in actual_table_names.values())}")
    
    return actual_table_names, view_citation_types 