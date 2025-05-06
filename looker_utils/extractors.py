#!/usr/bin/env python3
import re
import os
import glob
from collections import defaultdict
from looker_utils.utils import (
    extract_tables_from_liquid_block,
    extract_tables_from_sql,
    contains_explore_source,
    auto_detect_related_tables
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
    
    # Try to find sql_table_name definition (excluding comment lines)
    # 原始正则表达式只匹配整个表名在反引号内的情况
    # sql_table_match = re.search(r'sql_table_name:\s+`([^`]+)`', uncommented_content)
    
    # 新的正则表达式能够处理项目名、数据集和表名可能分别用反引号包围的情况
    sql_table_match = re.search(r'sql_table_name:\s+(?:`?([^`\s.]+)`?\.`?([^`\s.]+)`?\.`?([^`\s.;]+)`?|`([^`]+)`)', uncommented_content)
    
    if sql_table_match:
        if sql_table_match.group(4):  # 原格式: `project.dataset.table`
            table_name = sql_table_match.group(4)
        else:  # 新格式: `project`.`dataset`.`table` 或变体
            project = sql_table_match.group(1)
            dataset = sql_table_match.group(2)
            table = sql_table_match.group(3)
            table_name = f"{project}.{dataset}.{table}"
        
        tables.append(table_name)
        return tables, 'native'
    
    # Check the content of derived_table (if extracted)
    if derived_block:
        
        # Extract different formats of SQL definitions
        sql_match = None
        # 1) explicit block until ;; (LookML standard)
        sql_match = re.search(r'sql\s*:\s*([\s\S]*?);;', derived_block, re.DOTALL)
        if not sql_match:
            # 2) triple quotes / braces fallback
            for pattern in [r'sql:\s*{{{([^}]+)}}}', r'sql:\s*"""([\s\S]+?)"""', r'sql:\s*{([\s\S]+?)}', r'sql:\s*"([^"]+)"']:
                sql_match = re.search(pattern, derived_block, re.DOTALL)
                if sql_match:
                    break
        
        if sql_match:
            sql_text = sql_match.group(1)
            # First check if there are Liquid conditional blocks
            liquid_tables = extract_tables_from_liquid_block(sql_text, False)
            if liquid_tables:
                tables.extend(liquid_tables)
            
            # Then use more general SQL parsing to extract table names
            extracted_tables = extract_tables_from_sql(sql_text)
            for table in extracted_tables:
                if table not in tables:
                    tables.append(table)
        
        # If no SQL definition is found, search for table references directly in the entire derived_block
        else:
            # Check directly referenced tables (using backticks)
            table_refs = re.finditer(r'`([^`]+\.[^`]+\.[^`]+)`', derived_block)
            for match in table_refs:
                table_name = match.group(1)
                if table_name not in tables:
                    tables.append(table_name)
            
            # Check table references in Liquid conditional blocks
            liquid_tables = extract_tables_from_liquid_block(derived_block, False)
            for table in liquid_tables:
                if table not in tables:
                    tables.append(table)
    
    # 改进：如果找到多个表引用，尝试选择最相关的作为主表
    if len(tables) > 0:
        # 处理fact_purchased_items特殊情况：检查表名是否不正确（包含项目名，如curated-dwh.analytics_prod.curated-dwh）
        if len(tables) > 1:
            for i, table in enumerate(tables):
                parts = table.split('.')
                if len(parts) == 3:
                    # 检查表名部分是否等于项目名部分
                    if parts[0] == parts[2]:
                        # 这是一个可能的错误提取，尝试找到更好的替代表
                        for other_table in tables:
                            if other_table != table and other_table.endswith(view_name) or view_name in other_table:
                                # 找到更相关的表，将其作为主表
                                tables.remove(other_table)
                                tables.insert(0, other_table)
                                break
                        
                        # 移除错误的表引用
                        tables.remove(table)
                        break
        
        # 尝试查找与视图名相似的表名
        view_base_name = view_name.replace('fact_', '').replace('dim_', '')
        matched_tables = []
        
        for table in tables:
            table_parts = table.split('.')
            if len(table_parts) == 3:
                table_base = table_parts[2]  # 从全路径中提取表名
            elif len(table_parts) == 2:
                table_base = table_parts[1]
            else:
                table_base = table_parts[0]
                
            # 检查表名是否与视图名相似
            table_base = table_base.replace('fact_', '').replace('dim_', '')
            
            if view_base_name in table_base or table_base in view_base_name:
                matched_tables.append(table)
        
        # 如果找到匹配的表，使用第一个匹配的表作为主表
        if matched_tables:
            primary_table = matched_tables[0]
            # 将该表移到tables列表的首位
            if primary_table in tables:
                tables.remove(primary_table)
            tables.insert(0, primary_table)
        
        # 特殊处理fact_purchased_items视图
        if view_name == 'fact_purchased_items' and len(tables) > 0:
            # 查找fact_purchased_line_items表，将其设为主表
            for table in tables:
                if 'fact_purchased_line_items' in table and not table.endswith('_streaming') and not table.endswith('_flip'):
                    # 将其设为主表
                    tables.remove(table)
                    tables.insert(0, table)
                    break
    
    # 新增：如果使用了derived_table但实际引用了真实表，将citation_type设置为'native'
    if derived_block and tables:
        return tables, 'native'
    
    return tables, 'native' if tables else ''

# Extract actual table names from all view definition files
def extract_actual_table_names():
    actual_table_names = {}
    view_citation_types = {}  # New: record citation type for each view
    
    # Scan all view files
    view_files = glob.glob('views/**/*.view.lkml', recursive=True)
    
    # Create a set to record view files in the derived_views directory
    derived_view_files = [f for f in view_files if 'derived_views/' in f]
    print(f"DEBUG - Found {len(derived_view_files)} view files in the derived_views directory")
    
    # First process views in the derived_views directory, they might be based on explore_source
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
                        print(f"DEBUG - Found potential explore_source view in derived_views directory: {view_name}")
                        
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
            if 'derived_views/' in file_path and any(view_name in view_citation_types for view_name in view_citation_types if view_citation_types[view_name] == 'derived_explore'):
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