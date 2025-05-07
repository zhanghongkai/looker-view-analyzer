#!/usr/bin/env python3
import re
import os
import glob
from collections import defaultdict
from looker_utils.utils import (
    auto_detect_related_tables, 
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
    
    # 添加数据来源定义到视图信息中
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
                
                # Auto-detect variants - only call this when we've had to guess the table name
                # This prevents overriding actual detected tables with default project variants
                variants = auto_detect_related_tables(info['table_name'])
                for variant in variants:
                    if variant not in info['table_names']:
                        info['table_names'].append(variant)
                        
                if 'citation_type' not in info or info['citation_type'] not in ['derived_explore']:
                    info['citation_type'] = 'derived'
        
        # If we already have table names, make sure we're generating variants with the correct project prefix
        elif info['table_names'] and 'citation_type' in info and info['citation_type'] == 'native':
            original_tables = info['table_names'][:]
            for original_table in original_tables:
                # Only generate variants for complete three-part table names
                if original_table.count('.') == 2:
                    variants = auto_detect_related_tables(original_table)
                    for variant in variants:
                        if variant not in info['table_names']:
                            info['table_names'].append(variant)
    
    return view_list 

# 新添加的函数，整合了关系分析和表信息提取
def analyze_explores_and_extract_tables():
    print("分析探索与视图关系并提取表信息...")
    
    # 第一步：关系分析
    explore_to_views, unnest_views, explore_list, explore_to_model, view_from_alias = analyze_explores()
    print(f"分析了 {len(explore_to_views)} 个探索")
    print(f"识别了 {len(unnest_views)} 个通过unnest创建的视图")
    print(f"识别了 {len(view_from_alias)} 个别名视图关系")
    
    # 第二步：提取视图数据来源定义（用于调试）
    view_source_definitions = extract_view_source_definitions()
    print(f"提取了 {len(view_source_definitions)} 个视图的数据来源定义")
    
    # 第三步：从视图的数据来源定义中提取表信息
    actual_table_names, view_citation_types = extract_tables_from_views()
    print(f"从视图定义中提取了 {len(actual_table_names)} 个视图的表名")
    
    return explore_to_views, unnest_views, explore_list, explore_to_model, view_from_alias, actual_table_names, view_citation_types, view_source_definitions

# 提取视图数据来源定义，用于调试
def extract_view_source_definitions():
    view_source_definitions = {}
    
    # 查找所有视图文件
    view_files = glob.glob('views/**/*.view.lkml', recursive=True)
    # 也在其他目录中查找视图文件
    view_files += glob.glob('**/*.view.lkml', recursive=True)
    # 去除重复项
    view_files = list(set(view_files))
    
    print(f"DEBUG - 在所有目录中查找视图数据来源定义，找到 {len(view_files)} 个视图文件")
    
    # 处理所有视图文件
    for file_path in view_files:
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                
                # 提取所有视图名称
                view_matches = re.finditer(r'view:\s+(\w+)\s+{', content)
                for view_match in view_matches:
                    view_name = view_match.group(1)
                    view_start_pos = view_match.start()
                    
                    # 查找此视图定义的结束位置
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
                        continue  # 无法确定视图的结束位置
                    
                    # 提取当前视图的内容
                    view_content = content[view_start_pos:view_end_pos]
                    
                    # 提取sql_table_name定义
                    sql_table_match = re.search(r'sql_table_name:\s+([^;]+);', view_content)
                    if sql_table_match:
                        view_source_definitions[view_name] = {
                            'type': 'sql_table_name',
                            'definition': sql_table_match.group(1).strip()
                        }
                        continue
                    
                    # 提取derived_table定义
                    derived_block = None
                    dt_pos = view_content.find('derived_table')
                    if dt_pos != -1:
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
                                derived_block = view_content[brace_start + 1 : i - 1]
                                
                    if derived_block:
                        # 检查是否有explore_source
                        has_explore, explore_name = contains_explore_source(derived_block)
                        if has_explore:
                            view_source_definitions[view_name] = {
                                'type': 'explore_source',
                                'definition': f"explore_source: {explore_name}"
                            }
                        else:
                            # 提取SQL查询
                            sql_match = re.search(r'sql\s*:\s*([\s\S]*?);;', derived_block, re.DOTALL)
                            if sql_match:
                                sql_text = sql_match.group(1).strip()
                                # 删除截断代码，保留完整SQL
                                view_source_definitions[view_name] = {
                                    'type': 'derived_table_sql',
                                    'definition': sql_text
                                }
                            else:
                                view_source_definitions[view_name] = {
                                    'type': 'derived_table',
                                    'definition': 'derived_table with no clear SQL definition'
                                }
                    else:
                        view_source_definitions[view_name] = {
                            'type': 'unknown',
                            'definition': 'No sql_table_name or derived_table found'
                        }
                    
        except Exception as e:
            print(f"提取视图数据来源定义时出错 {file_path}: {e}")
    
    print(f"提取了 {len(view_source_definitions)} 个视图的数据来源定义")
    return view_source_definitions

# 提取表信息的函数，专注于从视图定义中提取表信息
def extract_tables_from_views():
    actual_table_names = {}
    view_citation_types = {}  # 记录每个视图的引用类型
    
    # 查找所有视图文件
    view_files = glob.glob('views/**/*.view.lkml', recursive=True)
    # 也在其他目录中查找视图文件
    view_files += glob.glob('**/*.view.lkml', recursive=True)
    # 去除重复项
    view_files = list(set(view_files))
    
    print(f"DEBUG - 在所有目录中找到了 {len(view_files)} 个视图文件")
    
    # 创建一个集合来记录可能包含派生视图的目录中的视图文件
    derived_view_files = [f for f in view_files if 'derived_views/' in f or 'derived_tables/' in f or 'flip_views/derived_tables/' in f]
    print(f"DEBUG - 找到了 {len(derived_view_files)} 个可能的派生视图文件")
    
    # 首先处理派生视图目录中的视图，它们可能基于explore_source
    for file_path in derived_view_files:
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                
                # 尝试查找explore_source关键字
                if 'explore_source:' in content or 'explore_source :' in content:
                    # 提取视图名称
                    view_match = re.search(r'view:\s+(\w+)\s+{', content)
                    if view_match:
                        view_name = view_match.group(1)
                        print(f"DEBUG - 在派生目录中找到了可能的explore_source视图: {view_name}")
                        
                        # 检查它是否实际包含explore_source
                        has_explore, explore_name = contains_explore_source(content, view_name)
                        if has_explore:
                            view_citation_types[view_name] = 'derived_explore'
                            print(f"DEBUG - 标记 {view_name} 为 derived_explore 类型")
        except Exception as e:
            print(f"处理派生视图 {file_path} 时出错: {e}")
    
    # 然后处理所有其他视图文件
    for file_path in view_files:
        try:
            # 跳过已处理的派生视图
            if file_path in derived_view_files and any(view_name in view_citation_types for view_name in view_citation_types if view_citation_types[view_name] == 'derived_explore'):
                continue
                
            with open(file_path, 'r') as f:
                content = f.read()
                
                # 提取所有视图名称，而不仅仅是第一个
                view_matches = re.finditer(r'view:\s+(\w+)\s+{', content)
                for view_match in view_matches:
                    view_name = view_match.group(1)
                    
                    # 跳过已处理的视图
                    if view_name in view_citation_types and view_citation_types[view_name] == 'derived_explore':
                        continue
                    
                    view_start_pos = view_match.start()
                    
                    # 查找此视图定义的结束位置
                    # 计算花括号的嵌套级别
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
                        continue  # 无法确定视图的结束位置
                    
                    # 提取当前视图的内容
                    view_content = content[view_start_pos:view_end_pos]
                    
                    # 调用函数处理单个视图
                    tables, citation_type = extract_tables_from_view_content(view_name, view_content)
                    
                    # 记录引用类型
                    if citation_type:
                        view_citation_types[view_name] = citation_type
                    
                    # 只有在找到表名时才添加到字典中
                    if tables:
                        actual_table_names[view_name] = tables
                    
        except Exception as e:
            print(f"处理 {file_path} 时出错: {e}")
    
    print(f"从视图定义中提取了 {len(actual_table_names)} 个视图的表名")
    print(f"提取的表引用总数: {sum(len(tables) for tables in actual_table_names.values())}")
    
    return actual_table_names, view_citation_types

# 从单个视图内容中提取表名
def extract_tables_from_view_content(view_name, content):
    # 此函数处理单个视图的内容并提取表名
    tables = []
    
    # 首先检查它是否包含explore_source关键字
    has_explore, explore_name = contains_explore_source(content, "")
    if has_explore:
        return [], 'derived_explore'
    
    # 预处理内容：按行分割
    content_lines = content.split('\n')
    # 过滤掉注释行（以#开头的行）
    uncommented_content = '\n'.join([line for line in content_lines if not line.strip().startswith('#')])
    
    # 在预处理的内容中检查explore_source
    has_explore, explore_name = contains_explore_source(uncommented_content)
    if has_explore:
        print(f"DEBUG - 在预处理内容中检测到explore_source视图: {view_name}, explore: {explore_name}")
        return [], 'derived_explore'
    
    # ------- 改进的derived_table块提取 -------
    derived_block = None
    dt_pos = content.find('derived_table')
    if dt_pos != -1:
        # 在关键字后找到第一个左花括号
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
        
        # 在derived_block中检查explore_source
        has_explore, explore_name = contains_explore_source(derived_block)
        if has_explore:
            print(f"DEBUG - 在derived_table中检测到explore_source: {view_name}, explore: {explore_name}")
            return [], 'derived_explore'
    else:
        derived_block = None  # 确保变量存在
    
    # 从uncommented_content中删除双引号，以便更好地匹配表名
    # 这有助于处理Looker中不同的引号样式（双引号、反引号或无引号）
    uncommented_content_no_quotes = uncommented_content.replace('"', '')
    
    # 尝试查找sql_table_name定义（不包括注释行）
    # 原始正则表达式只匹配用反引号括起来的表名
    # sql_table_match = re.search(r'sql_table_name:\s+`([^`]+)`', uncommented_content)
    
    # 新的正则表达式可以处理分别用反引号括起来的项目名、数据集和表名
    # 也处理没有引号的标准格式和去除双引号的格式
    sql_table_match = re.search(
        r'sql_table_name:\s+(?:'
        r'`?([^`\s.]+)`?\.`?([^`\s.]+)`?\.`?([^`\s.;]+)`?|'  # 可能带反引号的格式: `project`.`dataset`.`table`
        r'`([^`]+)`|'  # 整个引用都在反引号中的格式: `project.dataset.table`
        r'([^`\s.]+)\.([^`\s.]+)\.([^`\s.;]+)|'  # 不带引号的格式: project.dataset.table
        r'([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]+)'  # 去除双引号后的简单格式
        r')',
        uncommented_content_no_quotes
    )
    
    if sql_table_match:
        if sql_table_match.group(4):  # 原始格式: `project.dataset.table`
            table_name = sql_table_match.group(4)
        elif sql_table_match.group(7):  # 不带引号或反引号的格式: project.dataset.table
            project = sql_table_match.group(5)
            dataset = sql_table_match.group(6)
            table = sql_table_match.group(7)
            table_name = f"{project}.{dataset}.{table}"
        elif sql_table_match.group(10):  # 去除双引号后的简单格式
            project = sql_table_match.group(8)
            dataset = sql_table_match.group(9)
            table = sql_table_match.group(10)
            table_name = f"{project}.{dataset}.{table}"
        else:  # 新格式: `project`.`dataset`.`table` 或变体
            project = sql_table_match.group(1)
            dataset = sql_table_match.group(2)
            table = sql_table_match.group(3)
            table_name = f"{project}.{dataset}.{table}"
        
        tables.append(table_name)
        return tables, 'native'
    
    # 检查derived_table的内容（如果提取了）
    if derived_block:
        # 从derived_block中删除双引号，以便更好地解析SQL
        derived_block_no_quotes = derived_block.replace('"', '')
        
        # 提取不同格式的SQL定义
        sql_match = None
        # 1) 显式块，以;; 结束（LookML标准）
        sql_match = re.search(r'sql\s*:\s*([\s\S]*?);;', derived_block_no_quotes, re.DOTALL)
        if not sql_match:
            # 2) 三重引号/大括号后备
            for pattern in [r'sql:\s*{{{([^}]+)}}}', r'sql:\s*"""([\s\S]+?)"""', r'sql:\s*{([\s\S]+?)}', r'sql:\s*"([^"]+)"']:
                sql_match = re.search(pattern, derived_block_no_quotes, re.DOTALL)
                if sql_match:
                    break
        
        if sql_match:
            sql_text = sql_match.group(1)
            # 从SQL文本中删除双引号，以便更好地提取表
            sql_text_no_quotes = sql_text.replace('"', '')
            
            # 首先检查是否有Liquid条件块
            liquid_tables = extract_tables_from_liquid_block(sql_text_no_quotes, False)
            if liquid_tables:
                tables.extend(liquid_tables)
            
            # 然后使用更通用的SQL解析来提取表名
            extracted_tables = extract_tables_from_sql(sql_text_no_quotes)
            for table in extracted_tables:
                if table not in tables:
                    tables.append(table)
        
        # 如果没有找到SQL定义，直接在整个derived_block中搜索表引用
        else:
            # 检查直接引用的表（使用反引号）
            table_refs = re.finditer(r'`([^`]+\.[^`]+\.[^`]+)`', derived_block_no_quotes)
            for match in table_refs:
                table_name = match.group(1)
                if table_name not in tables:
                    tables.append(table_name)
            
            # 检查不带反引号的常规引用（例如，"schema.dataset.table"）
            table_refs_no_backticks = re.finditer(r'([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]+)', derived_block_no_quotes)
            for match in table_refs_no_backticks:
                table_name = f"{match.group(1)}.{match.group(2)}.{match.group(3)}"
                if table_name not in tables:
                    tables.append(table_name)
            
            # 检查Liquid条件块中的表引用
            liquid_tables = extract_tables_from_liquid_block(derived_block_no_quotes, False)
            for table in liquid_tables:
                if table not in tables:
                    tables.append(table)
    
    # 改进：如果找到多个表引用，尝试选择最相关的一个作为主表
    if len(tables) > 0:
        if len(tables) > 1:
            for i, table in enumerate(tables):
                parts = table.split('.')
                if len(parts) == 3:
                    # 检查表名部分是否等于项目名部分
                    if parts[0] == parts[2]:
                        # 这可能是一个不正确的提取尝试，尝试找到更好的替代
                        for other_table in tables:
                            if other_table != table and other_table.endswith(view_name) or view_name in other_table:
                                # 找到一个更相关的表，将其设置为主表
                                tables.remove(other_table)
                                tables.insert(0, other_table)
                                break
                        
                        # 删除不正确的表引用
                        tables.remove(table)
                        break
        
        # 尝试找到与视图名称相似的表名
        view_base_name = view_name.replace('fact_', '').replace('dim_', '')
        matched_tables = []
        
        for table in tables:
            table_parts = table.split('.')
            if len(table_parts) == 3:
                table_base = table_parts[2]  # 从完整路径中提取表名
            elif len(table_parts) == 2:
                table_base = table_parts[1]
            else:
                table_base = table_parts[0]
                
            # 检查表名是否与视图名称相似
            table_base = table_base.replace('fact_', '').replace('dim_', '')
            
            if view_base_name in table_base or table_base in view_base_name:
                matched_tables.append(table)
        
        # 如果找到匹配的表，使用第一个匹配的表作为主表
        if matched_tables:
            primary_table = matched_tables[0]
            # 将此表移到表列表的前面
            if primary_table in tables:
                tables.remove(primary_table)
            tables.insert(0, primary_table)
            
        # 优先考虑实际表名最短的表
        if len(tables) > 1:
            # 提取实际表名（完整表路径的最后一部分）
            table_with_lengths = []
            for table in tables:
                parts = table.split('.')
                actual_table = parts[-1] if len(parts) > 0 else table
                table_with_lengths.append((len(actual_table), table))
            
            # 按实际表名长度排序
            table_with_lengths.sort()
            
            if table_with_lengths:
                shortest_table = table_with_lengths[0][1]
                # 将最短名称的表移到前面
                tables.remove(shortest_table)
                tables.insert(0, shortest_table)
    
    # 新增：如果使用了derived_table但实际上引用了一个真实表，将citation_type设置为'native'
    if derived_block and tables:
        return tables, 'native'
    
    return tables, 'native' if tables else '' 