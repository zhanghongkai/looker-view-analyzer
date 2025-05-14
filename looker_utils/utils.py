#!/usr/bin/env python3
import re

# Global project settings variables
DEFAULT_PROJECT = 'your-company'
DEFAULT_DATASET = 'analytics_prod'
SNAPSHOT_PROJECT = 'your-company-snapshot'
SNAPSHOT_DATASET = 'analytics_prod_snapshots'

def set_global_project_settings(default_project=None, default_dataset=None, snapshot_project=None, snapshot_dataset=None):
    """Set global project setting variables"""
    global DEFAULT_PROJECT, DEFAULT_DATASET, SNAPSHOT_PROJECT, SNAPSHOT_DATASET
    
    if default_project:
        DEFAULT_PROJECT = default_project
    if default_dataset:
        DEFAULT_DATASET = default_dataset
    if snapshot_project:
        SNAPSHOT_PROJECT = snapshot_project
    if snapshot_dataset:
        SNAPSHOT_DATASET = snapshot_dataset
    
    print(f"Updated global project settings:")
    print(f"  DEFAULT_PROJECT: {DEFAULT_PROJECT}")
    print(f"  DEFAULT_DATASET: {DEFAULT_DATASET}")
    print(f"  SNAPSHOT_PROJECT: {SNAPSHOT_PROJECT}")
    print(f"  SNAPSHOT_DATASET: {SNAPSHOT_DATASET}")

# Detect and process Liquid conditional blocks
def extract_tables_from_liquid_block(content, is_debug=False):
    tables = []
    
    # Remove double quotes to normalize table reference formats
    content = content.replace('"', '')
    
    if is_debug:
        print(f"DEBUG - Processing Liquid conditional block: input content length={len(content)}")
    
    # Simpler approach: extract all table references from Liquid blocks, regardless of conditions
    # First match all table references in Liquid blocks (including nested ones)
    liquid_blocks = []
    # Match {% if ... %} ... {% elsif ... %} ... {% else %} ... {% endif %} format blocks
    if_blocks = re.finditer(r'{%\s*if[^%]*%}(.*?){%\s*endif\s*%}', content, re.DOTALL)
    for block in if_blocks:
        liquid_blocks.append(block.group(0))
    
    # If no complete conditional blocks found, try matching incomplete blocks (e.g. truncated ones)
    if not liquid_blocks:
        partial_blocks = re.finditer(r'{%\s*if[^}]+}([^{]+)', content, re.DOTALL)
        for block in partial_blocks:
            liquid_blocks.append(block.group(0))
    
    if is_debug:
        print(f"DEBUG - Found {len(liquid_blocks)} Liquid conditional blocks")
    
    # For each Liquid block, extract all possible table references
    for block in liquid_blocks:
        # 1. Complete table references with backticks: `project.dataset.table`
        backtick_refs = re.finditer(r'`([^`]+\.[^`]+\.[^`]+)`', block)
        for match in backtick_refs:
            table_ref = match.group(1)
            if table_ref not in tables:
                tables.append(table_ref)
        
        # 2. Partial table references: `dataset.table` - no longer adding default project prefix
        partial_refs = re.finditer(r'`([^`]+\.[^`]+)`', block)
        for match in partial_refs:
            table_ref = match.group(1)
            # Keep two-part table names directly, without adding default project prefix
            if table_ref not in tables:
                tables.append(table_ref)
                if is_debug:
                    print(f"DEBUG - Keeping two-part table name: {table_ref}")
        
        # 3. Direct FROM and JOIN statements: FROM project.dataset.table or JOIN project.dataset.table
        sql_patterns = [
            r'(?i)FROM\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)', 
            r'(?i)JOIN\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)',
            r'(?i)FROM\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)', 
            r'(?i)JOIN\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)'
        ]
        
        for pattern in sql_patterns:
            refs = re.finditer(pattern, block)
            for match in refs:
                table_ref = match.group(1)
                # Keep original table references, without adding default project prefix
                if table_ref not in tables:
                    tables.append(table_ref)
                    if is_debug:
                        print(f"DEBUG - Keeping original table reference: {table_ref}")
    
    if is_debug:
        print(f"DEBUG - Tables extracted from Liquid conditional blocks: {tables}")
    
    return tables

# Extract table names from SQL statements
def extract_tables_from_sql(sql, is_debug=False):
    # Remove SQL comments to avoid misidentification
    sql = re.sub(r'--.*?(\n|$)', ' ', sql)
    sql = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.DOTALL)
    
    # Remove double quotes to handle different quoting styles consistently
    # Double quotes in Looker SQL are often used for column names and table references
    # This helps normalize between "schema"."table" and `schema`.`table` formats
    sql = sql.replace('"', '')
    
    # Clean up whitespace for easier processing
    sql = re.sub(r'\s+', ' ', sql)
    
    # Additional pattern: project in backticks, e.g. `project-name`.dataset.table
    patterns = [
        r'`([^`]+)`\s*\.\s*([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)',  # backtick project
        # Standard reference patterns for project.dataset.table or dataset.table
        r'`([^`]+\.[^`]+\.[^`]+)`',  # `project.dataset.table`
        r'`([^`]+\.[^`]+)`',  # `dataset.table`
        
        # Handle FROM clauses with table aliases - add (?i) to make matching case-insensitive
        r'(?i)FROM\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)\s+[A-Za-z][A-Za-z0-9_]*',  # FROM project.dataset.table A
        r'(?i)FROM\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)\s+[A-Za-z][A-Za-z0-9_]*',  # FROM dataset.table A
        
        # Handle JOIN clauses with table aliases - add (?i) to make matching case-insensitive
        r'(?i)JOIN\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)\s+[A-Za-z][A-Za-z0-9_]*',  # JOIN project.dataset.table B
        r'(?i)JOIN\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)\s+[A-Za-z][A-Za-z0-9_]*',  # JOIN dataset.table B
        
        # Original patterns (without aliases) - add (?i) to make matching case-insensitive
        r'(?i)FROM\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)(?!\s*AS|\s*\w)',  # FROM project.dataset.table
        r'(?i)FROM\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)(?!\s*AS|\s*\w)',  # FROM dataset.table
        r'(?i)JOIN\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)(?!\s*AS|\s*\w)',  # JOIN project.dataset.table
        r'(?i)JOIN\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)(?!\s*AS|\s*\w)',  # JOIN dataset.table
        r'(?i)FROM\s+`([^`]+)`',  # FROM `table_reference`
        r'(?i)JOIN\s+`([^`]+)`',  # JOIN `table_reference`
        
        # Handle aliases with AS - add (?i) to make matching case-insensitive
        r'(?i)FROM\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)\s+AS\s+[A-Za-z][A-Za-z0-9_]*',  # FROM project.dataset.table AS A
        r'(?i)FROM\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)\s+AS\s+[A-Za-z][A-Za-z0-9_]*',  # FROM dataset.table AS A
        r'(?i)JOIN\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)\s+AS\s+[A-Za-z][A-Za-z0-9_]*',  # JOIN project.dataset.table AS B
        r'(?i)JOIN\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)\s+AS\s+[A-Za-z][A-Za-z0-9_]*',  # JOIN dataset.table AS B
        
        # UNNEST patterns - add (?i) to make matching case-insensitive
        r'(?i)UNNEST\(\(SELECT .*? FROM\s+`?([^`\s)]+\.[^`\s)]+\.[^`\s)]+)`?\s*',  # UNNEST((SELECT ... FROM project.dataset.table)
        r'(?i)UNNEST\(\(SELECT .*? FROM\s+`?([^`\s)]+\.[^`\s)]+)`?\s*',  # UNNEST((SELECT ... FROM dataset.table)
        
        # Table references in WITH statement subqueries - add (?i) to make matching case-insensitive
        r'(?i)WITH\s+\w+\s+AS\s*\(.*?FROM\s+`?([^`\s)]+\.[^`\s)]+\.[^`\s)]+)`?',
        r'(?i)WITH\s+\w+\s+AS\s*\(.*?FROM\s+`?([^`\s)]+\.[^`\s)]+)`?'
    ]
    
    tables = []
    for pattern in patterns:
        matches = re.finditer(pattern, sql)
        for match in matches:
            # Some patterns (e.g. backtick-project form) capture project, dataset, table as
            # separate groups.  If 3 groups are present, stitch them together; otherwise
            # fallback to the first group.
            if len(match.groups()) >= 3 and match.group(3):
                table_ref = f"{match.group(1)}.{match.group(2)}.{match.group(3)}"
            else:
                table_ref = match.group(1)
            
            # No longer adding default project prefix for incomplete table paths, use original references directly
            # For three-part table names (complete names), keep unchanged
            # For two-part or one-part table names, keep as is, without adding prefix
            if table_ref not in tables:
                tables.append(table_ref)
                if is_debug:
                    print(f"DEBUG - Keeping original table reference: {table_ref}")
            
            # Original auto-completion code has been removed
    
    # New debug output
    if is_debug:
        print(f"DEBUG - Extract SQL table names: input SQL={sql}")
        print(f"DEBUG - Extract SQL table names: extraction result={tables}")
    
    # Handle streaming table suffixes (_streaming) and partitioned tables (_20220101 format)
    base_tables = []
    for table in tables:
        base_table = re.sub(r'_streaming$', '', table)
        base_table = re.sub(r'_\d{8}$', '', base_table)
        
        if base_table not in base_tables and base_table != table:
            base_tables.append(base_table)
    
    # Merge the complete table list
    all_tables = tables + [t for t in base_tables if t not in tables]
    
    if is_debug and all_tables:
        print(f"DEBUG - Tables extracted from SQL: {all_tables}")
    
    return all_tables

# Add a dedicated function to identify explore_source type tables
def contains_explore_source(content, view_name=""):
    """Check if the content contains an explore_source definition"""
    debug_prefix = f"DEBUG - [{view_name}] " if view_name else "DEBUG - "
    
    # Remove double quotes to normalize formats
    content = content.replace('"', '')
    
    # First directly check for keywords
    if 'explore_source:' in content or 'explore_source :' in content:
        # Use more precise pattern matching for explore_source definitions
        patterns = [
            r'explore_source:\s*(\w+)',  # Standard format
            r'explore_source\s*:\s*(\w+)',  # Possible space variants
            r'derived_table\s*:\s*{\s*explore_source\s*:\s*(\w+)'  # Nested in derived_table
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.DOTALL)
            if match:
                explore_name = match.group(1)
                if view_name:
                    print(f"{debug_prefix}Found explore_source definition: {explore_name}")
                return True, explore_name
        
        # If keywords are found but no complete pattern match, still return True
        if view_name:
            print(f"{debug_prefix}Contains explore_source keyword, but no complete pattern match")
        return True, "unknown"
    
    return False, "" 