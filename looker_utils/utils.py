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

# Automatically detect related tables (streaming tables, partitioned tables, etc.)
def auto_detect_related_tables(base_table, is_debug=False):
    variants = []
    
    # Parse the base_table to extract its components
    parts = base_table.split('.')
    if len(parts) != 3:
        # If the base_table doesn't have three parts, it's not a complete reference
        # In this case, we shouldn't generate variants as we can't determine the correct prefix
        if is_debug:
            print(f"DEBUG - Cannot generate variants for incomplete table reference: {base_table}")
        return variants
    
    # Extract the project, dataset, and table name from the base_table
    project, dataset, table = parts
    
    # Generate streaming variant using the original project and dataset prefixes
    streaming_table = f"{project}.{dataset}.{table}_streaming"
    variants.append(streaming_table)
    
    # Generate FLIP variant if applicable (for specified tables that have FLIP versions)
    if "fact_purchased_line_items" in table:
        flip_table = f"{project}.{dataset}.{table}_flip"
        variants.append(flip_table)
        
        # Also add the FLIP streaming variant
        flip_streaming_table = f"{project}.{dataset}.{table}_flip_streaming"
        variants.append(flip_streaming_table)
    
    # Don't add variants with different project prefixes unless specifically needed
    # This is the key change to prevent adding "your-company" prefixed tables
    
    if is_debug:
        print(f"DEBUG - Variants generated for table {base_table}: {variants}")
    
    return variants

# Detect and process Liquid conditional blocks
def extract_tables_from_liquid_block(content, is_debug=False):
    tables = []
    
    # Remove double quotes to normalize table reference formats
    content = content.replace('"', '')
    
    # Extract table references from Liquid conditional blocks
    liquid_patterns = [
        # Table references in if-else blocks, using backticks
        r'{%\s*if\s+.*?%}.*?`([^`]+)`.*?{%\s*else\s*%}.*?`([^`]+)`.*?{%\s*endif\s*%}',
        # Table references in if blocks only, using backticks
        r'{%\s*if\s+.*?%}.*?`([^`]+)`.*?{%\s*endif\s*%}',
        # Table references in if blocks, without backticks, but with full path (project.dataset.table)
        r'{%\s*if\s+.*?%}.*?FROM\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+).*?{%\s*endif\s*%}',
        # Table references in if blocks, without backticks, but with partial path (dataset.table)
        r'{%\s*if\s+.*?%}.*?FROM\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+).*?{%\s*endif\s*%}',
        # JOIN statements in if blocks, full path
        r'{%\s*if\s+.*?%}.*?JOIN\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+).*?{%\s*endif\s*%}',
        # JOIN statements in if blocks, partial path
        r'{%\s*if\s+.*?%}.*?JOIN\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+).*?{%\s*endif\s*%}',
        # Table reference patterns in if-else-endif blocks
        r'{%\s*if\s+([^%]+)%}\s*`([^`]+)`\s*{%\s*else\s*%}\s*`([^`]+)`\s*{%\s*endif\s*%}',
    ]
    
    for pattern in liquid_patterns:
        matches = re.finditer(pattern, content, re.DOTALL)
        for match in matches:
            groups = match.groups()
            for group in groups:
                if not group:
                    continue
                
                # Skip condition text that isn't a table name
                if '_filters' in group or '==' in group or "'" in group:
                    continue
                
                # Handle cases with and without backticks
                table_ref = group.strip('`')
                
                # Complete the table name path ONLY if it's incomplete
                parts = table_ref.split('.')
                if len(parts) == 3:  # project.dataset.table - already complete
                    full_table = table_ref
                elif len(parts) == 2:  # dataset.table - needs project
                    full_table = f"{DEFAULT_PROJECT}.{parts[0]}.{parts[1]}"
                elif len(parts) == 1:  # table name only - needs project and dataset
                    full_table = f"{DEFAULT_PROJECT}.{DEFAULT_DATASET}.{parts[0]}"
                else:
                    continue
                
                if full_table not in tables:
                    tables.append(full_table)
    
    # Special detection for complete if-else structures
    if_else_blocks = re.finditer(r'{%\s*if\s+[^%]+%}([^{]+){%\s*else\s*%}([^{]+){%\s*endif\s*%}', content, re.DOTALL)
    for block in if_else_blocks:
        if_part = block.group(1)
        else_part = block.group(2)
        
        # Extract table references from the if part
        if_tables = re.finditer(r'`([^`]+)`', if_part)
        for table_match in if_tables:
            table_ref = table_match.group(1)
            # Complete the table name path ONLY if it's incomplete
            parts = table_ref.split('.')
            if len(parts) == 3:  # project.dataset.table - already complete
                full_table = table_ref
            elif len(parts) == 2:  # dataset.table - needs project
                full_table = f"{DEFAULT_PROJECT}.{parts[0]}.{parts[1]}"
            elif len(parts) == 1:  # table name only - needs project and dataset
                full_table = f"{DEFAULT_PROJECT}.{DEFAULT_DATASET}.{parts[0]}"
            else:
                continue
            
            if full_table not in tables:
                tables.append(full_table)
        
        # Extract table references from the else part
        else_tables = re.finditer(r'`([^`]+)`', else_part)
        for table_match in else_tables:
            table_ref = table_match.group(1)
            # Complete the table name path ONLY if it's incomplete
            parts = table_ref.split('.')
            if len(parts) == 3:  # project.dataset.table - already complete
                full_table = table_ref
            elif len(parts) == 2:  # dataset.table - needs project
                full_table = f"{DEFAULT_PROJECT}.{parts[0]}.{parts[1]}"
            elif len(parts) == 1:  # table name only - needs project and dataset
                full_table = f"{DEFAULT_PROJECT}.{DEFAULT_DATASET}.{parts[0]}"
            else:
                continue
            
            if full_table not in tables:
                tables.append(full_table)
    
    if is_debug and tables:
        print(f"DEBUG - Tables extracted from Liquid blocks: {tables}")
    
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
        r'FROM\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)(?!\s*AS|\s*\w)',  # FROM project.dataset.table
        r'FROM\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)(?!\s*AS|\s*\w)',  # FROM dataset.table
        r'JOIN\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)(?!\s*AS|\s*\w)',  # JOIN project.dataset.table
        r'JOIN\s+([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)(?!\s*AS|\s*\w)',  # JOIN dataset.table
        r'FROM\s+`([^`]+)`',  # FROM `table_reference`
        r'JOIN\s+`([^`]+)`',  # JOIN `table_reference`
        
        # UNNEST patterns
        r'UNNEST\(\(SELECT .*? FROM\s+`?([^`\s)]+\.[^`\s)]+\.[^`\s)]+)`?\s*',  # UNNEST((SELECT ... FROM project.dataset.table)
        r'UNNEST\(\(SELECT .*? FROM\s+`?([^`\s)]+\.[^`\s)]+)`?\s*',  # UNNEST((SELECT ... FROM dataset.table)
        
        # Table references in WITH statement subqueries
        r'WITH\s+\w+\s+AS\s*\(.*?FROM\s+`?([^`\s)]+\.[^`\s)]+\.[^`\s)]+)`?',
        r'WITH\s+\w+\s+AS\s*\(.*?FROM\s+`?([^`\s)]+\.[^`\s)]+)`?'
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
            
            # Complete the table path ONLY if it's incomplete
            parts = table_ref.split('.')
            if len(parts) == 3:  # project.dataset.table - already complete
                full_table = table_ref
            elif len(parts) == 2:  # dataset.table - needs project
                full_table = f"{DEFAULT_PROJECT}.{parts[0]}.{parts[1]}"
            elif len(parts) == 1:  # table name only, assume in default dataset
                full_table = f"{DEFAULT_PROJECT}.{DEFAULT_DATASET}.{parts[0]}"
            else:
                continue  # Not a valid format
            
            # Handle special cases
            if "'" in full_table or '"' in full_table:
                continue  # Skip table names containing quotes, might be false positives
            
            if 'TABLE_DATE_RANGE' in full_table or '$' in full_table:
                continue  # Skip cases with variable references
            
            if full_table not in tables:
                tables.append(full_table)
    
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