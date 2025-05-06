# Looker 视图分析工具架构

本文档详细描述了 Looker 视图分析工具的代码架构、组件功能和处理流程。该工具用于分析 Looker 项目结构，提取视图与实际数据库表之间的映射关系，并生成数据迁移所需的导出命令。

## 1. 文件结构

```
.
├── main.py                  # 主脚本入口点
└── looker_utils/            # 工具模块目录
    ├── __init__.py          # 包初始化文件
    ├── analyzers.py         # 分析探索/视图关系的函数
    ├── constants.py         # 全局常量定义
    ├── data_loaders.py      # 数据加载相关函数
    ├── extractors.py        # 表名和视图信息提取功能
    ├── reporters.py         # 报告和导出命令生成功能
    └── utils.py             # 通用工具函数
```

## 2. 主要模块功能

### 2.1 main.py

主脚本是整个工具的入口点，负责协调各个模块完成视图使用分析。主要功能：

- 解析命令行参数
- 配置全局项目设置（默认项目名称、数据集名称等）
- 管理工作目录和输出目录
- 调用各个模块的函数完成整个分析过程
- 生成报告和导出命令

### 2.2 looker_utils/constants.py

定义工具使用的全局常量，包括：

- `DEFAULT_PROJECT`：默认的 BigQuery 项目名称（默认值：'curated-dwh'）
- `SNAPSHOT_PROJECT`：快照表项目名称（默认值：'curated-dwh-snapshot'）

### 2.3 looker_utils/utils.py

提供整个工具使用的通用实用函数，主要功能：

- 设置全局项目配置
- 自动检测相关表（流式表、分区表等）
- 从 Liquid 条件块中提取表名
- 从 SQL 语句中提取表名
- 识别基于 explore_source 的视图

主要的功能函数包括：
- `set_global_project_settings()`: 设置全局项目配置
- `auto_detect_related_tables()`: 自动检测相关表
- `extract_tables_from_liquid_block()`: 从 Liquid 条件块中提取表名
- `extract_tables_from_sql()`: 从 SQL 语句中提取表名
- `contains_explore_source()`: 检查内容是否包含 explore_source 定义

### 2.4 looker_utils/data_loaders.py

负责加载数据和提取视图信息，主要功能：

- 加载 Explore 使用数据
- 提取项目中的所有视图

主要函数：
- `load_explore_usage()`: 从 CSV 文件加载 Explore 使用频率数据
- `extract_all_views()`: 扫描 Looker 项目结构并提取所有视图

### 2.5 looker_utils/extractors.py

负责从视图定义中提取表名等信息，主要功能：

- 从单个视图内容中提取表名
- 从所有视图定义文件中提取实际表名

关键函数：
- `extract_tables_from_view_content()`: 从单个视图内容提取表名
- `extract_actual_table_names()`: 从所有视图定义文件中提取实际表名

### 2.6 looker_utils/analyzers.py

分析 Explores 和视图之间的关系，主要功能：

- 分析 Explores 和视图之间的关系
- 识别通过 UNNEST 创建的视图
- 处理视图别名
- 更新视图列表中的表信息
- 计算实际使用频率

主要函数：
- `analyze_explores()`: 分析 Explores 和视图之间的关系
- `update_view_table_info()`: 更新视图列表中的表信息
- `calculate_actual_usage()`: 计算视图的实际使用频率

### 2.7 looker_utils/reporters.py

生成报告和导出命令，主要功能：

- 生成视图分析报告
- 生成导出命令

主要函数：
- `generate_report()`: 生成视图分析报告
- `generate_export_commands()`: 生成导出命令

## 3. 处理流程

整个工具的处理流程如下：

### 3.1 初始化和配置

1. 解析命令行参数 (`main.py`)
2. 设置全局项目配置 (`main.py`, `utils.py` 中的 `set_global_project_settings()`)
3. 确定输出目录 (`main.py`)
4. 如果指定了 Looker 路径，切换到该目录并分析目录结构 (`main.py`)

### 3.2 数据提取

1. 加载 Explore 使用频率数据（如果提供）(`data_loaders.py` 中的 `load_explore_usage()`)
2. 提取所有视图 (`data_loaders.py` 中的 `extract_all_views()`)
3. 从视图定义中提取实际表名 (`extractors.py` 中的 `extract_actual_table_names()`)

### 3.3 关系分析

1. 分析 Explores 和视图之间的关系 (`analyzers.py` 中的 `analyze_explores()`)
2. 识别通过 UNNEST 创建的视图 (`analyzers.py` 中的 `analyze_explores()`)
3. 识别通过别名创建的视图 (`analyzers.py` 中的 `analyze_explores()`)

### 3.4 表信息更新

1. 更新视图列表中的表信息 (`analyzers.py` 中的 `update_view_table_info()`)
2. 处理默认项目前缀和特殊表类型 (`analyzers.py` 中的 `update_view_table_info()` 和 `utils.py` 中的相关函数)

### 3.5 使用频率计算

1. 根据 Explore 使用频率计算视图的实际使用频率 (`analyzers.py` 中的 `calculate_actual_usage()`)
2. 如果未提供使用频率数据，则将所有视图的计算使用频率设置为 NULL (`main.py`)

### 3.6 报告生成

1. 生成视图分析报告 (`reporters.py` 中的 `generate_report()`)
2. 将结果保存到 CSV 文件中 (`reporters.py` 中的 `generate_report()`)

### 3.7 导出命令生成（可选）

1. 如果提供了 GCS 存储桶名称，生成导出命令 (`reporters.py` 中的 `generate_export_commands()`)
2. 将导出命令保存到文件中 (`reporters.py` 中的 `generate_export_commands()`)

## 4. 数据库兼容性

该工具主要针对 BigQuery 后端设计，但也考虑到了 Snowflake 等其他数据库系统。两种主要数据库系统在表引用方面的差异：

### 4.1 BigQuery 表引用格式

```
project.dataset.table
```

- **project**：项目 ID，类似于数据库集合
- **dataset**：数据集，类似于 schema
- **table**：表名

### 4.2 Snowflake 表引用格式

```
database.schema.table
```

- **database**：数据库名称
- **schema**：模式名称
- **table**：表名

由于这些差异，在解析表名时可能会出现问题。特别是，当工具默认按照 BigQuery 格式处理 Snowflake 格式的表引用时，可能会导致表名解析错误。例如，Snowflake 中的 `"FLIP_SYSTEM"."DBT_PROD"."BRANDS_CORE"` 可能被错误解析，导致最终结果变为 `curated-dwh.FLIP_SYSTEM.DBT_PROD`。

## 5. 使用示例

基本用法：
```bash
python main.py --looker_path /path/to/looker/project
```

高级用法（分析 Explore 使用并生成导出命令）：
```bash
python main.py --looker_path /path/to/looker/project \
               --explore_usage_file explore_usage.csv \
               --output_dir ./output \
               --export_gs_bucket your-gcs-bucket-name \
               --default_project custom-project \
               --default_dataset custom_dataset
```

## 6. 输出文件

- `view_analysis.csv`：包含所有视图到表映射信息的 CSV 文件
- `export_command.txt`：所有表的导出命令
- `export_command_active.txt`：仅包含使用频率大于 0 的表的导出命令（仅当提供了 Explore 使用频率数据时才会生成）

## 7. 已知问题和限制

1. **表名解析问题**：不同数据库系统（如 BigQuery 和 Snowflake）的表引用格式不同，可能导致表名解析错误
2. **默认项目前缀**：工具默认添加前缀，即使原始 SQL 中已经提供了完整的三段式引用
3. **引号处理**：工具可能无法正确处理不同引用样式（反引号、双引号等） 