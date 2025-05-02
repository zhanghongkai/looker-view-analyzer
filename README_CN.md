# Looker 视图使用分析工具

本项目完全由 Cursor AI 代理（更具体地说，使用具有思考能力的 claude-3.7-sonnet）编写，但已经通过作者对底层数据系统的丰富知识进行了仔细验证，确保它能为复杂的生产环境 Looker 项目产生预期的输出。鼓励用户将此作为良好的起点，并继续使用 AI 工具根据需要实现其他功能。

此工具用于分析 Looker 项目结构，提取视图与实际数据库表之间的映射关系，并生成用于数据迁移的导出命令。

## 功能特点

- 从 Looker 项目中提取视图和表的关系
- 分析视图使用模式
- 识别不同类型的视图（原生、派生、展开等）
- 为 BigQuery 表生成导出命令
- 创建视图到表关系的全面报告

## 安装

克隆此仓库：

```bash
git clone https://github.com/zhanghongkai/Looker-view-analyzer.git
cd Looker-view-analyzer
```

除了 Python 3.6+ 外，不需要额外的依赖项。

## 使用方法

使用 Python 3 运行脚本：

```bash
python main.py --looker_path /Looker项目路径 --activities_file activities.csv --output_dir ./output --export_gs_bucket 你的GCS存储桶名称
```

高级用法（使用自定义项目和数据集设置）：

```bash
python main.py --looker_path /Looker项目路径 \
               --activities_file activities.csv \
               --output_dir ./output \
               --export_gs_bucket 你的GCS存储桶名称 \
               --default_project 自定义项目名 \
               --default_dataset 自定义数据集名 \
               --snapshot_project 自定义快照项目名 \
               --snapshot_dataset 自定义快照数据集名
```

### 命令行参数

- `--looker_path`：Looker 项目目录的路径（如果脚本不在 Looker 项目目录中，则必须提供）
- `--activities_file`：包含探索活动数据的 CSV 文件路径（默认值：'activities.csv'）
- `--output_dir`：输出文件保存的目录（默认值：当前目录）
- `--export_gs_bucket`：导出命令的 GCS 存储桶名称（可选）。如果未提供，脚本将分析视图但不会生成导出命令。
- `--default_project`：默认 BigQuery 项目名称（默认值：'your-company'）
- `--default_dataset`：默认数据集名称（默认值：'analytics_prod'）
- `--snapshot_project`：快照表项目名称（默认值：'your-company-snapshot'）
- `--snapshot_dataset`：快照表数据集名称（默认值：'analytics_prod_snapshots'）

### 输入文件

- `activities.csv`：包含探索使用数据的 CSV 文件，其中包含探索名称和使用计数的列

### 输出文件

脚本最多生成三个输出文件：

- `updated_table_list.csv`：所有视图的视图到表映射信息（总是生成）
- `export_command.txt`：所有表的导出命令（仅当提供 `--export_gs_bucket` 时生成）
- `export_command_active.txt`：仅活跃表的导出命令（使用频率 > 0 的表）（仅当提供 `--export_gs_bucket` 时生成）

## 项目结构

```
.
├── main.py                  # 主脚本入口点
└── looker_utils/            # 工具模块目录
    ├── __init__.py          # 包初始化
    ├── analyzers.py         # 用于分析探索/视图关系的函数
    ├── data_loaders.py      # 用于加载数据的函数
    ├── extractors.py        # 用于提取表名和视图信息的函数
    ├── reporters.py         # 用于生成报告和导出命令的函数
    └── utils.py             # 通用工具函数
```

### 模块描述

- **main.py**：协调所有模块并提供命令行界面
- **data_loaders.py**：包含用于加载探索使用数据和提取视图的函数
- **extractors.py**：包含从视图定义中提取表名的函数
- **analyzers.py**：包含分析探索和视图之间关系的函数
- **reporters.py**：包含生成报告和导出命令的函数
- **utils.py**：包含跨模块使用的通用工具函数

## 引用类型

该工具将视图分为几种类型：

- **native**：直接引用数据库表的视图
- **derived**：从其他表上的 SQL 操作派生的视图
- **unnest**：使用 UNNEST 操作创建的视图
- **derived_explore**：基于探索的视图
- **derived_from**：从其他视图派生的视图（别名）
- **nested**：嵌套在其他视图中的视图

## 最佳实践

- 运行工具前确保您的 Looker 项目路径正确无误
- 如果您需要导出命令，请确保提供有效的 GCS 存储桶名称
- 对于大型 Looker 项目，请考虑使用 --output_dir 参数将输出保存到专用目录
- 使用自定义项目和数据集参数来适应您的特定 BigQuery 环境

## 常见问题

**问**：工具生成了错误的表引用怎么办？
**答**：检查您的 Looker 项目中的视图定义，并确保它们正确引用表。您也可以使用自定义项目和数据集参数来纠正默认设置。

**问**：为什么没有生成导出命令？
**答**：只有提供 `--export_gs_bucket` 参数时才会生成导出命令。请确保添加此参数并指定有效的 GCS 存储桶。

**问**：如何解释视图使用频率为零？
**答**：使用频率为零表示该视图可能未在任何探索中使用，或者活动数据文件中没有记录其使用情况。

## 许可证

本项目采用 MIT 许可证 - 详情请参阅 [LICENSE](LICENSE) 文件。

MIT 许可证

版权所有 (c) 2025 张鸿凯 (Hongkai Zhang)

特此免费授予任何获得本软件及其相关文档文件（"软件"）副本的人不受限制地处理本软件的权利，
包括但不限于使用、复制、修改、合并、发布、分发、再许可和/或出售软件副本的权利，
并允许向其提供本软件的人员这样做，但须符合以下条件：

上述版权声明和本许可声明应包含在本软件的所有副本或重要部分中。

本软件按"原样"提供，不提供任何形式的明示或暗示担保，包括但不限于对适销性、
特定用途的适用性和非侵权性的担保。在任何情况下，作者或版权持有人均不对任何索赔、
损害或其他责任负责，无论是因合同、侵权或其他原因引起的，与本软件或本软件的使用或
其他交易有关。

## 贡献

欢迎对该项目做出贡献！由于此工具是为解决特定用例而创建的，请在贡献时考虑以下几点：

1. **先开一个 issue**：在提交拉取请求之前，请开一个 issue 讨论你想要进行的更改。

2. **遵循代码风格**：尽量使你的贡献与现有代码风格保持一致。

3. **文档化你的更改**：必要时更新 README 或为你的代码添加注释。

4. **测试你的更改**：确保你的更改不会破坏现有功能。

贡献流程：

1. Fork 仓库
2. 创建你的功能分支（`git checkout -b feature/amazing-feature`）
3. 提交你的更改（`git commit -m 'Add some amazing feature'`）
4. 推送到分支（`git push origin feature/amazing-feature`）
5. 开一个拉取请求

如有问题或建议，请联系此项目的唯一作者和维护者：[张鸿凯 (Hongkai Zhang)](https://github.com/zhanghongkai) 