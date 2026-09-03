# SDK 冒烟检查

[English](README.md) | 简体中文

该调用方项目构建并安装 Python wheel 和 npm tarball，运行工具调用与子 Agent 任务，
再比较标准化结果。它检查包导入、TypeScript 类型声明和源码目录外的执行。
模型网关在本地运行，无需 API 凭据。

## 运行

需要 Python 3.11+，以及 `pip`、`wheel`、`setuptools>=77.0.3`；同时需要 Node.js 22+ 和 npm。
先安装仓库的 TypeScript 构建依赖：

```sh
npm --prefix typescript ci
cd sdk-parity-smoke
./run-local.sh
```

通过 `PYTHON=/path/to/python` 指定解释器。未指定时，脚本优先使用带有 pip 的仓库 `.venv/bin/python`，
否则使用 `python3`。

## 结果

每个场景一致时输出 `PASS`。比较内容包括最终回答、工具输入与结果、包版本。
模型调用次数不同会输出 `NOTICE`，不导致语义比较失败。

构建产物与标准化结果分别保存在 `.work/artifacts/` 和 `.work/results/`。
每次运行会重建当前调用方目录中的 `.work/`、`dist/` 和 `node_modules/`。

详细协议检查见[一致性测试](../conformance/README.zh-CN.md)。
