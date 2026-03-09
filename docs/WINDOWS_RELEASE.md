# Windows EXE 打包与发布

已配置 GitHub Actions 自动打包：
- 工作流文件：`.github/workflows/release-windows-exe.yml`
- 触发条件：推送 `v*` 标签（例如 `v1.0.0`）
- 产物：`jiaoyisuopiliang-<tag>-windows-x64.zip`
- 发布位置：GitHub Releases（自动创建对应版本）

## 你需要做的事情
在本地执行：

```bash
git tag v1.0.0
git push origin v1.0.0
```

然后去仓库 Actions 查看工作流执行情况，完成后到 Releases 下载 zip。

## Windows 用户使用
1. 下载并解压 zip。
2. 双击 `jiaoyisuopiliang.exe` 启动。
3. 首次启动会在 exe 同目录自动创建 `data/` 存配置和本地密钥。
