# 剧集排期规划系统

这是一个基于 Streamlit 的剧集排期规划系统，用于自动化处理多频道剧集排期工作。

## 功能特点

- 📊 支持 Excel 文件上传和处理
- 🎯 智能剧集分配算法
- 📅 7天自然周排期
- ⚠️ 剧库不足告警
- 📥 结果导出和下载

## 安装依赖

```bash
pip install -r requirements.txt
```

## 本地运行

```bash
streamlit run app.py
```

应用将在浏览器中自动打开，默认地址为 `http://localhost:8501`

## 部署到 Streamlit Cloud

1. 将代码推送到 GitHub 仓库
2. 访问 [Streamlit Cloud](https://streamlit.io/cloud)
3. 使用 GitHub 账号登录
4. 点击 "New app"
5. 选择你的仓库、分支和主文件（app.py）
6. 点击 "Deploy"

## 输入文件要求

上传的 Excel 文件必须包含以下3个工作表：

1. **1-本周排期剧单** - 待排期的剧集清单
2. **2-频道属性** - 频道的基本信息和配置
3. **3-过去30天已发剧单** - 历史发布记录

## 输出结果

系统会生成包含以下3个工作表的 Excel 文件：

1. **排期结果** - 完整的排期计划
2. **告警** - 剧库不足等告警信息
3. **频道优先级重排** - 频道优先级调整结果

## 配置参数

- **随机种子**: 控制随机分配的种子值，相同种子会产生相同结果
- **默认语种**: 当剧单语种为空时使用的默认语种列表

## 技术栈

- Python 3.8+
- Streamlit - Web 应用框架
- Pandas - 数据处理
- OpenPyXL - Excel 文件读写

## 许可证

MIT License
