import streamlit as st
import pandas as pd
import tempfile
import os
import io
from schedule_planner import run_scheduler

st.set_page_config(
    page_title="剧集排期规划系统",
    page_icon="🎬",
    layout="wide"
)

st.title("🎬 剧集排期规划系统")
st.markdown("---")

# 侧边栏配置
with st.sidebar:
    st.header("⚙️ 配置参数")

    seed = st.number_input(
        "随机种子",
        min_value=1,
        max_value=9999,
        value=2026,
        help="用于控制随机分配的种子值，相同种子会产生相同结果"
    )

    st.markdown("---")
    st.markdown("""
    ### 📋 使用说明
    1. 上传包含3个工作表的Excel文件
    2. 配置随机种子
    3. 点击"开始排期"按钮
    4. 下载生成的排期结果

    ### 📊 必需的工作表
    - `1-本周排期剧单`
    - `2-频道属性`
    - `3-过去30天已发剧单`
    """)

# 主界面
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("📤 上传输入文件")
    uploaded_file = st.file_uploader(
        "选择Excel文件",
        type=["xlsx", "xls"],
        help="请上传包含3个必需工作表的Excel文件"
    )

with col2:
    st.subheader("📊 文件状态")
    if uploaded_file is not None:
        st.success("✅ 文件已上传")
        st.info(f"文件名: {uploaded_file.name}")
        st.info(f"文件大小: {uploaded_file.size / 1024:.2f} KB")
    else:
        st.warning("⚠️ 请上传Excel文件")

# 文件预览
if uploaded_file is not None:
    st.markdown("---")
    st.subheader("👀 数据预览")

    try:
        xls = pd.ExcelFile(uploaded_file)
        sheet_names = xls.sheet_names

        st.write(f"**检测到的工作表:** {', '.join(sheet_names)}")

        # 检查必需的工作表
        required_sheets = ["1-本周排期剧单", "2-频道属性", "3-过去30天已发剧单"]
        missing_sheets = [s for s in required_sheets if s not in sheet_names]

        if missing_sheets:
            st.error(f"❌ 缺少必需的工作表: {', '.join(missing_sheets)}")
        else:
            st.success("✅ 所有必需的工作表都存在")

            # 预览每个工作表
            tab1, tab2, tab3 = st.tabs(required_sheets)

            with tab1:
                df1 = pd.read_excel(xls, sheet_name="1-本周排期剧单")
                st.write(f"**行数:** {len(df1)}")
                st.dataframe(df1.head(10), use_container_width=True)

            with tab2:
                df2 = pd.read_excel(xls, sheet_name="2-频道属性")
                st.write(f"**行数:** {len(df2)}")
                st.dataframe(df2.head(10), use_container_width=True)

            with tab3:
                df3 = pd.read_excel(xls, sheet_name="3-过去30天已发剧单")
                st.write(f"**行数:** {len(df3)}")
                st.dataframe(df3.head(10), use_container_width=True)

    except Exception as e:
        st.error(f"❌ 读取文件时出错: {str(e)}")

# 执行排期
if uploaded_file is not None:
    st.markdown("---")

    if st.button("🚀 开始排期", type="primary", use_container_width=True):
        try:
            with st.spinner("正在处理排期，请稍候..."):
                # 创建临时文件
                with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_input:
                    tmp_input.write(uploaded_file.getvalue())
                    tmp_input_path = tmp_input.name

                with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_output:
                    tmp_output_path = tmp_output.name

                # 执行排期
                run_scheduler(
                    input_excel=tmp_input_path,
                    output_excel=tmp_output_path,
                    seed=seed
                )

                # 读取结果
                with open(tmp_output_path, "rb") as f:
                    result_data = f.read()

                # 清理临时文件
                os.unlink(tmp_input_path)
                os.unlink(tmp_output_path)

            st.success("✅ 排期完成！")

            # 显示结果预览
            st.subheader("📊 结果预览")
            result_xls = pd.ExcelFile(io.BytesIO(result_data))

            tab_result, tab_warning, tab_priority = st.tabs(["排期结果", "告警信息", "频道优先级"])

            with tab_result:
                df_result = pd.read_excel(result_xls, sheet_name="排期结果")
                st.write(f"**总排期数:** {len(df_result)}")
                st.dataframe(df_result, use_container_width=True)

            with tab_warning:
                df_warning = pd.read_excel(result_xls, sheet_name="告警")
                if len(df_warning) > 0:
                    st.warning(f"⚠️ 发现 {len(df_warning)} 条告警")
                    st.dataframe(df_warning, use_container_width=True)
                else:
                    st.success("✅ 无告警信息")

            with tab_priority:
                df_priority = pd.read_excel(result_xls, sheet_name="频道优先级重排")
                st.write(f"**频道数:** {len(df_priority)}")
                st.dataframe(df_priority, use_container_width=True)

            # 下载按钮
            st.download_button(
                label="📥 下载排期结果",
                data=result_data,
                file_name=f"排期结果_{seed}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

        except Exception as e:
            st.error(f"❌ 排期过程中出错: {str(e)}")
            st.exception(e)

# 页脚
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: gray;'>
    <p>剧集排期规划系统 v1.0 | 基于 Streamlit 构建</p>
</div>
""", unsafe_allow_html=True)
