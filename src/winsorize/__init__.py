"""winsorize package.

winsorize
"""

import numpy as np
import structlog
from bigmodule import I

# 需要安装的第三方依赖包
# from bigmodule import R
# R.require("requests>=2.0", "isort==5.13.2")

# metadata
# 模块作者
author = "BigQuant"
# 模块分类
category = "数据处理"
# 模块显示名
friendly_name = "去极值"
# 文档地址, optional
doc_url = "https://bigquant.com/wiki/"
# 是否自动缓存结果
cacheable = True

logger = structlog.get_logger()

def winsorize_MAD(df, columns, median_deviate=5):
    for factor in columns:
        median = df[factor].median()
        MAD = np.median(abs(df[factor] - median))
        df.loc[df[factor] > median + median_deviate * MAD, factor] = median + median_deviate * MAD
        df.loc[df[factor] < median - median_deviate * MAD, factor] = median - median_deviate * MAD
    return df


def winsorize_3sigma(df, columns, n=3):
    for factor in columns:
        mean = df[factor].mean()
        std = np.std(df[factor])
        df.loc[df[factor] > mean + n * std, factor] = mean + n * std
        df.loc[df[factor] < mean - n * std, factor] = mean - n * std
    return df


def winsorize_percentile(df, columns, min =0.025,max = 0.975):
    for factor in columns:
        q = df[factor].quantile([min,max])
        df.loc[df[factor] > q.iloc[1], factor] = q.iloc[1]
        df.loc[df[factor] < q.iloc[0], factor] = q.iloc[0]
    return df


FUNCTION_NAME = {'MAD' : winsorize_MAD,
    '3倍标准差': winsorize_3sigma,
    '百分位法': winsorize_percentile,}


def run(
    input_data: I.port('输入数据', specific_type_name='DataSource'),
    features: I.port('因子列表', optional=True, specific_type_name='列表|DataSource')=None,
    columns_input: I.code('指定列', auto_complete_type='feature_fields,bigexpr_functions')='',
    function_name: I.choice('去极值方法', values=list(FUNCTION_NAME.keys()))='MAD',
    group: I.choice('截面选择', values=["date", "instrument"])='date',
)->[
    I.port("去极值后的数据", "data")  # type: ignore
]:
    import dai

    data = input_data.read()
    columns = []
    if features is None:
        if not columns_input:
            logger.error('请输入去极值的列名或连接输入因子列表模块')
        else:
            columns = [line.strip() for line in columns_input.splitlines() if line.strip() and not line.strip().startswith("#")]
    else:
        columns = features.read()

    # 截面数据去极值
    df = data.groupby(group).apply(FUNCTION_NAME[function_name], columns,)
    return I.Outputs(data=dai.DataSource.write_bdb(df))


def post_run(outputs):
    """后置运行函数"""
    return outputs
