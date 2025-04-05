"""清洗中文教育数据 - 通用语料格式

运行方式：命令行到此文件上一层目录，执行
PYTHONPATH=. python examples/finewebedu2general.py
"""

import json
import logging
from pathlib import Path
from typing import Iterator, Union

import pandas as pd

from mnbvc.formats.general import GeneralCorpus, convert_to_general_corpus
from mnbvc.utils.writer import SizeLimitedFileWriter


def get_logger(log_path: str) -> logging.Logger:
    """获取一个logger。"""
    logger = logging.getLogger(__name__)
    handler = logging.FileHandler(log_path)
    formatter = logging.Formatter(
        fmt="[%(asctime)s][%(name)s][%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def convert_parquet_to_general_corpus(
    path: Union[Path, str],
    logger: logging.Logger
) -> Iterator[GeneralCorpus]:
    """将 jsonl 转化成通用语料格式。"""
    logger.debug(f"处理文件: {path}")

    df_data = pd.read_parquet(path)

    for idx, row in df_data.iterrows():
        source = row["source"].strip()
        text = row["text"].strip()
        text = text.strip()
        if not text:
            continue

        # 生成text id
        text_id = f"{source}-{idx}"

        # 转换成通用语料格式
        corpus = convert_to_general_corpus(
            text_id=text_id,
            text=text,
        )
        corpus.extension_fields = json.dumps({"source": source})
        yield corpus

    logger.debug(f"转换结束: {path}")


if __name__ == "__main__":
    # 修改指向数据文件夹
    path = "data/finewebedu"
    input_folder = Path(path)

    # 输出文件夹
    output_folder = "data/finewebedu-output"

    # 修改 log 的保存位置
    log_path = "data/finewebedu-log.txt"
    logger = get_logger(log_path)

    # 写入
    writer = SizeLimitedFileWriter(
        output_folder=output_folder,
        filename_idx_first=0,  # 从 0 开始
        filename_idx_width=6,  # 每个数字宽度，比如 0 -> 000000.jsonl
        filename_idx_stride=1,  # 下一个文件的数字增量
        filename_fmt="{}.jsonl"  # 如果想要压缩好的输出可以修改成 "{}.jsonl.gz"
    )

    for path in input_folder.glob("**/*.parquet"):
        for corpus in convert_parquet_to_general_corpus(path, logger):
            writer.writeline(corpus.model_dump(by_alias=True))

    writer.close()