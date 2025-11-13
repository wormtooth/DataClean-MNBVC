"""清洗CCPDF数据 - 通用语料格式

运行方式：命令行到此文件上一层目录，执行
PYTHONPATH=. python examples/ccpdf2general.py
"""

import re
from pathlib import Path
from typing import Union
import datetime
import json

import pandas as pd

from mnbvc.formats.general import GeneralCorpus, convert_to_general_corpus
from mnbvc.utils import get_logger
from mnbvc.utils.writer import SizeLimitedFileWriter


def get_writer_from_path(path: Path, output_folder: Union[Path, str], writers: dict[str, SizeLimitedFileWriter]) -> SizeLimitedFileWriter:
    """根据路径找到相对应的writer。
    """
    # 根据路径找出语言信息
    lang_ptn = r"([a-z]*)_Hani"
    found = re.search(lang_ptn, str(path))
    lang = "default"
    if found:
        lang = found.group(1)

    # 增加相应语言写入工具
    if lang not in writers:
        writer = SizeLimitedFileWriter(
            output_folder=output_folder,
            filename_idx_first=0,  # 从 0 开始
            filename_idx_width=6,  # 每个数字宽度，比如 0 -> 000000.jsonl
            filename_idx_stride=1,  # 下一个文件的数字增量
            filename_fmt=lang + "_{}.jsonl"  # 如果想要压缩好的输出可以修改成 "{}.jsonl.gz"
        )
        writers[lang] = writer

    return writers[lang]


def break_text(text: str) -> list[str]:
    """将内容分解成行。
    
    保留空行 - 将在写入时去除但保留行号信息以供完全恢复的需要。
    """
    lines = text.splitlines()
    return lines


def process_parquet(path: Path, output_folder: Union[Path, str], writers: dict[str, SizeLimitedFileWriter]):
    """处理一个parquet文件。"""

    df_data = pd.read_parquet(path)
    writer = get_writer_from_path(path, output_folder, writers)

    for _, row in df_data.iterrows():
        data = row.to_dict()
        text = data.pop("text", None)
        if text is None:
            continue
        text_id = data["id"]
        create_time = f"{datetime.datetime.now():%Y%m%d}"
        try:
            date = pd.to_datetime(data["date"])
            create_time = f"{date:%Y%m%d}"
        except:
            pass
        texts = break_text(text)
        corpus = convert_to_general_corpus(
            text_id=text_id,
            text=texts,
            create_time=create_time
        )
        corpus.extension_fields = json.dumps(data, default=str)
        writer.writeline(corpus.model_dump(by_alias=True))


if __name__ == "__main__":
    # 修改指向数据文件夹
    path = "data/ccpdf"
    input_folder = Path(path)

    # 输出文件夹
    output_folder = Path("data/ccpdf/output")

    # 修改 log 的保存位置
    log_path = "data/ccpdf/log.txt"
    logger = get_logger(log_path)

    # 写入
    # 不同的语言用不一样的writer来保证文件名包含语言种类信息
    writers: dict[str, SizeLimitedFileWriter] = {}

    parquet_files = input_folder.glob("**/*.parquet")
    for path in parquet_files:
        try:
            process_parquet(path, output_folder, writers)
        except Exception as e:
            logger.error(f"Error processing {path}: {e}")

    # 结束写入
    for writer in writers.values():
        writer.close()
