"""清洗招商的金融数据 - 通用语料格式

运行方式：命令行到此文件上一层目录，执行
PYTHONPATH=. python examples/cmb2general.py
"""

import json
import logging
from pathlib import Path
from typing import Iterator, Union

import jsonlines

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


def convert_jsonl_to_general_corpus(
    path: Union[Path, str],
    logger: logging.Logger
) -> Iterator[GeneralCorpus]:
    """将 jsonl 转化成通用语料格式。"""
    logger.debug(f"处理文件: {path}")

    with jsonlines.open(path, "r") as reader:
        for data in reader:
            # 获取创建时间
            create_time = None
            try:
                dump = data["meta"]["dump"]
                year = dump.split("-")[0]
                create_time = f"{year}0101"
            except:
                logger.debug(f"Cannot find create time in {data.get('meta', {})}")

            # 转换成通用语料格式
            try:
                corpus = convert_to_general_corpus(
                    text_id=data["meta"].get("title", "").strip(),
                    text=data["text"],
                    create_time=create_time
                )
                corpus.extension_fields = json.dumps(data["meta"])
                yield corpus
            except Exception as e:
                logger.error(f"Cannot process [{e}]: {data}")
                continue

    logger.debug(f"转换结束: {path}")


if __name__ == "__main__":
    # 修改指向数据文件夹
    path = "data/CMB_FinDataSet_sample"
    input_folder = Path(path)

    # 输出文件夹
    output_folder = "data/cmb"

    # 修改 log 的保存位置
    log_path = "data/cmb_log.txt"
    logger = get_logger(log_path)

    # 写入
    writer = SizeLimitedFileWriter(
        output_folder=output_folder,
        filename_idx_first=0,  # 从 0 开始
        filename_idx_width=6,  # 每个数字宽度，比如 0 -> 000000.jsonl
        filename_idx_stride=1,  # 下一个文件的数字增量
        filename_fmt="{}.jsonl"  # 如果想要压缩好的输出可以修改成 "{}.jsonl.gz"
    )

    for jsonl_path in input_folder.glob("**/*.jsonl"):
        for corpus in convert_jsonl_to_general_corpus(jsonl_path, logger):
            writer.writeline(corpus.model_dump(by_alias=True))

    writer.close()
