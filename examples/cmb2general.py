"""清洗招商的金融数据 - 通用语料格式

运行方式：命令行到此文件上一层目录，执行
PYTHONPATH=. python examples/cmb2general.py
"""

import json
import logging
from pathlib import Path
from typing import Union, Iterator

from mnbvc.formats.general import convert_to_general_corpus, GeneralCorpus
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

    with open(path, "r", encoding="UTF-8-SIG", errors="ignore") as fp:
        text = fp.read().strip()
    if not text:
        logger.info(f"文件没有内容: {path}")
        return None

    # 将 \u2028 替换成空格
    # 不然会导致下面的 text.splitlines() 出问题
    text = text.replace("\u2028", " ")

    # 每一行是一个json
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        # 将数据转成dict
        try:
            data = json.loads(line)
        except Exception as e:
            logger.error(f"无法加载成JSON[{e}]: {line}")
            continue

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
