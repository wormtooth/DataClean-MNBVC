"""清洗wudao数据 - 通用语料格式

运行方式：命令行到此文件上一层目录，执行
PYTHONPATH=. python examples/wudao2general.py
"""

import os
import json
import logging
import uuid
from pathlib import Path
from typing import Iterator, Union

from tqdm import tqdm

from mnbvc.formats.general import GeneralCorpus, convert_to_general_corpus
from mnbvc.utils.writer import SizeLimitedFileWriter


def get_uuid() -> str:
    """获取一个 uuid。"""
    return uuid.uuid4().hex


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


def convert_json_to_general_corpus(
    path: Union[Path, str],
    logger: logging.Logger
) -> Iterator[GeneralCorpus]:
    """将 JSON 转化成通用语料格式。"""
    logger.debug(f"处理文件: {path}")

    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        for item in data:
            # 获取标题和内容
            title = item.get('title', '').strip()
            content = item.get('content', '').strip()
            if not content:
                continue

            # 生成text id - 每次运行都会生成新的id
            text_id = get_uuid()

            # 转换成通用语料格式
            corpus = convert_to_general_corpus(
                text_id=text_id,
                text=content,
            )
            
            # 添加扩展字段
            extension_fields = {
                "title": title,
                "dataType": item.get('dataType', ''),
                "uniqueKey": item.get('uniqueKey', ''),
                "titleUkey": item.get('titleUkey', '')
            }
            corpus.extension_fields = json.dumps(extension_fields, ensure_ascii=False)
            yield corpus

    logger.debug(f"转换结束: {path}")


if __name__ == "__main__":
    # 修改指向数据文件夹
    path = "F:/待检查/20230117/"
    input_folder = Path(path)

    # 输出文件夹
    output_folder = "F:/待检查/20230117_output"
    os.makedirs(output_folder, exist_ok=True)

    # 修改 log 的保存位置
    log_path = "F:/待检查/20230117_output_wudao.20230117.1.网页_log.txt"
    logger = get_logger(log_path)


    data_list = list(input_folder.glob("**/*.json"))

    for json_path in tqdm(data_list):    
        # 写入
        file_name = json_path.stem
        output_dir = json_path.relative_to(input_folder).parent
        writer = SizeLimitedFileWriter(
            output_folder=output_folder / output_dir,
            filename_idx_first=0,  # 从 0 开始
            filename_idx_width=6,  # 每个数字宽度，比如 0 -> 000000.jsonl
            filename_idx_stride=1,  # 下一个文件的数字增量
            filename_fmt=f"{file_name}_" + "{}.jsonl"  # 如果想要压缩好的输出可以修改成 "{}.jsonl.gz"
        )
        for corpus in convert_json_to_general_corpus(json_path, logger):
            writer.writeline(corpus.model_dump(by_alias=True))
        writer.close() 