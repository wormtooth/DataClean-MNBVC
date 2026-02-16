"""清洗历史数据 - 通用语料格式

用于处理数据包：20230112

运行方式：命令行到此文件上一层目录，执行
PYTHONPATH=. python examples/bloomberg2general.py
"""

import json
import logging
import uuid
from pathlib import Path
import re

import chardet

from mnbvc.formats.general import GeneralCorpus, convert_to_general_corpus
from mnbvc.utils.writer import SizeLimitedFileWriter
from mnbvc.utils import get_logger


def read_file(path: Path) -> str:
    """读取文本文件 - 用chardet检测编码。"""

    folder = path.parent.name
    encoding = "GB18030"
    if folder == "riddle.20230111.1.أصسُ":
        encoding = "utf-8"

    with open(path, "r", encoding=encoding, errors="ignore") as fp:
        text = fp.read()

    return text


def preprocessing_duzhe(text: str) -> tuple[str, dict]:
    """预处理文件夹: duzhe.20230111.2.杂志"""
    header = "| 注册 | 登陆 | 家园首页 | 家园简介 | 版主招聘 | 原创佳句 | 新春祝福 |"
    footer = "| 设为首页 | 加入收藏 | 联系我们 | 版权申明 |"
    
    text = text.replace(header, "").strip()
    footer_idx = text.find(footer)
    footer = ""
    if footer_idx != -1:
        footer = text[footer_idx:]
        text = text[:footer_idx].strip()
    
    extra_attrs = {}
    create_time_ptn = r"创建时间：(\d+)-(\d+)-(\d+)"
    found = re.search(create_time_ptn, footer)
    if found:
        year, month, day = map(int, found.groups())
        create_time = f"{year}{month:02d}{day:02d}"
        extra_attrs["create_time"] = create_time


    return text, extra_attrs


def preprocessing_txtsk(text: str) -> tuple[str, dict]:
    """预处理 txtsk.20230112.5.شستى"""
    header = "欢迎访问:   www.txtsk.com.cn"
    footer = "更多免费txt电子书，欢迎您到www.txtsk.com.cn下载"

    header_idx = text.find(header)
    if header_idx != -1:
        header_idx += len(header)
        text = text[header_idx:].strip()

    footer_idx = text.find(footer)
    if footer_idx != -1:
        text = text[:footer_idx].strip()
    
    return text, {}


def preprocessing_text(folder: str, filename: str, text: str) -> tuple[str, dict]:
    """预处理 - 根据文件所在的文件夹处理。"""
    if folder == "duzhe.20230111.2.杂志":
        return preprocessing_duzhe(text)
    if folder == "txtsk.20230112.5.شستى":
        return preprocessing_txtsk(text)
    if folder == "riddle.20230111.1.أصسُ":
        return text, {}
    return None, None


if __name__ == "__main__":
    # 历史数据文件夹
    input_folder = Path("data/20230112")

    # 结果输出文件夹
    output_folder = input_folder / "output"
    output_folder.mkdir(exist_ok=True)

    # 修改 log 的保存位置
    log_path = "data/20230112/log.txt"
    logger = get_logger(log_path)

    # writer
    writer = SizeLimitedFileWriter(output_folder)

    # 处理 txt 文件
    for path in sorted(input_folder.glob("**/*.txt")):
        folder = path.parent.name
        filename = path.name

        # text_id
        text_id = f"{folder}-{filename}"

        # create_time
        create_time = "20230112"

        # text data
        raw_text = read_file(path)
        if raw_text is None:
            continue

        attributes = {}
        # preprocessing
        text, extra_attrs = preprocessing_text(folder, filename, raw_text)
        if text is None:
            continue
        
        attributes.update(extra_attrs)
        corpus = convert_to_general_corpus(
            text_id=text_id,
            text=text,
            create_time=create_time,
        )
        for key, val in attributes.items():
            setattr(corpus, key, val)
        
        writer.writeline(corpus.model_dump())
    
    writer.close()
