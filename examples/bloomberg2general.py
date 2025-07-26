"""清洗彭博社 - 通用语料格式

运行方式：命令行到此文件上一层目录，执行
PYTHONPATH=. python examples/bloomberg2general.py
"""

import json
import re
from pathlib import Path
from typing import Union

from tqdm import tqdm

from mnbvc.formats.general import convert_to_general_corpus
from mnbvc.utils import get_logger
from mnbvc.utils.writer import SizeLimitedFileWriter

log_path = "data/bloomberg_log.txt"
logger = get_logger(log_path)


def parse_headers(lines: list[str]):
    """拆解新闻标题/作者/时间/链接。"""
    info = []
    for idx, line in enumerate(lines):
        if line.startswith("--"):
            info.append("")
            line = line[2:]
        line = line.strip()
        if not line:
            continue
        if info[-1]:
            info[-1] = info[-1] + " " + line
        else:
            info[-1] = line
        if len(info) == 4:
            break

    if len(info) != 4 or (not info[3].startswith("http")):
        raise Exception("Error parsing the headers")

    headers = {
        "title": info[0],
        "author": info[1],
        "timestamp": info[2],
        "url": info[3]
    }
    headers["author"] = (
        headers["author"]
        .replace("  ", "#")
        .replace(" ", "")
        .replace("#", " ")
    )
    if headers["author"].startswith("By"):
        headers["author"] = headers["author"][2:].strip()

    return headers, lines[idx + 1:]


def segment_text(text: str) -> list[str]:
    """拆解文本 - 主要是分离联系方式。"""
    text = text.strip()
    if not text:
        return text

    contact1 = "To contact the reporter on this story:"
    contact2 = "To contact the editor responsible for this story:"
    indexes = []
    for contact in [contact1, contact2]:
        idx = text.find(contact)
        if idx != -1:
            indexes.append(idx)
    indexes.sort()
    indexes = [0] + indexes + [len(text)]
    ret = []
    for a, b in zip(indexes[:-1], indexes[1:]):
        piece = text[a: b].strip()
        ret.append(piece)
    return ret


def parse_news(news: str) -> dict:
    """拆解彭博社新闻内容。"""
    lines = news.strip().splitlines()

    # find headers
    headers, texts = parse_headers(lines)

    data = headers
    # normalize text
    if texts:
        texts = map(str.strip, texts)
        text = " ".join(texts)
        ptn = r" {2,}"
        text = re.sub(ptn, " ", text)
        data["text"] = segment_text(text)

    return data


def convert_news_to_general_corpus(
    path: Union[Path, str]
) -> dict:
    """将彭博社新闻转化成通用语料格式。"""
    logger.debug(f"处理文件: {path}")

    with open(path, "r") as fp:
        news = fp.read()
    if not news.strip():
        logger.warning(f"Empty file: {path}")
        return None
    
    data = parse_news(news)
    
    create_time = data["timestamp"]
    idx = create_time.find("T")
    if idx != -1:
        create_time = create_time[:idx].replace("-", "").strip()
    corpus = convert_to_general_corpus(
        text_id=data["url"],
        text=data["text"],
        create_time=create_time,
    )
    fields = {
        "title": data["title"],
        "publish_time": data["timestamp"],
    }
    corpus.extension_fields = json.dumps(fields)

    logger.debug(f"转换结束: {path}")

    return corpus


if __name__ == "__main__":
    # 修改指向数据文件夹
    path = "data/bloomberg_news"
    input_folder = Path(path)

    # 输出文件夹
    output_folder = "data/bloomberg"

    # 写入
    writer = SizeLimitedFileWriter(
        output_folder=output_folder,
        filename_idx_first=0,  # 从 0 开始
        filename_idx_width=6,  # 每个数字宽度，比如 0 -> 000000.jsonl
        filename_idx_stride=1,  # 下一个文件的数字增量
        filename_fmt="{}.jsonl"  # 如果想要压缩好的输出可以修改成 "{}.jsonl.gz"
    )

    count = 0
    files = sorted(input_folder.glob("**/*"))
    for text_path in tqdm(files):
        if text_path.is_dir() or text_path.name.startswith("."):
            continue
        try:
            corpus = convert_news_to_general_corpus(text_path)
            if corpus is None:
                continue
        except Exception as e:
            logger.error(f"Error processing {text_path}: {e}")
        
        if corpus is not None:
            writer.writeline(corpus.model_dump(by_alias=True))

    writer.close()
