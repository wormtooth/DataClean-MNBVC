"""清洗历史数据 - 通用语料格式

用于处理数据包：20230112

运行方式：命令行到此文件上一层目录，执行
PYTHONPATH=. python examples/history2general.py
"""

import json
import re
from pathlib import Path
from typing import Union

from mnbvc.formats.general import convert_to_general_corpus
from mnbvc.formats.qa import QACorpus, QAMetaData
from mnbvc.utils import get_logger
from mnbvc.utils.writer import SizeLimitedFileWriter


def read_file(path: Path) -> str:
    """读取文本文件 - 用chardet检测编码。"""

    folder = path.parent.name
    encoding = "GB18030"
    if folder == "riddle.20230111.1.谜语":
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
    """预处理 txtsk.20230112.5.小说"""
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
    if folder == "txtsk.20230112.5.小说":
        return preprocessing_txtsk(text)
    if folder == "riddle.20230111.1.谜语":
        return text, {}
    return None, None


def get_writer(
    output_folder: Path,
    writers: dict[str, SizeLimitedFileWriter],
    path: Union[Path, str],
) -> SizeLimitedFileWriter:
    """根据文件路径返回对应的 writer"""

    keys = [
        "riddle.20230111.1.谜语",
        "duzhe.20230111.2.杂志",
        "github.20230111.3.文章",
        "afqmc.20230111.4.金融",
        "txtsk.20230112.5.小说",
    ]
    path = str(path)

    found_key = "default"
    for key in keys:
        if key in path:
            found_key = key
            break

    if found_key not in writers:
        filename_fmt = found_key + ".{}.jsonl.gz"
        writer = SizeLimitedFileWriter(
            output_folder, filename_fmt=filename_fmt)
        writers[found_key] = writer

    return writers[found_key]


if __name__ == "__main__":
    # 历史数据文件夹
    # 解压 20230112.zip，并重命名一下文件夹
    # riddle.20230111.1.أصسُ  -> riddle.20230111.1.谜语
    # txtsk.20230112.5.شستى  -> txtsk.20230112.5.小说
    input_folder = Path("data/20230112")

    # 结果输出文件夹
    output_folder = input_folder / "output"
    output_folder.mkdir(exist_ok=True)

    # 修改 log 的保存位置
    log_path = "data/20230112/log.txt"
    logger = get_logger(log_path)

    # writers
    writers = {}

    # 处理 txt 文件
    for path in sorted(input_folder.glob("**/*.txt")):
        folder = path.parent.name
        filename = path.name
        # 跳过根目录的 txt
        if folder == "20230112":
            continue
        writer = get_writer(output_folder, writers, path)

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

        writer.writeline(corpus.model_dump(by_alias=True))

    # 处理：github.20230111.3.文章
    article_folder = input_folder / "github.20230111.3.文章"
    for path in sorted(article_folder.glob("**/*.json")):
        writer = get_writer(output_folder, writers, path)
        with open(path, "r") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                data: dict = json.loads(line)
                content = data.get("content", "").strip()
                if not content:
                    content
                title = data.get("title", "微信文章")
                account = data.get("account", "微信账号")
                data.pop("content", None)

                corpus = convert_to_general_corpus(
                    text_id=f"{account}-{title}",
                    text=content,
                    create_time="20230111"
                )
                corpus.extension_fields = json.dumps(data, ensure_ascii=False)
                writer.writeline(corpus.model_dump(by_alias=True))

    # 处理：afqmc.20230111.4.金融
    finance_folder = input_folder / "afqmc.20230111.4.金融"
    qa_template = """
以下两个关于花呗的句子意思是否相同？
句子一：{sentence1}
句子二：{sentence2}
""".strip()
    for path in sorted(finance_folder.glob("*.json")):
        folder = path.parent.name
        filename = path.name

        # id prefix
        id_prefix = f"{folder}-{filename}"

        # create_time
        create_time = "20230112"

        with open(path, "r") as fp:
            for idx, line in enumerate(fp):
                data = json.loads(line)
                meta = QAMetaData()
                meta.extension_fields = json.dumps(data, ensure_ascii=False)
                answer = "未知"
                if "label" in data:
                    answer = "是" if data["label"] == "1" else "否"
                corpus = QACorpus(
                    id=f"{id_prefix}-{idx:05d}",
                    问=qa_template.format(**data),
                    答=answer,
                    元数据=meta
                )
                corpus.create_time = create_time
                writer.writeline(corpus.model_dump(by_alias=True))

    # 关闭所有writers
    for writer in writers.values():
        writer.close()
