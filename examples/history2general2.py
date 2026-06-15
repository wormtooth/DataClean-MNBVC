"""清洗历史数据 - 通用语料格式

用于处理数据包：20230115

运行方式：命令行到此文件上一层目录，执行
PYTHONPATH=. python examples/history2general2.py
"""

import json
import re
from pathlib import Path
from typing import Callable, Iterable, Union
import logging

from mnbvc.formats.general import convert_to_general_corpus
from mnbvc.formats.code import convert_to_code_corpus
from mnbvc.formats.qa import QACorpus, QAMetaData
from mnbvc.utils import get_logger
from mnbvc.utils.writer import SizeLimitedFileWriter
from mnbvc.utils.encoding import open_text


folder5_filemeta = {
    "single_dialogue.zip": {
        "files": [
            "single_dialogue.txt"
        ],
        "format": "general"
    },
    "weibo_classify_sentiment.zip": {
        "files": [
            "0_happiness.txt",
            "1_anger.txt",
            "2_hate.txt",
            "3_upset.txt"
        ],
        "format": "general"
    },
    "spam_messages.zip": {
        "files": [
            "DataSet_1.txt",
            "DataSet_2.txt",
            "DataSet_3.txt",
            "DataSet_4.txt",
            "DataSet_5.txt",
            "DataSet_6.txt"
        ],
        "format": "general"
    },
    "zhihu.zip": {
        "files": [
            "zhihu.json"
        ],
        "format": "zhihu"
    },
    "chinese_dict.zip": {
        "files": [
            "chinese_dict.txt"
        ],
        "format": "general"
    },
    "sentiment_dict.zip": {
        "files": [
            "sentiment_score.txt"
        ],
        "format": "code"
    },
    "another_sentiment_dict.zip": {
        "files": [
            "主张词语（中文）.txt",
            "主张词语（英文）.txt",
            "正面情感词语（中文）.txt",
            "正面情感词语（英文）.txt",
            "正面评价词语（中文）.txt",
            "正面评价词语（英文）.txt",
            "程度级别词语（中文）.txt",
            "程度级别词语（英文）.txt",
            "负面情感词语（中文）.txt",
            "负面情感词语（英文）.txt",
            "负面评价词语（中文）.txt",
            "负面评价词语（英文）.txt"
        ],
        "format": "code"
    }
}


def process_subfolder(
    input_folder: Path,
    subfolder: str,
    output_folder: Path,
    logger: logging.Logger,
    process_func: Callable[[Path, logging.Logger], Iterable[dict]],
    filename_fmt: Union[str, None] = None
):
    """Process a subfolder with a given function."""
    if filename_fmt is None:
        filename_fmt = subfolder + ".{}.jsonl"
    writer = SizeLimitedFileWriter(
        output_folder,
        filename_fmt=filename_fmt,
    )
    subfolder_path = input_folder / subfolder
    try:
        for data in process_func(subfolder_path, logger):
            writer.writeline(data)
    except Exception as e:
        logger.warning(f"Error processing {subfolder_path}: {e}")
    writer.close()


def process_article1(folder: Path, logger: logging.Logger) -> Iterable[dict]:
    """github.20230115.1.论文"""
    logger.info("github.20230115.1.论文")
    for json_path in folder.glob("**/*.json"):
        with open(json_path, "r") as fp:
            data = json.load(fp)

        for item in data:
            title = item.pop("title", "").strip()
            content = item.pop("content", "").strip()
            if (not title) or (not content):
                continue
            article = convert_to_general_corpus(
                text_id=title,
                text=content,
                create_time="20230115",
            )
            article.extension_fields = json.dumps(item, ensure_ascii=False)
            yield article.model_dump(by_alias=True)


def process_news3(folder: Path, logger: logging.Logger) -> Iterable[dict]:
    """github.20230115.3.新闻"""

    logger.info("github.20230115.3.新闻")

    def _remove_line_prefix(line):
        prefix_ptn = r"\d+ \|\|\|"
        line = re.sub(prefix_ptn, "", line)
        line = re.sub(r"\s", "", line)
        line = line.strip()
        return line

    def _get_label(line):
        idx = line.find("|||")
        if idx == -1:
            return line, ""
        label = line[idx + 3:].strip()
        line = line[:idx].strip()
        return line, label

    def _process_news(title, cases):
        line, label = _get_label(cases[-1])
        cases[-1] = line
        news = convert_to_general_corpus(
            text_id=title,
            text=cases,
            create_time="20230115",
        )
        extra = {"标签": label}
        news.extension_fields = json.dumps(extra, ensure_ascii=False)
        return news.model_dump(by_alias=True)

    for text_path in folder.glob("**/*.txt"):
        path_name = text_path.name.replace(".txt", "").strip()
        id = 1
        cases = []
        with open(text_path, "r") as fp:
            for line in fp:
                line = line.strip()
                if line.startswith("1 |||"):
                    if cases:
                        title = f"{path_name}-{id}"
                        news = _process_news(title, cases)
                        yield news
                        id += 1
                    cases = []
                cases.append(_remove_line_prefix(line))
        if cases:
            title = f"{path_name}-{id}"
            news = _process_news(title, cases)
            yield news
            id += 1


def process_qa4(folder: Path, logger: logging.Logger) -> Iterable[dict]:
    """github.20230115.4.问答"""

    logger.info("github.20230115.4.问答")

    def _process_qa(data):
        title = data.get("title", "UNKNOWN")
        context = data.get("context", "")
        for qa in data.get("qas", []):
            question = qa.get("question", "")
            answers = qa.get("answers", [])
            id = qa.get("id", "")
            answer = "\n".join(a["text"]
                               for a in answers if ("text" in a) and a["text"])
            meta = QAMetaData(
                回答明细=json.dumps(
                    {"answers": answers, "is_impossible": qa.get(
                        "is_impossible")},
                    ensure_ascii=False
                ),
                扩展字段=json.dumps(
                    dict(title=title, context=context, type=qa.get("type")),
                    ensure_ascii=False
                )
            )
            corpus = QACorpus(
                id=id,
                问=question,
                答=answer,
                时间="20230115",
                元数据=meta
            )
            yield corpus.model_dump(by_alias=True)

    for json_path in folder.glob("**/*.json"):
        try:
            with open(json_path, "r") as fp:
                raw_data = json.load(fp)
        except Exception as e:
            logger.warning(f"Fail to load {json_path}: {e}")
            continue

        try:
            data = raw_data["data"]
        except:
            logger.warning(f"No data in {json_path}")

        for item in data:
            for qa in item.get("paragraphs", []):
                yield from _process_qa(qa)


def process_qa5_general(folder: Path, logger: logging.Logger) -> Iterable[dict]:
    """github.20230115.5.问答 - 通用语料"""

    logger.info("github.20230115.5.问答 - 通用语料")

    for source, meta in folder5_filemeta.items():
        if meta["format"] != "general":
            continue
        logger.info(f"Processing files from {source}")

        for file in meta["files"]:
            extra = {
                "url": "https://github.com/GeneralZh/Chinese_Corpus.git",
                "zip": source,
                "file": file
            }
            path = folder / file
            try:
                text = open_text(path, logger=logger)
                data = convert_to_general_corpus(
                    text_id=file,
                    text=text,
                    create_time="20230115"
                )
                data.extension_fields = json.dumps(extra, ensure_ascii=False)
                data = data.model_dump(by_alias=True)
                yield data
            except Exception as e:
                logger.warning(f"Error processing {path}: {e}")


def process_qa5_code(folder: Path, logger: logging.Logger) -> Iterable[dict]:
    """github.20230115.5.问答 - 代码语料"""

    logger.info("github.20230115.5.问答 - 代码语料")

    for source, meta in folder5_filemeta.items():
        if meta["format"] != "code":
            continue
        logger.info(f"Processing files from {source}")

        for file in meta["files"]:
            path = folder / file
            try:
                data = convert_to_code_corpus(
                    source="github",
                    repo="GeneralZh/Chinese_Corpus",
                    path=source,
                    local_path=path
                )
                data = data.model_dump(by_alias=True)
                yield data
            except Exception as e:
                logger.warning(f"Error processing {path}: {e}")


def process_qa5_zhihu(folder: Path, logger: logging.Logger) -> Iterable[dict]:
    """github.20230115.5.问答 - 知乎问答语料"""

    logger.info("github.20230115.5.问答 - 知乎问答语料")

    def _get_create_time(data):
        time_fields = [
            "q_create_time",
            "q_last_modify"
        ]
        for field in time_fields:
            if not field in data:
                continue
            try:
                val = data[field].strip()[:10].replace("-", "")
                return val
            except:
                pass
        return "20230115"

    def _process_lines(lines, extra):
        text = "\n".join(lines)
        try:
            ptn1 = r"ObjectId\(\"(.*)\"\)" #ObjectId("55c9b970f9457a32a5ccf96b")
            text = re.sub(ptn1, r'"\1"', text)
            ptn2 = r"ISODate\(\"(.*)\"\)" # ISODate("2015-08-11T08:59:28.714Z")
            text = re.sub(ptn2, r'"\1"', text)
            data: dict = json.loads(text)
        except Exception as e:
            logger.warning(f"Cannot load data: {text}: {e}")
            return
        
        answers = data.pop("q_answers", [])
        title = data.get("q_title", "")
        question = data.get("q_content", "") or title
        for answer_data in answers or [{}]:
            answer = answer_data.get("a_content", "")
            meta = QAMetaData(
                回答明细=json.dumps(
                    {"问题": data, "答案": answer_data},
                    ensure_ascii=False
                ),
                扩展字段=json.dumps(
                    extra,
                    ensure_ascii=False
                )
            )
            corpus = QACorpus(
                id=data["_id"],
                问=question,
                答=answer,
                时间=_get_create_time(data),
                来源="知乎",
                元数据=meta
            )
            yield corpus.model_dump(by_alias=True)
        

    
    for source, meta in folder5_filemeta.items():
        if meta["format"] != "zhihu":
            continue
        logger.info(f"Processing files from {source}")
        
        for file in meta["files"]:
            extra = {
                "url": "https://github.com/GeneralZh/Chinese_Corpus.git",
                "zip": source,
                "file": file
            }
            path = folder / file
            lines = []
            with open(path) as fp:
                for line in fp:
                    line = line.rstrip()
                    lines.append(line)
                    if line == "{":
                        if len(lines) != 1:
                            lines = [line]
                    elif line == "}":
                        yield from _process_lines(lines=lines, extra=extra)
                        lines = []               


def process_novel6(folder: Path, logger: logging.Logger) -> Iterable[dict]:
    """aliyun.20230115.6.网络小说"""

    logger.info("aliyun.20230115.6.网络小说")

    for txt_path in folder.glob("**/*.txt"):
        try:
            raw_text = open_text(txt_path, logger=logger)
            text = raw_text.replace(
                "更多精校小说尽在知轩藏书下载：http://www.zxcs8.com/", "").strip("=\n ")
            data = convert_to_general_corpus(
                text_id=txt_path.name,
                text=text,
                create_time="20230115",
            )
            data = data.model_dump(by_alias=True)
            yield data
        except Exception as e:
            logger.warning(f"Error processing {txt_path}: {e}")


if __name__ == "__main__":
    # 历史数据文件夹
    input_folder = Path("data/20230115_sample")

    # 结果输出文件夹
    output_folder = input_folder / "output"
    output_folder.mkdir(exist_ok=True)

    # 修改 log 的保存位置
    log_path = input_folder / "log.txt"
    logger = get_logger(log_path)

    process_funcs = [
        ["github.20230115.1.论文", process_article1, {}],
        ["github.20230115.3.新闻", process_news3, {}],
        ["github.20230115.4.问答", process_qa4, {}],
        ["aliyun.20230115.6.网络小说", process_novel6, {}],
        ["github.20230115.5.问答", process_qa5_general, {"filename_fmt": "github.20230115.5.问答-通用格式-{}.jsonl"}],
        ["github.20230115.5.问答", process_qa5_code, {"filename_fmt": "github.20230115.5.问答-代码格式-{}.jsonl"}],
        ["github.20230115.5.问答", process_qa5_zhihu, {"filename_fmt": "github.20230115.5.问答-问答格式-{}.jsonl"}],
    ]

    for subfolder, process_func, kwargs in process_funcs:
        process_subfolder(
            input_folder,
            subfolder,
            output_folder,
            logger,
            process_func,
            **kwargs
        )
