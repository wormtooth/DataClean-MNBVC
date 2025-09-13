"""清洗NPR/CNN采访对话 - 论坛语料格式

运行方式：命令行到此文件上一层目录，执行
PYTHONPATH=. python examples/newsdialog2forum.py
"""

import json
import re
from pathlib import Path
from typing import Union

from tqdm import tqdm

from mnbvc.formats.forum import ForumCorpus, ForumMessage
from mnbvc.utils import get_logger
from mnbvc.utils.writer import SizeLimitedFileWriter

log_path = "data/news_dialog_log.txt"
logger = get_logger(log_path)


def convert_dialog_to_forum_corpus(
    id: int,
    dialog: dict
) -> dict:
    """将一个采访转换成论坛语料格式。
    每个采访包含：
    id: NPR-# or CNN-#
    program: 分类
    date: yyyy-mm-dd
    url: 采访链接
    summary: 采访总结
    utt: 对话
    speaker: 说话者，对应utt
    """
    logger.debug(f"处理采访: {dialog['id']}")
    
    create_time = dialog["date"]
    create_time = create_time.replace("-", "").strip()
    corpus = ForumCorpus(
        ID=id,
        主题=dialog.get("title", ""),
        来源=dialog["url"],
        时间=create_time
    )
    corpus.meta = {
        "源ID": dialog["id"],
        "摘要": dialog["summary"],
        "分类": dialog["program"]
    }
    
    replies = []
    for idx, (speaker, line) in enumerate(zip(dialog["speaker"], dialog["utt"])):
        extension_fields = {"说话者": speaker}
        msg = ForumMessage(
            楼ID=f"{idx}",
            回复=line,
            扩展字段=json.dumps(extension_fields)
        )
        replies.append(msg)
    corpus.replies = replies

    return corpus


if __name__ == "__main__":
    # 修改指向数据文件
    path = "data/news_dialogue.json"
    

    # 输出文件夹
    output_folder = "data/news_dialogue"

    # 写入
    writer = SizeLimitedFileWriter(
        output_folder=output_folder,
        filename_idx_first=0,  # 从 0 开始
        filename_idx_width=6,  # 每个数字宽度，比如 0 -> 000000.jsonl
        filename_idx_stride=1,  # 下一个文件的数字增量
        filename_fmt="{}.jsonl.gz"  # 如果想要压缩好的输出可以修改成 "{}.jsonl.gz"
    )

    with open(path) as fp:
        data = json.load(fp)
    for idx, item in tqdm(enumerate(data)):
        try:
            corpus = convert_dialog_to_forum_corpus(idx, item)
            if corpus is not None:
                writer.writeline(corpus.model_dump(by_alias=True))
        except Exception as e:
            logger.error(f"Error processing {item['id']}: {e}")

    writer.close()