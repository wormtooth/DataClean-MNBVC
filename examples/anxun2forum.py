"""清洗 AnXun 数据 - 论坛语料格式

原数据: https://github.com/soufianetahiri/Anxun-isoon

运行方式：命令行到此文件上一层目录，执行
PYTHONPATH=. python examples/anxun2forum.py
"""

import base64
import json
from pathlib import Path
from typing import Union

from bs4 import BeautifulSoup

from mnbvc.formats.forum import ForumCorpus, ForumMessage
from mnbvc.utils import get_logger
from mnbvc.utils.writer import SizeLimitedFileWriter

log_path = "data/anxun_log.txt"
logger = get_logger(log_path)
# 修改指向数据文件
input_folder = Path(
    "data/Anxun-isoon/InitialTranslations/LG-TRANS1/ORIGINAL/I-S00N/0")

processed_files = set()


def read_file_base64(path: Path) -> str:
    """读取文件内容被以 base64 编码。"""

    processed_files.add(str(path))
    with open(path, "rb") as fp:
        data = fp.read()
    ret = base64.b64encode(data).decode("utf-8")

    return ret


def process_conv_md(path: Union[Path, str]) -> Union[ForumCorpus, None]:
    """处理微信对话。"""

    logger.info(f"处理微信对话: {path}")
    processed_files.add(str(path))

    with open(path) as fp:
        soup = BeautifulSoup(fp.read(), "lxml")

    data = []
    create_time = None
    cols = ["time", "from", "to", "message"]
    speakers = set()
    for idx, tr in enumerate(soup.find_all("tr")):

        # extract message fields
        item = {}
        for col, td in zip(cols, tr.find_all("td")):
            item[col] = str(td).replace(
                "<td>", "").replace("</td>", "").strip()
        # skip header
        if not item:
            continue

        speakers.add(item["from"])
        speakers.add(item["to"])

        # use the first conversation timestamp as the create time
        if create_time is None:
            create_time = item["time"].split(" ")[0].replace("-", "")

        content = item.pop("message")
        if ("<a" in content) or ("<img" in content):
            attachments = process_attachment_md(content)
            if attachments:
                content = attachments[0].get("filename", "")
                item["attachments"] = attachments
        msg = ForumMessage(
            楼ID=f"{idx}",
            回复=content,
            扩展字段=json.dumps(item, ensure_ascii=False)
        )
        data.append(msg)

    file_name = path.name.replace(".md", "")
    speakers = ", ".join(sorted(speakers))
    corpus = ForumCorpus(
        ID=int(file_name),
        主题=f"微信对话: {speakers}",
        来源="https://github.com/soufianetahiri/Anxun-isoon",
        时间=create_time,
        回复=data
    )

    return corpus


def process_attachment_md(content: str) -> dict:
    """处理附件。"""
    soup = BeautifulSoup(content, "html.parser")
    attachments = []
    for child in soup.children:
        tag = child.name
        if not tag:
            continue

        if tag == "a":
            path = child.attrs["href"]
            if path.endswith(".png"):
                attachments.append({
                    "type": "image",
                    "path": path,
                })
            else:
                attachments.append({
                    "type": "file",
                    "path": child.attrs["href"],
                    "filename": child.text,
                })
        elif tag == "img":
            attachments.append({
                "type": "image",
                "path": child.attrs["src"],
            })
        elif tag == "br":
            continue
        else:
            attachments.append({
                "type": "text",
                "content": child.text,
            })

    # read in the file content in base64
    for item in attachments:
        if "path" not in item:
            continue
        path = item["path"]
        actual_path = input_folder / path
        filename = item.get("filename", path)
        if not actual_path.exists():
            logger.warning(f"Attachment {filename} not exist: {actual_path}")
            continue
        item["content"] = read_file_base64(actual_path)
        if path.endswith(".md"):
            processed_files.add(str(actual_path))
            with open(actual_path) as fp:
                subconetent = fp.read()
            item["attachments"] = process_attachment_md(subconetent)

    return attachments


if __name__ == "__main__":
    # 输出文件夹
    output_folder = Path("data/anxun")
    output_folder.mkdir(exist_ok=True, parents=True)

    writer = SizeLimitedFileWriter(
        output_folder=output_folder,
        filename_idx_first=0,  # 从 0 开始
        filename_idx_width=3,  # 每个数字宽度，比如 0 -> 000000.jsonl
        filename_idx_stride=1,  # 下一个文件的数字增量
        filename_fmt="anxun{}.jsonl"  # 如果想要压缩好的输出可以修改成 "{}.jsonl.gz"
    )

    for path in sorted(input_folder.glob("*.md")):
        file_name = path.name.replace(".md", "").strip()
        if not file_name.isdigit():
            continue

        file_name = int(file_name)
        output_path = output_folder / f"{file_name:02d}.jsonl"
        corpus = process_conv_md(path)
        data = corpus.model_dump_json(by_alias=True)
        writer.writeline(data)

    for path in sorted(input_folder.glob("*.md")):
        if str(path) not in processed_files:
            logger.warning(f"Markdown not processed: {path}")