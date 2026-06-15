"""代码语料格式
"""

import datetime
from typing import Union
from pathlib import Path

from pydantic import BaseModel, Field
from mnbvc.utils.encoding import detect_encoding
import hashlib


class CodeCorpus(BaseModel):
    """代码语料格式"""

    source: str = Field(
        description="代码来源，比如：github",
        alias="来源"
    )
    repo: str = Field(
        description="仓库名，比如：esbatmop/MNBVC",
        alias="仓库名"
    )
    path: str = Field(
        description="文件路径，比如：/main/README.md",
    )
    filename: str = Field(
        description="文件名，比如：README.md",
        alias="文件名"
    )
    ext: str = Field(
        description="文件扩展名，比如：md",
    )
    size: int = Field(
        description="文件大小",
    )
    original_encoding: str = Field(
        description="原始编码，比如：GBK",
        alias="原始编码"
    )
    md5: str = Field(
        description="文件的md5值",
    )
    text: str = Field(
        description="文件的内容，utf8格式",
    )
    create_time: str = Field(
        description="此语料生成的时间，格式为yyyymmdd。", alias="时间",
        default_factory=lambda: f"{datetime.date.today():%Y%m%d}"
    )


def convert_to_code_corpus(
    source: str, repo: str, path: str,
    local_path: Union[str, Path],
):
    """将文件转化成代码格式。"""
    local_path = Path(local_path)

    def _get_ext(name: str):
        idx = name.rfind(".")
        if idx == -1:
            return ""
        return name[idx + 1:]

    def _get_create_time():
        c_time = local_path.stat().st_ctime
        c_dt = datetime.datetime.fromtimestamp(c_time)
        return f"{c_dt:%Y%m%d}"

    raw = None
    with open(local_path, "rb") as fp:
        raw = fp.read()

    encoding = detect_encoding(raw_data=raw)
    text = raw.decode(encoding, errors="ignore")

    data = {
        "来源": source,
        "仓库名": repo,
        "path": path,
        "文件名": local_path.name,
        "ext": _get_ext(local_path.name),
        "size": local_path.stat().st_size,
        "原始编码": encoding,
        "md5": hashlib.md5(raw).hexdigest(),
        "text": text,
        "时间": _get_create_time(),
    }

    corpus = CodeCorpus(**data)
    return corpus
