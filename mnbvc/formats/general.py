"""通用语料格式
"""

import datetime
import hashlib
from typing import List, Union

from pydantic import BaseModel, Field, computed_field

from mnbvc.utils.simhash import Simhash


class GeneralParagraph(BaseModel):
    """通用语料格式 - 段落"""

    line_no: int = Field(
        description="该段落所在的行号。", alias="行号"
    )
    repeated: bool = Field(
        description="此段落是否在此文件之前出现过。", alias="是否重复", default=False
    )
    repeated_across_files: bool = Field(
        description="此段落是否在其他文件出现过。", alias="是否跨文件重复", default=False
    )
    content: str = Field(
        description="此段落的内容", alias="内容"
    )
    extension_fields: str = Field(
        description="关于此段落的其他信息。", alias="扩展字段",
        default=""
    )

    @computed_field
    @property
    def md5(self) -> str:
        if not hasattr(self, "_md5"):
            self._compute_md5()
        return self._md5

    def _compute_md5(self):
        self._md5 = hashlib.md5(self.content.encode()).hexdigest()
        return self._md5


class GeneralCorpus(BaseModel):
    """通用语料格式"""

    file_name: str = Field(
        description="语料的文件名", alias="文件名"
    )
    to_check: bool = Field(
        description="是否需要检查语料的重复率与质量。", alias="是否待查文件", default=False
    )
    repeated: bool = Field(
        description="", alias="是否重复文件", default=False
    )
    file_size: int = Field(
        description="", alias="文件大小", default=0
    )
    longest_length: int = Field(
        description="此语料中最长段落的长度。", alias="最长段落长度", default=0,
    )
    paragraphs_count: int = Field(
        description="此语料中段落的数量。", alias="段落数", default=0,
    )
    unique_paragraphs_count: int = Field(
        description="此语料中去重后段落的数量。", alias="去重段落数", default=0
    )
    low_quality_paragraphs_count: int = Field(
        description="此语料中低质量段落的数量。低质量段落指无关信息等。", alias="低质量段落数", default=0
    )
    paragraphs: List[GeneralParagraph] = Field(
        description="此语料的所有段落。", alias="段落"
    )
    create_time: str = Field(
        description="此语料生成的时间，格式为yyyymmdd。", alias="时间",
        default_factory=lambda: f"{datetime.date.today():%Y%m%d}"
    )
    extension_fields: str = Field(
        description="关于此语料的其他信息，为JSON格式。", alias="扩展字段", default=""
    )

    @computed_field
    @property
    def simhash(self) -> int:
        texts = [
            paragraph.content
            for paragraph in self.paragraphs
        ]
        simhash_val = Simhash(texts).value
        return simhash_val

    @classmethod
    def name(cls):
        return "通用语料格式"


def convert_to_general_corpus(
        text_id: str,
        text: Union[str, List[str]],
        create_time: str = None,
        strip=True,
) -> GeneralCorpus:
    """将文件转化成通用语料格式。

    Returns:
        GeneralCorpusFormat: 将文件转化后的通用语料格式
    """

    if create_time is None:
        create_time = f"{datetime.datetime.today():%Y%m%d}"

    if type(text) is str:
        lines = text.split("\n")
    else:
        lines = text

    paragraphs = []
    max_len = -1

    hashes = set()

    for idx, line in enumerate(lines):
        if not line.strip():
            continue
        if strip:
            line = line.strip()

        line_dict = {
          "行号": idx + 1,
          "内容": line,
        }
        paragraph = GeneralParagraph(**line_dict)

        paragraphs.append(paragraph)
        max_len = max(max_len, len(line))

        # 去重
        paragraph.repeated = paragraph.md5 in hashes
        hashes.add(paragraph.md5)

    corpus_info = {
      "文件名": text_id,
      "是否待查文件": False,
      "是否重复文件": False,
      "文件大小": len(text),
      "最长段落长度": max_len,
      "段落数": len(paragraphs),
      "去重段落数": len(hashes),
      "低质量段落数": 0,
      "段落": paragraphs,
      "时间": create_time
    }
    corpus = GeneralCorpus(**corpus_info)

    return corpus


if __name__ == "__main__":
    text = "第一行\n第二行\n第一行"
    corpus = convert_to_general_corpus("1", text)
    corpus.create_time = ""
    txt = corpus.model_dump(by_alias=True)
    print(txt)
