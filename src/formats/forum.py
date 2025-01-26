"""论坛语料格式
"""

import datetime
from typing import Any, List

from pydantic import BaseModel, Field


class ForumMessage(BaseModel):
    """论坛语料格式 - 论坛楼层消息"""
    id: str = Field(description="回复ID", alias="楼ID")
    content: str = Field(description="回复的内容", alias="回复")
    extension_fields: str = Field(
        description="关于此回复的其他信息。", alias="扩展字段",
        default=""
    )


class ForumCorpus(BaseModel):
    """论坛语料格式"""
    id: int = Field(description="帖子ID", alias="ID")
    subject: str = Field(description="帖子的主题", alias="主题")
    source: str = Field(description="帖子的来源，一般为网站名。", alias="来源", default="")
    replies: List[ForumMessage] = Field(
        description="对主题的讨论。第一个回复是主楼。", alias="回复", default=[])
    meta: Any = Field(description="帖子的其他信息。", alias="元数据", default="")
    create_time: str = Field(
        description="此语料生成的时间，格式为yyyymmdd。", alias="时间",
        default_factory=lambda: f"{datetime.date.today():%Y%m%d}"
    )

    @classmethod
    def name(cls):
        return "论坛语料格式"


if __name__ == "__main__":
    import json

    message1 = ForumMessage(
        楼ID="1",
        回复="主楼内容",
        扩展字段=json.dumps(dict(author="作者"), ensure_ascii=False)
    )
    message2 = ForumMessage(
        楼ID="2",
        回复="回复",
        扩展字段=json.dumps(dict(author="作者2"), ensure_ascii=False)
    )
    corpus = ForumCorpus(
        ID=1,
        主题="主题测试",
        来源="来源测试",
        回复=[message1, message2],
    )
    txt = corpus.model_dump(by_alias=True)
    print(txt)
