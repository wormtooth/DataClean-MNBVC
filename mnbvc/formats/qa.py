from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class QAMetaData(BaseModel):
    create_time: str = Field(
        description="创建时间 - 格式：%Y%m%d %H:%M:%S",
        default_factory=lambda: f"{datetime.now():%Y%m%d %H:%M:%S}"
    )
    question_details: str = Field(
        description="关于问题的细节。", alias="问题明细", default="")
    answer_details: str = Field(
        description="关于回答的细节。", alias="回答明细", default="")
    extension_fields: str = Field(
        description="关于此问答的其他信息。", alias="扩展字段",
        default=""
    )

    @field_validator("create_time")
    def time_format(cls, v):
        format_string = "%Y%m%d %H:%M:%S"
        try:
            datetime.strptime(v, format_string)
            return v
        except Exception as e:
            raise e


class QACorpus(BaseModel):
    """问答语料格式"""

    id: str
    question: str = Field(description="问题", alias="问")
    answer: str = Field(description="答案", alias="答")
    source: str = Field(description="问答的来源。", alias="来源", default="")
    meta: QAMetaData = Field(description="问答的相关信息。", alias="元数据")
    create_time: str = Field(
        description="此语料生成的时间，格式为yyyymmdd。", alias="时间",
        default_factory=lambda: f"{datetime.now():%Y%m%d}"
    )
