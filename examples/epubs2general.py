"""将EPUB数据清洗成通用语料格式。

样例数据来源：https://github.com/it-ebooks-0/52pojie-2008-2021
请将样例数据下载到本地文件并相应修改 DATA_INPUT_FOLDER

EPUB数据实际上是多个 HTML 文件的压缩包。其他 HTML 的清洗任务可以参考此样例。
"""

import json
import logging
import re
from functools import cached_property
from html.parser import HTMLParser
from multiprocessing import Process, Queue
from pathlib import Path
from threading import Thread
from typing import Dict, Iterator, List, Optional, Tuple, Union

import ebooklib
from ebooklib import epub

from mnbvc.formats.general import convert_to_general_corpus
from mnbvc.utils.writer import SizeLimitedFileWriter, writer_worker_for_thread

DATA_INPUT_FOLDER = "data/52pojie-2008-2021"


logger = logging.getLogger(__name__)


def get_epub_paths(limit: Optional[int] = None):
    """获取 DATA_INPUT_FOLDER 中 epub 文件。

    如果设置 limit, 则取前 limit 数量的文件。"""
    folder = Path(DATA_INPUT_FOLDER)
    paths = list(folder.glob("*.epub"))
    if limit:
        paths = paths[:limit]
    return paths


class ArticleParser(HTMLParser):
    """从 HTML 中提取文章。保留代码区块。
    """

    def __init__(self):
        self.texts: List[str] = []
        self.current_text: str = ""
        self.in_pre: bool = False
        self.img_count = 0
        super().__init__()

    def add_text(self):
        """保存当前文字。
        """
        if self.current_text:
            self.texts.append(self.current_text)
        self.current_text = ""

    def handle_starttag(self, tag: str, attrs: List[Tuple[Union[str, None]]]) -> None:
        if tag == "pre":
            self.in_pre = True
        elif tag == "img":
            self.img_count += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in ["p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote"]:
            self.add_text()
        elif tag == "br":
            if self.in_pre:
                self.current_text += "\n"
            else:
                self.add_text()
        elif tag == "pre":
            self.in_pre = False
            self.add_text()

    def handle_data(self, data: str) -> None:
        if not self.in_pre:
            data = data.strip()
        self.current_text += data

    def handle_startendtag(self, tag: str, attrs: List[Tuple[Union[str, None]]]) -> None:
        if tag == "br":
            if self.in_pre:
                self.current_text += "\n"
            else:
                self.add_text()
        elif tag == "img":
            self.img_count += 1

    def feed(self, data: str) -> None:
        self.clear()
        super().feed(data)
        self.add_text()

    def clear(self):
        self.texts.clear()
        self.current_text = ""
        self.img_count = 0


class EpubConverter:

    def __init__(self, book_path: Union[str, Path]):
        self.book_path = book_path
        self.ebook = epub.read_epub(book_path, {"ignore_ncx": True})

    @cached_property
    def publish_date(self):
        return self.ebook.get_metadata("DC", "date")[0][0].replace("-", "")

    @cached_property
    def book_title(self):
        return self.ebook.get_metadata("DC", "title")[0][0]

    @cached_property
    def contents(self) -> List[Tuple[str, str]]:
        """使用正则表达式获取 epub 文件内容目录。返回列表，每个元素为 (文件名，文件链接)。
        """
        nav = list(self.ebook.get_items_of_type(ebooklib.ITEM_NAVIGATION))[0]
        nav = nav.get_content().decode()
        start_idx = [
            m.end()
            for m in re.finditer(r"\<navpoint .*\>", nav, flags=re.IGNORECASE)
        ]
        end_idx = [
            m.start()
            for m in re.finditer(r"\</navpoint\>", nav, flags=re.IGNORECASE)
        ]
        assert len(start_idx) == len(end_idx)
        contents = []
        for s, e in zip(start_idx, end_idx):
            navpoint = nav[s: e].strip()
            i = navpoint.find("<text>") + 6
            j = navpoint.find("</text>")
            title = navpoint[i: j].strip()
            i = navpoint.find("<content src=") + 13
            j = navpoint.find("/>", i)
            src = navpoint[i: j].strip('"')
            contents.append((title, src))
        return contents

    def convert_article(self, idx: int) -> Dict[str, str]:
        """提取一篇文章。"""
        title, src = self.contents[idx]
        title = title.replace("\n", " ").strip()
        item = self.ebook.get_item_with_href(src)
        if item is None:
            logger.warning(
                f"Cannot find {title}({src}) in book ({self.book_path})")
            return None
        html = item.get_content().decode()
        parser = ArticleParser()
        parser.feed(html)
        data = {
            "title": title,
            "texts": parser.texts,
            "date": self.publish_date,
            "html": html,
            "img_count": parser.img_count
        }
        data = self.post_process(data)
        return data

    def post_process(self, data: Dict[str, str]) -> Dict[str, str]:
        """后处理文章内容。"""
        if ("img_count" in data) and (data["img_count"] > 0):
            data["ext_field"] = {"图片数量": data["img_count"]}
        return data

    def convert(self) -> Iterator[Dict[str, str]]:
        """转换一个epub文件。调用self.convert_article来转换每一篇文章。"""
        for idx in range(len(self.contents)):
            data = self.convert_article(idx)
            if data is None:
                continue
            # 将文章转化为通用格式
            corpus = convert_to_general_corpus(
                text_id=data["title"],
                text=data["texts"],
                create_time=data["date"],
            )
            # 有扩展字段则将其加入
            ext_field = data.get("ext_field", None)
            ext_field_str = ""
            if ext_field is not None:
                ext_field_str = json.dumps(ext_field, ensure_ascii=False)
            corpus.extension_fields = ext_field_str
            yield corpus


def epub_converter_worker(epub_paths: List[Union[Path, str]], output_queue: Queue):
    """将给定的 epub 清洗成通用格式并输出到 output_queue"""

    for path in epub_paths:
        book = EpubConverter(path)
        for corpus in book.convert():
            output_queue.put(corpus)


def main():
    input_folder = Path(DATA_INPUT_FOLDER)
    # 我们将结果放在 DATA_INPUT_FOLDER 下的 output 文件夹
    output_folder = input_folder / "output"

    # 作为样例，这里只处理前 10 个 epub 文件。
    epub_paths = get_epub_paths(limit=10)
    queue = Queue()

    # 设置 worker 进程处理文件 - 这里只用了一个进程
    worker_proc = Process(target=epub_converter_worker,
                          args=(epub_paths, queue))

    # 在主进程中使用一个线程进行写入操作
    # 注意：SizeLimitedFileWriter 并不可以直接用于多线程或者多进程
    # 如果需要，则每个线程/进程需要有自己独立的 SizeLimitedFileWriter
    # 而且要保证每个 SizeLimitedFileWriter 写入的文件并不冲突
    writer = SizeLimitedFileWriter(output_folder)
    writer_thread = Thread(target=writer_worker_for_thread, args=(writer, queue))

    # 开始转换
    worker_proc.start()
    writer_thread.start()

    # 先确保清洗进程全部结束
    worker_proc.join()

    # 停止写入操作
    queue.put(None)
    writer_thread.join()


if __name__ == "__main__":
    main()
