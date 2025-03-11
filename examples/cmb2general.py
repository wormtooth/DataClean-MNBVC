"""清洗招商的金融数据 - 通用语料格式
"""

import zipfile
from multiprocessing import Queue, Process
from threading import Thread
from pathlib import Path
from typing import Union

from mnbvc.formats.general import convert_to_general_corpus
from mnbvc.utils.writer import SizeLimitedFileWriter, writer_worker
import json


def read_jsonl_from_zip(path: Union[Path, str], output_queue: Queue):
    """读取 zip 文件中的所有 jsonl。"""
    if type(path) is str:
        path = Path(path)
    zf = zipfile.ZipFile(path)
    jsonl_files = [
        name
        for name in zf.namelist()
        if name.endswith(".jsonl") and (not name.startswith("__MACOSX"))
    ]
    for jsonl_name in jsonl_files:
        output_queue.put(jsonl_name)
    zf.close()


def convert_jsonl_to_general_corpus(
    path: Union[Path, str],
    input_queue: Queue,
    output_queue: Queue
):
    """将 jsonl 转化成通用语料格式。"""
    if type(path) is str:
        path = Path(path)
    zf = zipfile.ZipFile(path)

    while True:
        jsonl_name = input_queue.get()
        # exit if None for jsonl name
        if jsonl_name is None:
            break
        text = zf.read(jsonl_name).decode(errors="ignore", encoding="UTF-8-SIG")
        for line in text.splitlines():
            try:
                data = json.loads(line)
            except Exception as e:
                print(e)
                continue
            create_time = None
            try:
                dump = data["meta"]["dump"]
                year = dump.split("-")[0]
                create_time = f"{year}0101"
            except:
                pass
            try:
                corpus = convert_to_general_corpus(
                    text_id=data["meta"].get("title", ""),
                    text=data["text"],
                    create_time=create_time
                )
                corpus.extension_fields = json.dumps(data["meta"])
            except Exception as e:
                print(e)
                continue
            output_queue.put(corpus)
    
    zf.close()


if __name__ == "__main__":
    # 修改指向 zip 压缩文件的路径
    path = "data/CMB_FinDataSet_sample.zip"
    
    num_converter = 4

    jsonl_queue = Queue()
    corpus_queue = Queue()

    # 一个进程负责从 zip 文件获取 jsonl
    reader_proc = Process(
        target=read_jsonl_from_zip,
        args=(path, jsonl_queue)
    )
    reader_proc.start()

    # 转换进程，用 num_converter 控制数量
    converter_procs = []
    for _ in range(num_converter):
        proc = Process(
            target=convert_jsonl_to_general_corpus,
            args=(path, jsonl_queue, corpus_queue),
        )
        proc.start()
        converter_procs.append(proc)
    
    # 写入线程 - 主进程没用其他任务，所以就用线程了
    writer = SizeLimitedFileWriter(output_folder="data/cmb", filename_fmt="{}.jsonl")
    writer_thread = Thread(
        target=writer_worker,
        args=(writer, corpus_queue)
    )
    writer_thread.start()

    # 有序地完成所有进程
    # 首先保证所有 jsonl 文件都已经读取完毕
    reader_proc.join()

    # 然后保证所有有 jsonl 文件都已经清洗
    for _ in range(num_converter):
        jsonl_queue.put(None)
    for proc in converter_procs:
        proc.join()
    
    # 最后保证所有数据都已经写入
    corpus_queue.put(None)
    writer_thread.join()
