"""清洗招商的金融数据 - 通用语料格式
"""

import json
import logging
import zipfile
from logging.handlers import QueueHandler, QueueListener
from multiprocessing import Process, Queue
from pathlib import Path
from typing import Union

from mnbvc.formats.general import convert_to_general_corpus
from mnbvc.utils.writer import writer_worker


def get_queued_logger(logger_name: str, log_queue: Queue) -> logging.Logger:
    """获取一个可以在多线程使用的logger。"""
    logger = logging.getLogger(logger_name)
    queue_handler = QueueHandler(log_queue)
    formatter = logging.Formatter(
        fmt="[%(asctime)s][%(name)s][%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    queue_handler.setFormatter(formatter)
    logger.addHandler(queue_handler)
    logger.setLevel(logging.DEBUG)
    return logger


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
    woker_id: int,
    path: Union[Path, str],
    input_queue: Queue,
    output_queue: Queue,
    log_queue: Queue
):
    """将 jsonl 转化成通用语料格式。"""
    if type(path) is str:
        path = Path(path)
    zf = zipfile.ZipFile(path)
    logger = get_queued_logger(f"转换进程-{woker_id}", log_queue)
    logger.info("开始转换进程..")
    while True:
        jsonl_name = input_queue.get()
        # 退出判断
        if jsonl_name is None:
            break
        logger.debug(f"处理文件: {jsonl_name}")

        # 将 \u2028 替换成空格
        # 不然会导致下面的 text.splitlines() 出问题
        text = zf.read(jsonl_name).decode(
            errors="ignore", encoding="UTF-8-SIG"
        ).replace("\u2028", " ")

        # 每一行是一个json
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            # 将数据转成dict
            try:
                data = json.loads(line)
            except Exception as e:
                logger.error(f"无法加载成JSON[{e}]: {line}")
                continue

            # 获取创建时间
            create_time = None
            try:
                dump = data["meta"]["dump"]
                year = dump.split("-")[0]
                create_time = f"{year}0101"
            except:
                logger.debug(
                    f"Cannot find create time in {data.get('meta', {})}")

            # 转换成通用语料格式
            try:
                corpus = convert_to_general_corpus(
                    text_id=data["meta"].get("title", ""),
                    text=data["text"],
                    create_time=create_time
                )
                corpus.extension_fields = json.dumps(data["meta"])
            except Exception as e:
                logger.error(f"Cannot process [{e}]: {data}")
                continue

            # 将转换语料放入队列中
            output_queue.put(corpus)

    zf.close()
    logger.info("转换进程结束")


if __name__ == "__main__":
    # 修改指向 zip 压缩文件的路径
    path = "data/CMB_FinDataSet_sample.zip"

    # 输出文件夹
    output_folder = "data/cmb"

    # 修改 log 的保存位置
    log_path = "data/cmb_log.txt"
    log_queue = Queue()
    listener = QueueListener(log_queue, logging.FileHandler(log_path))
    listener.start()

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
    for idx in range(num_converter):
        proc = Process(
            target=convert_jsonl_to_general_corpus,
            args=(idx, path, jsonl_queue, corpus_queue, log_queue),
        )
        proc.start()
        converter_procs.append(proc)

    # 写入进程
    writer_kwargs=dict(
        output_folder=output_folder,
        filename_fmt="{}.jsonl"
    )
    writer_proc = Process(
        target=writer_worker,
        args=(writer_kwargs, corpus_queue)
    )
    writer_proc.start()

    # 有序地完成所有进程
    # 首先保证所有 jsonl 文件都已经读取完毕
    reader_proc.join()

    # 然后保证所有有 jsonl 文件都已经清洗
    for _ in range(num_converter):
        jsonl_queue.put(None)
    for proc in converter_procs:
        proc.join()

    # 保证所有数据都已经写入
    corpus_queue.put(None)
    writer_proc.join()

    # 停止 log
    listener.enqueue_sentinel()
