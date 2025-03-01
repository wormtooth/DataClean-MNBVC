import gzip
import json
from multiprocessing import Queue
from pathlib import Path

from pydantic import BaseModel


class SizeLimitedFileWriter:
    """用于写入限制大小的文件。

    使用场景：可以使用此类生成大小需要限制在 500 MB 左右的 jsonl 文件。
    """

    def __init__(
        self,
        output_folder,
        filename_idx_first=0,
        filename_idx_width=3,
        filename_idx_stride=1,
        filename_fmt="{}.jsonl",  # .jsonl.gz
        file_size_limit_mb=500
      ):
        # 文件存储相关： 文件夹
        self.output_folder = Path(output_folder)
        if not self.output_folder.exists():
            self.output_folder.mkdir()

        # 文件存储相关： 文件名生成规律
        self.filename_idx_current = filename_idx_first  # 第一个文件编号
        self.filename_idx_width = filename_idx_width  # 文件编号的宽度，比如宽度为3时，编号1会变成 001
        self.filename_idx_stride = 1 if int(
          filename_idx_stride) <= 0 else int(filename_idx_stride)  # 下一个文件编号的增量
        self.filename_fmt = filename_fmt  # 文件名的模版，例子： {}.jsonl -> 001.jsonl

        # 文件大小相关
        if file_size_limit_mb <= 0:
            raise Exception(
                f"File size limit must be positive, got {file_size_limit_mb}")
        self.file_size_limit = int(
            file_size_limit_mb * (1 << 20))  # MB -> Bytes
        self.file_size_current = 0

        self.fp = None
        self._open = open
        if filename_fmt.endswith(".gz"):
            self._open = gzip.open
        self.open_next_file()

    def next_filepath(self):
        """生成下一个文件路径。
        """
        filename_idx = str(self.filename_idx_current)
        if len(filename_idx) < self.filename_idx_width:
            filename_idx = "0" * (self.filename_idx_width -
                                  len(filename_idx)) + filename_idx
        filepath = self.filename_fmt.format(filename_idx)
        filepath = self.output_folder / filepath

        self.filename_idx_current += self.filename_idx_stride

        return filepath

    def open_next_file(self):
        """打开下一个文件以供写入。
        """
        self.close()
        path = self.next_filepath()
        self.fp = self._open(path, "wb")
        self.file_size_current = 0

    def close(self):
        if self.fp is not None:
            self.fp.close()
            self.fp = None
            self.file_size_current = 0

    def is_full(self):
        return self.file_size_current >= self.file_size_limit

    def write(self, data, force=False):
        if self.is_full() and (not force):
            self.open_next_file()

        data = self._convert_obj_to_bytes(data)
        self.file_size_current += self.fp.write(data)

    def writeline(self, data):
        self.write(data)
        self.write(b"\n", force=True)

    def _convert_obj_to_bytes(self, data):

        if type(data) is bytes:
            return data
        elif type(data) is str:
            return data.encode()
        else:
            data_str = json.dumps(data, ensure_ascii=False)
            return data_str.encode()

    def __enter__(self):
        return self

    def __exit__(self, type, valce, traceback):
        self.close()

    def __del__(self):
        self.close()


def writer_worker(writer: SizeLimitedFileWriter, queue: Queue):
    """用于多进程/线程。"""
    while True:
        data = queue.get()
        if data is None:
            break
        if isinstance(data, BaseModel):
            data = data.model_dump(by_alias=True)
        writer.writeline(data)
