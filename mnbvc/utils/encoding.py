import chardet

def detect_encoding(raw_data: bytes) -> str:
    """检测编码"""

    test_data = raw_data[:4096]
    result = chardet.detect(test_data)
    return result["encoding"]
    

def open_text(path, encodings=["GB18030", "UTF-8"], logger=None, include_encoding=False):
    """先尝试用给定的编码打开文件。不行才尝试用chardet"""

    with open(path, "rb") as fp:
        raw_data = fp.read()

    for encoding in encodings:
        try:
            raw_text = raw_data.decode(encoding)
            if include_encoding:
                return raw_text, encoding
            else:
                return raw_text
        except Exception as e:
            logger.debug(f"Cannot open {path} with encoding: {encoding}: {e}")

    # chardet
    encoding = detect_encoding(raw_data)
    logger.info(f"Chardet for {path}: {encoding}")
    raw_text = raw_data.decode(encoding, errors="ignore")
    if include_encoding:
        return raw_text, encoding
    else:
        return raw_text