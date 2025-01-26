from formats.general import convert_to_general_corpus
from utils.writer import SizeLimitedFileWriter

if __name__ == "__main__":
    text = "第一行\n第二行\n第一行"
    corpus = convert_to_general_corpus("1", text)
    txt = corpus.model_dump(by_alias=True)
    with SizeLimitedFileWriter(output_folder="data/test") as writer:
        writer.writeline(txt)
    
    with SizeLimitedFileWriter(output_folder="data/test", filename_idx_first=3, filename_fmt="{}.jsonl.gz") as writer:
        writer.writeline(txt)