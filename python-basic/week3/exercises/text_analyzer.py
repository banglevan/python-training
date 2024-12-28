"""
Bài tập 2: Thống kê từ trong văn bản
Yêu cầu:
1. Đọc và xử lý văn bản từ file
2. Thống kê tần suất xuất hiện của từ
3. Phân tích cấu trúc câu
4. Tạo word cloud
"""

from typing import Dict, List, Tuple, Set
from collections import Counter
from pathlib import Path
import re
import json
from dataclasses import dataclass
import matplotlib.pyplot as plt
from wordcloud import WordCloud

@dataclass
class Sentence:
    """Class đại diện cho một câu."""
    text: str
    words: List[str]
    length: int
    
    @classmethod
    def from_text(cls, text: str) -> 'Sentence':
        """
        Tạo instance từ text.
        
        Args:
            text (str): Nội dung câu
            
        Returns:
            Sentence: Instance mới
        """
        # Chuẩn hóa câu
        text = text.strip()
        # Tách từ và lọc ký tự đặc biệt
        words = [
            word.lower() for word in re.findall(r'\b\w+\b', text)
        ]
        return cls(text, words, len(words))

@dataclass
class TextStatistics:
    """Class chứa thống kê văn bản."""
    total_words: int
    unique_words: int
    word_frequency: Counter
    sentence_lengths: List[int]
    avg_sentence_length: float
    longest_sentence: Sentence
    shortest_sentence: Sentence

class TextAnalyzer:
    """Class phân tích văn bản."""
    
    def __init__(self, stop_words_file: str = "stop_words.txt"):
        """
        Khởi tạo analyzer.
        
        Args:
            stop_words_file (str): File chứa stop words
        """
        self.stop_words = self._load_stop_words(stop_words_file)
        self.statistics: TextStatistics = None

    def analyze_file(self, file_path: str) -> TextStatistics:
        """
        Phân tích văn bản từ file.
        
        Args:
            file_path (str): Đường dẫn file
            
        Returns:
            TextStatistics: Kết quả thống kê
            
        Raises:
            FileNotFoundError: Nếu file không tồn tại
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Không tìm thấy file: {file_path}")
            
        text = path.read_text(encoding='utf-8')
        return self.analyze_text(text)

    def analyze_text(self, text: str) -> TextStatistics:
        """
        Phân tích văn bản.
        
        Args:
            text (str): Nội dung văn bản
            
        Returns:
            TextStatistics: Kết quả thống kê
        """
        # Tách câu
        sentences = [
            Sentence.from_text(s)
            for s in re.split(r'[.!?]+', text)
            if s.strip()
        ]
        
        # Thống kê từ
        all_words = []
        for sentence in sentences:
            all_words.extend(
                word for word in sentence.words
                if word not in self.stop_words
            )
            
        word_freq = Counter(all_words)
        
        # Tìm câu dài/ngắn nhất
        if sentences:
            longest = max(sentences, key=lambda s: s.length)
            shortest = min(sentences, key=lambda s: s.length)
            avg_length = sum(s.length for s in sentences) / len(sentences)
        else:
            longest = shortest = Sentence("", [], 0)
            avg_length = 0
        
        self.statistics = TextStatistics(
            total_words=len(all_words),
            unique_words=len(word_freq),
            word_frequency=word_freq,
            sentence_lengths=[s.length for s in sentences],
            avg_sentence_length=avg_length,
            longest_sentence=longest,
            shortest_sentence=shortest
        )
        
        return self.statistics

    def generate_word_cloud(
        self,
        output_file: str = "wordcloud.png",
        width: int = 800,
        height: int = 400
    ) -> bool:
        """
        Tạo word cloud từ kết quả phân tích.
        
        Args:
            output_file (str): File ảnh đầu ra
            width (int): Chiều rộng ảnh
            height (int): Chiều cao ảnh
            
        Returns:
            bool: True nếu tạo thành công
            
        Raises:
            ValueError: Nếu chưa có dữ liệu thống kê
        """
        if not self.statistics:
            raise ValueError("Chưa có dữ liệu thống kê")
            
        try:
            wordcloud = WordCloud(
                width=width,
                height=height,
                background_color='white'
            )
            
            # Tạo word cloud từ frequency
            wordcloud.generate_from_frequencies(
                self.statistics.word_frequency
            )
            
            # Lưu ảnh
            plt.figure(figsize=(10, 5))
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis('off')
            plt.savefig(output_file)
            plt.close()
            
            return True
        except Exception as e:
            print(f"Lỗi tạo word cloud: {e}")
            return False

    def export_statistics(self, output_file: str) -> bool:
        """
        Export kết quả thống kê ra file JSON.
        
        Args:
            output_file (str): File JSON đầu ra
            
        Returns:
            bool: True nếu export thành công
            
        Raises:
            ValueError: Nếu chưa có dữ liệu thống kê
        """
        if not self.statistics:
            raise ValueError("Chưa có dữ liệu thống kê")
            
        try:
            data = {
                "total_words": self.statistics.total_words,
                "unique_words": self.statistics.unique_words,
                "word_frequency": dict(self.statistics.word_frequency),
                "sentence_lengths": self.statistics.sentence_lengths,
                "avg_sentence_length": self.statistics.avg_sentence_length,
                "longest_sentence": {
                    "text": self.statistics.longest_sentence.text,
                    "length": self.statistics.longest_sentence.length
                },
                "shortest_sentence": {
                    "text": self.statistics.shortest_sentence.text,
                    "length": self.statistics.shortest_sentence.length
                }
            }
            
            Path(output_file).write_text(
                json.dumps(data, indent=4),
                encoding='utf-8'
            )
            return True
        except Exception as e:
            print(f"Lỗi export: {e}")
            return False

    def _load_stop_words(self, file_path: str) -> Set[str]:
        """
        Đọc danh sách stop words từ file.
        
        Args:
            file_path (str): Đường dẫn file
            
        Returns:
            Set[str]: Tập hợp stop words
        """
        try:
            path = Path(file_path)
            if path.exists():
                return set(
                    word.strip().lower()
                    for word in path.read_text(encoding='utf-8').splitlines()
                    if word.strip()
                )
        except Exception as e:
            print(f"Lỗi đọc stop words: {e}")
        return set()

def main():
    """Chương trình chính."""
    analyzer = TextAnalyzer()

    while True:
        print("\nPhân Tích Văn Bản")
        print("1. Phân tích từ file")
        print("2. Phân tích văn bản nhập trực tiếp")
        print("3. Tạo word cloud")
        print("4. Export kết quả")
        print("5. Thoát")

        choice = input("\nChọn chức năng (1-5): ")

        if choice == '5':
            break
        elif choice == '1':
            file_path = input("Nhập đường dẫn file: ")
            try:
                stats = analyzer.analyze_file(file_path)
                print("\nKết quả phân tích:")
                print(f"Tổng số từ: {stats.total_words}")
                print(f"Số từ khác nhau: {stats.unique_words}")
                print(f"Độ dài câu trung bình: {stats.avg_sentence_length:.2f}")
                print("\nTop 10 từ xuất hiện nhiều nhất:")
                for word, count in stats.word_frequency.most_common(10):
                    print(f"- {word}: {count}")
            except FileNotFoundError:
                print("Không tìm thấy file!")
                
        elif choice == '2':
            text = input("Nhập văn bản: ")
            stats = analyzer.analyze_text(text)
            print("\nKết quả phân tích:")
            print(f"Tổng số từ: {stats.total_words}")
            print(f"Số từ khác nhau: {stats.unique_words}")
            print(f"Độ dài câu trung bình: {stats.avg_sentence_length:.2f}")
            print("\nTop 10 từ xuất hiện nhiều nhất:")
            for word, count in stats.word_frequency.most_common(10):
                print(f"- {word}: {count}")
                
        elif choice == '3':
            if not analyzer.statistics:
                print("Chưa có dữ liệu phân tích!")
                continue
                
            output_file = input("Nhập tên file đầu ra (mặc định: wordcloud.png): ")
            if not output_file:
                output_file = "wordcloud.png"
                
            if analyzer.generate_word_cloud(output_file):
                print(f"Đã tạo word cloud: {output_file}")
            else:
                print("Tạo word cloud thất bại!")
                
        elif choice == '4':
            if not analyzer.statistics:
                print("Chưa có dữ liệu phân tích!")
                continue
                
            output_file = input("Nhập tên file JSON đầu ra: ")
            if analyzer.export_statistics(output_file):
                print(f"Đã export kết quả: {output_file}")
            else:
                print("Export thất bại!")
                
        else:
            print("Lựa chọn không hợp lệ!")

if __name__ == "__main__":
    main() 