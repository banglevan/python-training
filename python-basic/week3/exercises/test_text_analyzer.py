"""
Unit tests cho text_analyzer.py
Tiêu chí đánh giá:
1. Text Analysis (40%): Phân tích văn bản chính xác
2. Statistics (30%): Thống kê số liệu đúng
3. Visualization (20%): Tạo word cloud thành công
4. File I/O (10%): Xử lý file ổn định
"""

import pytest
from pathlib import Path
import json
from collections import Counter
from text_analyzer import (
    Sentence,
    TextStatistics,
    TextAnalyzer
)

@pytest.fixture
def temp_dir(tmp_path):
    """Tạo thư mục tạm cho testing."""
    return tmp_path

@pytest.fixture
def sample_text():
    """Tạo văn bản mẫu."""
    return """
    This is a sample text. It contains multiple sentences!
    Some sentences are short. Others are much longer and more complex.
    This text will be used for testing the analyzer's functionality?
    """

@pytest.fixture
def stop_words_file(temp_dir):
    """Tạo file stop words."""
    file_path = temp_dir / "stop_words.txt"
    file_path.write_text("a\nan\nthe\nis\nare")
    return str(file_path)

@pytest.fixture
def text_file(temp_dir, sample_text):
    """Tạo file văn bản."""
    file_path = temp_dir / "sample.txt"
    file_path.write_text(sample_text)
    return str(file_path)

class TestSentence:
    """Test class Sentence."""
    
    def test_sentence_creation(self):
        """Test tạo câu từ text."""
        text = "This is a test sentence."
        sentence = Sentence.from_text(text)
        
        assert sentence.text == text.strip()
        assert len(sentence.words) == 5
        assert sentence.length == 5
        assert all(word.islower() for word in sentence.words)

    def test_sentence_normalization(self):
        """Test chuẩn hóa câu."""
        text = "  This  IS a TEST!  "
        sentence = Sentence.from_text(text)
        
        assert sentence.text == text.strip()
        assert sentence.words == ["this", "is", "a", "test"]
        assert sentence.length == 4

    def test_special_characters(self):
        """Test xử lý ký tự đặc biệt."""
        text = "Hello, world! This is a test-case."
        sentence = Sentence.from_text(text)
        
        assert "hello" in sentence.words
        assert "world" in sentence.words
        assert "test" in sentence.words
        assert "case" in sentence.words
        assert "," not in sentence.words
        assert "!" not in sentence.words
        assert "-" not in sentence.words

class TestTextAnalyzer:
    """Test class TextAnalyzer."""
    
    class TestTextAnalysis:
        """
        Test phân tích văn bản (40%)
        Pass: Phân tích chính xác ≥ 95% cases
        Fail: Phân tích chính xác < 95% cases
        """
        
        def test_word_counting(self, stop_words_file, sample_text):
            """Test đếm từ."""
            analyzer = TextAnalyzer(stop_words_file)
            stats = analyzer.analyze_text(sample_text)
            
            # Kiểm tra số từ (không tính stop words)
            expected_words = [
                word.lower()
                for word in sample_text.split()
                if word.lower() not in analyzer.stop_words
            ]
            assert stats.total_words == len(expected_words)
            assert stats.unique_words == len(set(expected_words))

        def test_sentence_analysis(self, stop_words_file, sample_text):
            """Test phân tích câu."""
            analyzer = TextAnalyzer(stop_words_file)
            stats = analyzer.analyze_text(sample_text)
            
            # Kiểm tra số câu
            sentences = [s for s in sample_text.split('.') if s.strip()]
            assert len(stats.sentence_lengths) == len(sentences)
            
            # Kiểm tra câu dài/ngắn nhất
            assert stats.longest_sentence.length == max(stats.sentence_lengths)
            assert stats.shortest_sentence.length == min(stats.sentence_lengths)

        def test_word_frequency(self, stop_words_file, sample_text):
            """Test tần suất từ."""
            analyzer = TextAnalyzer(stop_words_file)
            stats = analyzer.analyze_text(sample_text)
            
            # Kiểm tra từ xuất hiện nhiều nhất
            most_common = stats.word_frequency.most_common(1)
            assert len(most_common) > 0
            assert isinstance(most_common[0][1], int)
            assert most_common[0][1] > 0

    class TestStatistics:
        """
        Test thống kê số liệu (30%)
        Pass: Tính toán đúng ≥ 95% metrics
        Fail: Tính toán đúng < 95% metrics
        """
        
        def test_basic_metrics(self, stop_words_file, sample_text):
            """Test các metrics cơ bản."""
            analyzer = TextAnalyzer(stop_words_file)
            stats = analyzer.analyze_text(sample_text)
            
            assert stats.total_words > 0
            assert stats.unique_words > 0
            assert stats.unique_words <= stats.total_words
            assert stats.avg_sentence_length > 0
            assert isinstance(stats.word_frequency, Counter)

        def test_sentence_metrics(self, stop_words_file, sample_text):
            """Test metrics về câu."""
            analyzer = TextAnalyzer(stop_words_file)
            stats = analyzer.analyze_text(sample_text)
            
            assert len(stats.sentence_lengths) > 0
            assert all(length > 0 for length in stats.sentence_lengths)
            assert stats.longest_sentence.length == max(stats.sentence_lengths)
            assert stats.shortest_sentence.length == min(stats.sentence_lengths)
            assert stats.avg_sentence_length == (
                sum(stats.sentence_lengths) / len(stats.sentence_lengths)
            )

        def test_empty_text(self, stop_words_file):
            """Test với văn bản trống."""
            analyzer = TextAnalyzer(stop_words_file)
            stats = analyzer.analyze_text("")
            
            assert stats.total_words == 0
            assert stats.unique_words == 0
            assert stats.avg_sentence_length == 0
            assert len(stats.sentence_lengths) == 0
            assert len(stats.word_frequency) == 0

    class TestVisualization:
        """
        Test tạo word cloud (20%)
        Pass: Tạo thành công và đúng format
        Fail: Không tạo được hoặc sai format
        """
        
        def test_word_cloud_generation(self, stop_words_file, sample_text, temp_dir):
            """Test tạo word cloud."""
            analyzer = TextAnalyzer(stop_words_file)
            analyzer.analyze_text(sample_text)
            
            output_file = temp_dir / "wordcloud.png"
            assert analyzer.generate_word_cloud(str(output_file)) is True
            assert output_file.exists()
            assert output_file.stat().st_size > 0

        def test_word_cloud_without_data(self, stop_words_file, temp_dir):
            """Test tạo word cloud khi chưa có dữ liệu."""
            analyzer = TextAnalyzer(stop_words_file)
            output_file = temp_dir / "wordcloud.png"
            
            with pytest.raises(ValueError):
                analyzer.generate_word_cloud(str(output_file))

    class TestFileIO:
        """
        Test xử lý file (10%)
        Pass: Xử lý đúng mọi trường hợp
        Fail: Có trường hợp xử lý sai
        """
        
        def test_file_analysis(self, stop_words_file, text_file):
            """Test phân tích từ file."""
            analyzer = TextAnalyzer(stop_words_file)
            stats = analyzer.analyze_file(text_file)
            
            assert stats is not None
            assert stats.total_words > 0
            assert stats.unique_words > 0

        def test_nonexistent_file(self, stop_words_file):
            """Test với file không tồn tại."""
            analyzer = TextAnalyzer(stop_words_file)
            
            with pytest.raises(FileNotFoundError):
                analyzer.analyze_file("nonexistent.txt")

        def test_export_statistics(self, stop_words_file, sample_text, temp_dir):
            """Test export kết quả."""
            analyzer = TextAnalyzer(stop_words_file)
            analyzer.analyze_text(sample_text)
            
            output_file = temp_dir / "stats.json"
            assert analyzer.export_statistics(str(output_file)) is True
            
            # Kiểm tra file JSON
            data = json.loads(output_file.read_text())
            assert "total_words" in data
            assert "unique_words" in data
            assert "word_frequency" in data
            assert "sentence_lengths" in data
            assert "avg_sentence_length" in data
            assert "longest_sentence" in data
            assert "shortest_sentence" in data

        def test_export_without_data(self, stop_words_file, temp_dir):
            """Test export khi chưa có dữ liệu."""
            analyzer = TextAnalyzer(stop_words_file)
            output_file = temp_dir / "stats.json"
            
            with pytest.raises(ValueError):
                analyzer.export_statistics(str(output_file)) 