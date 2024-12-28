"""
Bài tập: Sử dụng external packages
1. requests: HTTP client
2. beautifulsoup4: HTML parser
3. pandas: Data analysis
4. matplotlib: Data visualization
5. pillow: Image processing
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import matplotlib.pyplot as plt
from PIL import Image
import io
from pathlib import Path
from typing import List, Dict, Optional, Union
from dataclasses import dataclass

@dataclass
class Product:
    """Class chứa thông tin sản phẩm."""
    name: str
    price: float
    rating: float
    image_url: str

def scrape_products(url: str) -> List[Product]:
    """
    Scrape thông tin sản phẩm từ trang web.
    
    Args:
        url (str): URL trang web
        
    Returns:
        List[Product]: Danh sách sản phẩm
    """
    # Gửi request
    response = requests.get(url)
    response.raise_for_status()
    
    # Parse HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    products = []
    
    # Tìm các sản phẩm
    for item in soup.find_all('div', class_='product-item'):
        name = item.find('h2', class_='product-name').text.strip()
        price = float(
            item.find('span', class_='price')
            .text.strip()
            .replace('$', '')
        )
        rating = float(
            item.find('div', class_='rating')
            .get('data-rating', 0)
        )
        image_url = item.find('img', class_='product-image')['src']
        
        products.append(Product(name, price, rating, image_url))
    
    return products

def analyze_products(products: List[Product]) -> pd.DataFrame:
    """
    Phân tích dữ liệu sản phẩm.
    
    Args:
        products (List[Product]): Danh sách sản phẩm
        
    Returns:
        pd.DataFrame: DataFrame chứa dữ liệu phân tích
    """
    # Tạo DataFrame
    df = pd.DataFrame([
        {
            'name': p.name,
            'price': p.price,
            'rating': p.rating
        }
        for p in products
    ])
    
    # Thêm thống kê
    df['price_range'] = pd.cut(
        df['price'],
        bins=[0, 50, 100, 200, float('inf')],
        labels=['0-50', '50-100', '100-200', '200+']
    )
    
    df['rating_group'] = pd.cut(
        df['rating'],
        bins=[0, 2, 3, 4, 5],
        labels=['0-2', '2-3', '3-4', '4-5']
    )
    
    return df

def visualize_data(df: pd.DataFrame, output_dir: Path):
    """
    Tạo biểu đồ từ dữ liệu.
    
    Args:
        df (pd.DataFrame): DataFrame dữ liệu
        output_dir (Path): Thư mục output
    """
    # Tạo thư mục output
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Price distribution
    plt.figure(figsize=(10, 6))
    df['price'].hist(bins=20)
    plt.title('Price Distribution')
    plt.xlabel('Price ($)')
    plt.ylabel('Count')
    plt.savefig(output_dir / 'price_dist.png')
    plt.close()
    
    # Rating distribution
    plt.figure(figsize=(10, 6))
    df['rating'].hist(bins=10)
    plt.title('Rating Distribution')
    plt.xlabel('Rating')
    plt.ylabel('Count')
    plt.savefig(output_dir / 'rating_dist.png')
    plt.close()
    
    # Price range counts
    plt.figure(figsize=(10, 6))
    df['price_range'].value_counts().plot(kind='bar')
    plt.title('Price Ranges')
    plt.xlabel('Range ($)')
    plt.ylabel('Count')
    plt.savefig(output_dir / 'price_ranges.png')
    plt.close()
    
    # Rating vs Price
    plt.figure(figsize=(10, 6))
    plt.scatter(df['price'], df['rating'])
    plt.title('Rating vs Price')
    plt.xlabel('Price ($)')
    plt.ylabel('Rating')
    plt.savefig(output_dir / 'rating_vs_price.png')
    plt.close()

def process_images(
    products: List[Product],
    output_dir: Path,
    size: tuple[int, int] = (200, 200)
):
    """
    Tải và xử lý ảnh sản phẩm.
    
    Args:
        products (List[Product]): Danh sách sản phẩm
        output_dir (Path): Thư mục output
        size (tuple[int, int]): Kích thước ảnh mới
    """
    # Tạo thư mục output
    output_dir.mkdir(parents=True, exist_ok=True)
    
    for product in products:
        try:
            # Tải ảnh
            response = requests.get(product.image_url)
            response.raise_for_status()
            
            # Mở ảnh
            image = Image.open(io.BytesIO(response.content))
            
            # Resize
            image = image.resize(size)
            
            # Lưu ảnh
            image.save(
                output_dir / f"{product.name}.jpg",
                "JPEG",
                quality=85,
                optimize=True
            )
            
        except Exception as e:
            print(f"Error processing {product.name}: {e}")

def main():
    """Hàm chính."""
    # URL demo
    url = "https://example.com/products"
    
    try:
        # Scrape data
        print("Scraping products...")
        products = scrape_products(url)
        print(f"Found {len(products)} products")
        
        # Analyze data
        print("\nAnalyzing data...")
        df = analyze_products(products)
        print("\nSummary:")
        print(df.describe())
        
        # Visualize
        print("\nCreating visualizations...")
        output_dir = Path("output")
        visualize_data(df, output_dir / "plots")
        
        # Process images
        print("\nProcessing images...")
        process_images(products, output_dir / "images")
        
        print("\nDone!")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main() 