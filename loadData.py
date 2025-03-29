import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import csv
from vnstock import *
from multiprocessing import Pool, Value
import time

# Biến đếm toàn cục
counter = None

def init_counter():
    global counter
    counter = Value('i', 0)  # Biến đếm kiểu integer

# Hàm lấy dữ liệu từ một trang
def extract_history_table(url):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"Không thể truy cập {url}. Status code: {response.status_code}")
            return None
        
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", {"id": "history"})
        
        if not table or len(table.find_all("tr")) <= 1:
            print(f"Không còn dữ liệu ở {url}")
            return None
        
        rows = []
        for tr in table.find_all("tr")[1:]:  # Lấy tất cả dòng trừ header
            if "Thưởng cổ phiếu" not in tr.get_text():
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                rows.append(cells)
        
        return rows if rows else None
    except Exception as e:
        print(f"Lỗi khi tải {url}: {e}")
        return None

# Hàm tải dữ liệu cho một mã
def download_stock_history(args):
    stock_id, total, stock_to_company = args  # Nhận stock_id, total_stocks và từ điển tên công ty
    global counter
    all_data = []
    page = 1
    
    while True:
        url = f"https://www.cophieu68.vn/quote/history.php?cP={page}&id={stock_id}"
        print(f"Đang tải trang {page} cho {stock_id}: {url}")
        
        rows = extract_history_table(url)
        if rows:
            # Thêm mã cổ phiếu ở đầu và tên công ty ở cuối
            company_name = stock_to_company.get(stock_id, "Unknown")
            for row in rows:
                row.insert(0, stock_id)  # Thêm ticker ở đầu
                row.append(company_name)  # Thêm tên công ty ở cuối
            all_data.extend(rows)
            page += 1
            time.sleep(1)  # Nghỉ 1 giây để tránh quá tải server
        else:
            print(f"Đã tải hết dữ liệu cho {stock_id} sau {page - 1} trang.")
            break
    
    # Cập nhật tiến trình
    with counter.get_lock():
        counter.value += 1
        print(f"Đã crawl mã {stock_id} ({counter.value}/{total} - {counter.value/total*100:.2f}%)")
    
    return all_data if all_data else None

# Hàm chính
def download_all_stocks_history(output_file, num_processes=4):
    # Lấy danh sách mã và tên công ty từ vnstock
    companies_df = Listing().all_symbols()
    ticker = list(companies_df['ticker'])
    stock_to_company = dict(zip(companies_df['ticker'], companies_df['organ_name']))
    
    total_stocks = len(ticker)
    print(f"Tìm thấy {total_stocks} mã cổ phiếu: {ticker[:10]}...")
    
    # Crawl song song
    with Pool(processes=num_processes, initializer=init_counter) as pool:
        results = pool.imap_unordered(download_stock_history, [(stock, total_stocks, stock_to_company) for stock in ticker])
        all_data = [row for result in results if result is not None for row in result]
    
    # Ghi tất cả dữ liệu vào file CSV
    if all_data:
        headers = ["Stock", "Date", "Close", "Volume", "Open", "High", "Low", "Foreign Buy", "Foreign Sell", "Foreign Value", "Company Name"]
        with open(output_file, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(headers)  # Ghi tiêu đề
            writer.writerows(all_data)  # Ghi dữ liệu
        print(f"Đã lưu {len(all_data)} dòng vào {output_file}")
    else:
        print("Không có dữ liệu để lưu.")

# Chạy chương trình
if __name__ == "__main__":
    data_folder = "dataStock"  # Thư mục lưu file
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
    
    output_file = os.path.join(data_folder, "stock.csv")
    download_all_stocks_history(output_file, num_processes=4)