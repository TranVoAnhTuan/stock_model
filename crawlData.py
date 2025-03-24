import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
from vnstock import *
from multiprocessing import Pool


# Tạo thư mục 'data' nếu chưa tồn tại
data_folder = "data"
if not os.path.exists(data_folder):
    os.makedirs(data_folder)

# Hàm lấy bảng thứ 3 từ một trang
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
        
        headers = ["Date", "Close", "Volume", "Open", "High", "Low", "Foreign Buy", "Foreign Sell", "Foreign Value"]
        rows = []
        for tr in table.find_all("tr")[1:]:
            if "Thưởng cổ phiếu" not in tr.get_text():
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                rows.append(cells)
        
        df = pd.DataFrame(rows, columns=headers)
        return df
    except Exception as e:
        print(f"Lỗi khi tải {url}: {e}")
        return None

# Hàm tải dữ liệu cho một mã (dùng trong multiprocessing)
def download_stock_history(stock_id):
    all_data = []
    page = 1
    
    while True:
        url = f"https://www.cophieu68.vn/quote/history.php?cP={page}&id={stock_id}"
        print(f"Đang tải dữ liệu từ trang {page} cho {stock_id}: {url}")
        
        df = extract_history_table(url)
        if df is not None and not df.empty:
            all_data.append(df)
            page += 1
            time.sleep(1)  # Giảm từ 2s xuống 1s
        else:
            print(f"Đã tải hết dữ liệu cho {stock_id} sau {page - 1} trang.")
            break
    
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        file_path = os.path.join(data_folder, f"{stock_id}.csv")
        final_df.to_csv(file_path, index=False)
        print(f"Dữ liệu của {stock_id} đã được lưu vào {file_path}")
        return stock_id, True
    else:
        print(f"Không có dữ liệu để lưu cho {stock_id}.")
        return stock_id, False

# Hàm chính: Tải dữ liệu song song cho tất cả mã
def download_all_stocks_history(ticker, num_processes=4):
    print(f"Tìm thấy {len(ticker)} mã cổ phiếu: {ticker[:10]}...")
    
    # Sử dụng multiprocessing Pool để chạy song song
    with Pool(processes=num_processes) as pool:
        results = pool.map(download_stock_history, ticker)
        
    # Phân loại mã thành công và thất bại
    successful = [r[0] for r in results if r[1]]
    failed = [r[0] for r in results if not r[1]]

    # Ghi mã thành công vào file successful_stocks.txt
    successful_file = os.path.join(data_folder, "successful_stocks.txt")
    with open(successful_file, "w") as f:
        f.write("\n".join(successful))
    print(f"Đã ghi {len(successful)} mã thành công vào {successful_file}")
    
    # Ghi mã thất bại vào file failed_stocks.txt
    failed_file = os.path.join(data_folder, "failed_stocks.txt")
    with open(failed_file, "w") as f:
        f.write("\n".join(failed))
    print(f"Đã ghi {len(failed)} mã thất bại vào {failed_file}")
    
    # In thông tin tóm tắt
    print(f"\nHoàn thành: {len(successful)} mã thành công, {len(failed)} mã thất bại.")
    
    #auto shutdown
    print("crawl du lieu hoan tat. May se tat trong 10 giay")
    time.sleep(10)
    os.system("shutdown now")

# Chạy chương trình
if __name__ == "__main__":
    # Lấy danh sách mã cổ phiếu từ vnstock
    ticker = Listing().all_symbols()
    ticker = list(ticker[:]['ticker'])
    
    # Chạy song song với 4 tiến trình (có thể tăng nếu máy mạnh)
    download_all_stocks_history(ticker, num_processes=4)
