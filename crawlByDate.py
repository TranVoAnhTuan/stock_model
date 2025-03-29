import requests
from bs4 import BeautifulSoup
import os
import csv
from multiprocessing import Pool, Value
from vnstock import Listing

# Biến đếm toàn cục để theo dõi tiến trình
counter = None

def init_counter():
    global counter
    counter = Value('i', 0)  # Biến đếm kiểu integer

# Hàm lấy 1 dòng đầu tiên từ trang 1 trên web
def extract_top2_from_web(args):
    stock_id, stock_to_company = args
    global counter
    url = f"https://www.cophieu68.vn/quote/history.php?cP=1&id={stock_id}"
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
        for tr in table.find_all("tr")[1:2]:  # Chỉ lấy 1 dòng đầu
            if "Thưởng cổ phiếu" not in tr.get_text():
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                cells.insert(0, stock_id)  # Thêm stock_id vào đầu
                company_name = stock_to_company.get(stock_id, "Unknown")
                cells.append(company_name)  # Thêm tên công ty vào cuối
                rows.append(cells)

        if not rows:
            print(f"Không có dữ liệu hợp lệ ở {url}")
            return None

        # Cập nhật và in tiến trình
        with counter.get_lock():
            counter.value += 1
            print(f"Đã crawl mã {stock_id} ({counter.value}/{total_stocks} - {counter.value/total_stocks*100:.2f}%)")

        return rows
    except Exception as e:
        print(f"Lỗi khi tải {url}: {e}")
        return None

# Đường dẫn tới thư mục 'data'
data_folder = "/home/jacktran/stock_model"
successful_file = os.path.join(data_folder, "stock_2025.txt")
output_file = os.path.join(data_folder, "merged_file.csv")

# Lấy danh sách mã và tên công ty từ vnstock
try:
    companies_df = Listing().all_symbols()
    ticker = list(companies_df['ticker'])
    stock_to_company = dict(zip(companies_df['ticker'], companies_df['organ_name']))
except Exception as e:
    print(f"Lỗi khi lấy danh sách mã cổ phiếu từ vnstock: {e}")
    exit()

# Đọc danh sách mã cổ phiếu cần xử lý từ file
try:
    with open(successful_file, 'r') as f:
        stock_list_from_file = [line.strip() for line in f]
except FileNotFoundError:
    print(f"Lỗi: Không tìm thấy file {successful_file}")
    exit()

# Lọc danh sách mã cổ phiếu từ file để chỉ lấy những mã có trong danh sách từ vnstock
total_stocks = len(stock_list_from_file)
print(f"Tổng số mã cổ phiếu cần xử lý: {total_stocks}")

if __name__ == "__main__":
    init_counter()
    pool_args = [(stock, stock_to_company) for stock in stock_list_from_file]
    with Pool(processes=4) as pool:
        results = pool.map(extract_top2_from_web, pool_args)

    # Gộp tất cả các dòng đã lấy
    all_rows = [row for sublist in results if sublist for row in sublist]

    if all_rows:
        # Xác định header
        headers = ["Stock", "Date", "Close", "Volume", "Open", "High", "Low", "Foreign Buy", "Foreign Sell", "Foreign Value", "Company Name"]

        # Ghi dữ liệu vào file CSV
        with open(output_file, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(all_rows)
        print(f"Đã lưu dữ liệu vào file: {output_file}")
    else:
        print("Không có dữ liệu nào được thu thập.")