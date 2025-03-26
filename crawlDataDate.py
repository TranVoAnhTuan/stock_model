import requests
from bs4 import BeautifulSoup
import os
import csv
from multiprocessing import Pool, Value

# Biến đếm toàn cục để theo dõi tiến trình
counter = None

def init_counter():
    global counter
    counter = Value('i', 0)  # Biến đếm kiểu integer

# Hàm lấy 2 dòng đầu tiên từ trang 1 trên web
def extract_top2_from_web(stock_id):
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
        for tr in table.find_all("tr")[2:4]:  # Chỉ lấy 2 dòng đầu
            if "Thưởng cổ phiếu" not in tr.get_text():
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                cells.append(stock_id)  # Thêm stock_id vào cuối
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
successful_file = os.path.join(data_folder, "successful_stocks.txt")
output_file = os.path.join(data_folder, "merged_file.csv")

# Đọc danh sách mã thành công
with open(successful_file, "r") as f:
    successful_stocks = f.read().splitlines()

# Số lượng mã cần crawl
total_stocks = len(successful_stocks)

# Crawl 2 dòng đầu từ trang 1 song song
print(f"Bắt đầu crawl 2 dòng đầu từ trang 1 cho {total_stocks} mã song song...")
with Pool(processes=4, initializer=init_counter) as pool:
    top2_data = list(pool.imap_unordered(extract_top2_from_web, successful_stocks))

# Lọc bỏ None và chuẩn bị dữ liệu để ghi
rows_to_append = []
for data in top2_data:
    if data is not None:
        rows_to_append.extend(data)

# Ghi thêm dữ liệu vào file CSV hiện có
if rows_to_append:
    with open(output_file, mode='a', newline='') as f:
        writer = csv.writer(f)
        # Tiêu đề cột (nếu cần kiểm tra file gốc không có tiêu đề thì bỏ dòng này)
        # writer.writerow(["Date", "Close", "Volume", "Open", "High", "Low", "Foreign Buy", "Foreign Sell", "Foreign Value", "Stock"])
        writer.writerows(rows_to_append)
    print(f"Đã ghép {len(rows_to_append)} dòng vào {output_file}")
else:
    print("Không có dữ liệu để ghép.")

