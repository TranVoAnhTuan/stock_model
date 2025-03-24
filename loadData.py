import requests
import os
from bs4 import BeautifulSoup
import pandas as pd
import time
from vnstock import * 

# Hàm lấy bảng thứ 3 từ một trang
def extract_history_table(url):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"Không thể truy cập {url}. Status code: {response.status_code}")
        return None
    
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"id": "history"})
    
    if not table or len(table.find_all("tr")) <= 1:
        print(f"Không còn dữ liệu ở {url}")
        return None
    
    headers = [
        "Date", "Close", "Volume", "Open", "High", "Low", "Foreign Buy", "Foreign Sell", "Foreign Value"
    ]
    
    rows = []
    for tr in table.find_all("tr")[1:]:
        if "Thưởng cổ phiếu" not in tr.get_text():
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            rows.append(cells)
    
    df = pd.DataFrame(rows, columns=headers)
    return df

# Hàm tải dữ liệu từ tất cả các trang cho một mã
def download_all_history_data(stock_id):
    all_data = []
    page = 1
    
    while True:
        url = f"https://www.cophieu68.vn/quote/history.php?cP={page}&id={stock_id}"
        print(f"Đang tải dữ liệu từ trang {page} cho {stock_id}: {url}")
        
        df = extract_history_table(url)
        if df is not None and not df.empty:
            all_data.append(df)
            page += 1
            time.sleep(2)
        else:
            print(f"Đã tải hết dữ liệu cho {stock_id} sau {page - 1} trang.")
            break
    
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        file_path = os.path.join("/home/jacktran/stock_model/data",f"history_data_{stock_id}_all_pages.csv")
        final_df.to_csv(file_path, index=False)
        print(f"Dữ liệu của {stock_id} đã được lưu vào history_data_{stock_id}_all_pages.xlsx")
        return final_df
    else:
        print(f"Không có dữ liệu để lưu cho {stock_id}.")
        return None

# Hàm chính: Tải dữ liệu cho tất cả mã cổ phiếu
def download_all_stocks_history():
    # Lấy danh sách mã cổ phiếu từ vnstock
    ticker =Listing().all_symbols()
    stock_codes = list(ticker['ticker'].str.lower())
    print(f"Tìm thấy {len(stock_codes)} mã cổ phiếu: {stock_codes[:10]}...")  # In 10 mã đầu tiên
    
    for stock_id in stock_codes:
        print(f"\nBắt đầu tải dữ liệu cho mã {stock_id}")
        download_all_history_data(stock_id)
        time.sleep(5)  # Đợi 5 giây giữa các mã để tránh bị chặn

# Chạy chương trình
if __name__ == "__main__":
    download_all_stocks_history()
