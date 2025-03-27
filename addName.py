import csv
import os
from vnstock import *

# Bước 1: Đọc danh sách mã cổ phiếu từ file successful_stocks.txt
with open("successful_stocks.txt", "r") as file:
    successful_stocks = [line.strip() for line in file if line.strip()]

print("Danh sách mã cổ phiếu từ successful_stocks.txt:", successful_stocks)

# Bước 2: Lấy thông tin công ty từ vnstock
# Lấy danh sách tất cả công ty niêm yết
companies_df = Listing().all_symbols()

# Tạo từ điển ánh xạ mã cổ phiếu (ticker) với tên công ty (company_name)
stock_to_company = dict(zip(companies_df['ticker'], companies_df['organ_name']))

# Lấy tên công ty tương ứng với các mã trong successful_stocks
stock_company_mapping = {}
for stock in successful_stocks:
    if stock in stock_to_company:
        stock_company_mapping[stock] = stock_to_company[stock]
    else:
        stock_company_mapping[stock] = "Không tìm thấy tên công ty"


# Bước 3: Đọc file CSV và thêm cột CompanyName
data_folder = "/home/jacktran/stock_model"
input_file = os.path.join(data_folder, "merged_file.csv")
output_file = os.path.join(data_folder, "merged_stock_history_transformed_with_company.csv")

# Đọc file CSV và ghi file mới
with open(input_file, "r", newline="", encoding="utf-8") as infile, \
     open(output_file, "w", newline="", encoding="utf-8") as outfile:
    
    # Đọc file CSV
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    
    # Đọc dòng tiêu đề
    header = next(reader)
    # Thêm cột CompanyName vào tiêu đề
    header.append("CompanyName")
    writer.writerow(header)
    
    # Đọc từng dòng dữ liệu
    missing_companies = set()  # Để lưu các mã không tìm thấy tên công ty
    for row in reader:
        # Kiểm tra xem hàng có đủ cột không
        if len(row) <= 9:  # Nếu hàng không có đủ 9 cột (index 8)
            print(f"Dòng không đủ cột: {row}")
            row.append("Không tìm thấy mã cổ phiếu")
        else:
            stock = row[9]  # Cột Stock ở vị trí thứ 9 (index 8)
            # Lấy tên công ty từ từ điển ánh xạ
            company_name = stock_company_mapping.get(stock, "Không tìm thấy tên công ty")
            if company_name == "Không tìm thấy tên công ty":
                missing_companies.add(stock)
            # Thêm tên công ty vào cuối dòng
            row.append(company_name)
        writer.writerow(row)

# Bước 4: In thông báo về các mã không tìm thấy tên công ty
if missing_companies:
    print("Các mã cổ phiếu không tìm thấy tên công ty:")
    for stock in missing_companies:
        print(f"- {stock}")

print(f"Đã lưu file CSV mới với cột CompanyName tại: {output_file}")