import pandas as pd
import os
import glob

# Đường dẫn tới thư mục 'data'
data_folder = "data"

# Hàm ghép tất cả file Excel trong thư mục 'data'
def merge_all_files(data_folder):
    all_data = []
    # Lấy danh sách tất cả file Excel có dạng 'history_data_*_all_pages.xlsx'
    excel_files = glob.glob(os.path.join(data_folder, "history_data_*_all_pages.xlsx"))
    
    print(f"Tìm thấy {len(excel_files)} file Excel để ghép...")
    for file in excel_files:
        try:
            df = pd.read_excel(file)
            # Thêm cột 'Stock' từ tên file nếu chưa có
            stock_id = os.path.basename(file).replace("history_data_", "").replace("_all_pages.xlsx", "")
            if 'Stock' not in df.columns:
                df['Stock'] = stock_id
            all_data.append(df)
            print(f"Đã đọc file: {file}")
        except Exception as e:
            print(f"Lỗi khi đọc file {file}: {e}")
    
    if all_data:
        # Ghép tất cả DataFrame thành một
        merged_df = pd.concat(all_data, ignore_index=True)
        # Chuyển cột 'Date' thành datetime và sắp xếp
        merged_df['Date'] = pd.to_datetime(merged_df['Date'], format='%d/%m/%Y')
        merged_df = merged_df.sort_values(by=['Date', 'Stock'])
        # Lưu vào file tổng hợp
        merged_file = os.path.join(data_folder, "merged_stock_history.xlsx")
        merged_df.to_excel(merged_file, index=False)
        print(f"Đã ghép tất cả dữ liệu vào {merged_file}")
        return merged_df
    else:
        print("Không có dữ liệu để ghép.")
        return None

# Chạy chương trình
if __name__ == "__main__":
    merge_all_files(data_folder)
