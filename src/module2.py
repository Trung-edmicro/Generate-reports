import os
import pandas as pd
from pdf_generator import pdf_generator
from data_processor_ori import processor_data, evaluate_answers ,save_results
import time

def process_results(input_file1, input_file2, output_excel, output_pdf_dir):
    # Ghi lại thời điểm bắt đầu toàn bộ quá trình
    start_time_total = time.time()
    # Đọc file kết quả làm bài & ma trận kiến thức
    df1, df2 = processor_data(input_file1, input_file2)

    # Xử lý
    df1 = evaluate_answers(df1, df2)

    # Xuất file Excel
    save_results(df1, output_excel, output_pdf_dir)

    # Xuất từng file PDF theo id
    for _, row in df1.iterrows():
        if 'id trường' not in row or pd.isna(row['id trường']):
            continue

        school_id = str(int(row['id trường']))
        school_folder = os.path.join(output_pdf_dir, school_id)
        os.makedirs(school_folder, exist_ok=True)
        
        if "Câu trả lời" not in df1.columns or not str(row["Câu trả lời"]).strip():
            continue

        pdf_generator(row, school_folder)
    # Ghi lại thời điểm kết thúc toàn bộ quá trình
    end_time_total = time.time()        
    print(f"File Excel đã được lưu tại: {output_excel}")
    print(f"Các file PDF đã được lưu trong thư mục: {output_pdf_dir}")
    print(f"Toàn bộ quá trình mất {end_time_total - start_time_total:.2f} giây")

input_file1 = "E:\Edmicro\Generate-reports\data\input\inputtest.xlsx"
input_file2 = "E:\Edmicro\Generate-reports\data\input\matran.xlsx"
output_excel = "E:\Edmicro\Generate-reports\data\output\output.xlsx"
output_pdf_dir = "E:\Edmicro\Generate-reports\data\output\DanhSachTheoTenTruong"
process_results(input_file1, input_file2, output_excel, output_pdf_dir)