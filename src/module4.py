import os
import pandas as pd
from pdf_generator import pdf_generator
from data_processor import processor_data, evaluate_answers, save_results
import time

def process_results(input_file1, input_file2, output_excel, output_pdf_dir):
    start_time_total = time.time()

    # Đọc dữ liệu
    df1, df2 = processor_data(input_file1, input_file2)

    # Xử lý đánh giá câu trả lời (Đã tối ưu đa tiến trình)
    df1 = evaluate_answers(df1, df2)

    # Xuất file Excel
    save_results(df1, output_excel, output_pdf_dir)

    # Xuất file PDF bằng đa tiến trình
    pdf_generator(df1, output_pdf_dir)

    end_time_total = time.time()
    print(f"✅ File Excel đã được lưu tại: {output_excel}")
    print(f"✅ Các file PDF đã được lưu trong thư mục: {output_pdf_dir}")
    print(f"⏳ Toàn bộ quá trình mất {end_time_total - start_time_total:.2f} giây")

if __name__ == "__main__":
    input_file1 = "E:\Edmicro\Generate-reports\data\input\inputtest.xlsx"
    input_file2 = "E:\Edmicro\Generate-reports\data\input\matran.xlsx"
    output_excel = "E:\Edmicro\Generate-reports\data\output\output.xlsx"
    output_pdf_dir = "E:\Edmicro\Generate-reports\data\output\DanhSachTheoTenTruong"

    process_results(input_file1, input_file2, output_excel, output_pdf_dir)
