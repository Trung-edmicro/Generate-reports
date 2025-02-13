from pdf_generator import pdf_generator
from data_processor import processor_data, evaluate_answers ,save_results

def process_results(input_file1, input_file2, output_excel, output_pdf_dir):
    # Đọc file kết quả làm bài & ma trận kiến thức
    df1, df2 = processor_data(input_file1, input_file2)

    # Xử lý
    df1 = evaluate_answers(df1, df2)

    # Xuất file Excel
    save_results(df1, output_excel, output_pdf_dir)

    # Xuất từng file PDF
    for _, row in df1.iterrows():
        if "Câu trả lời" not in df1.columns or not str(row["Câu trả lời"]).strip():
            continue
        
        pdf_generator(row, output_pdf_dir)

    print(f"File Excel đã được lưu tại: {output_excel}")
    print(f"Các file PDF đã được lưu trong thư mục: {output_pdf_dir}")

input_file1 = "data/input/inputtest.xlsx"
input_file2 = "data/input/matran.xlsx"
output_excel = "data/output/output.xlsx"
output_pdf_dir = "data/output/DanhSachTheoTungHocSinh/"
process_results(input_file1, input_file2, output_excel, output_pdf_dir)