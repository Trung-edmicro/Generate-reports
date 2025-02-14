import os
import pandas as pd
from pdf_generator import pdf_generator
from data_processor import processor_data, evaluate_answers ,save_results
import threading
import queue
import time

def process_batch(batch_df, batch_number, results_queue):
    """
    Hàm xử lý một batch.
    """
    print(f"Thread đang xử lý batch {batch_number}...")
    # (Thêm code để xử lý từng lô nếu cần)
    # Ví dụ: Gọi API Gemini
    # results = call_gemini_api(batch_df.to_dict(orient='records'))
    # (Sau khi xử lý, trả về kết quả vào results_queue)
    time.sleep(1)  # Giả lập thời gian xử lý
    results_queue.put((batch_number, batch_df))  # Trả về batch number và batch_df để lưu đúng thứ tự
    print(f"Thread đã hoàn thành xử lý batch {batch_number}")

def process_results(input_file1, input_file2, output_excel, output_pdf_dir, batch_size, num_threads):
    # Đọc file kết quả làm bài & ma trận kiến thức
    df1, df2 = processor_data(input_file1, input_file2)

    # Xủ lý
    df1 = evaluate_answers(df1, df2)

    # Tạo một queue để chứa các batch cần xử lý
    batch_queue = queue.Queue()

    # Tạo một queue để chứa kết quả từ các thread
    results_queue = queue.Queue()

    for i in range(0, len(df1), batch_size):
        batch_df = df1[i:i + batch_size]
        # In ra kích thước của từng lô để kiểm tra
        print(f"Kích thước của lô {i // batch_size + 1}: {len(batch_df)}")
        # Lưu cả batch number để đảm bảo thứ tự
        batch_queue.put((i // batch_size + 1, batch_df))

    # Tạo và khởi động các thread
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=lambda: worker(batch_queue, results_queue), daemon=True)
        threads.append(thread)
        thread.start()

    # Chờ tất cả các batch được xử lý
    batch_queue.join()  # Chờ đến khi tất cả các task trong queue hoàn thành

    # Thu thập kết quả từ các thread và sắp xếp theo thứ tự
    results = []
    while not results_queue.empty():
        results.append(results_queue.get())
    results.sort(key=lambda x: x[0])  # Sắp xếp theo batch number    

    # Xuất file Excel
    save_results(df1, output_excel, output_pdf_dir)

    # Xuất từng file PDF theo tên
    for _, row in df1.iterrows():
        school_name = str(row['Tên trường']) if 'Tên trường' in df1.columns else "Unknown_School" 
        school_folder = os.path.join(output_pdf_dir, school_name)
        os.makedirs(school_folder, exist_ok=True)
    
        if "Câu trả lời" not in df1.columns or not str(row["Câu trả lời"]).strip():
            continue

        pdf_generator(row, school_folder)

    print(f"File Excel đã được lưu tại: {output_excel}")
    print(f"Các file PDF đã được lưu trong thư mục: {output_pdf_dir}")

def worker(batch_queue, results_queue):
    """
    Hàm worker cho mỗi thread.
    """
    while True:
        try:
            batch_number, batch_df = batch_queue.get(timeout=5)  # Lấy một batch từ queue
            process_batch(batch_df, batch_number, results_queue)  # Xử lý batch
            batch_queue.task_done()  # Báo cho queue biết task đã hoàn thành
        except queue.Empty:
            break  # Thoát khỏi vòng lặp nếu queue rỗng    

input_file1 = "E:\Edmicro\Generate-reports\data\input\inputtest.xlsx"
input_file2 = "E:\Edmicro\Generate-reports\data\input\matran.xlsx"
output_excel = "E:\Edmicro\Generate-reports\data\output\output.xlsx"
output_pdf_dir = "E:\Edmicro\Generate-reports\data\output\DanhSachTheoTenTruong"
batch_size = 15
num_threads = 5
process_results(input_file1, input_file2, output_excel, output_pdf_dir, batch_size, num_threads)