# Generate-reports

Dự án này là một ứng dụng Python tự động hóa việc xử lý kết quả thi của học sinh, tạo báo cáo Excel và PDF cá nhân cho từng học sinh dựa trên dữ liệu đầu vào từ file Excel kết quả, ma trận.

## Cấu trúc dự án

```
baocaoketqua/
├── assets/
│   ├── fonts/          # Fonts sử dụng cho PDF (DejaVu Sans)
│   └── images/         # Hình ảnh header và footer cho PDF
├── data/
│   ├── 0.Dữ liệu trường/  # Dữ liệu trường học
│   ├── input/          # File đầu vào (Excel, ma trận, templates)
│   └── output/         # File đầu ra (Excel, PDF, zip)
├── docs/
│   ├── prompt.txt      # Prompt cho AI
├── logs/
│   └── app.log         # Log ứng dụng
├── src/
│   ├── __init__.py
│   ├── main.py         # Entry point chính
│   ├── data_processor.py  # Xử lý dữ liệu, đánh giá câu trả lời
│   ├── pdf_generator.py   # Tạo PDF báo cáo
│   ├── ai_comment_generator.py  # Tạo nhận xét bằng AI
│   ├── logger_config.py  # Cấu hình logging
│   ├── module1.py to module4.py  # Các module xử lý cho output khác nhau
│   ├── moduleTestAI.py  # Module test AI
│   ├── ui/             # Giao diện người dùng
│   └── utils/          # Utilities
├── tests/
│   └── test_main.py    # Test cases
├── requirements.txt    # Dependencies
├── README.md       # Tài liệu này
└── .gitignore
```

## Chức năng chính

### 1. Xử lý dữ liệu thi

- Đọc file kết quả làm bài từ Excel (`input.xlsx`)
- Đọc ma trận kiến thức (`matran.xlsx`)
- Đánh giá câu trả lời của học sinh
- Tính điểm, xếp hạng trong lớp và khối
- Phân tích mức độ kiến thức cơ bản và nâng cao

### 2. Tạo báo cáo Excel

- Xuất kết quả tổng hợp ra file Excel với các cột:
  - Thông tin học sinh (Họ và tên, Lớp, Trường)
  - Kết quả thi (Số câu đúng/sai, Điểm)
  - Xếp hạng (Trong lớp, Trong khối)
  - Phân tích kiến thức (Cơ bản, Nâng cao)
  - Nhận xét và nội dung cần cải thiện

### 3. Tạo báo cáo PDF cá nhân

- Tạo PDF cho từng học sinh với:
  - Thông tin cá nhân
  - Kết quả chi tiết
  - Nhận xét về kết quả
  - Gợi ý cải thiện dựa trên câu trả lời sai
  - Hình ảnh header/footer

### 4. Tích hợp AI

- Sử dụng Google Generative AI để tạo nhận xét tự động

## Cách sử dụng

### 1. Cài đặt dependencies

```bash
pip install -r requirements.txt
```

### 2. Chuẩn bị dữ liệu đầu vào

- Đặt file `input.xlsx` (kết quả làm bài) vào `data/input/`
- Đặt file `matran.xlsx` (ma trận kiến thức) vào `data/input/`
- Đảm bảo format đúng:
  - `input.xlsx`: Cột "Câu trả lời", "Đáp án", "Họ và tên", "Lớp", "Trường"
  - `matran.xlsx`: Cột "Cấp độ nhận thức", "Chủ đề", "Nội dung", "Bài", "Link bài luyện"

### 3. Chạy ứng dụng

```bash
python src/main.py
```

### 4. Kiểm tra kết quả

- File Excel tổng hợp: `data/output/output.xlsx`
- PDF cá nhân: `data/output/DanhSachTheoTenTruong/` (theo tên trường)
- PDF theo học sinh: `data/output/DanhSachTheoTungHocSinh/`

## Dependencies

- `fpdf2`: Tạo PDF
- `PyPDF2`: Xử lý PDF
- `google-generativeai`: Tích hợp AI cho nhận xét
- `pandas`: Xử lý dữ liệu Excel
- `openpyxl`: Đọc/ghi Excel

## Lưu ý

- Đảm bảo đường dẫn file chính xác trong code
- Font DejaVu Sans phải có trong `assets/fonts/`
- Hình ảnh header/footer trong `assets/images/`
- Log được ghi vào `logs/app.log`
