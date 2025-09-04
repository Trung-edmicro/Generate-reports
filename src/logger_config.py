import logging
import os

# Tạo thư mục logs nếu chưa có
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Định dạng log
log_format = "%(asctime)s - %(levelname)s - %(message)s"
log_level = logging.INFO

# Xóa tất cả handler cũ nếu có
logging.getLogger().handlers.clear()

# Tạo logger
logger = logging.getLogger(__name__)
logger.setLevel(log_level)

# Handler ghi log vào file
file_handler = logging.FileHandler(os.path.join(log_dir, "app.log"), encoding="utf-8")
file_handler.setFormatter(logging.Formatter(log_format))

# Handler hiển thị log ra terminal
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(log_format))

# Gán handler vào logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)
