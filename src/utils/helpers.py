import os
from logger_config import logger

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROMPT_FILE = os.path.join(BASE_DIR, "prompt_template.txt")

def load_prompt(file_path=PROMPT_FILE, **kwargs):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            template = f.read().strip()
        return template.format(**kwargs)
    except Exception as e:
        logger.error(f"Lỗi khi đọc file prompt ({file_path}): {e}")
        return "Không thể đọc prompt."
