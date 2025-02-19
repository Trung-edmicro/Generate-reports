import sys
from PyQt5.QtWidgets import QApplication, QWidget, QHBoxLayout, QListWidget, QStackedWidget
from ui.pages.ui_generator_report import GeneratorReport
from ui.pages.ui_home import HomePage

class ReportWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()
    
    def initUI(self):
        self.setWindowTitle("Tạo báo cáo")
        self.setGeometry(100, 100, 1000, 600)

        # Tạo layout chính
        main_layout = QHBoxLayout()

        # Tạo menu bên trái
        self.menu = QListWidget()
        self.menu.addItems(["Trang chủ", "Tạo báo cáo kết quả", "Cài đặt", "Thoát"])
        self.menu.setFixedWidth(200)
        self.menu.currentRowChanged.connect(self.display_page)

        # Tạo khu vực hiển thị nội dung
        self.pages = QStackedWidget()
        
        # Thêm trang vào QStackedWidget
        self.generator_report_page = GeneratorReport()
        self.home_page = HomePage()

        self.pages.addWidget(self.home_page)
        self.pages.addWidget(self.generator_report_page)
        self.pages.addWidget(QWidget())

        main_layout.addWidget(self.menu)
        main_layout.addWidget(self.pages, stretch=1)

        self.setLayout(main_layout)

    def display_page(self, index):
        if self.menu.item(index).text() == "Thoát":
            QApplication.quit()
        else:
            self.pages.setCurrentIndex(index)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ReportWindow()
    window.show()
    sys.exit(app.exec_())