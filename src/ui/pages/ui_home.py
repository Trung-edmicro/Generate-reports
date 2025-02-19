from PyQt5.QtWidgets import QWidget, QLabel, QVBoxLayout

class HomePage(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        layout = QVBoxLayout()
        
        label = QLabel("Chào mừng bạn đến với hệ thống!")
        layout.addWidget(label)
        
        description = QLabel("Sử dụng menu bên trái để điều hướng các chức năng.")
        layout.addWidget(description)
        
        self.setLayout(layout)
