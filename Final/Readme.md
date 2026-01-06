# Financial Fraud Detection Pipeline
## 1. Giới thiệu về project

* Trong kỷ nguyên số, các giao dịch tài chính (thẻ tín dụng, ví điện tử) diễn ra với tốc độ chóng mặt. Tuy nhiên, đi kèm với đó là rủi ro gian lận ngày càng tinh vi, gây thiệt hại hàng tỷ USD mỗi năm. Việc phát hiện sớm các giao dịch bất thường không chỉ bảo vệ tài sản mà còn là yêu cầu cốt lõi của các hệ thống Fintech hiện đại.
* **Mục tiêu của project:** Xây dựng một Data Pipeline kết hợp giữa **Batch Processing** (để huấn luyện mô hình) và **Stream Processing** (để phát hiện gian lận thời gian thực). Hệ thống sử dụng kiến trúc Medallion (Bronze, Silver, Gold) để quản lý luồng dữ liệu giao dịch từ khi phát sinh cho đến khi đưa ra cảnh báo gian lận.

## 2. Dataset

* Project sử dụng dữ liệu mô phỏng từ **Synthetic Financial Datasets for Fraud Detection**. Dữ liệu mô phỏng các hành vi giao dịch thực tế như rút tiền, chuyển khoản và thanh toán.
* **Kích thước dữ liệu:** Hơn 6 triệu bản ghi giao dịch.
* **Các trường dữ liệu chính:**

| Field | Description |
| --- | --- |
| step | Đơn vị thời gian (giả lập 1 step = 1 giờ). |
| type | Loại giao dịch: CASH-IN, CASH-OUT, DEBIT, PAYMENT, TRANSFER. |
| amount | Số tiền giao dịch. |
| nameOrig | ID khách hàng khởi tạo giao dịch. |
| oldbalanceOrg | Số dư ban đầu trước giao dịch. |
| newbalanceOrig | Số dư mới sau giao dịch. |
| nameDest | ID người nhận/Điểm giao dịch. |
| isFraud | Nhãn giao dịch gian lận (0: Bình thường, 1: Gian lận). |
| isFlaggedFraud | Hệ thống hiện tại đã đánh dấu gian lận hay chưa (dùng để so sánh). |

## 3. Công nghệ sử dụng và kiến trúc hệ thống

* **Terraform:** Khởi tạo hạ tầng trên GCP (GCS, BigQuery, Pub/Sub).
* **Kafka:** Tiếp nhận luồng giao dịch (Streaming Data) để kiểm tra gian lận ngay lập tức.
* **Spark Streaming:** Xử lý luồng, tính toán các chỉ số nghi vấn.
* **dbt & Spark Batch:** Xử lý dữ liệu lịch sử tại BigQuery để cập nhật các đặc trưng (features) cho mô hình nhận diện.
* **Looker Studio:** Hiển thị Dashboard theo dõi tỉ lệ gian lận theo thời gian và các khu vực rủi ro cao.

## 4. Các bước thực hiện

1. **Ingestion:** Dữ liệu giao dịch được đẩy vào Kafka Topic.
2. **Processing:** Spark Streaming đọc từ Kafka, làm sạch và thực hiện Feature Engineering
3. **Detection:** Áp dụng logic/mô hình ML để gán nhãn Fraud.
4. **Storage:** Lưu trữ kết quả vào BigQuery.
5. **Analytics:** dbt tổng hợp dữ liệu để báo cáo định kỳ.



