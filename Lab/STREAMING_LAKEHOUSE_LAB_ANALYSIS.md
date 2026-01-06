# Streaming Lakehouse Lab - Phân tích và Đề xuất

## 📊 Tổng quan về Streaming Lakehouse

### Định nghĩa
**Streaming Lakehouse** (hay "Streamhouse") là kiến trúc kết hợp:
- **Lakehouse**: ACID transactions, schema evolution, time travel (như Data Warehouse + Data Lake)
- **Streaming-first**: Real-time processing là nền tảng, không phải add-on
- **Unified pipeline**: Batch và streaming dùng **chung code/logic**, không tách riêng

### Core Concept: "End of Lambda Architecture"
- **Lambda**: 2 pipelines riêng (batch + streaming) → Phức tạp, tốn kém
- **Kappa**: 1 pipeline streaming → Đơn giản nhưng khó xử lý historical data
- **Streaming Lakehouse**: 1 unified pipeline → Đơn giản + xử lý được cả real-time và historical

## 🔄 So sánh với các kiến trúc khác

### Lambda Architecture (Old)
```
Batch Layer (Hadoop/Spark Batch) ──┐
                                    ├──→ Serving Layer
Streaming Layer (Kafka Streams) ───┘
```
**Vấn đề**: 2 codebases, 2 systems, phức tạp maintain

### Kappa Architecture (Current)
```
Kafka Log ──→ Streaming Processing ──→ Results
```
**Vấn đề**: Reprocessing toàn bộ data tốn kém, khó xử lý historical data lớn

### Streaming Lakehouse (New - 2025-2026)
```
Kafka ──→ Spark Streaming ──→ Iceberg Tables ──→ Query/ML
         (unified code)      (Bronze/Silver/Gold)
         
Batch Processing ──→ Same Iceberg Tables ──→ Same Query/ML
(same code, different trigger)
```
**Ưu điểm**: 
- ✅ 1 codebase cho batch và streaming
- ✅ Historical data trong Iceberg (không cần reprocess)
- ✅ Real-time + batch cùng query được
- ✅ ACID transactions, time travel

## 🎯 Mục tiêu bài lab Streaming Lakehouse

### Learning Objectives
1. Hiểu tại sao Streaming Lakehouse thay thế Lambda/Kappa
2. Thực hành unified pipeline (batch + streaming cùng code)
3. Sử dụng Iceberg như storage layer cho cả streaming và batch
4. Implement Medallion Architecture với streaming
5. So sánh performance và complexity với Lambda/Kappa

### Điểm khác biệt với Data Lakehouse Lab hiện tại
- **Data Lakehouse Lab**: Tập trung integration (Kafka → Spark → Iceberg → dbt → GE → Airflow)
- **Streaming Lakehouse Lab**: Tập trung **unified pipeline**, so sánh với Lambda/Kappa, batch+stream cùng code

## 📚 Đề xuất cấu trúc bài lab

### Lab Structure: 5-6 Notebooks

#### **Notebook 1: Streaming Lakehouse Fundamentals**
**Mục tiêu**: Hiểu concepts, so sánh với Lambda/Kappa

**Nội dung**:
- Streaming Lakehouse là gì?
- Tại sao "End of Lambda Architecture"?
- So sánh Lambda vs Kappa vs Streaming Lakehouse
- Unified pipeline concept
- Table formats (Iceberg) cho streaming

**Exercises**:
1. So sánh architecture diagrams
2. Phân tích ưu/nhược điểm từng kiến trúc
3. Use case mapping (khi nào dùng gì)

#### **Notebook 2: Unified Pipeline với Spark + Iceberg**
**Mục tiêu**: Viết code xử lý batch và streaming dùng chung logic

**Nội dung**:
- Spark Structured Streaming → Iceberg
- Spark Batch → Same Iceberg tables
- Shared transformation logic
- Code reuse patterns

**Exercises**:
1. Viết transformation function (dùng chung)
2. Streaming pipeline → Iceberg Bronze
3. Batch pipeline → Same Iceberg Bronze (same code)
4. So sánh kết quả

#### **Notebook 3: Medallion Architecture với Streaming**
**Mục tiêu**: Implement Bronze → Silver → Gold với streaming

**Nội dung**:
- Bronze: Raw streaming data từ Kafka
- Silver: Cleaned/transformed (streaming + batch)
- Gold: Aggregated (streaming + batch)
- Streaming transformations giữa các layers

**Exercises**:
1. Kafka → Spark Streaming → Iceberg Bronze
2. Streaming transformation: Bronze → Silver
3. Batch backfill: Historical data → Silver (same code)
4. Streaming aggregation: Silver → Gold
5. Query Gold layer (real-time + historical)

#### **Notebook 4: Batch và Streaming - Same Code**
**Mục tiêu**: Chứng minh batch và streaming dùng chung code

**Nội dung**:
- Shared transformation functions
- Spark DataFrame API (works cho cả batch và streaming)
- Conditional logic: batch mode vs streaming mode
- Code patterns và best practices

**Exercises**:
1. Viết shared transformation function
2. Apply cho streaming (readStream)
3. Apply cho batch (read) - same function
4. Compare results
5. Performance comparison

#### **Notebook 5: Advanced: Time Travel và Schema Evolution**
**Mục tiêu**: Tận dụng Iceberg features trong streaming context

**Nội dung**:
- Time travel queries trên streaming data
- Schema evolution với streaming writes
- Snapshot management
- Query historical snapshots

**Exercises**:
1. Write streaming data với schema evolution
2. Time travel: Query data tại thời điểm cụ thể
3. Compare snapshots (before/after transformation)
4. Rollback scenarios

#### **Notebook 6: Real-world Use Case (Optional)**
**Mục tiêu**: End-to-end pipeline thực tế

**Nội dung**:
- Complete pipeline: Kafka → Streaming → Iceberg → Analytics
- Real-time dashboards
- Batch reports (same data)
- Performance monitoring

**Exercises**:
1. Build complete pipeline
2. Real-time metrics
3. Batch reports
4. Compare với Lambda architecture (complexity, cost)

## 🏗️ Architecture Design

### Streaming Lakehouse Architecture
```
┌─────────────┐
│   Kafka     │  Real-time events
│  (Source)   │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────┐
│   Spark Structured Streaming         │
│   (Unified Processing Logic)         │
│   - Transformations                  │
│   - Aggregations                     │
│   - Windows                           │
└──────┬───────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│   Apache Iceberg Tables              │
│   ┌─────────┐  ┌─────────┐  ┌──────┐ │
│   │ Bronze  │→ │ Silver  │→ │ Gold │ │
│   │ (Raw)   │  │(Cleaned)│  │(Agg) │ │
│   └─────────┘  └─────────┘  └──────┘ │
└──────┬───────────────────────────────┘
       │
       ├─────────────────┬──────────────┐
       ▼                 ▼              ▼
┌──────────┐    ┌──────────────┐  ┌─────────┐
│ Real-time│    │ Batch Reports│  │   ML    │
│ Analytics│    │ (Same Data)  │  │ Models  │
└──────────┘    └──────────────┘  └─────────┘

Parallel: Batch Processing (Same Code)
┌─────────────┐
│ Batch Data  │  Historical files
│  (Source)   │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────┐
│   Spark Batch Processing             │
│   (Same Transformation Logic)         │
│   - Same functions                   │
│   - Same aggregations                │
└──────┬───────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│   Same Iceberg Tables                │
│   (Bronze → Silver → Gold)           │
└─────────────────────────────────────┘
```

### Key Points:
1. **Unified Code**: Batch và streaming dùng chung transformation logic
2. **Unified Storage**: Cùng Iceberg tables cho cả batch và streaming
3. **Unified Query**: Cùng query engine cho real-time và historical
4. **No Duplication**: Không cần maintain 2 codebases

## 🛠️ Technology Stack

### Core Technologies
- **Apache Spark 3.5+**: Structured Streaming + Batch
- **Apache Iceberg**: Table format (unified storage)
- **Apache Kafka**: Event streaming source
- **Python/PySpark**: Development language

### Supporting Technologies
- **Jupyter Notebooks**: Interactive learning
- **Docker Compose**: Infrastructure
- **PostgreSQL**: Metadata catalog (optional)
- **MinIO/S3**: Object storage (optional)

### Dependencies
```python
pyspark==3.5.0
pyiceberg  # For Iceberg Python API
kafka-python  # For Kafka producer/consumer
pandas, numpy  # Data manipulation
```

## 📋 Chi tiết từng Notebook

### Notebook 1: Fundamentals (Conceptual)
**Thời lượng**: 30-45 phút
**Loại**: Mostly markdown + diagrams

**Sections**:
1. Introduction to Streaming Lakehouse
2. Lambda Architecture (problems)
3. Kappa Architecture (limitations)
4. Streaming Lakehouse (solution)
5. Comparison table
6. When to use what

**Output**: Understanding, no code

### Notebook 2: Unified Pipeline
**Thời lượng**: 60-90 phút
**Loại**: Hands-on coding

**Sections**:
1. Setup: Spark + Iceberg configuration
2. Shared transformation function
3. Streaming pipeline → Iceberg
4. Batch pipeline → Same Iceberg (same code)
5. Verify: Query results từ cả 2 pipelines
6. Code comparison: Show code reuse

**Key Code Pattern**:
```python
# Shared transformation (dùng cho cả batch và streaming)
def transform_data(df):
    return df.select(...).filter(...).groupBy(...)

# Streaming
stream_df = spark.readStream.format("kafka")...
stream_df.transform(transform_data).writeStream.format("iceberg")...

# Batch (same code!)
batch_df = spark.read.parquet(...)
batch_df.transform(transform_data).write.format("iceberg")...
```

### Notebook 3: Medallion với Streaming
**Thời lượng**: 90-120 phút
**Loại**: Complete pipeline

**Sections**:
1. Bronze: Kafka → Spark Streaming → Iceberg Bronze
2. Silver: Streaming transformation Bronze → Silver
3. Gold: Streaming aggregation Silver → Gold
4. Batch backfill: Historical data → Silver (same transformation)
5. Query: Real-time + historical data từ Gold

**Architecture**:
```
Kafka → Spark Streaming → Iceberg Bronze
                              ↓
                    (Streaming transform)
                              ↓
                        Iceberg Silver
                              ↓
                    (Streaming aggregate)
                              ↓
                         Iceberg Gold
                              ↓
                    (Query: Real-time + Batch)
```

### Notebook 4: Code Reuse Patterns
**Thời lượng**: 60-90 phút
**Loại**: Best practices

**Sections**:
1. Function design cho reuse
2. Configuration-driven processing
3. Testing: Unit test shared functions
4. Performance: Compare batch vs streaming
5. Best practices

**Patterns**:
- Shared transformation functions
- Configuration objects
- Factory patterns
- Testing strategies

### Notebook 5: Advanced Features
**Thời lượng**: 60-90 phút
**Loại**: Advanced topics

**Sections**:
1. Time travel với streaming data
2. Schema evolution trong streaming
3. Snapshot management
4. Query optimization
5. Monitoring và observability

### Notebook 6: Real-world Use Case (Optional)
**Thời lượng**: 90-120 phút
**Loại**: End-to-end project

**Use Case**: E-commerce real-time analytics
- Real-time: User events → Streaming → Gold tables
- Batch: Historical orders → Batch → Same Gold tables
- Analytics: Query Gold cho real-time dashboards + batch reports

## 🎓 Learning Path

### Prerequisites
- ✅ Spark Lab (batch + streaming)
- ✅ Kafka Lab
- ✅ PyIceberg Lab (hoặc Spark + Iceberg)
- ✅ Hiểu Medallion Architecture

### Progression
1. **Conceptual** (Notebook 1): Hiểu tại sao cần Streaming Lakehouse
2. **Basic** (Notebook 2): Unified pipeline cơ bản
3. **Intermediate** (Notebook 3): Medallion với streaming
4. **Advanced** (Notebook 4-5): Patterns và best practices
5. **Real-world** (Notebook 6): Complete use case

## 💡 Key Differentiators

### So với Data Lakehouse Lab hiện tại:
- **Focus**: Unified pipeline vs Integration
- **Comparison**: Lambda/Kappa vs Streaming Lakehouse
- **Code reuse**: Batch + streaming cùng code
- **Philosophy**: "End of Lambda" concept

### So với Spark Streaming Lab:
- **Storage**: Iceberg tables vs memory/console
- **Architecture**: Unified vs separate batch/stream
- **Use case**: Production patterns vs concepts

## 📊 Expected Outcomes

Sau khi hoàn thành lab, học viên sẽ:
1. ✅ Hiểu tại sao Streaming Lakehouse là xu hướng
2. ✅ Có thể viết unified pipeline (batch + streaming)
3. ✅ Implement Medallion Architecture với streaming
4. ✅ So sánh và chọn architecture phù hợp
5. ✅ Apply best practices cho production

## 🤔 Questions to Consider

### 1. Lab độc lập hay phần của Data Lakehouse Lab?
**Option A**: Lab riêng "Streaming Lakehouse Lab"
- ✅ Focus rõ ràng
- ✅ Có thể học độc lập
- ✅ Dễ maintain

**Option B**: Thêm vào Data Lakehouse Lab
- ✅ Tích hợp sẵn
- ⚠️ Có thể làm lab quá dài

**Khuyến nghị**: **Lab riêng** - vì concept đủ lớn và quan trọng

### 2. Có cần demo Lambda/Kappa không?
**Option A**: Chỉ giải thích, không implement
- ✅ Tiết kiệm thời gian
- ✅ Focus vào Streaming Lakehouse

**Option B**: Implement đơn giản Lambda/Kappa để so sánh
- ✅ Học viên thấy rõ sự khác biệt
- ⚠️ Tốn thời gian hơn

**Khuyến nghị**: **Option A** - Giải thích + diagrams, không cần implement đầy đủ

### 3. Công nghệ: Iceberg hay Delta Lake?
**Current**: Đang dùng Iceberg trong labs
**Khuyến nghị**: **Tiếp tục Iceberg** vì:
- ✅ Đã có trong lab hiện tại
- ✅ Hỗ trợ streaming tốt
- ✅ Open source, không vendor lock-in

Có thể mention Delta Lake như alternative

### 4. Có cần Airflow integration không?
**Option A**: Có Airflow để orchestrate
- ✅ Production-ready
- ✅ Tích hợp với lab hiện tại

**Option B**: Không có Airflow, focus vào pipeline
- ✅ Đơn giản hơn
- ✅ Focus vào core concept

**Khuyến nghị**: **Optional** - Có thể mention nhưng không bắt buộc

## 📝 Sample Exercise Ideas

### Exercise 1: Code Reuse Demonstration
```python
# Task: Viết function xử lý sales data
# - Tính total revenue
# - Group by category
# - Filter high-value transactions

# Apply cho:
# 1. Streaming data từ Kafka
# 2. Batch data từ files
# 3. So sánh kết quả
```

### Exercise 2: Medallion với Streaming
```python
# Task: Build Bronze → Silver → Gold
# Bronze: Raw events từ Kafka
# Silver: Cleaned, deduplicated
# Gold: Aggregated by hour, category

# Requirements:
# - Streaming: Real-time processing
# - Batch: Backfill historical (same code)
# - Query: Real-time + historical từ Gold
```

### Exercise 3: Performance Comparison
```python
# Task: So sánh 3 approaches
# 1. Lambda: Batch + Streaming separate
# 2. Kappa: Streaming only
# 3. Streaming Lakehouse: Unified

# Metrics:
# - Code complexity (lines of code)
# - Processing time
# - Storage efficiency
# - Maintenance effort
```

## 🎯 Success Criteria

Lab được coi là thành công nếu:
1. ✅ Học viên hiểu tại sao Streaming Lakehouse > Lambda/Kappa
2. ✅ Có thể viết unified pipeline
3. ✅ Implement được Medallion với streaming
4. ✅ So sánh được các architectures
5. ✅ Apply được vào use case thực tế

## 📚 References

- "The End of Lambda Architecture" - Majid Azimi
- "Streaming Lakehouse" concepts
- Apache Iceberg documentation
- Spark Structured Streaming guide
- Kai Waehner: "Kappa Architecture in 2025"

## ✅ Next Steps

1. **Review phân tích này** - Xác nhận direction
2. **Quyết định scope** - 5-6 notebooks hay ít hơn?
3. **Design chi tiết** - Từng notebook cụ thể
4. **Implementation** - Code và exercises
5. **Testing** - Verify với real data

---

**Status**: Phân tích hoàn tất, chờ quyết định để bắt đầu implementation

