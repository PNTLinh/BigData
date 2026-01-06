# Streaming Lakehouse Lab - Unified Batch & Stream Processing

## 📋 Overview

Lab này dạy về **Streaming Lakehouse Architecture** - kiến trúc hiện đại thay thế Lambda/Kappa, cho phép batch và streaming xử lý **chung trên cùng storage và code**.

**Domain**: **Stock Trading Data** (real-time stock quotes, trades, market data)

## 🎯 Learning Objectives

Sau khi hoàn thành lab này, bạn sẽ có thể:

- ✅ Hiểu tại sao Streaming Lakehouse thay thế Lambda/Kappa
- ✅ Xây dựng unified pipeline (batch + streaming cùng code)
- ✅ Implement Medallion Architecture với streaming (Bronze → Silver → Gold)
- ✅ Sử dụng Iceberg như unified storage cho cả batch và streaming
- ✅ Query real-time và historical data từ cùng tables
- ✅ Sử dụng time travel và schema evolution trong streaming context

## 🏗️ Architecture Overview

```
┌─────────────┐
│   Kafka     │  Stock trades/quotes stream
│  (Source)   │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────┐
│   Spark Structured Streaming        │
│   (Unified Processing Logic)         │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│   Apache Iceberg Tables              │
│   ┌─────────┐  ┌─────────┐  ┌──────┐ │
│   │ Bronze  │→ │ Silver  │→ │ Gold │ │
│   │ (Raw)   │  │(Cleaned)│  │(Agg) │ │
│   └─────────┘  └─────────┘  └──────┘ │
└──────┬──────────────────────────────┘
       │
       ├─────────────────┬──────────────┐
       ▼                 ▼              ▼
┌──────────┐    ┌──────────────┐  ┌─────────┐
│ Real-time│    │ Batch Reports│  │   ML    │
│ Analytics│    │ (Same Data) │  │ Models  │
└──────────┘    └──────────────┘  └─────────┘

Parallel: Batch Processing (Same Code)
┌─────────────┐
│ Batch Files │  Historical trades
│  (Source)   │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────┐
│   Spark Batch Processing             │
│   (Same Transformation Logic)         │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│   Same Iceberg Tables                │
│   (Bronze → Silver → Gold)            │
└─────────────────────────────────────┘
```

## 📚 Lab Structure

### Lab 1: Streaming Lakehouse Fundamentals
**File**: `notebooks/01_streaming_lakehouse_fundamentals.ipynb`

**Topics Covered**:
- Streaming Lakehouse là gì?
- So sánh Lambda vs Kappa vs Streaming Lakehouse
- Tại sao "End of Lambda Architecture"?
- Unified pipeline concept
- Domain: Stock Trading Data

**Exercises**:
1. So sánh architecture diagrams
2. Phân tích ưu/nhược điểm
3. Use case mapping

### Lab 2: Bronze Layer - Raw Streaming Data
**File**: `notebooks/02_bronze_layer_streaming.ipynb`

**Topics Covered**:
- Ingest từ Kafka vào Iceberg Bronze
- Stock trade data schema
- Streaming write to Iceberg
- Checkpointing và fault tolerance

**Exercises**:
1. Setup Kafka producer (stock trades simulator)
2. Spark Streaming từ Kafka
3. Write to Iceberg Bronze table
4. Verify raw data

### Lab 3: Silver Layer - Cleaned & Deduplicated
**File**: `notebooks/03_silver_layer_cleaning.ipynb`

**Topics Covered**:
- Read Bronze như stream
- Data cleaning và normalization
- Deduplication với watermarking
- Upsert operations với Iceberg
- Schema validation

**Exercises**:
1. Stream từ Bronze → Silver
2. Clean và normalize stock data
3. Deduplicate với watermark
4. Upsert vào Silver table

### Lab 4: Gold Layer - Aggregations & Features
**File**: `notebooks/04_gold_layer_aggregations.ipynb`

**Topics Covered**:
- Windowed aggregations (1-minute, 5-minute)
- Real-time metrics (avg price, volume, volatility)
- Feature engineering
- Write to Gold tables

**Exercises**:
1. Aggregate Silver → Gold (by minute)
2. Calculate real-time metrics
3. Create feature tables
4. Query Gold tables

### Lab 5: Unified Batch & Streaming
**File**: `notebooks/05_unified_batch_streaming.ipynb`

**Topics Covered**:
- Shared transformation functions
- Batch processing với same code
- Same Iceberg tables cho batch và streaming
- Code reuse patterns

**Exercises**:
1. Viết shared transformation function
2. Apply cho streaming (real-time)
3. Apply cho batch (historical backfill)
4. Compare results
5. Performance comparison

### Lab 6: Query, Time Travel & Advanced Features
**File**: `notebooks/06_query_time_travel.ipynb`

**Topics Covered**:
- Query Gold tables (real-time + historical)
- Time travel queries
- Schema evolution với streaming
- Snapshot management
- Integration với BI tools

**Exercises**:
1. Query real-time data từ Gold
2. Query historical data (same table)
3. Time travel: Query tại thời điểm cụ thể
4. Schema evolution demo
5. Compare snapshots

## 🏗️ Domain: Stock Trading Data

### Data Schema

**Stock Trade Events**:
```json
{
  "trade_id": "TRD_001",
  "symbol": "AAPL",
  "price": 175.50,
  "volume": 100,
  "timestamp": "2025-01-15T10:30:00Z",
  "trade_type": "BUY",
  "exchange": "NASDAQ"
}
```

**Stock Quote Events**:
```json
{
  "quote_id": "QTE_001",
  "symbol": "AAPL",
  "bid": 175.45,
  "ask": 175.55,
  "timestamp": "2025-01-15T10:30:00Z",
  "exchange": "NASDAQ"
}
```

### Use Cases
- **Real-time**: Live trading dashboard, alerts
- **Historical**: Backtesting, analysis, reports
- **ML**: Price prediction, volatility models
- **Analytics**: Volume analysis, market trends

## 🚀 Quick Start

### Prerequisites
- Docker và Docker Compose
- Python 3.10+
- Hoàn thành: Spark Lab, Kafka Lab, PyIceberg Lab

### Setup
```bash
cd Streaming_Lakehouse_lab
docker-compose up -d
# Wait for services to be ready
python scripts/setup_lab.py
```

### Access Points
- **Jupyter Lab**: http://localhost:8888
- **Spark Master UI**: http://localhost:8080
- **Kafka**: localhost:9092
- **MinIO S3** (for Iceberg): http://localhost:9000

## 📊 Sample Data

### Stock Trading Simulator
```python
# scripts/stock_trade_simulator.py
# Generates realistic stock trade events
# Symbols: AAPL, GOOGL, MSFT, AMZN, TSLA
# Frequency: 1-10 events/second
```

## 🔧 Technology Stack

- **Apache Spark 3.5+**: Structured Streaming + Batch
- **Apache Iceberg**: Table format (unified storage)
- **Apache Kafka**: Event streaming source
- **MinIO**: S3-compatible storage for Iceberg
- **Python/PySpark**: Development

## 📚 Key Concepts

### Unified Pipeline
- Batch và streaming dùng **chung code**
- Cùng Iceberg tables
- Cùng query engine

### Medallion Architecture
- **Bronze**: Raw streaming data
- **Silver**: Cleaned, deduplicated
- **Gold**: Aggregated, features

### Advantages over Lambda/Kappa
- ✅ Simpler: 1 codebase vs 2 (Lambda)
- ✅ Efficient: Historical data in Iceberg vs reprocess (Kappa)
- ✅ Unified: Same tables for real-time and batch

## 🎓 Learning Path

1. **Lab 1**: Understand concepts
2. **Lab 2-4**: Build Medallion layers với streaming
3. **Lab 5**: Unified batch + streaming
4. **Lab 6**: Advanced features và querying

## 📖 References

- "The End of Lambda Architecture" - Majid Azimi
- Apache Iceberg documentation
- Spark Structured Streaming guide
- Streaming Lakehouse concepts

## ✅ Expected Outcomes

Sau khi hoàn thành:
- ✅ Hiểu Streaming Lakehouse architecture
- ✅ Có thể build unified pipeline
- ✅ Implement Medallion với streaming
- ✅ Query real-time + historical từ cùng tables
- ✅ Apply vào use case thực tế

