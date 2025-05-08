CREATE OR REPLACE TABLE retail_transactions
(
    CustomerID UInt32,
    ProductID String,
    Quantity UInt32,
    Price UInt32,
    TransactionDate DateTime,
    PaymentMethod String,
    Street String,
    City Nullable(String),
    State Nullable(String),
    ZipCode Nullable(UInt32),
    ProductCategory String,
    DiscountAppliedPercent Float32,
    TotalAmount Float32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(TransactionDate)
ORDER BY (ProductID, TransactionDate);
