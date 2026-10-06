Objectives

Provision and configure Cosmos DB in Microsoft Fabric as an operational data store

Implement real-time streaming using Eventhouse and KQL for POS transaction data

Build cross-database analytics leveraging Cosmos DB's automatic mirroring to OneLake

Create data warehouses and perform ETL operations from streaming to structured data

Implement Reverse ETL patterns to update operational systems with analytical insights

Deploy personalized recommendation models using customer behavior data



***********************************************

Code for building silver layer for analytics 

.create-or-alter function with (folder="Silver") vw_Pos_Sales() {
transactions_live
// best available event timestamp
| extend EventTs = coalesce(
    todatetime(column_ifexists("timestamp", datetime(null))),
    todatetime(column_ifexists("EventEnqueuedUtcTime", datetime(null))),
    todatetime(column_ifexists("EventProcessedUtcTime", datetime(null))),
    ingestion_time()
)
// purchases only
| where tostring(column_ifexists("transactionType","")) == "purchase"
// project needed fields and make sure Items is an array (empty if missing)
| project
    TransactionId = tostring(column_ifexists("transactionId","")),
    EventTs,
    CustomerId = tostring(column_ifexists("customerId","")),
    ShopId     = tostring(column_ifexists("shopId","")),
    PaymentMethod = tostring(column_ifexists("paymentMethod","")),
    Items = iif(isnull(column_ifexists("items", dynamic(null))), dynamic([]), column_ifexists("items", dynamic(null))),
    // optional top-level totals (may be null)
    TotalQuantity_t = toint(column_ifexists("totalQuantity", int(null))),
    TotalAmount_t   = todouble(column_ifexists("totalAmount",  real(null))),
    LoyaltyPointsEarned   = toint(coalesce(column_ifexists("loyaltyPointsEarned", int(null)), 0)),
    LoyaltyPointsRedeemed = toint(coalesce(column_ifexists("loyaltyPointsRedeemed", int(null)), 0))
// explode items (works even if empty array)
| mv-expand Items to typeof(dynamic)
| extend
    qty  = toint(coalesce(Items.quantity, 0)),
    unit = todouble(coalesce(Items.unitPrice, 0.0))
| extend
    lt   = todouble(coalesce(Items.totalPrice, qty * unit))
// roll up per transaction
| summarize
    TotalQuantity_i = sum(qty),
    TotalAmount_i   = sum(lt),
    EventTimestamp  = any(EventTs),
    CustomerId      = any(CustomerId),
    ShopId          = any(ShopId),
    PaymentMethod   = any(PaymentMethod),
    LoyaltyPointsEarned   = any(LoyaltyPointsEarned),
    LoyaltyPointsRedeemed = any(LoyaltyPointsRedeemed),
    TotalQuantity_t = any(TotalQuantity_t),
    TotalAmount_t   = any(TotalAmount_t)
by TransactionId
// prefer top-level totals when present; else use item-derived
| extend
    TotalQuantity = iif(isnotnull(TotalQuantity_t), TotalQuantity_t, TotalQuantity_i),
    TotalAmount   = iif(isnotnull(TotalAmount_t),   TotalAmount_t,   TotalAmount_i),
    DateKey   = toint(format_datetime(EventTimestamp, "yyyyMMdd")),
    TimeKey   = toint(format_datetime(EventTimestamp, "HHmmss")),
    CreatedAt = EventTimestamp
| project
    TransactionId, DateKey, TimeKey,
    CustomerId, ShopId,
    TotalQuantity, TotalAmount,
    PaymentMethod,
    LoyaltyPointsEarned, LoyaltyPointsRedeemed,
    CreatedAt
}

*************************************************

Transform and Load Eventhouse Data
This notebook transforms streaming POS transactions from Cosmos DB in Fabric into clean, conformed facts for the Fourth Coffee Gold Layer. It reads Silver layer data from KQL functions, joins with warehouse dimensions to assign stage keys, stages Parquet files with normalized types that are used to run COPY INTO commands in the data warehouse to rapidly load the FactSales and FactSalesLineItem tables.

from pyspark.sql.window import Window
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import DecimalType

dec2 = DecimalType(10,2)



dw_fact_sales          = "dbo.FactSales"
dw_fact_sales_line     = "dbo.FactSalesLineItems"
dw_v_dim_customer_key  = "dbo.vDimCustomerKey"
dw_v_dim_shop_key      = "dbo.vDimShopKey"
dw_v_dim_menu_key      = "dbo.vDimMenuItemKey"
dw_v_fact_max          = "dbo.vFactSalesMaxKey"

kustoCluster = "" # Find in fc_commerce_eventhouse > Copy Query URI in Eventhouse Details on right or in textfile
kustoDatabase = "fc_commerce_eventhouse"
pos_sales = "vw_Pos_Sales()"
pos_line_items = "vw_Pos_LineItems_Sales()"

workspace_guid = "" # Copy first guid in browser address (eg https://app.fabric.microsoft.com/groups/[your workspace guid]/)
lakehouse_guid= "" # Open lakehouse from your workspace and copy second guid in the browser address (eg https://app.fabric.microsoft.com/groups/[your workspace guid]/lakehouses/[lakehouse guid])

WAREHOUSE = "fc_commerce_wh" # go to warehouse > settings > copy database name
SERVER    = ""  # go to warehouse > settings > copy sql endpoint (e.g., "abcd1234-...-workspace.z01.datawarehouse.fabric.microsoft.com")

# 1) Get an Entra ID token from the Fabric runtime 
from notebookutils.mssparkutils import credentials
token = credentials.getToken("pbi")  # ~60–90 min lifetime
# 2) Build JDBC URL & properties
jdbc_url = (
    f"jdbc:sqlserver://{SERVER}:1433;"
    f"database={WAREHOUSE};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.datawarehouse.fabric.microsoft.com;"
    "loginTimeout=30"
)
props = {
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "accessToken": token
}

accessToken = mssparkutils.credentials.getToken(kustoCluster)
_____________________________________________________________________________
Create Convenience Helpers and Read streaming data from KQL (Silver layer)

# Convenience helpers
def read_kql_function(func: str):
    # Kusto (KQL) Spark connector
    return (spark.read\
    .format("com.microsoft.kusto.spark.synapse.datasource")\
    .option("accessToken", accessToken)\
    .option("kustoCluster", kustoCluster)\
    .option("kustoDatabase", kustoDatabase)\
    .option("kustoQuery", func).load())


hdr_raw  = read_kql_function(pos_sales)   # TransactionId, DateKey, TimeKey, CustomerId, ShopId, TotalQuantity, TotalAmount, PaymentMethod, LoyaltyPoints*, CreatedAt
line_raw = read_kql_function(pos_line_items)    # TransactionId, LineNumber, DateKey, TimeKey, (CustomerId, ShopId optional), MenuItemId/Key, Quantity, UnitPrice, LineTotal, PaymentMethod, Size, CreatedAt

print(hdr_raw.count(), "header rows (silver)")
print(line_raw.count(), "line rows (silver)")

def read_dw_table(table: str):
    return spark.read.jdbc(jdbc_url, table=table, properties=props)

print(read_dw_table(dw_fact_sales))
______________________________________________________________________________

Resolve business → surrogate keys (dimension lookups) and Assign new SalesKey values


# Optionally read existing TransactionIds to enforce idempotency
existing_txn_df = (read_dw_table(dw_fact_sales)
                   .select("TransactionId")
                   .dropDuplicates())

# Left-anti to keep only brand-new TransactionIds
hdr_new = (hdr_raw
           .join(existing_txn_df, on="TransactionId", how="left_anti"))

# Resolve CustomerKey & ShopKey
hdr_keys = (hdr_new
  .join(v_cust.select("CustomerId","CustomerKey"), on="CustomerId", how="left")
  .join(v_shop.select("ShopId","ShopKey"), on="ShopId", how="left"))

# Assign SalesKey = MaxSalesKey + row_number()
window_tx = Window.orderBy(F.col("TransactionId"))
hdr_final = (hdr_keys
  .withColumn("rn", F.row_number().over(window_tx))
  .withColumn("SalesKey", F.col("rn") + F.lit(int(max_sales_key)))
  .drop("rn"))

# Reorder/select to match FactSales schema
hdr_out = (hdr_final.select(
    "SalesKey",
    "TransactionId","DateKey","TimeKey",
    "CustomerKey","ShopKey",
    "TotalQuantity","TotalAmount",
    "PaymentMethod",
    "LoyaltyPointsEarned","LoyaltyPointsRedeemed",
    "CreatedAt"
))

tx_to_key = hdr_out.select("TransactionId", "SalesKey").dropDuplicates()

hdr_out.printSchema()
print(hdr_out.count(), "new header rows to insert")


___________________________________________________________________________


Normalize item data and handle mixed keys

# --- Normalize the line DataFrame (handle both MenuItemId/menuItemId and keep incoming MenuItemKey too) ---
line_src = (line_raw
    .withColumn("LineMenuItemId", F.coalesce(F.col("MenuItemId"), F.col("menuItemId")))
    .select(
        "TransactionId","LineNumber",
        "DateKey","TimeKey",
        "MenuItemKey",          # incoming key (may be null or wrong; we'll coalesce to DW)
        "LineMenuItemId",
        "Quantity","UnitPrice","LineTotal",
        "PaymentMethod","Size","CreatedAt"
    )
)

# --- Prepare the DW menu lookup with clear aliases and dedup on business key ---
v_menu_sel = (v_menu
    .withColumn("DimMenuItemId", F.coalesce(F.col("MenuItemId"), F.col("menuItemId")))
    .select("DimMenuItemId", "MenuItemKey")
    .dropDuplicates(["DimMenuItemId"])
)

# --- Join (DW as source of truth), then coalesce DW key -> incoming key ---
line_joined = (line_src.alias("l")
    .join(v_menu_sel.alias("m"),
          F.col("l.LineMenuItemId") == F.col("m.DimMenuItemId"),
          "left")
    .withColumn("MenuItemKeyFinal",
                F.coalesce(F.col("m.MenuItemKey"), F.col("l.MenuItemKey")))
)

# --- Keep only the line items for transactions that will be inserted in this run ---
tx_new_ids = hdr_out.select("TransactionId").dropDuplicates()
line_joined_new = line_joined.join(tx_new_ids, on="TransactionId", how="inner")

# Bring in SalesKey from header map with clear aliases
line_new = (line_joined_new.alias("l")
    .join(tx_to_key.alias("k"),
          F.col("l.TransactionId") == F.col("k.TransactionId"),
          "left")
)

# Final column order — qualify every column to avoid ambiguity,
# and use the DW-normalized key you created (MenuItemKeyFinal)
line_out = (line_new.select(
    F.col("l.TransactionId").alias("TransactionId"),
    F.col("k.SalesKey").alias("SalesKey"),
    F.col("l.LineNumber").alias("LineNumber"),
    F.col("l.DateKey").alias("DateKey"),
    F.col("l.TimeKey").alias("TimeKey"),
    F.col("l.MenuItemKeyFinal").alias("MenuItemKey"),
    F.col("l.Quantity").alias("Quantity"),
    F.col("l.UnitPrice").alias("UnitPrice"),
    F.col("l.LineTotal").alias("LineTotal"),
    F.col("l.PaymentMethod").alias("PaymentMethod"),
    F.col("l.Size").alias("Size"),
    F.col("l.CreatedAt").alias("CreatedAt")
))

line_out.printSchema()
print(line_out.count(), "new line rows to insert")


____________________________________________________________

 Stage Gold data to OneLake as Parquet

 hdr_stage_path  = f"Files/factsales/"
line_stage_path = f"Files/factsales_lineitems/"


hdr_stage = (hdr_out
  .withColumn("TotalAmount", F.round("TotalAmount", 2).cast(dec2))
  .withColumn("LoyaltyPointsEarned",  F.col("LoyaltyPointsEarned").cast("int"))
  .withColumn("LoyaltyPointsRedeemed",F.col("LoyaltyPointsRedeemed").cast("int"))
)

line_stage = (line_out
  .withColumn("UnitPrice", F.round("UnitPrice", 2).cast(dec2))
  .withColumn("LineTotal", F.round("LineTotal", 2).cast(dec2))
  .withColumn("Quantity",  F.col("Quantity").cast("int"))
)


# Coalesce to 1–2 files; Parquet is fastest for COPY
# WARNING: This will overwrite any data at these target locations!
(hdr_stage.coalesce(1)
    .write.mode("overwrite")
    .option("compression", "snappy")
    .parquet(hdr_stage_path))

(line_stage.coalesce(1)
    .write.mode("overwrite")
    .option("compression", "snappy")
    .parquet(line_stage_path))

print("Staged to OneLake:")
print("  ", hdr_stage_path)
print("  ", line_stage_path)

_____________________________________________________________

Copy SQL Script and lakehouse urls to load data into Data Warehouse

-- =============================
-- COPY headers (FactSales)
-- =============================
COPY INTO dbo.FactSales
(
    SalesKey             1,
    TransactionId        2,
    DateKey              3,
    TimeKey              4,
    CustomerKey          5,
    ShopKey              6,
    TotalQuantity        7,
    TotalAmount          8,
    PaymentMethod        9,
    LoyaltyPointsEarned 10,
    LoyaltyPointsRedeemed 11,
    CreatedAt           12
)
FROM '[lakehouse sales url]' 
WITH (
    FILE_TYPE = 'PARQUET'
);



-- =============================
-- COPY lines (FactSalesLineItems)
-- =============================
COPY INTO dbo.FactSalesLineItems
(
    TransactionId  1,
    SalesKey       2,
    LineNumber     3,
    DateKey        4,
    TimeKey        5,
    MenuItemKey    6,
    Quantity       7,
    UnitPrice      8,
    LineTotal      9,
    PaymentMethod 10,
    Size          11,
    CreatedAt     12
)
FROM '[lakehouse items urls]' 
WITH (
    FILE_TYPE = 'PARQUET'
);

_____________________________________



lakehouse_url_sales = f"https://onelake.dfs.fabric.microsoft.com/{workspace_guid}/{lakehouse_guid}/{hdr_stage_path}"
lakehouse_url_items = f"https://onelake.dfs.fabric.microsoft.com/{workspace_guid}/{lakehouse_guid}/{line_stage_path}"

print("Your OneLake URL for FactSales (copy for COPY INTO, etc):\n", lakehouse_url_sales)
print("Your OneLake URL for FactSalesLineItems (copy for COPY INTO, etc):\n", lakehouse_url_items)







