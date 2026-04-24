# Telecom Energy Efficiency – Data Platform (Databricks, PySpark)

## 📌 Overview

This project demonstrates the design and implementation of a scalable data platform to support energy efficiency analysis in telecom networks.

The platform integrates multiple data sources such as:
- Network KPIs (2G / 4G / 5G)
- Network configuration data (e.g., cell-level metadata)
- Energy consumption data

The goal is to enable reliable, consistent, and analysis-ready datasets for downstream data science and optimization models.

---

## 🏗️ Architecture

The solution follows a **Medallion Architecture**:

- **Bronze** → Raw ingestion with metadata (source file, ingestion timestamp)
- **Silver** → Cleaned and standardized data
- **Gold** → Feature-engineered datasets ready for analytics and ML

---

## ⚙️ Key Components

### 🔹 Data Ingestion
- Multi-source ingestion pipelines
- Support for versioned datasets (v1–v4)
- Metadata tracking (source_batch, ingestion_ts)

---

### 🔹 Version-Aware Merge Logic
- Handles overlapping datasets across versions
- Prioritizes latest versions
- Backfills missing values from previous versions

---

### 🔹 Data Quality Framework
- Null value tracking
- Duplicate detection
- Coverage validation
- Monitoring-ready outputs

---

### 🔹 Feature Engineering
- Time-based features (hour, day of week)
- Behavioral flags (weekend, sleep periods)
- Domain-specific KPIs

---

## ⚠️ Challenges & Solutions

### 1. Inconsistent Schemas Across Versions
Different KPI versions contained:
- Missing columns
- New columns
- Different column ordering

**Solution:**
- Dynamic schema alignment
- Robust ingestion logic
- Version-aware merge strategy

---

### 2. Incorrect Data Ingestion (Column Misalignment)
Some files had inconsistent column ordering, causing incorrect mappings.

**Solution:**
- Identified issue at Bronze layer
- Redesigned ingestion logic to enforce schema consistency

---

### 3. Missing Data & KPI Availability
Certain KPIs were not available in earlier versions.

**Solution:**
- Implemented controlled null handling
- Differentiated between:
  - missing data
  - true zero values
  - derived ratios

---

## 📊 Impact

- Enabled consistent datasets for data science modeling
- Improved data reliability across multiple data deliveries
- Provided reusable data platform framework for future projects

---

## 🧩 Tech Stack

- PySpark
- Databricks
- Delta Lake
- Azure Data Lake (conceptually)

---

## 📁 Repository Structure

- `src/` → reusable logic (ingestion, processing, quality, features)
- `notebooks/` → simplified demonstrations of core concepts

---

## 📝 Notes

This repository is a simplified and anonymized version of a real-world PoC.  
No sensitive or client-specific data is included.
