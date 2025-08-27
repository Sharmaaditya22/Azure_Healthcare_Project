# Azure_Healthcare_Project

Domain – Healthcare Revenue Cycle Management (RCM)

Revenue Cycle Management (RCM) is the process hospitals and healthcare providers use to manage financial aspects from the time a patient schedules an appointment until the provider receives payment.

This project implements an Azure-based Data Lakehouse architecture for RCM data integration, transformation, and analytics. It unifies hospital EMR data, payer files, and public healthcare APIs into a single, trusted data warehouse.

🏗️ Architecture

🔑 Key Components

Data Sources

Hospital EMRs (Azure SQL – EMR A & EMR B)

Payer Files (CSV-based uploads)

Public APIs (ICD codes, reference datasets)

Landing Layer

Raw ingestion of files, APIs, and database extracts

Data stored in its original format

Bronze Layer

Standardized storage in Parquet format

Retains full history for compliance and traceability

Silver Layer

Built using Databricks (PySpark)

Data cleaning, transformations, and integration across sources

Conformed dimensions (Patients, Providers, Claims, Payments, ICD mappings)

Gold Layer

Modeled into Delta Lake star schema

Fact tables for Claims, Payments, Denials, Billing

Dimension tables for Patients, Providers, Diagnosis Codes, Insurance

🚀 Features

✅ End-to-End ADF Pipeline (via ARM Template)

✅ Incremental & batch ingestion from SQL, CSV, APIs

✅ PySpark transformations for Silver & Gold layers

✅ Delta Lake for ACID transactions and time travel

✅ Healthcare RCM-ready data model (Claims → Payments → Denials)