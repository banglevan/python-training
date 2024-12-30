# API Documentation

## Overview
The Analytics Platform API provides endpoints for accessing metrics, generating reports, and managing data ingestion.

## Authentication
All API endpoints require authentication using JWT tokens. Include the token in the Authorization header:
```
Authorization: Bearer <your_token>
```

## Endpoints

### Sales Metrics
```http
GET /api/metrics/sales
```

Query Parameters:
- `start_date` (optional): Start date (YYYY-MM-DD)
- `end_date` (optional): End date (YYYY-MM-DD)
- `group_by` (optional): Grouping dimension

Response:
```json
{
  "metrics": [
    {
      "date": "2024-01-01",
      "total_revenue": 1000.00,
      "transaction_count": 50,
      "avg_transaction_value": 20.00
    }
  ],
  "trends": {
    "total_revenue": {
      "direction": "up",
      "strength": 0.75
    }
  }
}
```

### Customer Metrics
```http
GET /api/metrics/customers
```

Query Parameters:
- `start_date` (optional): Start date (YYYY-MM-DD)
- `end_date` (optional): End date (YYYY-MM-DD)

Response:
```json
{
  "metrics": [
    {
      "date": "2024-01-01",
      "active_customers": 1000,
      "new_customers": 50,
      "retention_rate": 0.85
    }
  ],
  "trends": {
    "retention_rate": {
      "direction": "stable",
      "strength": 0.1
    }
  }
}
```

### Product Metrics
```http
GET /api/metrics/products
```

Query Parameters:
- `start_date` (optional): Start date (YYYY-MM-DD)
- `end_date` (optional): End date (YYYY-MM-DD)
- `category` (optional): Product category

Response:
```json
{
  "metrics": [
    {
      "product_id": "P001",
      "units_sold": 100,
      "revenue": 1000.00,
      "avg_price": 10.00
    }
  ],
  "top_products": {
    "P001": 1000.00,
    "P002": 800.00
  }
}
```

### Generate Report
```http
POST /api/reports/generate
```

Request Body:
```json
{
  "type": "sales",
  "start_date": "2024-01-01",
  "end_date": "2024-01-31",
  "format": "pdf"
}
```

Response:
```json
{
  "report_id": "R123",
  "status": "completed",
  "download_url": "/reports/R123.pdf"
}
```

## Error Responses
```json
{
  "error": "Invalid date format",
  "detail": "start_date must be in YYYY-MM-DD format",
  "status_code": 400
}
```

## Rate Limiting
- 100 requests per minute per API key
- 1000 requests per hour per API key 