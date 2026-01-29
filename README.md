# Reltio SFDC Sync

AWS Lambda function that synchronizes data between Salesforce (SFDC) and Reltio360.

## Overview

This service extracts subscription and account data from Salesforce and pushes it to Reltio360 for unified data management.

## Features

- Pulls active subscription data from Salesforce
- Syncs account, contract, and tenant information
- Handles usage metrics (RSU, API, RIH, Consolidated Profiles)
- Email notifications for job status

## Setup

### Prerequisites

- Python 3.x
- AWS Lambda (for deployment)
- Salesforce API access
- Reltio360 API access

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/genafeldman/reltio-sfdc-sync.git
   cd reltio-sfdc-sync
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure environment variables:
   ```bash
   cp .env.template .env
   # Edit .env with your credentials
   ```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `r360_url` | Reltio360 API URL |
| `env` | Environment name |
| `pms_url` | Platform Management Service URL |
| `TO_ADDRESS` | Email notification recipients |
| `RELTIO_TOKEN` | Reltio API token |
| `GMAIL_SENDER` | Sender email address |
| `GMAIL_USER` | Gmail username |
| `GMAIL_PASS` | Gmail app password |
| `pms_auth` | PMS authentication |
| `PMS_USERNAME` | PMS username |
| `PMS_PASS` | PMS password |
| `sf_url` | Salesforce instance URL |
| `sf_client_id` | Salesforce client ID |
| `sf_client_secret` | Salesforce client secret |
| `sf_username` | Salesforce username |
| `sf_pass` | Salesforce password |
| `sequence` | Job sequence identifier |

## Local Testing

```bash
source .env
python test_local.py
```

## Files

- `lambda_function.py` - Main Lambda handler
- `account.json` - Account entity template
- `contract.json` - Contract entity template
- `tenant.json` - Tenant entity template
- `base_package_list.json` - Base package configuration
- `contract_detail_list.json` - Contract detail template
- `contract_account.json` - Contract-account mapping template
