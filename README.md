# Stock Scanner Cloud

A cloud-based stock scanner application built with Python and deployed to Google Cloud Platform.

## Overview

This project provides a stock trading scanner that monitors market conditions and identifies potential trading opportunities. It's containerized with Docker and deployed using Google Cloud Build.

## Features

- Real-time stock market scanning
- Trade analysis and identification
- Cloud-native deployment with Docker
- Automated CI/CD pipeline with Google Cloud Build
- REST API interface for accessing scan results

## Project Structure

```
├── app.py                 # Flask/FastAPI web application
├── trade_scan.py         # Core stock scanning logic
├── requirements.txt      # Python dependencies
├── Dockerfile            # Docker container configuration
├── cloudbuild.yaml       # Google Cloud Build configuration
├── .dockerignore         # Files to exclude from Docker build
├── .gcloudignore        # Files to exclude from Cloud deployment
└── README.md            # This file
```

## Requirements

- Python 3.x
- Dependencies listed in `requirements.txt`

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/heartsblanks/stock-scanner-cloud.git
   cd stock-scanner-cloud
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Running Locally

```bash
python app.py
```

## Docker Deployment

Build the Docker image:
```bash
docker build -t stock-scanner-cloud .
```

Run the container:
```bash
docker run -p 8080:8080 stock-scanner-cloud
```

## Cloud Deployment

This project is configured for deployment to Google Cloud Platform using Cloud Build. The `cloudbuild.yaml` file defines the build and deployment pipeline.

## Usage

The application provides a REST API for stock scanning operations. Refer to the `app.py` file for available endpoints.

## Testing

⚠️ **Note**: This project is for testing purposes only and should not be used for real trading decisions.

## License

[Add license information here]

## Contributing

[Add contribution guidelines here]