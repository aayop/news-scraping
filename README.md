# News Intelligence Platform

A comprehensive news aggregation and analytics platform that scrapes, processes, and visualizes news from multiple Moroccan and international sources.

## 🏗️ Architecture

The platform follows a modern data lake architecture with three layers:

- **Bronze Layer**: Raw scraped data (JSON files)
- **Silver Layer**: Cleaned and normalized data
- **Gold Layer**: Analytics and aggregated insights

## 📊 Features

- **Multi-source scraping**: 6 news sources (Hespress, BBC, Al Jazeera, Reuters, Barlamane, Akhbarona)
- **Data quality checks**: Comprehensive validation across all layers
- **Real-time dashboard**: Interactive visualization with charts and analytics
- **Automated pipeline**: Orchestrated ETL pipeline with error handling
- **Docker containerization**: Production-ready deployment
- **CI/CD**: GitHub Actions for automated testing and deployment

## 🚀 Quick Start

### Local Development

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd news_scraping_project
   ```

2. **Install dependencies**
   ```bash
   cd scrapers
   pip install -r requirements.txt
   cd ..
   ```

3. **Run the complete pipeline**
   ```bash
   python pipeline.py
   ```

4. **View the dashboard**
   ```bash
   python -m http.server 8000
   # Open http://localhost:8000/dashboard.html
   ```

### Docker Deployment

1. **Build and run with Docker Compose**
   ```bash
   docker-compose up --build
   ```

2. **Run with dashboard server**
   ```bash
   docker-compose --profile dashboard up --build
   # Dashboard available at http://localhost:8080
   ```

3. **Run scheduled pipeline**
   ```bash
   docker-compose --profile scheduler up
   ```

### Deployment helper script

A shell helper script is included for teammates to validate and start the Docker deployment from the project root.

```bash
bash docker_setup.sh
```

The script verifies required files, checks Docker and Compose, builds the image, and starts the containers.

## 📁 Project Structure

```
news_scraping_project/
├── scrapers/                    # Web scraping modules
│   ├── main_scraper.py         # Orchestrates all scrapers
│   ├── *_scraper.py            # Individual source scrapers
│   └── requirements.txt        # Python dependencies
├── data_lake/                  # Data lake storage
│   ├── bronze/                 # Raw scraped data
│   ├── silver/                 # Cleaned data
│   └── gold/                   # Analytics tables
├── dashboard.html              # Interactive dashboard
├── silver_processor.py         # Data cleaning pipeline
├── gold_processor.py           # Analytics generation
├── quality_checker.py          # Data quality validation
├── pipeline.py                 # Main orchestration script
├── Dockerfile                  # Container definition
├── docker-compose.yml          # Multi-container setup
├── nginx.conf                  # Web server config
└── .github/workflows/          # CI/CD pipelines
```

## 🔧 Usage

### Pipeline Commands

```bash
# Run complete pipeline
python pipeline.py

# Skip scraping (use existing data)
python pipeline.py --skip-scraping

# Force continue on failures
python pipeline.py --force

# Run individual components
python scrapers/main_scraper.py    # Scraping only
python silver_processor.py         # Cleaning only
python gold_processor.py           # Analytics only
python quality_checker.py          # Quality checks only
```

### Docker Commands

```bash
# Build image
docker build -t news-intelligence .

# Run container
docker run -v $(pwd)/data_lake:/app/data_lake news-intelligence

# Run with Docker Compose
docker-compose up news-intelligence
docker-compose up dashboard-server  # With web server
```

## 📈 Dashboard

The interactive dashboard provides:

- **Real-time statistics**: Article counts, sources, languages
- **Source distribution**: Pie/donut charts by news source
- **Language analysis**: Content language breakdown
- **Temporal trends**: Articles over time
- **Keyword analysis**: Trending topics and frequency
- **Recent articles**: Latest news feed

Access the dashboard at `http://localhost:8000/dashboard.html` when running locally.

## 🔍 Data Quality

The platform includes comprehensive quality checks:

- **Completeness**: Required fields validation
- **Consistency**: Cross-layer data integrity
- **Accuracy**: Content and metadata validation
- **Timeliness**: Data freshness monitoring
- **Uniqueness**: Duplicate detection

Run quality checks: `python quality_checker.py`

## 🔒 Security & Best Practices

- **Container security**: Non-root user, minimal base image
- **Dependency scanning**: Automated vulnerability checks
- **Code quality**: Linting and testing in CI/CD
- **Data persistence**: Volume mounting for data retention
- **Error handling**: Comprehensive logging and recovery

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Run the pipeline: `python pipeline.py`
6. Submit a pull request

## 📝 Configuration

### Environment Variables

```bash
# Scraping configuration
SCRAPER_TIMEOUT=30
MAX_ARTICLES_PER_SOURCE=50

# Pipeline options
PYTHONPATH=/app
PYTHONUNBUFFERED=1
```

### GitHub Secrets (for CI/CD)

```bash
DOCKER_USERNAME=your-dockerhub-username
DOCKER_PASSWORD=your-dockerhub-password
SERVER_HOST=your-server-ip
SERVER_USER=your-server-username
SERVER_SSH_KEY=your-ssh-private-key
```

## 📊 Monitoring & Logs

- **Pipeline reports**: `reports/pipeline_report_*.json`
- **Quality reports**: `reports/quality_report_*.json`
- **Application logs**: `logs/` directory
- **Container logs**: `docker-compose logs`

## 🚨 Troubleshooting

### Common Issues

1. **Scraping failures**: Check network connectivity and source URLs
2. **Quality check failures**: Review data in `data_lake/` directories
3. **Dashboard not loading**: Ensure data exists in Gold layer
4. **Docker build failures**: Check Docker and system resources

### Debug Mode

```bash
# Run with verbose logging
PYTHONUNBUFFERED=1 python pipeline.py

# Test individual components
python -c "import silver_processor; print('OK')"
```

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Built for EMSI Casablanca — IADATA 2025/2026
- Big Data News Analytics Platform
- Multi-language support (Arabic, French, English)