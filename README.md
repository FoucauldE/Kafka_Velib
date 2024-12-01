# Bike Sharing Monitoring using Kafka

This repository provides a real-time monitoring system for Bike Sharing Systems using Kafka. It connects to the public JCDecaux API to gather bike station data, processes the data, and sends alerts for empty stations.

## Steps to Get Started

First clone the repository and insert [your private API key](https://developer.jcdecaux.com/#/signup) in the `config/private_config.py` file, following the template `config/template_private_config.py`.

After starting Zookeeper and Kafka, you might need to set the Python Path
```bash
   $env:PYTHONPATH="<your_project_path>"
```

Run the scripts in separate terminals to start the data streaming pipeline:
```bash
   python scripts/ingest-data.py
   python scripts/stations-activity.py
   python scripts/empty-stations.py
   python scripts/alert-empty-stations.py
```