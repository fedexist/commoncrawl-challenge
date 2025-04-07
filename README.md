## TODO for production readiness

1. Proper secret management via environment variables injection: for simplicity purpose I am hardcoding all passwords
2. Regularly update data/hosts for ad based domain detection
3. Have a sensor triggering the airflow pipeline whenever a new partition is detected
