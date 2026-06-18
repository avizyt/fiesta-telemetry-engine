import random
from locust import HttpUser, task, between


class TelemetryLoadTest(HttpUser):
    # Simulate a user waiting between 0.1 and 0.5 seconds between requests
    wait_time = between(0.1, 0.5)

    @task
    def submit_telemetry_batch(self):
        """Simulates a microservice pushing a batch of logs to the ingestion gateway."""

        # Generate a random batch size between 1 and 20 logs per HTTP request
        batch_size = random.randint(1, 20)

        payload = {
            "logs": [
                {
                    "service_name": random.choice(
                        ["payment-api", "inventory-service", "auth-gateway"]
                    ),
                    "environment": "production",
                    "log_level": random.choice(["INFO", "WARN", "ERROR"]),
                    "message": f"Simulated load test event {random.randint(1000, 9999)}",
                    "duration_ms": random.uniform(5.0, 1500.0),
                }
                for _ in range(batch_size)
            ]
        }

        # Execute the POST request, asserting the dev tenant API key
        self.client.post(
            "/api/v1/telemetry/submit",
            json=payload,
            headers={"X-API-Key": "dev_secret_key"},
        )
