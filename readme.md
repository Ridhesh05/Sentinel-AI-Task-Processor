# Sentinel AI Task Processor

This project is a backend system that processes AI tasks asynchronously.
Users submit AI tasks (like summarization or classification),
the system queues them safely, processes them in background,
and allows users to fetch results later.

The system is designed to:
- keep APIs fast
- handle traffic spikes
- avoid data loss
- support background AI processing
