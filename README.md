# Crossref Event Data Agents

The Agents that Crossref operates for Event Data. An Agent talks to a data source (usually an API) and builds Evidence Records, which are sent to the Percolator. 

This codebase includes all Agents that Crossref operates. As such, all Agents are versioned in step. Prior to this repository all Agents had different codebases and versioning.

## Running

Most agents run on a schedule:

    lein run start-schedule «agent-name»*

Some require daemons (e.g. Wikipedia and Twitter)

    lein run run-daemons «agent-name»*

Run a one-off cycle of the scheduled job:

    lein run run-schedule-once «agent-name»*

Current options:

Schedule

 - hypothesis
 - newsfeed
 - reddit
 - reddit-links
 - stackexchange
 - twitter (update rules on schedule)

Daemon

 - twitter (ingest stream)
 - wikipedia

## Configuration

The following environment variables must be set:

 - `AGENT_CHECKPOINT_S3_BUCKET_NAME`
 - `AGENT_CHECKPOINT_S3_KEY`
 - `AGENT_CHECKPOINT_S3_REGION_NAME`
 - `AGENT_CHECKPOINT_S3_SECRET`
 - `GLOBAL_ARTIFACT_URL_BASE`
 - `GLOBAL_ARTIFACT_URL_BASE`, e.g. https://artifact.eventdata.crossref.org
 - `GLOBAL_JWT_SECRETS`
 - `GLOBAL_KAFKA_BOOTSTRAP_SERVERS`
 - `GLOBAL_STATUS_TOPIC`
 - `PERCOLATOR_INPUT_EVIDENCE_RECORD_TOPIC`
 - `PERCOLATOR_ROBOTS_CACHE_REDIS_HOST`
 - `PERCOLATOR_ROBOTS_CACHE_REDIS_PORT`
 - `TWITTER_GNIP_PASSWORD`
 - `TWITTER_GNIP_RULES_URL`
 - `TWITTER_GNIP_USERNAME`
 - `TWITTER_POWERTRACK_ENDPOINT`