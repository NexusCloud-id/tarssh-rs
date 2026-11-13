# Operations

## Deploy

1. Install Docker Engine and Compose v2.
2. Configure an existing Alloy receiver and Loki writer.
3. Set `TARSSH_ALLOY_URL` to the private Alloy HTTP endpoint.
4. Set a unique `TARSSH_DEPLOYMENT` for this host.
5. Validate Compose before starting:

   ```sh
   docker compose --env-file /dev/null config --quiet
   ```

6. Start the service:

   ```sh
   docker compose --env-file /dev/null up -d --build
   ```

7. Merge the Prometheus scrape job and import the Grafana dashboard.
8. Confirm `/metrics`, connection limits, and Alloy delivery counters.

Keep the metrics port on loopback or a private network. Restrict Alloy's HTTP
receiver to the intended network. Use verified TLS across a trust boundary.
Do not map the tarpit to port 22 while administrative SSH still needs that
port.

## Multiple hosts

Give each host a stable deployment ID, for example `prod-ams-01` and
`prod-fra-01`. Reusing one ID joins metrics and logs from different hosts and
makes host-level diagnosis unreliable.

## Routine checks

- Check `tarssh_connections_active` against the configured connection limit.
- Check rejected connections, accept errors, queue drops, and push failures.
- Check Alloy and Loki health separately from tarpit availability.
- Keep the deployed source revision recorded with the host change. For a
  published image, record and deploy its immutable `@sha256:...` digest.

Logs are best effort. A successful push means Alloy accepted the batch; it does
not prove Loki has stored every event.

## Upgrade

1. Check out the intended source revision.
2. Validate the revision in a non-production host.
3. Run the Compose and observability checks.
4. Rebuild and restart the service:

   ```sh
   docker compose --env-file /dev/null up -d --build
   ```

5. Verify metrics, connections, and delivery counters.
6. Keep the previous source revision for rollback.

## Roll back

1. Stop or redeploy only the tarpit service.
2. Restore the previous source revision.
3. Rebuild and start the service:

   ```sh
   docker compose --env-file /dev/null up -d --build
   ```

4. Verify `/metrics`, connection handling, and Alloy delivery.
5. Record the failed source revision and evidence in an issue without posting secrets,
   client payloads, private endpoints, or personal data.

## Incident handling

For a vulnerability, use the private process in [SECURITY.md](SECURITY.md).
For an ordinary bug, open an issue with the source revision, environment, minimal
reproduction, and redacted output. Do not include credentials, client payloads,
private endpoints, or personal data.
