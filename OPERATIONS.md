# Operations

## Deploy

1. Install Docker Engine and Compose v2.
2. Configure an existing Alloy receiver and Loki writer.
3. Set `TARSSH_ALLOY_URL` to the private Alloy HTTP endpoint.
4. Set a unique `TARSSH_DEPLOYMENT` for this host.
5. Validate Compose before starting:

   ```sh
   TARSSH_IMAGE=tarssh-rs:local docker compose --env-file /dev/null config --quiet
   ```

6. Start the service:

   ```sh
   TARSSH_IMAGE=tarssh-rs:local docker compose --env-file /dev/null up -d --build
   ```

7. Merge the Prometheus scrape job. Import the Grafana dashboard.
8. Confirm `/metrics`, the connection limit, and Alloy delivery counters.

Keep the metrics port on loopback or a private network. Restrict Alloy's HTTP
receiver to the intended network. Use verified TLS across a trust boundary.
Do not map the tarpit to port 22 while administrative SSH needs it.

## Multiple hosts

Give each host a stable deployment ID, for example `prod-ams-01` and
`prod-fra-01`. Reusing one ID joins metrics and logs from different hosts, which
prevents reliable host-level diagnosis.

## Routine checks

- Check `tarssh_connections_active` against the configured connection limit.
- Check rejected connections, accept errors, queue drops, and push failures.
- Check Alloy and Loki health separately from tarpit availability.
- Keep the deployed source revision recorded with the host change. For a
  published image, record and deploy its immutable `@sha256:...` digest via
  `TARSSH_IMAGE`.

Logs are best effort. A successful push means Alloy accepted the batch; it does
not prove that Loki stored every event.

## Upgrade

1. Check out the intended source revision.
2. Validate the revision in a non-production host.
3. Run the Compose and observability checks.
4. Rebuild and restart the service:

   ```sh
   TARSSH_IMAGE=tarssh-rs:local docker compose --env-file /dev/null up -d --build
   ```

   For a published image, set `TARSSH_IMAGE` to its immutable digest and use:

   ```sh
   docker compose --env-file /dev/null up -d --no-build --pull always
   ```

5. Verify metrics, connections, and delivery counters.
6. Keep the previous source revision for rollback.

## Roll back

1. Stop or redeploy only the tarpit service.
2. For a source deployment, restore the previous source revision, then rebuild
   and start the service:

   ```sh
   TARSSH_IMAGE=tarssh-rs:local docker compose --env-file /dev/null up -d --build
   ```

3. For an image deployment, restore the previous immutable `TARSSH_IMAGE`
   digest, then run:

   ```sh
   docker compose --env-file /dev/null up -d --no-build --pull always
   ```
4. Verify `/metrics`, connection handling, and Alloy delivery.
5. Record the failed source revision or image digest and evidence in an issue without posting secrets,
   client payloads, private endpoints, or personal data.

## Incident handling

For a vulnerability, use the private process in [SECURITY.md](SECURITY.md).
For an ordinary bug, open an issue with the source revision, environment, minimal
reproduction, and redacted output. Do not include credentials, client payloads,
private endpoints, or personal data.
