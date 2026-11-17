# Deploy to the cloud: one Ubuntu server, step by step

This puts the **whole project** on one rented Ubuntu server: the database, the daily data pipeline (with its
scheduler), and the Metabase dashboard. Everything runs in Docker and restarts by itself after a reboot.

```
 your computer ──SSH (port 22 only)──►  Ubuntu server
                                        ├─ Postgres  (the crime data)
                                        ├─ pipeline  (loads new data every day at midnight US Eastern)
                                        ├─ Prefect   (the scheduler + run history)
                                        └─ Metabase  (the dashboard)
```

**By default nothing is open to the internet except SSH.** Postgres, Metabase, Prefect and pgAdmin only listen on the
server itself (`127.0.0.1`). You look at the dashboard through an SSH tunnel (step 8). That is deliberate: it keeps
your database and dashboard private. If you want people to open the dashboard in a browser **without** any setup,
step 14 adds an optional public, view-only link (this opens ports 80 and 443).

You will need: a cloud account (any provider), a Socrata app token, and a terminal (macOS/Linux Terminal, or
PowerShell on Windows 10/11, which includes `ssh` and `scp`). Expect about 30 to 45 minutes.

---

## 1. Get a Socrata app token (2 minutes)

Sign in at https://data.cambridgema.gov, open your profile → **Developer Settings** → **Create New App Token**.
Keep the token handy. You will put it in a file on the server in step 7. Never paste it into chat or email.

## 2. Create the server

In your cloud provider's console, create a virtual machine ("instance", "droplet", "VM"):

| Setting | Choose |
|---|---|
| Image / OS | **Ubuntu 24.04 LTS** (22.04 LTS also works) |
| Size | **2 vCPU, 4 GB RAM** (Metabase is the hungry part; 2 GB RAM is too small without swap) |
| Disk | 30 GB or more |
| Login | **SSH key** (not a password). To make one, run this on *your* computer: `ssh-keygen -t ed25519` and upload the contents of `~/.ssh/id_ed25519.pub` when the provider asks for a public key |
| Firewall / security group | Allow inbound **TCP 22 (SSH) only**. Do **not** open 3000, 4200, 5433 or 8081 |

Write down the server's **public IP address** and the login user name (`ubuntu` on most providers; some use `root`).

## 3. Connect and install Docker

From your computer (replace the IP and user):

```bash
ssh ubuntu@YOUR_SERVER_IP
```

Answer `yes` to the fingerprint question. You are now typing commands **on the server**. Install Docker:

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
exit
```

`exit` logs you out; log back in (`ssh ubuntu@YOUR_SERVER_IP`) so the new group takes effect. Check it:

```bash
docker --version
docker compose version
systemctl is-enabled docker      # should print: enabled   (Docker then starts by itself after a reboot)
```

*Optional, only if your server has 2 GB RAM or less:* add swap so Metabase doesn't get killed:

```bash
sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

*Recommended:* turn on the server's own firewall too (SSH only):

```bash
sudo ufw allow OpenSSH && sudo ufw --force enable
```

## 4. Upload the project

On **your computer**, in the folder that contains `crime-pipeline.zip`:

```bash
scp crime-pipeline.zip ubuntu@YOUR_SERVER_IP:~/
```

On the **server**:

```bash
sudo apt-get update && sudo apt-get install -y unzip
unzip crime-pipeline.zip
cd crime-pipeline
chmod +x deploy.sh
```

The zip contains no secrets (no `.env`). You create that on the server next.

## 5. First run of `deploy.sh`: it creates your settings file

```bash
./deploy.sh
```

The first time, it creates a `.env` file with **randomly generated passwords** and then stops with instructions.
(You can ignore all the passwords; Docker uses them for you.)

## 6. Put your Socrata token in `.env`

```bash
nano .env
```

Find the line `SOCRATA_APP_TOKEN=` and paste your token after the `=` (no spaces, no quotes). Optionally change
`METABASE_ADMIN_EMAIL` to your real email. Save with **Ctrl+O**, **Enter**, then exit with **Ctrl+X**.

## 7. Run `deploy.sh` again: this does everything

```bash
./deploy.sh
```

It takes about 5 to 15 minutes the first time. It will:

1. build the pipeline image,
2. start Postgres, Metabase and the Prefect scheduler and wait until they are healthy,
3. do the **first data load** (about 12,000 rows from the Cambridge API),
4. start the daily-schedule service,
5. create the Metabase admin account, connect the database and build the dashboard.

At the end it prints "DONE" with next steps. If anything fails, it stops and says why; fix that and run
`./deploy.sh` again. It is safe to rerun, and it never duplicates data.

## 8. Look at the dashboard (SSH tunnel)

On **your computer** (not the server), open a **new** terminal and run:

```bash
ssh -L 3000:localhost:3000 -L 4200:localhost:4200 ubuntu@YOUR_SERVER_IP
```

Leave that window open. Now open in your browser:

* **Dashboard:** http://localhost:3000, log in with the `METABASE_ADMIN_EMAIL` and `METABASE_ADMIN_PASSWORD` from the
  server's `.env`. To read the password on the server: `grep METABASE_ADMIN .env`. Open **Our analytics →
  Cambridge Police Incidents Dashboard**. You can change the password under your profile.
* **Prefect API:** http://localhost:4200/docs (the interactive API page). Note: on the AWS server the Prefect *web UI* was **not** served (the root page returns "Not Found", cause not determined), so use the API check in step 9 to see run history.

Close the tunnel window when you are done; the dashboard is then unreachable from outside, as intended.

## 9. Check the daily schedule

The pipeline runs by itself every day at **midnight US Eastern time** (04:00 or 05:00 UTC). On the server, in the
`crime-pipeline` folder:

```bash
docker compose ps                              # every service should say "Up" (healthy where shown)
docker compose logs --tail 50 pipeline         # should say the flow is being served
docker compose exec -T postgres_cpd sh -c 'psql -U "$POSTGRES_USER" -d cpd_db -c "SELECT count(*) AS rows, max(date_time) AS latest_incident FROM cpd_incidents WHERE NOT deleted"'
```

The next morning, ask Prefect's API for the latest runs (run on the server):

```bash
curl -s -X POST localhost:4200/api/flow_runs/filter -H 'content-type: application/json' -d '{"limit":5,"sort":"START_TIME_DESC"}' | python3 -m json.tool | grep -E '"name"|state_name|auto_scheduled|start_time'
```

You should see a run with `"state_name": "Completed"` and `"auto_scheduled": true` (a manual run shows `false`). The Cambridge data itself is published with a delay of roughly two weeks, so the latest incident
date will trail today's date. That is expected.

To run the load right now: `docker compose run --rm -T pipeline python etl_pipeline.py run`

## 10. Everyday operations

| I want to… | Command (in `crime-pipeline`) |
|---|---|
| See what is running | `docker compose ps` |
| Watch pipeline logs | `docker compose logs -f pipeline` (Ctrl+C to stop watching) |
| Restart the pipeline | `docker compose restart pipeline` |
| Stop everything (data is kept) | `docker compose down` |
| Start everything again | `docker compose up -d` |
| Update to a new version of the code | upload the new zip, unzip over the folder (keep your `.env`), run `./deploy.sh` |
| Refresh the dashboard's Location list after new locations appear | `docker compose run --rm -T pipeline python scripts/setup_metabase.py` |
| Browse the database in pgAdmin | `docker compose --profile admin up -d pgadmin`, add `-L 8081:localhost:8081` to your tunnel, open http://localhost:8081 |

**After a reboot** everything comes back by itself (Docker starts on boot and every service is `restart: unless-stopped`).

> **Danger:** `docker compose down -v` (note the `-v`) **deletes the database volumes**. Never run it unless you
> want to wipe all data.

## 11. Backups (do this)

The data can be re-downloaded from Cambridge, but the Metabase dashboards live only on your server. Back up both:

```bash
# crime data
docker compose exec -T postgres_cpd sh -c 'pg_dump -U "$POSTGRES_USER" cpd_db' | gzip > cpd_backup_$(date +%F).sql.gz
# Metabase settings, users and dashboards
docker compose exec -T postgres_mb sh -c 'pg_dump -U "$POSTGRES_USER" metabaseappdb' | gzip > metabase_backup_$(date +%F).sql.gz
```

Copy the files to your own computer (run on your computer): `scp ubuntu@YOUR_SERVER_IP:~/crime-pipeline/*.sql.gz .`
Also turn on your cloud provider's automatic **VM snapshots/backups** if it offers them.

## 12. Security checklist

* Only port 22 is open in the provider firewall (step 2), plus 80 and 443 **only if you did step 14**. Confirm from
  your own computer that `curl -m 5 http://YOUR_SERVER_IP:3000` **times out or is refused** (and the same for
  4200, 5432, 5433 and 8081).
* On the server, `docker compose ps` shows ports like `127.0.0.1:3000->3000`. If you ever see `0.0.0.0:`, stop and fix it.
* `.env` holds all secrets and has permissions `600`. Never copy it into a zip, email, chat or Git.
* If your Socrata token leaks, delete it in Socrata, make a new one, edit `.env`, then `docker compose up -d --force-recreate pipeline`.
* Do not "just open port 3000" to share the dashboard. Use step 14, which puts an HTTPS proxy in front of Metabase.

## 13. Troubleshooting

| Symptom | Fix |
|---|---|
| `deploy.sh: Permission denied` | `chmod +x deploy.sh` |
| `permission denied while trying to connect to the Docker daemon` | You skipped the `usermod` + re-login in step 3 |
| Script stops: "SOCRATA_APP_TOKEN is empty" | Edit `.env` (step 6) and rerun, or answer `y` to continue without a token (Socrata may throttle) |
| A step times out waiting for "healthy" | `docker compose ps` then `docker compose logs metabase` (Metabase needs 1 to 3 minutes and enough RAM; see swap in step 3) |
| First load fails with a rate-limit / HTTP 429 message | Add a valid token to `.env`, rerun `./deploy.sh`. The pipeline retries with backoff on its own |
| Dashboard shows no data | Confirm the load: the row-count command in step 9. If it is 0, `docker compose logs pipeline` and run the load by hand |
| "Source schema changed" in the logs | Cambridge changed the dataset's columns. The run stops on purpose without loading anything; the message lists exactly what changed |
| Disk filling up | Container logs are capped; the data itself is small (tens of MB). Check with `df -h` |
| Public link: the page won't load or the certificate fails | Check the security group allows inbound TCP **80 and 443**, then `docker compose logs caddy`. Caddy needs port 80 open to prove it owns the name |
| Public link is slow or times out only from some places | The server is far away from you (e.g. Sydney). Test on the server: `curl -s -o /dev/null -w '%{time_total}\n' https://YOUR_DOMAIN/api/health` should be well under a second |
| Want to start over completely | `docker compose down -v` (**deletes all data**), then `./deploy.sh` |

## 14. Optional: a public, view-only link (no login, any browser)

This lets anyone with a long secret link open the dashboard and use its three filters, with no account and no SSH.
It adds a small web server ("Caddy") in front of Metabase that handles HTTPS and the certificate automatically.

**What becomes visible to the internet**

| Reachable from anywhere | Not reachable |
|---|---|
| Ports **80 and 443** (Caddy; port 80 only redirects to HTTPS and proves to Let's Encrypt that you own the name) | Postgres, Prefect, pgAdmin and Metabase's own port 3000 stay on `127.0.0.1` |
| The shared dashboard at `https://YOUR_DOMAIN/public/dashboard/<long-random-id>`: its charts and filters. Visitors can run its 9 queries with any filter values | Other dashboards, the database, the Metabase admin screens (they need your login) |
| Metabase's login page at `https://YOUR_DOMAIN/` (protected only by the admin password in `.env`, so keep it long and random) | |

The data shown is the public Cambridge police log. Remember that anyone you give the link to can pass it on.

**Set it up**

1. **Cloud firewall:** in your provider's security group, allow inbound **TCP 80** and **TCP 443** from anywhere.
   (On AWS: EC2 → Security Groups → your group → Inbound rules → Add rule.) Keep everything else closed.
2. **Choose a name that points at your server.** The easy way needs no domain purchase: **sslip.io** turns your IP
   into a name. For server IP `3.24.232.21` the name is `3-24-232-21.sslip.io` (dashes instead of dots).
   If your IP can change (stop/start without a fixed "Elastic IP"), the name and link change too.
3. On the server, in the `crime-pipeline` folder, add the name to `.env` and start Caddy:

   ```bash
   echo 'SITE_DOMAIN=3-24-232-21.sslip.io' >> .env      # use YOUR name
   docker compose --profile public up -d caddy
   docker compose logs caddy | grep -i "certificate obtained"    # appears within a minute
   ```

   (`./deploy.sh` also starts Caddy automatically whenever `SITE_DOMAIN` is set.)
4. Check it from your own computer: `curl -s https://YOUR_DOMAIN/api/health` should print `{"status":"ok"}`.
5. **Tell Metabase its public address and turn on sharing** (log in through the tunnel, or at `https://YOUR_DOMAIN`):
   **Settings (gear) → Admin settings → General → Site URL** = `https://YOUR_DOMAIN`, then
   **Admin settings → Settings → Public sharing → Enable**.
6. **Create the link:** open the dashboard → click the **Sharing** icon (top right) → **Public link** → switch it on →
   copy the URL. It looks like `https://YOUR_DOMAIN/public/dashboard/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.
   Open it in a private/incognito window to see exactly what a visitor sees.

**Using it:** send people the link. They see the same dashboard with the **Date range**, **Incident type** and
**Location** filters at the top (pick a value, then **Add filter**). Nothing to install, nothing to sign in to.

**Turn it off** (from the least to the most drastic):

| To… | Do this |
|---|---|
| Kill just this link | Dashboard → **Sharing** → **Public link** → switch **off** (or "Remove link"). The old URL stops working at once; turning it on again makes a **new** URL |
| Stop all public sharing in Metabase | **Admin settings → Settings → Public sharing → Disable** |
| Take the web server offline | `docker compose --profile public stop caddy` (use `down`/`rm` to remove the container) |
| Close the doors at the firewall | Remove the inbound **80** and **443** rules in the security group, and delete the `SITE_DOMAIN` line from `.env` so `deploy.sh` doesn't restart Caddy |

**Notes:** Caddy renews the certificate by itself about every 60 days; it needs port 80 open for that, and its
certificates live in the `caddy_data` Docker volume (do not delete it). The certificate request puts your name in
public certificate-transparency logs, and internet scanners will find the site within minutes: that is normal.
The first load after a quiet period can be slow, and visitors far from the server (a Sydney server viewed from
the US) will notice latency on every chart.

## 15. Stop paying

Deleting the VM in the provider console stops the charges (and deletes everything on it). Take the backups from step 11 first.
