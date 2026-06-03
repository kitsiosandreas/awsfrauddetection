# Fraud Detection Demo — Presenter Runbook

**Audience:** CTO + technical stakeholders, Forex/CFD broker  
**Duration:** 45 minutes  
**Setup time:** 15 minutes before presenting  

---

## Pre-Demo Checklist (T-15 min)

- [ ] AWS Console open — switch to the deployment region
- [ ] OpenSearch Dashboards tab open and logged in
- [ ] ECS task for data generator is **RUNNING** (confirm in ECS console)
- [ ] All three Flink applications show status **RUNNING**
- [ ] SNS subscription confirmed (check your email for the confirmation link)
- [ ] Presenter laptop connected to external display
- [ ] Browser tabs pre-opened:
  1. OpenSearch Dashboards → Fraud Overview dashboard
  2. CloudWatch → Managed Flink → `fraud-demo-coordinated-trading` metrics
  3. DynamoDB → `fraud-demo-alerts` → Items
  4. ECS console → `fraud-demo-datagen` service

---

## Opening (2 min)

> *"What we're looking at is a live fraud detection system. Every event you see on this dashboard is being generated right now by a synthetic data generator running on AWS — simulating real Forex traders, as well as several fraud typologies we've found to be most relevant to your business."*

> *"The platform combines two detection layers: rule-based logic that fires immediately on known patterns, and machine learning models that catch the sophisticated stuff — the attacks that deliberately stay under the rule thresholds."*

> *"Let me walk you through three scenarios."*

---

## Scenario 1 — System Abuse & Abusive Registrations (10 min)

**Story:** *"The CTO mentioned API abuse and bot registrations. Let me show you how these look on the platform."*

### Step 1 — Switch generator to registration_burst mode

In ECS console or terminal:
```bash
# Update the ECS task environment variable
aws ecs update-service \
  --cluster fraud-demo-datagen \
  --service fraud-demo-datagen \
  --force-new-deployment

# Or locally:
python data_generator/main.py --scenario registration_burst --tps 200
```

### What to point at on the dashboard:
1. **Event rate counter** — API calls per second spiking (top of Fraud Overview)
2. **System Abuse panel** — "Registration Burst — IP" alerts appearing within 30 seconds
3. **Shared Canvas Fingerprint** alert — multiple accounts with identical browser fingerprints
4. **API rate alerts** — single accounts generating hundreds of calls/min

### Key talking point:
> *"A single rule catches a single bot. But these bots rotate IPs. The ML model looks at the session behaviour — form fill time of 2 seconds, same browser canvas hash, requests arriving at machine-like intervals — and clusters them together. One ring, 50 accounts, one alert."*

### Demo action — show the DynamoDB alert record:
Navigate to DynamoDB → `fraud-demo-alerts` → Items, filter by `typology = SYSTEM_ABUSE`.
Show the `signals` field containing `["REGISTRATION_BURST_FROM_IP", "SHARED_CANVAS_FINGERPRINT", "ML_BOT_DETECTED"]`.

---

## Scenario 2 — Account Takeover (10 min)

**Story:** *"This is the one your fraud team probably deals with most. An account holder's credentials get stolen — credential stuffing, phishing, dark web purchase — and the attacker logs in."*

### Step 1 — Inject an ATO event

```bash
python data_generator/main.py --scenario ato_attack --tps 50 --accounts 100
```

### What to show:

1. **Timeline view** on the Account Takeover dashboard:
   - Normal login history from AU/Sydney on known device
   - Burst of failed logins from NG/Lagos (credential stuffing)
   - Successful login from NG/Lagos on NEW device
   - Password reset event
   - Withdrawal request

2. **Geo-velocity alert** — fires within seconds:
   > *"The rule fired immediately. The last login was in Sydney. Five minutes later — login from Lagos. That would require flying at 4,000 km/h. The rule knows this is impossible."*

3. **ML risk score** — show the login risk endpoint result:
   > *"But what if the attacker uses a VPN and appears to be in Sydney? The rule doesn't fire. The ML model still does — because the session profile looks completely different from 12 months of baseline: off-hours login, 2-minute session, new device hash, immediate withdrawal. The model gives this a 94% fraud probability."*

4. **Withdrawal alert** — show the password reset → withdrawal chain:
   > *"And we have a CEP pattern: password reset followed by a withdrawal in under 3 minutes. That's the automated 'drain and run'."*

### Demo action — show Step Functions execution:
Navigate to Step Functions → `fraud-demo-investigation` → show the execution that was triggered by the CRITICAL alert. Show the `AutoBlock` state was entered.

---

## Scenario 3 — Coordinated Abusive Trading (15 min)

**Story:** *"This is the hardest one to catch — and the one that demonstrates the clearest ML value. We're going to watch accounts that individually look completely clean... until you see them together."*

### Step 1 — Start with normal scenario (show clean dashboard)

```bash
python data_generator/main.py --scenario normal --tps 100 --accounts 500
```

Point at the dashboard: no alerts. All accounts within normal parameters.

> *"Look — velocity rules are green. Win rates are normal. Nothing stands out on any individual account."*

### Step 2 — Enable coordinated ring

```bash
python data_generator/main.py --scenario coordinated_ring --tps 200 --accounts 500
```

### What to show — building the story:

1. **Individual account view** — pick one ring member's account:
   - Trade count: normal ✓
   - Win rate: a bit high, but not alarming ✓
   - Instrument: EURUSD — very common ✓
   - Rule alerts: none ✓

   > *"If I'm looking at this account alone, nothing fires. It trades 6-8 times a day on EURUSD, wins slightly more than it loses. Legitimate."*

2. **Zoom out — the Coordinated Trading dashboard**:
   Show the Neptune graph visualisation (or the OpenSearch account correlation panel):
   - 8 accounts, all EURUSD, all BUY, all within a 300ms window
   - Shared /24 IP subnet
   - Correlated P&L curves (Pearson r = 0.93)

   > *"But when the ML model looks across all accounts simultaneously, it finds these 8 accounts entered the same trade within 300 milliseconds of each other. They're on the same IP subnet. Their P&L curves are 93% correlated. That's not coincidence — that's coordination."*

3. **The alert** — show the `COORDINATED_RING` alert in OpenSearch:
   - Detection method: ML
   - Cluster ID: linking all 8 accounts
   - Signals: `["COORDINATED_TIMING", "CORRELATED_PNL", "SHARED_IP_SUBNET", "ML_ISOLATION_FOREST"]`

   > *"The rule-based layer passed them all. The ML layer caught the ring. One alert, eight accounts, cluster suspended."*

### Demo action — show CloudWatch Flink metrics:
Navigate to CloudWatch → `fraud-demo-coordinated-trading` → show:
- Records processed/sec
- Alert records emitted/sec
- SageMaker endpoint invocations

---

## Dashboard Walkthrough (5 min)

Navigate to Fraud Overview dashboard and highlight:

| Panel | What to say |
|-------|------------|
| **Total alerts today** | Running count across all typologies |
| **Alerts by severity** | CRITICAL → MEDIUM breakdown |
| **Alerts by typology** | Compare volume across the three |
| **Events/sec** | Visualise the live data stream |
| **Geo heatmap** | Where alerts are originating |
| **Alert latency** | Time from event to alert (target <2s for rules, <5s for ML) |

---

## Architecture Conversation Points

**"How does this connect to your existing systems?"**
> RDS replaces or mirrors your existing PostgreSQL/Oracle account database. MSK sits alongside your existing trade engine. The detection layer is read-only — no changes to your trade execution path.

**"What does it take to tune the rules for our thresholds?"**
> All thresholds are externalised in the Flink job configuration — no code changes, no redeployment. Rules for velocity, win rate, registration burst are all parameterised.

**"How long to train the ML models on our real data?"**
> The models are deliberately simple (Isolation Forest, Random Forest, Autoencoder) — they train in under an hour on labelled data. With 90 days of your labelled trade history, we'd expect a substantial improvement in precision vs. the synthetic baseline you see here.

**"What about false positives?"**
> Every alert goes through the Step Functions workflow. The system is designed for human-in-the-loop on anything below CRITICAL. The investigation dashboard shows signal breakdown per alert so your analyst can make a decision in <30 seconds.

---

## Teardown

```bash
cd cdk && cdk destroy --all
```

All resources are tagged `Project: FraudDetectionDemo` for cost tracking.
Estimated AWS cost for the full demo stack: **~$8-15/hour** while running.
