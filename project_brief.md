# The February Dip
## A GA4 BigQuery Data Investigation Project

---

## Scenario

You are a **Data / Web Analyst** at *Google Merchandise Store*, a mid-size e-commerce brand that sells branded apparel and accessories. The company exports its GA4 data to BigQuery daily.

It's **Friday, February 9, 2024**. Your manager sends you this message:

> "Revenue last week was way down compared to January. Finance noticed it Friday and flagged it to me over the weekend. Can you dig into the GA4 BigQuery data and tell me what's going on? I need a clear answer by end of week — what happened, when it started, and what we should do about it."

Your job is to investigate using only the GA4 BigQuery export. There is no GA4 UI access — only raw BQ data.

---

## Your Tasks

Work through these tasks in order. Each builds on the previous.

---

### Task 1 — Establish the Baseline & Spot the Anomaly

Write a query that shows **daily sessions and purchases** across the full date range so you can visually identify when things went wrong.

**Guidance:**
- A "session" = a unique `(user_pseudo_id, ga_session_id)` pair where `ga_session_id` lives inside `event_params`
- You will need to **unnest** `event_params` to extract values — this is the core GA4 BQ skill
- Use a 7-day rolling average to smooth the trend

**Deliverable:** A query returning columns: `event_date`, `sessions`, `purchases`, `cvr_pct`, `rolling_7d_sessions`

**Key BQ/GA4 skill tested:** Unnesting `REPEATED RECORD` fields, window functions

---

### Task 2 — Traffic Source Breakdown

Break down **sessions by traffic medium** over time. Compare the pre-anomaly period (Jan 1 – Feb 4) vs. the anomaly period (Feb 5 – Mar 1).

**Guidance:**
- `traffic_source.medium` is a top-level field (no unnesting needed)
- Show absolute session counts AND percentage share for each period
- Which medium changed the most, both in absolute and relative terms?

**Deliverable:** A query (or pair of queries) showing medium-level session share for both periods

**Key skill tested:** Period comparison with CTEs, conditional aggregation

---

### Task 3 — Conversion Funnel Analysis

Build a **full conversion funnel** showing how many unique sessions reached each step:

```
session_start → view_item → add_to_cart → begin_checkout → purchase
```

Show this funnel for **both periods side by side** so the drop-off at each step is visible.

**Guidance:**
- A session "reaches" a step if it contains at least one event of that type
- Calculate the step-to-step drop-off rate
- Which step has the biggest drop-off change between periods?

**Deliverable:** A query returning: `step`, `normal_period_sessions`, `anomaly_period_sessions`, `normal_dropoff_pct`, `anomaly_dropoff_pct`

**Key skill tested:** Multi-step funnel logic with CTEs, COUNTIF / conditional COUNT

---

### Task 4 — Device Segmentation

Segment the funnel (or at minimum the `begin_checkout → purchase` step) **by device category** (`desktop`, `mobile`, `tablet`) across both periods.

**Guidance:**
- `device.category` is a top-level field
- Focus especially on the last step of the funnel (checkout → purchase)
- Something specific to one device category may explain a large portion of the drop

**Deliverable:** A query showing checkout-to-purchase rate by `device_category` × `period`

**Key skill tested:** Multi-dimensional segmentation, spotting a device-specific anomaly

---

### Task 5 — Synthesis Query

Write a **single summary query** that brings together the key signals from Tasks 1–4 into one result set: daily date, sessions, purchases, CVR, dominant medium, and mobile purchase count. This is the "executive view" you'd use to explain the situation to your manager.

**Deliverable:** One clean CTE-based query that a non-technical stakeholder could understand the output of

---

### Task 6 — Written Investigation Report

Write a structured findings report as if delivering it to your manager. It should be clear enough for a non-technical reader but show analytical rigour.

**Required sections:**

1. **Executive Summary** (3–5 sentences: what happened, when, impact)
2. **Methodology** (how you approached the investigation, what queries you ran)
3. **Findings** (present each root cause with supporting data from your queries)
4. **Root Cause(s)** (your conclusion — be specific)
5. **Recommendations** (what should the team do now and to prevent recurrence)

**Length:** 400–700 words

---

## Evaluation Criteria

When you submit your work, it will be graded on:

| Area | What's assessed |
|---|---|
| **GA4/BQ schema fluency** | Correct unnesting of `event_params` and other nested fields |
| **SQL quality** | Clean CTEs, readable aliases, no unnecessary complexity |
| **Analytical thinking** | Did you find both root causes, or just one? |
| **Funnel construction** | Correct session-level (not event-level) funnel logic |
| **Report clarity** | Can a non-technical reader understand the findings? |
| **Recommendations** | Are they specific, actionable, and tied to the findings? |

---

## Tips & Hints

- **Always work at session level for the funnel**, not event level. Counting events instead of sessions is a common mistake.
- The `event_params` field is a `REPEATED RECORD`. To extract a specific parameter (e.g. `ga_session_id`), use:
  ```sql
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id
  ```
- `event_date` is stored as a `STRING` in `YYYYMMDD` format. Use `PARSE_DATE('%Y%m%d', event_date)` to convert it for date arithmetic.
- Start simple — get a working query with basic counts before adding complexity.
- Comment your SQL. Interviewers read queries; comments show your thinking.

---

## Deliverables Checklist

- [ ] Task 1 SQL query (trend + rolling average)
- [ ] Task 2 SQL query (traffic source breakdown)
- [ ] Task 3 SQL query (conversion funnel)
- [ ] Task 4 SQL query (device segmentation)
- [ ] Task 5 SQL query (synthesis)
- [ ] Task 6 Written report (400–700 words)

Good luck — and remember, the data has a story. Follow the numbers.
