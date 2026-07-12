# Understanding the Data — a plain-English primer

A gentle, example-led walk through what the data *means*, the group → company → counterparty
**hierarchy**, the columns in the final `curated.edge` table, and what the drill notebook
([`notebook/drill.ipynb`](../notebook/drill.ipynb)) is actually showing. Every example is a real row
from the built warehouse. For the design/engineering rationale, see [`WRITEUP.md`](WRITEUP.md).

---

## 1. The real-world picture: who's who

Freemarket is a payments network. Money moves between businesses, and Freemarket earns a fee on each
movement. There are four kinds of "thing" in the data:

- **Group** — a corporate client of Freemarket. Example: **Koba2Maya** (`192230405343`), a Gaming
  company on the "Silver" commercial tier. Think of a group as the *umbrella brand*.
- **Direct company** — an actual legal entity that belongs to a group and holds the accounts that
  transact. Koba2Maya isn't one company — it's ~9 legal entities in different countries:
  - `Koba2Maya Operations Ltd`, `Koba2Maya Group Holdings Ltd` (Lithuania), `Koba2Maya Group
    Holdings Ltd` (Malta), `Koba2Maya Holding S.à r.l.` (Cayman Islands / Isle of Man)… — same
    brand, separate legal entities.
- **Counterparty** — whoever a company sends money to or receives it from. A counterparty can be:
  - **another Freemarket group** → drawn as a **circle** (e.g. Koba2Maya trades with "Get Tranzzact"
    and "Crytopera" — both groups), or
  - an **outside party** not linked to any group → drawn as a **diamond** (e.g. `CP242`).
- **Deposit / Withdrawal / Fee** — a deposit is money coming **in**, a withdrawal is money going
  **out**, and the fee is Freemarket's revenue on it.

---

## 2. The hierarchy — and where it came from

The hierarchy is three levels:

```
Group  ──▶  Direct company  ──▶  transactions with Counterparties
(Koba2Maya)   (Operations Ltd)     (CP242, Get Tranzzact, …)
```

This hierarchy wasn't invented — it comes from **deterministic keys in the raw sources**:

- `companies.json` gives each company a `parentGroup` → that links **company → group**.
- The transaction sheet records the **company** (`dc_id`) that transacted and the **counterparty**.
- So every transaction chains up: `dc_id → company → parentGroup → group`. No guessing, no
  name-matching.

"Drill" just means moving along that chain: look at the whole **group's** money flow, or drill
**down** to see which specific **company** did it, then roll back **up** (the group total is exactly
the sum of its companies).

---

## 3. The columns in `curated.edge`

`curated.edge` is the final table the network view reads from. **One row = one focal company ↔ one
counterparty, in one direction, in one month.** Here is a real row, then each column in plain terms:

| direction | focal_group_id | focal_company_name | counterpart_id | counterpart_is_group | month | gbp_volume | txn_count | fee |
|---|---|---|---|---|---|---|---|---|
| inflow | 192230405343 | Koba2Maya Operations Ltd | CP242 | False | 2025-09 | £52,541,186 | 1 | £520 |

**In English:** *"In September 2025, Koba2Maya Operations Ltd (a legal entity of the Koba2Maya group)
received one payment of ~£52.5m from an outside counterparty (CP242); Freemarket earned £520 in
fees."*

- **direction** — `inflow` (money **into** the focal group, from a deposit) or `outflow` (money
  **out**, from a withdrawal).
- **focal_group_id** — the group you've put at the **centre** of the view ("focal" = the star's
  centre). Here, Koba2Maya.
- **focal_company_id** / **focal_company_name** — the specific legal entity under that group that
  actually transacted. `focal_company_id` is the key; `focal_company_name` ("Koba2Maya Operations
  Ltd") is the readable label. This is the **drill-down** level.
- **counterpart_id** — who the money moved with. If they're a group, it's the group id (circle); if
  standalone, their own id (diamond).
- **counterpart_is_group** — `True` = the counterpart is itself one of our groups (circle), `False`
  = an outside standalone party (diamond).
- **source_node_id / target_node_id** — the directed arrow's ends (who → who) at group level, for
  drawing the graph.
- **month / year** — the period, so you can slice by time.
- **gbp_volume** — total money moved (in GBP) for that row.
- **txn_count** — how many individual transactions are rolled into that row.
- **gbp_fee_revenue** — the fees Freemarket earned on them.

> **Why ids matter:** Koba2Maya has *two* entities both named "Koba2Maya Group Holdings Ltd" (one in
> Lithuania, one in Malta). Same name, different `focal_company_id` — which is why the id is the real
> key and the name is just a label.

---

## 4. What the drill notebook is actually showing

[`notebook/drill.ipynb`](../notebook/drill.ipynb) centres on Koba2Maya and walks the hierarchy. Each
cell answers one plain question:

1. **Group grain (the star-map view)** — combine *all* of Koba2Maya's companies and show money
   in/out per counterparty. → *"Who does Koba2Maya, as a whole, move money with?"* This is the
   picture the star map draws.
2. **Drill down to companies** — the same group, now split by which entity did it: "Operations Ltd
   £66m, Group Holdings Ltd £18m, …". → *"Which of Koba2Maya's legal entities actually transacted?"*
3. **Drill into one company** — take the biggest entity and show *its* counterparties. → *"Who
   specifically did Koba2Maya Operations Ltd deal with?"*
4. **Roll back up** — prove that summing the companies equals the group total, to the penny
   (`volume_matches = True`). → *"Can I trust the drill — is anything lost or double-counted?"* (No.)
5. **By month** — the same, sliced to a period. → *"How did this change over Jul–Dec 2025?"*

**The point of the whole thing:** a Commercial or Finance person can start at "Koba2Maya moved £X
this quarter", then drill to find *which entity* and *which counterparty* drove it — or roll up to
see the client as a whole — and the numbers always reconcile.
