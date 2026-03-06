import random
from collections import defaultdict
from datetime import UTC, datetime, timedelta

from psycopg2.extras import Json, execute_values

from app.db import get_connection


RANDOM_SEED = 42


def slugify(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in value)
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-")


def random_datetime(start: datetime, end: datetime) -> datetime:
    delta_seconds = int((end - start).total_seconds())
    if delta_seconds <= 0:
        return start
    return start + timedelta(seconds=random.randint(0, delta_seconds))


def pick_weighted(items, weights):
    return random.choices(items, weights=weights, k=1)[0]


def build_company_name(index: int) -> str:
    prefixes = [
        "Northstar",
        "Summit",
        "BluePeak",
        "Velocity",
        "Crescent",
        "Harbor",
        "Granite",
        "Orbit",
        "Signal",
        "Evergreen",
        "Atlas",
        "Beacon",
        "Mariner",
        "Vertex",
        "Bridge",
    ]
    suffixes = [
        "Analytics",
        "Systems",
        "Health",
        "Capital",
        "Security",
        "Retail",
        "Industrial",
        "Cloud",
        "Data",
        "Logistics",
        "Advisors",
        "Software",
        "Group",
        "Networks",
        "Energy",
    ]
    return f"{prefixes[index % len(prefixes)]} {suffixes[(index * 3) % len(suffixes)]}"


def generate_accounts(cur, now: datetime):
    tiers = ["Tier 1", "Tier 2", "Tier 3"]
    tier_weights = [0.3, 0.45, 0.25]

    accounts = []
    for i in range(50):
        company_name = build_company_name(i)
        # Guarantee deterministic uniqueness even if base generator repeats.
        if any(existing[0] == company_name for existing in accounts):
            company_name = f"{company_name} {i + 1}"
        domain = f"{slugify(company_name)}.com"
        target_tier = pick_weighted(tiers, tier_weights)
        created_at = now - timedelta(days=random.randint(120, 720))
        accounts.append((company_name, domain, target_tier, created_at))

    query = """
        INSERT INTO accounts (company_name, domain, target_tier, created_at)
        VALUES %s
        RETURNING id, company_name, domain
    """
    rows = execute_values(cur, query, accounts, fetch=True)
    return [{"id": row[0], "company_name": row[1], "domain": row[2]} for row in rows]


def generate_contacts(cur, accounts):
    first_names = [
        "Avery", "Jordan", "Taylor", "Riley", "Parker", "Quinn", "Morgan", "Casey", "Reese", "Skyler",
        "Logan", "Harper", "Rowan", "Dakota", "Kendall", "Jamie", "Cameron", "Blake", "Emerson", "Elliot",
    ]
    last_names = [
        "Nguyen", "Patel", "Garcia", "Chen", "Rivera", "Morris", "Walker", "Reed", "Carter", "Diaz",
        "Foster", "Bennett", "Simmons", "Brooks", "Barnes", "Price", "Griffin", "Hughes", "Perry", "Powell",
    ]
    titles = [
        "VP Marketing",
        "Director of Demand Gen",
        "Head of Revenue Ops",
        "Senior Growth Manager",
        "CMO",
        "Digital Marketing Lead",
        "Director of Sales Ops",
        "RevOps Manager",
    ]

    contacts = []
    contact_registry = defaultdict(list)
    for i in range(75):
        account = accounts[i % len(accounts)]
        first = first_names[i % len(first_names)]
        last = last_names[(i * 3) % len(last_names)]
        full_name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}{i}@{account['domain']}"
        linkedin_slug = slugify(f"{first}-{last}-{i}")
        linkedin_url = f"https://www.linkedin.com/in/{linkedin_slug}"
        title = random.choice(titles)
        contacts.append((account["id"], full_name, email, linkedin_url, title))

    query = """
        INSERT INTO contacts (account_id, full_name, email, linkedin_url, title)
        VALUES %s
        RETURNING id, account_id, full_name, linkedin_url
    """
    rows = execute_values(cur, query, contacts, fetch=True)
    for row in rows:
        contact_registry[row[1]].append(
            {
                "id": row[0],
                "full_name": row[2],
                "linkedin_url": row[3],
            }
        )
    return contact_registry


def generate_posts(cur, now: datetime):
    authors = [
        "Nina Rahman",
        "Caleb Morris",
        "Iris Delgado",
        "Arjun Mehta",
        "Leah Bennett",
    ]
    topics = [
        "Attribution strategy",
        "Pipeline visibility",
        "Signal-based GTM",
        "Organic social playbooks",
        "Revenue analytics",
        "Account intent modeling",
        "Demand generation",
        "Marketing operations",
    ]
    post_records = []
    for i in range(25):
        author_name = authors[i % len(authors)]
        topic = topics[(i * 2) % len(topics)]
        post_url = f"https://www.linkedin.com/posts/<REDACTED_POST>{i + 1:03d}"
        cta_url = f"https://catalystlabs.io/resources/{slugify(topic)}-{i + 1}"
        created_at = now - timedelta(days=random.randint(20, 140), hours=random.randint(0, 23))
        post_records.append((author_name, post_url, topic, cta_url, created_at))

    query = """
        INSERT INTO posts (author_name, post_url, topic, cta_url, created_at)
        VALUES %s
        RETURNING id, created_at
    """
    rows = execute_values(cur, query, post_records, fetch=True)
    return [{"id": row[0], "created_at": row[1]} for row in rows]


def build_social_event(actor_name, actor_linkedin_url, actor_company_raw, post, event_type, timestamp):
    base_metadata = {
        "channel": "linkedin",
        "device": random.choice(["desktop", "mobile"]),
    }
    if event_type == "reaction":
        base_metadata["reaction_type"] = random.choice(["like", "insightful", "celebrate", "support"])
    elif event_type == "comment":
        base_metadata["comment_length"] = random.randint(12, 220)
    elif event_type == "share":
        base_metadata["share_with_note"] = random.choice([True, False])
    return (
        post["id"],
        actor_name,
        actor_linkedin_url,
        actor_company_raw,
        event_type,
        timestamp,
        Json(base_metadata),
    )


def generate_social_events(cur, now, posts, accounts, contact_registry):
    event_types = ["reaction", "comment", "share"]
    event_type_weights = [0.7, 0.22, 0.08]
    hot_posts = random.sample(posts, 5)

    account_ids = [account["id"] for account in accounts]
    influenced_accounts = account_ids[:12]
    engaged_no_convert_accounts = account_ids[12:22]

    account_last_engagement = {}
    social_events = []

    def make_actor(account_id):
        company_name = next(a["company_name"] for a in accounts if a["id"] == account_id)
        contacts = contact_registry.get(account_id, [])
        if contacts and random.random() < 0.65:
            contact = random.choice(contacts)
            return contact["full_name"], contact["linkedin_url"], company_name

        synthetic_first = random.choice(["Alex", "Sam", "Mika", "Devon", "Noel", "Kris", "Lee", "Jules"])
        synthetic_last = random.choice(["Turner", "Shah", "Lopez", "Kim", "Cruz", "Ahmed", "Wright", "King"])
        actor_name = f"{synthetic_first} {synthetic_last}"
        linkedin_url = f"https://www.linkedin.com/in/{slugify(actor_name)}-{random.randint(100, 999)}"
        return actor_name, linkedin_url, company_name

    for account_id in influenced_accounts:
        burst_start = now - timedelta(days=random.randint(35, 90))
        burst_posts = random.sample(posts, 3)
        for _ in range(10):
            post = pick_weighted(posts, [7 if p in hot_posts else 2 for p in posts])
            post = random.choice(burst_posts if random.random() < 0.7 else posts)
            event_ts = random_datetime(max(post["created_at"], burst_start), burst_start + timedelta(days=20))
            event_type = pick_weighted(event_types, event_type_weights)
            actor_name, actor_linkedin_url, actor_company_raw = make_actor(account_id)
            social_events.append(
                build_social_event(actor_name, actor_linkedin_url, actor_company_raw, post, event_type, event_ts)
            )
            account_last_engagement[account_id] = max(account_last_engagement.get(account_id, event_ts), event_ts)

    for account_id in engaged_no_convert_accounts:
        start = now - timedelta(days=random.randint(50, 140))
        end = now - timedelta(days=random.randint(10, 30))
        for _ in range(6):
            post = pick_weighted(posts, [6 if p in hot_posts else 2 for p in posts])
            event_ts = random_datetime(max(post["created_at"], start), end)
            event_type = pick_weighted(event_types, event_type_weights)
            actor_name, actor_linkedin_url, actor_company_raw = make_actor(account_id)
            social_events.append(
                build_social_event(actor_name, actor_linkedin_url, actor_company_raw, post, event_type, event_ts)
            )
            account_last_engagement[account_id] = max(account_last_engagement.get(account_id, event_ts), event_ts)

    while len(social_events) < 340:
        post = pick_weighted(posts, [8 if p in hot_posts else 2 for p in posts])
        event_type = pick_weighted(event_types, event_type_weights)
        if random.random() < 0.78:
            account_id = pick_weighted(
                account_ids,
                [6 if a in influenced_accounts else 4 if a in engaged_no_convert_accounts else 1 for a in account_ids],
            )
            actor_name, actor_linkedin_url, actor_company_raw = make_actor(account_id)
        else:
            account_id = None
            actor_name = random.choice(["Chris Lane", "Dana Ross", "Robin Hart", "Kai Fox", "Peyton Snow"])
            actor_linkedin_url = f"https://www.linkedin.com/in/{slugify(actor_name)}-{random.randint(100, 999)}"
            actor_company_raw = random.choice([
                "Stealth Startup",
                "Independent Consultant",
                "Unknown",
                "Future Ventures",
                "Freelance",
            ])

        event_ts = random_datetime(post["created_at"], now - timedelta(hours=2))
        social_events.append(
            build_social_event(actor_name, actor_linkedin_url, actor_company_raw, post, event_type, event_ts)
        )

        if account_id is not None:
            account_last_engagement[account_id] = max(account_last_engagement.get(account_id, event_ts), event_ts)

    query = """
        INSERT INTO social_events (
            post_id,
            actor_name,
            actor_linkedin_url,
            actor_company_raw,
            event_type,
            event_timestamp,
            metadata_json
        ) VALUES %s
    """
    execute_values(cur, query, social_events)

    return influenced_accounts, engaged_no_convert_accounts, account_last_engagement


def generate_website_events(cur, now, accounts, influenced_accounts, engaged_no_convert_accounts, account_last_engagement):
    page_urls = [
        "https://catalystlabs.io/",
        "https://catalystlabs.io/product",
        "https://catalystlabs.io/pricing",
        "https://catalystlabs.io/resources/linkedin-attribution-guide",
        "https://catalystlabs.io/case-studies/pipeline-visibility",
        "https://catalystlabs.io/demo",
    ]
    website_events = []

    for account_id in influenced_accounts:
        baseline = account_last_engagement.get(account_id, now - timedelta(days=30))
        for _ in range(4):
            event_ts = random_datetime(baseline + timedelta(days=2), baseline + timedelta(days=35))
            website_events.append(
                (
                    account_id,
                    None,
                    random.choice(page_urls),
                    "linkedin",
                    random.choice(["organic-thought-leadership", "exec-post-series", "linkedin-engagement-retarget"]),
                    event_ts,
                )
            )

    for account_id in engaged_no_convert_accounts:
        baseline = account_last_engagement.get(account_id, now - timedelta(days=60))
        for _ in range(2):
            event_ts = random_datetime(baseline + timedelta(days=1), now - timedelta(days=3))
            website_events.append(
                (
                    account_id,
                    None,
                    random.choice(page_urls),
                    random.choice(["linkedin", "direct"]),
                    random.choice(["organic-thought-leadership", "newsletter", "none"]),
                    event_ts,
                )
            )

    account_ids = [a["id"] for a in accounts]
    while len(website_events) < 150:
        if random.random() < 0.62:
            account_id = random.choice(account_ids)
            anonymous_visitor_id = None
        else:
            account_id = None
            anonymous_visitor_id = f"anon-{random.randint(100000, 999999)}"

        event_ts = random_datetime(now - timedelta(days=120), now - timedelta(hours=1))
        website_events.append(
            (
                account_id,
                anonymous_visitor_id,
                random.choice(page_urls),
                random.choice(["linkedin", "google", "direct", "newsletter", "partner"]),
                random.choice([
                    "organic-thought-leadership",
                    "organic-search",
                    "brand-awareness",
                    "product-launch-q1",
                    "none",
                ]),
                event_ts,
            )
        )

    query = """
        INSERT INTO website_events (
            account_id,
            anonymous_visitor_id,
            page_url,
            utm_source,
            utm_campaign,
            event_timestamp
        ) VALUES %s
    """
    execute_values(cur, query, website_events)


def generate_opportunities(cur, now, accounts, influenced_accounts, account_last_engagement):
    product_names = ["Attribution Engine", "Signal Hub", "Revenue Graph", "Pipeline Monitor"]
    stages = ["Discovery", "Evaluation", "Proposal", "Closed Won"]

    opportunities = []
    influenced_set = set(influenced_accounts)

    for account_id in influenced_accounts:
        last_engagement = account_last_engagement.get(account_id, now - timedelta(days=50))
        created_at = random_datetime(last_engagement + timedelta(days=7), last_engagement + timedelta(days=50))
        stage = pick_weighted(stages, [0.2, 0.25, 0.25, 0.3])
        closed_won_at = None
        if stage == "Closed Won":
            closed_won_at = created_at + timedelta(days=random.randint(15, 55))
        account_name = next(a["company_name"] for a in accounts if a["id"] == account_id)
        opportunities.append(
            (
                account_id,
                f"{account_name} - {random.choice(product_names)}",
                stage,
                round(random.uniform(22000, 180000), 2),
                created_at,
                closed_won_at,
            )
        )

    non_influenced_accounts = [a["id"] for a in accounts if a["id"] not in influenced_set]
    for _ in range(8):
        account_id = random.choice(non_influenced_accounts)
        created_at = random_datetime(now - timedelta(days=100), now - timedelta(days=2))
        stage = pick_weighted(stages, [0.35, 0.3, 0.25, 0.1])
        closed_won_at = None
        if stage == "Closed Won":
            closed_won_at = created_at + timedelta(days=random.randint(20, 70))
        account_name = next(a["company_name"] for a in accounts if a["id"] == account_id)
        opportunities.append(
            (
                account_id,
                f"{account_name} - {random.choice(product_names)}",
                stage,
                round(random.uniform(15000, 120000), 2),
                created_at,
                closed_won_at,
            )
        )

    query = """
        INSERT INTO opportunities (
            account_id,
            opportunity_name,
            stage,
            amount,
            created_at,
            closed_won_at
        ) VALUES %s
    """
    execute_values(cur, query, opportunities)


def main() -> None:
    # Only seed source-of-truth input tables here.
    # Derived tables are intentionally left empty for downstream jobs.
    random.seed(RANDOM_SEED)
    now = datetime.now(UTC)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                TRUNCATE TABLE
                    opportunity_influence,
                    account_intent_scores,
                    opportunities,
                    website_events,
                    social_events,
                    contacts,
                    accounts,
                    posts
                RESTART IDENTITY CASCADE;
                """
            )

            accounts = generate_accounts(cur, now)
            contact_registry = generate_contacts(cur, accounts)
            posts = generate_posts(cur, now)
            (
                influenced_accounts,
                engaged_no_convert_accounts,
                account_last_engagement,
            ) = generate_social_events(cur, now, posts, accounts, contact_registry)
            generate_website_events(
                cur,
                now,
                accounts,
                influenced_accounts,
                engaged_no_convert_accounts,
                account_last_engagement,
            )
            generate_opportunities(cur, now, accounts, influenced_accounts, account_last_engagement)

        conn.commit()

    print("Seed data inserted successfully.")
    print("- posts: 25")
    print("- social_events: 340")
    print("- accounts: 50")
    print("- contacts: 75")
    print("- website_events: 150")
    print("- opportunities: 20")
    print("- account_intent_scores: 0 (derived, seeded later)")
    print("- opportunity_influence: 0 (derived, seeded later)")


if __name__ == "__main__":
    main()
