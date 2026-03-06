from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

from app.linkedin_ingestion.base import LinkedInAdapter
from app.linkedin_ingestion.types import AdapterBatch, NormalizedPost, NormalizedSocialEvent


class MockLinkedInAdapter(LinkedInAdapter):
    def __init__(self, posts: int = 20, events: int = 250, source_name: str = "mock") -> None:
        self.posts = posts
        self.events = events
        self.source_name = source_name

    def collect(self) -> AdapterBatch:
        random.seed(73)
        now = datetime.now(UTC)

        authors = ["Nina Rahman", "Arjun Mehta", "Leah Bennett", "Caleb Morris", "Iris Delgado"]
        topics = [
            "LinkedIn organic strategy",
            "Pipeline influence",
            "Attribution foundations",
            "Revenue operations",
            "Marketing analytics",
            "Demand generation",
        ]
        companies = [
            "Northstar Analytics",
            "Orbit Cloud",
            "Signal Security",
            "Vertex Software",
            "Bridge Capital",
            "Harbor Logistics",
            "Granite Industrial",
            "Summit Systems",
            "Crescent Retail",
            "Evergreen Health",
        ]

        posts: list[NormalizedPost] = []
        for index in range(self.posts):
            topic = topics[index % len(topics)]
            created_at = now - timedelta(days=random.randint(10, 120), hours=random.randint(0, 20))
            post_url = f"https://www.linkedin.com/posts/<REDACTED_POST>{index + 1:03d}"
            posts.append(
                NormalizedPost(
                    post_url=post_url,
                    author_name=authors[index % len(authors)],
                    topic=topic,
                    cta_url=f"https://catalystlabs.io/resources/{topic.lower().replace(' ', '-')}-{index + 1}",
                    created_at=created_at,
                    source_name=self.source_name,
                    raw_payload_json={"generator": "mock", "index": index + 1},
                )
            )

        hot_post_indices = set(random.sample(range(self.posts), max(1, self.posts // 4)))
        post_weights = [8 if index in hot_post_indices else 2 for index in range(self.posts)]

        repeated_company_pool = companies[:4]
        event_type_weights = {
            "post_impression": 0.28,
            "post_like": 0.25,
            "post_comment": 0.16,
            "post_repost": 0.08,
            "post_link_click": 0.16,
        }
        event_types = list(event_type_weights.keys())
        weights = list(event_type_weights.values())

        events: list[NormalizedSocialEvent] = []
        for idx in range(self.events):
            selected_post_index = random.choices(list(range(self.posts)), weights=post_weights, k=1)[0]
            post = posts[selected_post_index]

            company = random.choice(repeated_company_pool if random.random() < 0.55 else companies)
            actor_name = random.choice(["Alex Kim", "Sam Patel", "Jordan Lee", "Taylor Brooks", "Casey Reed", None])
            if actor_name is not None and random.random() < 0.35:
                actor_name = f"{actor_name.split()[0]} {random.choice(['Nguyen', 'Rivera', 'Shah', 'Cruz'])}"

            actor_url = None
            if actor_name is not None and random.random() < 0.7:
                slug = actor_name.lower().replace(" ", "-")
                actor_url = f"https://www.linkedin.com/in/{slug}-{random.randint(100, 999)}"

            event_type = random.choices(event_types, weights=weights, k=1)[0]
            event_timestamp = post.created_at + timedelta(days=random.randint(0, 45), hours=random.randint(0, 22))

            # Generate a subset of high-intent post-link clicks close together for repeated companies.
            if company in repeated_company_pool and random.random() < 0.25:
                event_type = "post_link_click"
                event_timestamp = post.created_at + timedelta(days=random.randint(5, 18), hours=random.randint(0, 6))

            metadata_json = {
                "source_name": self.source_name,
                "import_mode": "mock",
                "raw_row_id": f"mock-{idx + 1}",
                "aggregated_import": False,
                "actor_origin": "mock_generated",
                "original_columns": ["generated"],
                "signal_strength": "high" if event_type == "post_link_click" else "normal",
            }

            events.append(
                NormalizedSocialEvent(
                    post_url=post.post_url,
                    actor_name=actor_name,
                    actor_linkedin_url=actor_url,
                    actor_company_raw=company,
                    event_type=event_type,
                    event_timestamp=event_timestamp,
                    metadata_json=metadata_json,
                    source_name=self.source_name,
                    import_mode="mock",
                    aggregated_import=False,
                )
            )

        return AdapterBatch(
            posts=posts,
            events=events,
            row_count=self.events,
            skipped_rows=0,
            warnings=[],
        )
