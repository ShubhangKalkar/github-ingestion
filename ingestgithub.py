import os
import time
import requests
import json

BASE_URL = "https://api.github.com"
OWNER = "apache"
REPO = "airflow"

# Optional but strongly recommended
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

HEADERS = {
    "Accept": "application/vnd.github+json"
}

if GITHUB_TOKEN:
    HEADERS["Authorization"] = f"Bearer {GITHUB_TOKEN}"

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)


def fetch_paginated(endpoint, params=None, max_pages=5):
    """
    Fetch paginated GitHub API data.
    max_pages is used to control API usage.
    """
    results = []
    page = 1

    while page <= max_pages:
        response = requests.get(
            f"{BASE_URL}{endpoint}",
            headers=HEADERS,
            params={**(params or {}), "per_page": 100, "page": page}
        )

        if response.status_code != 200:
            print(f"Failed {endpoint}: {response.status_code}")
            break

        data = response.json()
        if not data:
            break

        results.extend(data)
        page += 1
        time.sleep(0.5)  # polite rate limiting

    return results


def save_json(filename, data):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    return len(data)


def main():
    row_counts = {}

    # 1. Commits
    commits = fetch_paginated(f"/repos/{OWNER}/{REPO}/commits")
    row_counts["commits"] = save_json("commits.json", commits)

    # 2. Pull Requests
    pulls = fetch_paginated(f"/repos/{OWNER}/{REPO}/pulls", params={"state": "all"})
    row_counts["pulls"] = save_json("pulls.json", pulls)

    # 3. Pull Request Comments
    pr_comments = fetch_paginated(f"/repos/{OWNER}/{REPO}/pulls/comments")
    row_counts["pull_comments"] = save_json("pull_comments.json", pr_comments)

    # 4. Issues (includes PRs — we’ll clean later)
    issues = fetch_paginated(f"/repos/{OWNER}/{REPO}/issues", params={"state": "all"})
    row_counts["issues"] = save_json("issues.json", issues)

    # 5. Pull Reviews (sample first 20 PRs to stay safe)
    pull_reviews = []
    for pr in pulls[:20]:
        pr_number = pr["number"]
        reviews = fetch_paginated(
            f"/repos/{OWNER}/{REPO}/pulls/{pr_number}/reviews"
        )
        pull_reviews.extend(reviews)

    row_counts["pull_reviews"] = save_json("pull_reviews.json", pull_reviews)

    print("\nIngestion Complete — Row Counts:")
    for k, v in row_counts.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
