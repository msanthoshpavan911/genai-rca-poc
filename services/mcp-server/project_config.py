"""
=============================================================================
Per-Project OpenSearch Index Configuration
=============================================================================

Each client project ships logs into its own OpenSearch index/alias via
Fluentbit. Index names are expected to change over time (rollovers, alias
repoints, new projects onboarded) — the underlying field schema is assumed
constant across all of them. Only the index name and, optionally, which
field holds the searchable log text are configured per project; everything
else (level, exception, loggingId, instance, @timestamp, ...) is shared.

Override any index name via env var without touching code:
    PROJECT_APP_LAUNCHPAD_INDEX=some-new-alias-name

Add a new project by adding one entry to PROJECTS below.
"""

import os

DEFAULT_SEARCH_FIELD = "msg"

PROJECTS = {
    "app_launchpad": {
        "display_name": "App Launchpad",
        "index": os.getenv(
            "PROJECT_APP_LAUNCHPAD_INDEX",
            "fluentbit-csg-gr2v_app_launchpad-alias",
        ),
        "search_field": os.getenv(
            "PROJECT_APP_LAUNCHPAD_SEARCH_FIELD", DEFAULT_SEARCH_FIELD
        ),
    },
    # TODO: add the remaining 3 projects once their index names are confirmed.
    # "project_b": {
    #     "display_name": "Project B",
    #     "index": os.getenv("PROJECT_B_INDEX", ""),
    #     "search_field": os.getenv("PROJECT_B_SEARCH_FIELD", DEFAULT_SEARCH_FIELD),
    # },
}


def get_project_config(project_id: str) -> dict:
    if project_id not in PROJECTS:
        raise KeyError(f"Unknown project_id '{project_id}'. Known: {list(PROJECTS)}")
    return PROJECTS[project_id]


def list_projects() -> list:
    return [
        {"project_id": pid, "display_name": cfg["display_name"]}
        for pid, cfg in PROJECTS.items()
    ]
