"""GraphQL query strings for the GitHub v4 API."""

__all__: list[str] = ["REPO_FIELDS_FRAGMENT", "SEARCH_REPOS_QUERY"]

REPO_FIELDS_FRAGMENT: str = """
fragment RepoFields on Repository {
  id
  name
  nameWithOwner
  owner { login __typename }
  description
  url
  homepageUrl
  primaryLanguage { name }
  repositoryTopics(first: 20) { nodes { topic { name } } }
  licenseInfo { spdxId }
  isArchived
  isFork
  isTemplate
  stargazerCount
  forkCount
  watchers { totalCount }
  issues(states: OPEN) { totalCount }
  closedIssues: issues(states: CLOSED) { totalCount }
  createdAt
  updatedAt
  pushedAt
  diskUsage
  defaultBranchRef {
    target {
      ... on Commit {
        history(first: 1) { nodes { committedDate } }
      }
    }
  }
}
"""

SEARCH_REPOS_QUERY: str = (
    REPO_FIELDS_FRAGMENT
    + """
query SearchRepos($q: String!, $after: String) {
  search(query: $q, type: REPOSITORY, first: 100, after: $after) {
    repositoryCount
    pageInfo { endCursor hasNextPage }
    nodes { ...RepoFields }
  }
  rateLimit { remaining resetAt cost }
}
"""
)
