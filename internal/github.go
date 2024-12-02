package internal

import (
	"context"
	"log"
	"time"

	"github.com/google/go-github/github"
	"golang.org/x/oauth2"
)

type PullsLastUpdate struct {
	Minimum string
	Force   bool
}

func FetchGitHubPullsByRepo(envVars EnvVars, repo *github.Repository, pullsLastUpdate map[string]*PullsLastUpdate, counterPulls map[string]*int) ([]*github.PullRequest, error) {

	ctx := context.Background()

	var client *github.Client

	if envVars.GitHub.Token == "" {
		client = github.NewClient(nil)
	} else {
		ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: envVars.GitHub.Token})
		tc := oauth2.NewClient(ctx, ts)
		client = github.NewClient(tc)
	}

	var allPulls []*github.PullRequest

	var lastUpdatedTime time.Time
	var forceUpdate bool
	var err error

	repoName := repo.GetName()
	pullLastUpdate := pullsLastUpdate[repoName].Minimum
	forceUpdate = pullsLastUpdate[repoName].Force

	*counterPulls["repos"]++

	if pullLastUpdate == "" || forceUpdate {

		opts := &github.PullRequestListOptions{
			State:       "all",
			Sort:        "created",
			Direction:   "desc",
			ListOptions: github.ListOptions{PerPage: 100},
		}

		for {
			pulls, resp, err := client.PullRequests.List(ctx, *repo.Owner.Login, *repo.Name, opts)
			if err != nil {
				return allPulls, err
			}

			*counterPulls["pulls_api_requests"]++
			*counterPulls["pulls"] += len(pulls)
			*counterPulls["pulls_full"] += len(pulls)

			log.Printf("GitHub API: Repo Full: %s, Total requests: %d, repos: %d, pulls: %d", *repo.Name, *counterPulls["pulls_api_requests"], *counterPulls["repos"], *counterPulls["pulls"])

			allPulls = append(allPulls, pulls...)

			if resp.NextPage == 0 {
				break
			}

			opts.Page = resp.NextPage

		}
		*counterPulls["repos_full"]++
	} else {

		opts := &github.PullRequestListOptions{
			State:       "all",
			Sort:        "updated",
			Direction:   "desc",
			ListOptions: github.ListOptions{PerPage: 100},
		}

		lastUpdatedTime, err = time.Parse(time.RFC3339, pullLastUpdate)
		if err != nil {
			log.Printf("Error parsing startedAt: %v", err)
		}

		for {
			pulls, resp, err := client.PullRequests.List(ctx, *repo.Owner.Login, *repo.Name, opts)
			if err != nil {
				return allPulls, err
			}
			*counterPulls["pulls_api_requests"]++

			log.Printf("GitHub API: Repo Update: %s, Total requests: %d, repos: %d, pulls: %d", *repo.Name, *counterPulls["pulls_api_requests"], *counterPulls["repos"], *counterPulls["pulls"])

			dateBreak := false
			for _, pull := range pulls {
				if pull.UpdatedAt != nil && lastUpdatedTime.After(*pull.UpdatedAt) {

					dateBreak = true
					break
				}
				*counterPulls["pulls"]++
				*counterPulls["pulls_latest"]++

				allPulls = append(allPulls, pull)
			}

			if resp.NextPage == 0 || dateBreak {
				break
			}

			opts.Page = resp.NextPage

		}

		*counterPulls["repos_latest"]++
	}

	return allPulls, nil
}

func FetchGitHubRepos(envVars EnvVars) ([]*github.Repository, int, error) {

	org := envVars.GitHub.Organisation
	token := envVars.GitHub.Token

	log.Printf("GitHub API: Fetch Repos: Org: %s: Start", org)
	ctx := context.Background()
	var client *github.Client

	if token == "" {
		client = github.NewClient(nil)
	} else {
		ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
		tc := oauth2.NewClient(ctx, ts)
		client = github.NewClient(tc)
	}

	opt := &github.RepositoryListByOrgOptions{
		ListOptions: github.ListOptions{PerPage: 100},
		Type:        "public",
	}

	var counter int

	var allRepos []*github.Repository
	for {
		repos, resp, err := client.Repositories.ListByOrg(ctx, org, opt)
		if err != nil {
			return nil, counter, err
		}
		allRepos = append(allRepos, repos...)
		counter++
		log.Printf("GitHub API: Fetch Repos: Org: %s: API Request: %d", org, counter)

		if resp.NextPage == 0 {
			break
		} else {
			opt.Page = resp.NextPage
		}
	}

	log.Printf("GitHub API: Fetch Repos: Org: %s: Repos: %d: API requests: %d, Finish", org, len(allRepos), counter)
	return allRepos, counter, nil
}
