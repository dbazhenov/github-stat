package internal

import (
	"context"
	"log"

	"github.com/google/go-github/github"
	"golang.org/x/oauth2"
)

func FetchGitHubRepos(ctx context.Context, token string, org string) ([]*github.Repository, int, error) {

	var client *github.Client

	if token == "" {
		client = github.NewClient(nil)
	} else {
		ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
		tc := oauth2.NewClient(ctx, ts)
		client = github.NewClient(tc)
	}

	opt := &github.RepositoryListByOrgOptions{
		ListOptions: github.ListOptions{PerPage: 10},
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
		if resp.NextPage == 0 {
			break
		}
		counter++
		log.Printf("GitHub API Request: %d", counter)
		opt.Page = resp.NextPage
	}

	return allRepos, counter, nil
}

func FetchGitHubReposTest(ctx context.Context, token string, org string) ([]*github.Repository, int, error) {

	var client *github.Client

	if token == "" {
		client = github.NewClient(nil)
	} else {
		ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
		tc := oauth2.NewClient(ctx, ts)
		client = github.NewClient(tc)
	}

	opt := &github.RepositoryListByOrgOptions{
		ListOptions: github.ListOptions{PerPage: 10},
		Type:        "public",
	}

	var counter int
	var allRepos []*github.Repository
	for i := 0; i < 3; i++ {
		repos, resp, err := client.Repositories.ListByOrg(ctx, org, opt)
		if err != nil {
			return nil, counter, err
		}
		allRepos = append(allRepos, repos...)
		if resp.NextPage == 0 {
			break
		}
		counter++
		opt.Page = resp.NextPage
	}

	return allRepos, counter, nil
}
