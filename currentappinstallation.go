package shopify

import (
	"context"

	"github.com/gempages/go-shopify-graphql-model/graph/model"
)

type AppService interface {
	GetCurrentAppInstallation(ctx context.Context) (*model.AppInstallation, error)
}

type AppServiceOp struct {
	client *Client
}

var _ AppService = &AppServiceOp{}

const queryCurrentAppInstallation = `
	query {
		currentAppInstallation {
			id
			app {
				id
				title
				embedded
				isPostPurchaseAppInUse
				developerType
			}
		}
	}
`

func (a *AppServiceOp) GetCurrentAppInstallation(ctx context.Context) (*model.AppInstallation, error) {
	out := struct {
		CurrentAppInstallation *model.AppInstallation `json:"currentAppInstallation"`
	}{}

	err := a.client.gql.QueryString(ctx, queryCurrentAppInstallation, nil, &out)
	if err != nil {
		return nil, err
	}

	return out.CurrentAppInstallation, nil
}
