package shopify

import (
	"context"
	"fmt"

	"github.com/gempages/go-shopify-graphql-model/graph/model"
)

type AppService interface {
	GetCurrentAppInstallation(ctx context.Context) (*model.AppInstallation, error)
	GetActiveAppSubscriptions(ctx context.Context) ([]model.AppSubscription, error)
}

type AppServiceOp struct {
	client *Client
}

var _ AppService = &AppServiceOp{}

var queryCurrentAppInstallation = fmt.Sprintf(`
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
	activeSubscriptions {
	  createdAt
	  currentPeriodEnd
	  id
	  name
	  returnUrl
	  status
	  test
	  trialDays
	  lineItems {
		id
		%s
	  }
	}
  }
}
`, appSubscriptionLineItemPlan)

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

var queryActiveSubscriptions = fmt.Sprintf(`
query {
	currentAppInstallation {
		activeSubscriptions {
			createdAt
			currentPeriodEnd
			id
			name
			returnUrl
			status
			test
			trialDays
		}
	}
}
`)

func (a *AppServiceOp) GetActiveAppSubscriptions(ctx context.Context) ([]model.AppSubscription, error) {
	out := struct {
		CurrentAppInstallation *model.AppInstallation `json:"currentAppInstallation"`
	}{}

	err := a.client.gql.QueryString(ctx, queryActiveSubscriptions, nil, &out)
	if err != nil {
		return nil, err
	}

	return out.CurrentAppInstallation.ActiveSubscriptions, nil
}
