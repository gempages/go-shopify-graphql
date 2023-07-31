package shopify

import (
	"context"
	"fmt"

	"github.com/gempages/go-shopify-graphql-model/graph/model"
)

type WebhookService interface {
	NewWebhookSubscription(topic model.WebhookSubscriptionTopic, input model.WebhookSubscriptionInput) (output *model.WebhookSubscription, err error)
	NewEventBridgeWebhookSubscription(topic model.WebhookSubscriptionTopic, input model.EventBridgeWebhookSubscriptionInput) (output *model.WebhookSubscription, err error)

	ListWebhookSubscriptions(topics []model.WebhookSubscriptionTopic) (output []*model.WebhookSubscription, err error)
	DeleteWebhook(webhookID string) (deletedID *string, err error)
}

type WebhookServiceOp struct {
	client *Client
}

var _ WebhookService = &WebhookServiceOp{}

type mutationWebhookCreate struct {
	WebhookCreateResult *model.WebhookSubscriptionCreatePayload `graphql:"webhookSubscriptionCreate(topic: $topic, webhookSubscription: $webhookSubscription)" json:"webhookSubscriptionCreate"`
}

type mutationWebhookDelete struct {
	WebhookDeleteResult *model.WebhookSubscriptionDeletePayload `graphql:"webhookSubscriptionDelete(id: $id)" json:"webhookSubscriptionDelete"`
}

type mutationEventBridgeWebhookCreate struct {
	EventBridgeWebhookCreateResult *model.EventBridgeWebhookSubscriptionCreatePayload `graphql:"eventBridgeWebhookSubscriptionCreate(topic: $topic, webhookSubscription: $webhookSubscription)" json:"eventBridgeWebhookSubscriptionCreate"`
}

// NOTE: Have to use this because writeQuery function will not write structs that implements UnmarshalJSON function
const webhookSubscriptionCreateSelects = `
userErrors {
	field
	message
}
webhookSubscription {
	callbackUrl
	createdAt
	format
	id
	includeFields
	legacyResourceId
	metafieldNamespaces
	privateMetafieldNamespaces
	topic
	updatedAt
	endpoint {
		__typename
		...on WebhookEventBridgeEndpoint {
			arn
		}
		...on WebhookHttpEndpoint {
			callbackUrl
		}
	}
}`

func (w WebhookServiceOp) NewWebhookSubscription(topic model.WebhookSubscriptionTopic, input model.WebhookSubscriptionInput) (output *model.WebhookSubscription, err error) {
	m := fmt.Sprintf(`mutation($topic: WebhookSubscriptionTopic!, $webhookSubscription: WebhookSubscriptionInput!) {
	webhookSubscriptionCreate(topic: $topic, webhookSubscription: $webhookSubscription) {
		%s
	}}`, webhookSubscriptionCreateSelects)
	v := mutationWebhookCreate{}
	vars := map[string]interface{}{
		"topic":               topic,
		"webhookSubscription": input,
	}
	err = w.client.gql.MutateString(context.Background(), m, vars, &v)
	if err != nil {
		return
	}

	if len(v.WebhookCreateResult.UserErrors) > 0 {
		err = fmt.Errorf("%+v", v.WebhookCreateResult.UserErrors)
		return
	}

	return v.WebhookCreateResult.WebhookSubscription, nil
}

func (w WebhookServiceOp) NewEventBridgeWebhookSubscription(topic model.WebhookSubscriptionTopic, input model.EventBridgeWebhookSubscriptionInput) (output *model.WebhookSubscription, err error) {
	m := fmt.Sprintf(`mutation($topic: WebhookSubscriptionTopic!, $webhookSubscription: EventBridgeWebhookSubscriptionInput!) {
	eventBridgeWebhookSubscriptionCreate(topic: $topic, webhookSubscription: $webhookSubscription) {
		%s
	}}`, webhookSubscriptionCreateSelects)
	v := mutationEventBridgeWebhookCreate{}
	vars := map[string]interface{}{
		"topic":               topic,
		"webhookSubscription": input,
	}

	err = w.client.gql.MutateString(context.Background(), m, vars, &v)
	if err != nil {
		return
	}

	if len(v.EventBridgeWebhookCreateResult.UserErrors) > 0 {
		err = fmt.Errorf("%+v", v.EventBridgeWebhookCreateResult.UserErrors)
		return
	}

	return v.EventBridgeWebhookCreateResult.WebhookSubscription, nil
}

func (w WebhookServiceOp) DeleteWebhook(webhookID string) (deletedID *string, err error) {
	m := mutationWebhookDelete{}
	vars := map[string]interface{}{
		"id": webhookID,
	}
	err = w.client.gql.Mutate(context.Background(), &m, vars)
	if err != nil {
		return
	}

	if len(m.WebhookDeleteResult.UserErrors) > 0 {
		err = fmt.Errorf("%+v", m.WebhookDeleteResult.UserErrors)
		return
	}
	return m.WebhookDeleteResult.DeletedWebhookSubscriptionID, nil
}

func (w WebhookServiceOp) ListWebhookSubscriptions(topics []model.WebhookSubscriptionTopic) (output []*model.WebhookSubscription, err error) {
	queryFormat := `query webhookSubscriptions($first: Int!, $topics: [WebhookSubscriptionTopic!]%s) {
		webhookSubscriptions(first: $first, topics: $topics%s) {
		  edges {
			cursor
			node {
			  id,
			  topic,
			  endpoint {
				__typename
				... on WebhookHttpEndpoint {
				  callbackUrl
				}
				... on WebhookEventBridgeEndpoint{
				  arn
				}
			  }
			  callbackUrl
			  format
			  topic
			  includeFields
			  createdAt
			  updatedAt
			}
		  }
		  pageInfo {
			hasNextPage
		  }
		}
	  }`

	var (
		cursor string
		vars   = map[string]interface{}{
			"first":  200,
			"topics": topics,
		}
	)
	for {
		var (
			query string
			out   model.QueryRoot
		)
		if cursor != "" {
			vars["after"] = cursor
			query = fmt.Sprintf(queryFormat, ", $after: String", ", after: $after")
		} else {
			query = fmt.Sprintf(queryFormat, "", "")
		}
		err = w.client.gql.QueryString(context.Background(), query, vars, &out)
		if err != nil {
			return
		}
		for _, wh := range out.WebhookSubscriptions.Edges {
			output = append(output, wh.Node)
		}
		if out.WebhookSubscriptions.PageInfo.HasNextPage {
			cursor = out.WebhookSubscriptions.Edges[len(out.WebhookSubscriptions.Edges)-1].Cursor
		} else {
			break
		}
	}
	return
}
