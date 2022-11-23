package shopify

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"time"

	"github.com/gempages/go-helper/tracing"
	"github.com/gempages/go-shopify-graphql/graphql"
	"github.com/gempages/go-shopify-graphql/rand"
	"github.com/gempages/go-shopify-graphql/utils"
	"github.com/getsentry/sentry-go"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
)

type BulkOperationService interface {
	BulkQuery(ctx context.Context, query string, v interface{}) error

	PostBulkQuery(ctx context.Context, query string) (graphql.ID, error)
	GetCurrentBulkQuery(ctx context.Context) (CurrentBulkOperation, error)
	GetCurrentBulkQueryResultURL(ctx context.Context) (string, error)
	WaitForCurrentBulkQuery(ctx context.Context, interval time.Duration) (CurrentBulkOperation, error)
	ShouldGetBulkQueryResultURL(ctx context.Context, id graphql.ID) (string, error)
	CancelRunningBulkQuery(ctx context.Context) error
	BulkQueryRunOnly(ctx context.Context, query string, out interface{}) (id graphql.ID, err error)
	GetBulkQueryResult(ctx context.Context, id graphql.ID) (bulkOperation CurrentBulkOperation, err error)
	MarshalBulkResult(ctx context.Context, url string, out interface{}) error
}

type BulkOperationServiceOp struct {
	client *Client
}

type queryCurrentBulkOperation struct {
	CurrentBulkOperation CurrentBulkOperation
}

type CurrentBulkOperation struct {
	ID             graphql.ID     `json:"id"`
	Status         graphql.String `json:"status"`
	ErrorCode      graphql.String `json:"errorCode"`
	CreatedAt      graphql.String `json:"createdAt"`
	CompletedAt    graphql.String `json:"completedAt"`
	ObjectCount    graphql.String `json:"objectCount"`
	FileSize       graphql.String `json:"fileSize"`
	URL            graphql.String `json:"url"`
	PartialDataURL graphql.String `json:"partialDataUrl"`
	Query          graphql.String `json:"query"`
}

type bulkOperationRunQueryResult struct {
	BulkOperation struct {
		ID graphql.ID `json:"id"`
	} `json:"bulkOperation"`
	UserErrors []UserErrors `json:"userErrors"`
}

type mutationBulkOperationRunQuery struct {
	BulkOperationRunQueryResult bulkOperationRunQueryResult `graphql:"bulkOperationRunQuery(query: $query)" json:"bulkOperationRunQuery"`
}

type bulkOperationCancelResult struct {
	BulkOperation struct {
		ID graphql.ID `json:"id"`
	} `json:"bulkOperation"`
	UserErrors []UserErrors `json:"userErrors"`
}

type mutationBulkOperationRunQueryCancel struct {
	BulkOperationCancelResult bulkOperationCancelResult `graphql:"bulkOperationCancel(id: $id)" json:"bulkOperationCancel"`
}

var gidRegex *regexp.Regexp

func init() {
	gidRegex = regexp.MustCompile(`^gid://shopify/(\w+)/\d+$`)
}

func (s *BulkOperationServiceOp) PostBulkQuery(ctx context.Context, query string) (graphql.ID, error) {
	m := mutationBulkOperationRunQuery{}
	vars := map[string]interface{}{
		"query": graphql.String(query),
	}

	err := s.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return nil, err
	}
	if len(m.BulkOperationRunQueryResult.UserErrors) > 0 {
		return nil, fmt.Errorf("%+v", m.BulkOperationRunQueryResult.UserErrors)
	}

	return m.BulkOperationRunQueryResult.BulkOperation.ID, nil
}

func (s *BulkOperationServiceOp) GetCurrentBulkQuery(ctx context.Context) (CurrentBulkOperation, error) {
	q := queryCurrentBulkOperation{}
	err := s.client.gql.Query(ctx, &q, nil)
	if err != nil {
		return CurrentBulkOperation{}, err
	}

	return q.CurrentBulkOperation, nil
}

func (s *BulkOperationServiceOp) GetCurrentBulkQueryResultURL(ctx context.Context) (url string, err error) {
	return s.ShouldGetBulkQueryResultURL(ctx, nil)
}

func (s *BulkOperationServiceOp) ShouldGetBulkQueryResultURL(ctx context.Context, id graphql.ID) (url string, err error) {
	q, err := s.GetCurrentBulkQuery(ctx)
	if err != nil {
		return
	}

	if id != nil && q.ID != id {
		err = fmt.Errorf("Bulk operation ID doesn't match, got=%v, want=%v", q.ID, id)
		return
	}

	q, err = s.WaitForCurrentBulkQuery(ctx, 1*time.Second)
	if q.Status != "COMPLETED" {
		err = fmt.Errorf("Bulk operation didn't complete, status=%s, error_code=%s", q.Status, q.ErrorCode)
		return
	}

	if q.ErrorCode != "" {
		err = fmt.Errorf("Bulk operation error: %s", q.ErrorCode)
		return
	}

	if q.ObjectCount == "0" {
		return
	}

	url = string(q.URL)
	return
}

func (s *BulkOperationServiceOp) WaitForCurrentBulkQuery(ctx context.Context, interval time.Duration) (CurrentBulkOperation, error) {
	q, err := s.GetCurrentBulkQuery(ctx)
	if err != nil {
		return q, fmt.Errorf("CurrentBulkOperation query error: %s", err)
	}

	for q.Status == "CREATED" || q.Status == "RUNNING" || q.Status == "CANCELING" {
		span := sentry.StartSpan(ctx, "time.sleep")
		span.Description = "interval"
		time.Sleep(interval)
		tracing.FinishSpan(span, ctx.Err())

		q, err = s.GetCurrentBulkQuery(ctx)
		if err != nil {
			return q, fmt.Errorf("CurrentBulkOperation query error: %s", err)
		}
	}

	return q, nil
}

func (s *BulkOperationServiceOp) CancelRunningBulkQuery(ctx context.Context) (err error) {
	q, err := s.GetCurrentBulkQuery(ctx)
	if err != nil {
		return
	}

	if q.Status == "CREATED" || q.Status == "RUNNING" {
		log.Debugln("Canceling running operation")
		operationID := q.ID

		m := mutationBulkOperationRunQueryCancel{}
		vars := map[string]interface{}{
			"id": operationID,
		}

		err = s.client.gql.Mutate(ctx, &m, vars)
		if err != nil {
			return err
		}
		if len(m.BulkOperationCancelResult.UserErrors) > 0 {
			return fmt.Errorf("%+v", m.BulkOperationCancelResult.UserErrors)
		}

		q, err = s.GetCurrentBulkQuery(ctx)
		if err != nil {
			return
		}
		for q.Status == "CREATED" || q.Status == "RUNNING" || q.Status == "CANCELING" {
			log.Tracef("Bulk operation still %s...", q.Status)
			q, err = s.GetCurrentBulkQuery(ctx)
			if err != nil {
				return
			}
		}
		log.Debugln("Bulk operation cancelled")
	}

	return
}

func (s *BulkOperationServiceOp) BulkQuery(ctx context.Context, query string, out interface{}) error {
	var err error

	// sentry tracing
	span := sentry.StartSpan(ctx, "shopify_graphql.bulk_query")
	span.Description = utils.GetDescriptionFromQuery(query)
	span.SetTag("query", query)
	defer func() {
		tracing.FinishSpan(span, err)
	}()
	// end sentry tracing

	ctx = span.Context()
	_, err = s.WaitForCurrentBulkQuery(ctx, 1*time.Second)
	if err != nil {
		return err
	}

	id, err := s.PostBulkQuery(ctx, query)
	if err != nil {
		return err
	}

	if id == nil {
		return fmt.Errorf("Posted operation ID is nil")
	}

	url, err := s.ShouldGetBulkQueryResultURL(ctx, id)
	if err != nil {
		return err
	}

	if url == "" {
		return fmt.Errorf("Operation result URL is empty")
	}

	filename := fmt.Sprintf("%s%s", rand.String(10), ".jsonl")
	resultFile := filepath.Join(os.TempDir(), filename)
	err = utils.DownloadFile(ctx, resultFile, url)
	if err != nil {
		return err
	}

	err = parseBulkQueryResult(resultFile, out)
	if err != nil {
		return err
	}

	return nil
}

func (s *BulkOperationServiceOp) MarshalBulkResult(ctx context.Context, url string, out interface{}) error {
	filename := fmt.Sprintf("%s%s", rand.String(10), ".jsonl")
	resultFile := filepath.Join(os.TempDir(), filename)
	err := utils.DownloadFile(ctx, resultFile, url)
	if err != nil {
		return err
	}

	err = parseBulkQueryResult(resultFile, out)
	if err != nil {
		return err
	}

	return nil
}
func (s *BulkOperationServiceOp) BulkQueryRunOnly(ctx context.Context, query string, out interface{}) (id graphql.ID, err error) {
	_, err = s.WaitForCurrentBulkQuery(ctx, 1*time.Second)
	if err != nil {
		return "", err
	}

	id, err = s.PostBulkQuery(ctx, query)
	if err != nil {
		return "", err
	}

	if id == nil {
		return "", fmt.Errorf("Posted operation ID is nil")
	}

	return id, nil
	/////////////
	// url, err := s.ShouldGetBulkQueryResultURL(id)
	// if err != nil {
	// 	return err
	// }

	// if url == "" {
	// 	return fmt.Errorf("Operation result URL is empty")
	// }

	// fmt.Println(url)
	// filename := fmt.Sprintf("%s%s", rand.String(10), ".jsonl")
	// resultFile := filepath.Join(os.TempDir(), filename)
	// err = utils.DownloadFile(resultFile, url)
	// if err != nil {
	// 	return err
	// }

	// err = parseBulkQueryResult(resultFile, out)
	// if err != nil {
	// 	return err
	// }

	// return nil
}

// GetBulkQueryResult get current status of bulk querry id
func (s *BulkOperationServiceOp) GetBulkQueryResult(ctx context.Context, id graphql.ID) (bulkOperation CurrentBulkOperation, err error) {
	q, err := s.GetCurrentBulkQuery(ctx)
	if err != nil {
		return
	}

	if id != nil && q.ID != id {
		err = fmt.Errorf("Bulk operation ID doesn't match, got=%v, want=%v", q.ID, id)
		return q, err
	}
	return q, nil
}

func parseBulkQueryResult(resultFile string, out interface{}) (err error) {
	if reflect.TypeOf(out).Kind() != reflect.Ptr {
		err = fmt.Errorf("the out arg is not a pointer")
		return
	}

	outValue := reflect.ValueOf(out)
	outSlice := outValue.Elem()
	if outSlice.Kind() != reflect.Slice {
		err = fmt.Errorf("the out arg is not a pointer to a slice interface")
		return
	}

	sliceItemType := outSlice.Type().Elem() // slice item type
	sliceItemKind := sliceItemType.Kind()
	itemType := sliceItemType // slice item underlying type
	if sliceItemKind == reflect.Ptr {
		itemType = itemType.Elem()
	}

	f, err := os.Open(resultFile)
	if err != nil {
		return
	}
	defer utils.CloseFile(f)

	reader := bufio.NewReader(f)
	json := jsoniter.ConfigFastest

	childrenLookup := make(map[string]interface{})

	for {
		var line []byte
		line, err = reader.ReadBytes('\n')
		if err != nil {
			break
		}

		parentID := json.Get(line, "__parentId")
		if parentID.LastError() == nil {
			gid := json.Get(line, "id")
			if gid.LastError() != nil {
				return fmt.Errorf("Connection type must query `id` field")
			}
			childObjType, childrenFieldName, err := concludeObjectType(gid.ToString())
			if err != nil {
				return err
			}
			childItem := reflect.New(childObjType).Interface()
			err = json.Unmarshal(line, &childItem)
			if err != nil {
				return err
			}
			childItemVal := reflect.ValueOf(childItem).Elem()

			var childrenSlice reflect.Value
			var children map[string]interface{}
			if val, ok := childrenLookup[parentID.ToString()]; ok {
				children = val.(map[string]interface{})
			} else {
				children = make(map[string]interface{})
			}

			if val, ok := children[childrenFieldName]; ok {
				childrenSlice = reflect.ValueOf(val)
			} else {
				childrenSlice = reflect.MakeSlice(reflect.SliceOf(childObjType), 0, 10)
			}

			childrenSlice = reflect.Append(childrenSlice, childItemVal)

			children[childrenFieldName] = childrenSlice.Interface()
			childrenLookup[parentID.ToString()] = children

			continue
		}

		item := reflect.New(itemType).Interface()
		err = json.Unmarshal(line, &item)
		if err != nil {
			return
		}
		itemVal := reflect.ValueOf(item)

		if sliceItemKind == reflect.Ptr {
			outSlice.Set(reflect.Append(outSlice, itemVal))
		} else {
			outSlice.Set(reflect.Append(outSlice, itemVal.Elem()))
		}
	}

	if len(childrenLookup) > 0 {
		for i := 0; i < outSlice.Len(); i++ {
			parent := outSlice.Index(i)
			if parent.Kind() == reflect.Ptr {
				parent = parent.Elem()
			}
			parentIDField := parent.FieldByName("ID")
			if parentIDField.IsZero() {
				return fmt.Errorf("No ID field on the first level")
			}
			parentID := parentIDField.Interface().(string)
			if children, ok := childrenLookup[parentID]; ok {
				childrenVal := reflect.ValueOf(children)
				iter := childrenVal.MapRange()
				for iter.Next() {
					k := iter.Key()
					v := reflect.ValueOf(iter.Value().Interface())
					field := parent.FieldByName(k.String())
					if !field.IsValid() {
						return fmt.Errorf("Field '%s' not defined on the parent type %s", k.String(), parent.Type().String())
					}
					field.Set(v)
				}
			}
		}
	}

	if err != nil && err != io.EOF {
		return
	}

	err = nil
	return
}

func concludeObjectType(gid string) (reflect.Type, string, error) {
	submatches := gidRegex.FindStringSubmatch(gid)
	if len(submatches) != 2 {
		return reflect.TypeOf(nil), "", fmt.Errorf("malformed gid=`%s`", gid)
	}
	resource := submatches[1]
	switch resource {
	case "LineItem":
		return reflect.TypeOf(LineItem{}), fmt.Sprintf("%ss", resource), nil
	case "FulfillmentOrderLineItem":
		return reflect.TypeOf(FulfillmentOrderLineItem{}), fmt.Sprintf("%ss", resource), nil
	case "Metafield":
		return reflect.TypeOf(Metafield{}), fmt.Sprintf("%ss", resource), nil
	case "Order":
		return reflect.TypeOf(Order{}), fmt.Sprintf("%ss", resource), nil
	case "Product":
		return reflect.TypeOf(ProductBulkResult{}), fmt.Sprintf("%ss", resource), nil
	case "ProductVariant":
		return reflect.TypeOf(ProductVariant{}), fmt.Sprintf("%ss", resource), nil
	case "Collection":
		return reflect.TypeOf(Collection{}), fmt.Sprintf("%ss", resource), nil
	case "ProductImage":
		return reflect.TypeOf(ProductImage{}), fmt.Sprintf("%ss", resource), nil
	default:
		return reflect.TypeOf(nil), "", fmt.Errorf("`%s` not implemented type", resource)
	}
}
