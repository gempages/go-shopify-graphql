package shopify

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/gempages/go-helper/errors"
	"github.com/gempages/go-shopify-graphql-model/graph/model"
	"github.com/gempages/go-shopify-graphql/graphql"
	"github.com/spf13/cast"
)

type FileService interface {
	Upload(ctx context.Context, fileContent []byte, fileName, mimetype string) (*model.GenericFile, error)
	QueryGenericFile(ctx context.Context, fileID string) (*model.GenericFile, error)
	Delete(ctx context.Context, fileID []graphql.ID) ([]string, error)
}

type FileServiceOp struct {
	client *Client
}

var _ FileService = &FileServiceOp{}

type mutationStagedUploadsCreate struct {
	StagedUploadsCreateResult model.StagedUploadsCreatePayload `graphql:"stagedUploadsCreate(input: $input)" json:"stagedUploadsCreate"`
}

type mutationFileCreate struct {
	FileCreateResult model.FileCreatePayload `graphql:"fileCreate(files: $files)" json:"fileCreate"`
}

type mutationFileDelete struct {
	FileDeleteResult model.FileDeletePayload `graphql:"fileDelete(fileIds: $fileIds)" json:"fileDelete"`
}

type multipartFormWithFile struct {
	contentType string
	data        *bytes.Buffer
}

const fileFieldName = "file"
const queryGenericFile = `
		query files($query: String!) {
			files(first: 1, query: $query) {
				edges {
					node {
						id
						fileStatus
						... on GenericFile {
							id
							url
							originalFileSize
							mimeType
							fileStatus
							fileErrors {
								code
								details
								message
							}
							__typename
						}
					}
				}
			}
		}
	`

func (s *FileServiceOp) Upload(ctx context.Context, fileContent []byte, fileName, mimetype string) (*model.GenericFile, error) {
	fileSize := len(fileContent)
	stageCreated, err := s.stagedUploadsCreate(cast.ToString(fileSize), fileName, mimetype)
	if err != nil {
		return nil, fmt.Errorf("s.stagedUploadsCreate: %w", err)
	}

	err = s.uploadFileToStage(ctx, fileContent, fileName, stageCreated)
	if err != nil {
		return nil, fmt.Errorf("s.uploadFileToStage: %w", err)
	}

	result, err := s.fileCreate(ctx, stageCreated)
	if err != nil {
		return nil, fmt.Errorf("s.fileCreate: %w", err)
	}

	fileInfo, err := s.QueryGenericFile(ctx, result.Files[0].GetID())
	if err != nil {
		return nil, fmt.Errorf("s.QueryGenericFile: %w", err)
	}

	return fileInfo, nil
}

func (s *FileServiceOp) stagedUploadsCreate(fileSize, fileName, mimetype string) (*model.StagedMediaUploadTarget, error) {
	m := mutationStagedUploadsCreate{}
	method := model.StagedUploadHTTPMethodTypePost

	err := s.client.gql.Mutate(context.Background(), &m, map[string]interface{}{
		"input": []model.StagedUploadInput{
			{
				FileSize:   &fileSize,
				Filename:   fileName,
				HTTPMethod: &method,
				MimeType:   mimetype,
				Resource:   model.StagedUploadTargetGenerateUploadResourceFile,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("gql.Mutate: %w", err)
	}

	if len(m.StagedUploadsCreateResult.UserErrors) > 0 {
		return nil, fmt.Errorf("%+v", m.StagedUploadsCreateResult.UserErrors)
	}

	return &m.StagedUploadsCreateResult.StagedTargets[0], nil
}

func (s *FileServiceOp) uploadFileToStage(
	ctx context.Context, file []byte, fileName string, stageCreated *model.StagedMediaUploadTarget,
) error {

	multiForm, err := createMultipartFormWithFile(file, fileName, stageCreated)
	if err != nil {
		return fmt.Errorf("s.createMultipartFormWithFile: %w", err)
	}

	// Perform the POST request to the temp target
	postTempTargetURL := stageCreated.URL
	postTempTargetHeaders := map[string]string{
		"Content-Type":   multiForm.contentType,
		"Content-Length": cast.ToString(len(file)),
	}

	err = performHTTPPostWithHeaders(ctx, *postTempTargetURL, multiForm.data, postTempTargetHeaders)
	if err != nil {
		return err
	}

	return nil
}

func (s *FileServiceOp) fileCreate(ctx context.Context, stageCreated *model.StagedMediaUploadTarget) (*model.FileCreatePayload, error) {
	out := mutationFileCreate{}

	vars := map[string]interface{}{
		"files": []model.FileCreateInput{
			{
				OriginalSource: *stageCreated.ResourceURL,
			},
		},
	}

	m := `
	mutation fileCreate($files: [FileCreateInput!]!) {
		fileCreate(files: $files) {
			files {
				id
				alt
				fileStatus
				__typename
			}
			userErrors {
				field
				message
			}
		}
	}
	`

	err := s.client.gql.MutateString(ctx, m, vars, &out)
	if err != nil {
		return nil, err
	}

	if len(out.FileCreateResult.UserErrors) > 0 {
		return nil, fmt.Errorf("%+v", out.FileCreateResult.UserErrors)
	}

	return &out.FileCreateResult, nil
}

func (s *FileServiceOp) QueryGenericFile(ctx context.Context, fileID string) (*model.GenericFile, error) {
	out := struct {
		Files *model.FileConnection `json:"files"`
	}{}

	fileID = getShopifyID(fileID)
	vars := map[string]interface{}{
		"query": graphql.String(fmt.Sprintf("id:%s", fileID)),
	}
	err := s.client.gql.QueryString(ctx, queryGenericFile, vars, &out)
	if err != nil {
		return nil, fmt.Errorf("gql.QueryString: %w", err)
	}

	if len(out.Files.Edges) <= 0 {
		return nil, fmt.Errorf("file is not found")
	}

	if len(out.Files.Edges[0].Node.GetFileErrors()) > 0 {
		return nil, fmt.Errorf("%+v", out.Files.Edges[0].Node.GetFileErrors())
	}

	return out.Files.Edges[0].Node.(*model.GenericFile), nil
}

func (s *FileServiceOp) Delete(ctx context.Context, fileID []graphql.ID) ([]string, error) {
	m := mutationFileDelete{}
	vars := map[string]interface{}{
		"fileIds": fileID,
	}

	err := s.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return nil, fmt.Errorf("gql.Mutate: %w", err)
	}

	if len(m.FileDeleteResult.UserErrors) > 0 {
		return nil, fmt.Errorf("%+v", m.FileDeleteResult.UserErrors)
	}

	return m.FileDeleteResult.DeletedFileIds, nil
}

func createMultipartFormWithFile(
	file []byte, fileName string, stageCreated *model.StagedMediaUploadTarget) (*multipartFormWithFile, error) {
	// Create a buffer to store the file contents
	fileBuffer := bytes.NewBuffer(file)

	// Create a multipart form and add parameters
	form := &bytes.Buffer{}
	writer := multipart.NewWriter(form)
	defer writer.Close()
	for _, param := range stageCreated.Parameters {
		writer.WriteField(param.Name, param.Value)
	}

	// Add the file to the form
	fileWriter, err := writer.CreateFormFile(fileFieldName, fileName)
	if err != nil {
		return nil, fmt.Errorf("writer.CreateFormFile: %w", err)
	}
	_, err = io.Copy(fileWriter, fileBuffer)
	if err != nil {
		return nil, fmt.Errorf("io.Copy: %w", err)
	}

	return &multipartFormWithFile{
		contentType: writer.FormDataContentType(),
		data:        form,
	}, nil
}

func performHTTPPostWithHeaders(ctx context.Context, url string, body io.Reader, headers map[string]string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return err
	}

	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("DefaultClient.Do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		bodyContent, _ := io.ReadAll(resp.Body)
		return errors.NewErrorWithContext(ctx, fmt.Errorf("non-201 Created status code: %v", resp.Status), map[string]any{"body": string(bodyContent)})
	}

	return nil
}

func getShopifyID(shopifyBaseID string) string {
	return strings.Replace(shopifyBaseID, "gid://shopify/GenericFile/", "", 1)
}
