package shopify

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/gempages/go-helper/errors"
	"github.com/gempages/go-shopify-graphql-model/graph/model"
	"github.com/gempages/go-shopify-graphql/graphql"
	"github.com/spf13/cast"
)

type FileService interface {
	Upload(ctx context.Context, input *UploadInput) (model.File, error)
	QueryFile(ctx context.Context, fileID string) (model.File, error)
	QueryGenericFile(ctx context.Context, fileID string) (*model.GenericFile, error)
	QueryMediaImage(ctx context.Context, fileID string) (*model.MediaImage, error)
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

// UploadInput
// In the case of uploading an image via URL, we only need to provide the 'OriginalSource' parameter
// If you upload an image using 'File' you need to provide all the data except 'OriginalSource'
type UploadInput struct {
	Filename       string
	Mimetype       string
	OriginalSource *string   // Only for upload Image, use OriginalSource when upload by url
	File           io.Reader // use File when upload by file
	FileSize       int64
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
						... on MediaImage {
							id
							image {
								id
								originalSrc: url
								width
								height
							}
							mediaErrors {
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

func (s *FileServiceOp) QueryFile(ctx context.Context, fileID string) (model.File, error) {
	return s.queryFile(ctx, fileID)
}

func (s *FileServiceOp) QueryGenericFile(ctx context.Context, fileID string) (*model.GenericFile, error) {
	file, err := s.queryFile(ctx, fileID)
	if err != nil {
		return nil, err
	}

	return file.(*model.GenericFile), nil
}

func (s *FileServiceOp) QueryMediaImage(ctx context.Context, fileID string) (*model.MediaImage, error) {
	file, err := s.queryFile(ctx, fileID)
	if err != nil {
		return nil, err
	}

	return file.(*model.MediaImage), nil
}

func (s *FileServiceOp) Upload(ctx context.Context, input *UploadInput) (model.File, error) {
	var (
		fileCreatePayload *model.FileCreatePayload
		err               error
	)

	if input.OriginalSource != nil {
		// If original source is found, upload via url
		fileCreatePayload, err = s.fileCreate(ctx, *input.OriginalSource)
		if err != nil {
			return nil, fmt.Errorf("s.fileCreate: %w", err)
		}
	} else {
		// Upload via file
		fileCreatePayload, err = s.upload(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("s.upload: %w", err)
		}
	}

	result, err := s.getUploadResult(ctx, fileCreatePayload.Files[0].GetID(), time.Second*2)
	if err != nil {
		return nil, fmt.Errorf("s.getUploadResult: %w", err)
	}

	return result, nil
}

func (s *FileServiceOp) upload(ctx context.Context, input *UploadInput) (*model.FileCreatePayload, error) {
	fileSizeStr := cast.ToString(input.FileSize)
	stageCreated, err := s.stagedUploadsCreate(fileSizeStr, input.Filename, input.Mimetype)
	if err != nil {
		return nil, fmt.Errorf("s.stagedUploadsCreate: %w", err)
	}

	err = s.uploadFileToStage(ctx, input.File, input.Filename, fileSizeStr, stageCreated)
	if err != nil {
		return nil, fmt.Errorf("s.uploadFileToStage: %w", err)
	}

	result, err := s.fileCreate(ctx, *stageCreated.ResourceURL)
	if err != nil {
		return nil, fmt.Errorf("s.fileCreate: %w", err)
	}

	return result, nil
}

func (s *FileServiceOp) stagedUploadsCreate(fileSize, fileName, mimetype string) (*model.StagedMediaUploadTarget, error) {
	m := mutationStagedUploadsCreate{}
	method := model.StagedUploadHTTPMethodTypePost

	resource := fileTargetResource(mimetype)
	err := s.client.gql.Mutate(context.Background(), &m, map[string]interface{}{
		"input": []model.StagedUploadInput{
			{
				FileSize:   &fileSize,
				Filename:   fileName,
				HTTPMethod: &method,
				MimeType:   mimetype,
				Resource:   resource,
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
	ctx context.Context, file io.Reader, fileName, fileSize string, stageCreated *model.StagedMediaUploadTarget,
) error {

	multiForm, err := createMultipartFormWithFile(file, fileName, stageCreated)
	if err != nil {
		return fmt.Errorf("s.createMultipartFormWithFile: %w", err)
	}

	// Perform the POST request to the temp target
	postTempTargetURL := stageCreated.URL
	postTempTargetHeaders := map[string]string{
		"Content-Type":   multiForm.contentType,
		"Content-Length": fileSize,
	}

	err = performHTTPPostWithHeaders(ctx, *postTempTargetURL, multiForm.data, postTempTargetHeaders)
	if err != nil {
		return err
	}

	return nil
}

func (s *FileServiceOp) fileCreate(ctx context.Context, resourceURL string) (*model.FileCreatePayload, error) {
	out := mutationFileCreate{}

	vars := map[string]interface{}{
		"files": []model.FileCreateInput{
			{
				OriginalSource: resourceURL,
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

// getUploadResult continues querying until a result found or an error occurs.
func (s *FileServiceOp) getUploadResult(ctx context.Context, fileID string, interval time.Duration) (model.File, error) {
	for {
		file, err := s.QueryFile(ctx, fileID)
		if IsRateLimitError(err) {
			time.Sleep(interval)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("s.QueryFile: %w", err)
		}

		switch file.GetFileStatus() {
		case model.FileStatusReady:
			return file, nil
		case model.FileStatusFailed:
			fileErrors := file.GetFileErrors()
			if len(fileErrors) > 0 {
				return nil, &fileErrors[0]
			}
			// Handle errors for images
			if mediaImage, ok := file.(*model.MediaImage); ok && len(mediaImage.MediaErrors) > 0 {
				return nil, &mediaImage.MediaErrors[0]
			}
			// Unknown error
			errData := map[string]any{
				"fileID": fileID,
			}
			return nil, errors.NewErrorWithContext(ctx, fmt.Errorf("upload file to shopify failed"), errData)
		default:
			time.Sleep(interval)
			continue
		}
	}
}

func (s *FileServiceOp) queryFile(ctx context.Context, fileID string) (model.File, error) {
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

	return out.Files.Edges[0].Node, nil
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
	file io.Reader, fileName string, stageCreated *model.StagedMediaUploadTarget) (*multipartFormWithFile, error) {

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
	_, err = io.Copy(fileWriter, file)
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
	regexPattern := `^(gid://shopify/MediaImage/|gid://shopify/GenericFile/)`
	re := regexp.MustCompile(regexPattern)

	return re.ReplaceAllString(shopifyBaseID, "")
}

func fileTargetResource(mimetype string) model.StagedUploadTargetGenerateUploadResource {
	if strings.Contains(mimetype, "image") {
		return model.StagedUploadTargetGenerateUploadResourceImage
	}

	return model.StagedUploadTargetGenerateUploadResourceFile
}
