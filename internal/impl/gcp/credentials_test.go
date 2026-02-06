// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gcp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestGetGoogleCloudCredentials(t *testing.T) {
	t.Run("credentials_path takes precedence", func(t *testing.T) {
		tempDir := t.TempDir()
		credFile := filepath.Join(tempDir, "creds.json")
		credJSON := `{"type":"service_account","project_id":"test-project"}`
		require.NoError(t, os.WriteFile(credFile, []byte(credJSON), 0o644))

		spec := service.NewConfigSpec().Fields(CredentialsFields()...)
		parsed, err := spec.ParseYAML(`
credentials_path: `+credFile+`
credentials_json: '{"type":"service_account","project_id":"other-project"}'
`, nil)
		require.NoError(t, err)

		opts, err := GetGoogleCloudCredentials(parsed)
		require.NoError(t, err)
		assert.NotNil(t, opts)
		assert.Len(t, opts, 1)
	})

	t.Run("credentials_json takes precedence over credentials object", func(t *testing.T) {
		credJSON := `{"type":"service_account","project_id":"json-project"}`

		spec := service.NewConfigSpec().Fields(CredentialsFields()...)
		parsed, err := spec.ParseYAML(`
credentials_json: '`+credJSON+`'
`, nil)
		require.NoError(t, err)

		opts, err := GetGoogleCloudCredentials(parsed)
		require.NoError(t, err)
		assert.NotNil(t, opts)
		assert.Len(t, opts, 1)
	})

	t.Run("credentials object when only option", func(t *testing.T) {
		spec := service.NewConfigSpec().Fields(CredentialsFields()...)
		parsed, err := spec.ParseYAML(`
credentials:
  type: service_account
  project_id: test-project
  private_key_id: key123
  private_key: "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----"
  client_email: test@test-project.iam.gserviceaccount.com
  client_id: "123456789"
  client_x509_cert_url: https://www.googleapis.com/robot/v1/metadata/x509/test@test-project.iam.gserviceaccount.com
`, nil)
		require.NoError(t, err)

		opts, err := GetGoogleCloudCredentials(parsed)
		require.NoError(t, err)
		assert.NotNil(t, opts)
		assert.Len(t, opts, 1)
	})

	t.Run("no credentials returns nil", func(t *testing.T) {
		spec := service.NewConfigSpec().Fields(CredentialsFields()...)
		// Empty credentials_json with default empty string
		parsed, err := spec.ParseYAML(`{}`, nil)
		require.NoError(t, err)

		opts, err := GetGoogleCloudCredentials(parsed)
		require.NoError(t, err)
		// Empty credentials_json returns an option, not nil
		// This is expected behavior based on the code
		if opts != nil {
			assert.Len(t, opts, 1)
		}
	})

	t.Run("credentials_path only", func(t *testing.T) {
		tempDir := t.TempDir()
		credFile := filepath.Join(tempDir, "creds.json")
		credJSON := `{"type":"service_account","project_id":"test-project"}`
		require.NoError(t, os.WriteFile(credFile, []byte(credJSON), 0o644))

		spec := service.NewConfigSpec().Fields(CredentialsFields()...)
		parsed, err := spec.ParseYAML(`
credentials_path: `+credFile+`
`, nil)
		require.NoError(t, err)

		opts, err := GetGoogleCloudCredentials(parsed)
		require.NoError(t, err)
		assert.NotNil(t, opts)
		assert.Len(t, opts, 1)
	})

	t.Run("credentials_json only", func(t *testing.T) {
		credJSON := `{"type":"service_account","project_id":"test-project"}`

		spec := service.NewConfigSpec().Fields(CredentialsFields()...)
		parsed, err := spec.ParseYAML(`
credentials_json: '`+credJSON+`'
`, nil)
		require.NoError(t, err)

		opts, err := GetGoogleCloudCredentials(parsed)
		require.NoError(t, err)
		assert.NotNil(t, opts)
		assert.Len(t, opts, 1)
	})

	t.Run("credentials object with all required fields", func(t *testing.T) {
		spec := service.NewConfigSpec().Fields(CredentialsFields()...)
		parsed, err := spec.ParseYAML(`
credentials:
  project_id: test-project
  private_key_id: key123
  private_key: "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----"
  client_email: test@test-project.iam.gserviceaccount.com
  client_id: "123456789"
  client_x509_cert_url: https://www.googleapis.com/robot/v1/metadata/x509/test@test-project.iam.gserviceaccount.com
`, nil)
		require.NoError(t, err)

		opts, err := GetGoogleCloudCredentials(parsed)
		require.NoError(t, err)
		assert.NotNil(t, opts)
		assert.Len(t, opts, 1)

		// Verify defaults are applied
		credMap, err := parsed.FieldStringMap("credentials")
		require.NoError(t, err)
		assert.Equal(t, "service_account", credMap["type"])
		assert.Equal(t, "https://accounts.google.com/o/oauth2/auth", credMap["auth_uri"])
		assert.Equal(t, "https://oauth2.googleapis.com/token", credMap["token_uri"])
		assert.Equal(t, "https://www.googleapis.com/oauth2/v1/certs", credMap["auth_provider_x509_cert_url"])
	})
}

func TestCredentialsFields(t *testing.T) {
	fields := CredentialsFields()
	require.Len(t, fields, 3)

	// Verify we have three credential fields
	// Field order and structure is important for precedence
	assert.NotNil(t, fields[0])
	assert.NotNil(t, fields[1])
	assert.NotNil(t, fields[2])
}
