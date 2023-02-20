// Copyright 2023 Spry Fox Networks
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbapi

const (
	COLLECTION_CAMARA_QOD_PROV_SESSION    = "camara.qod.provisionedData.session"
	COLLECTION_CAMARA_QOD_SERVICE_SESSION = "camara.qod.service.session"
	COLLECTION_CAMARA_QOD_SERVICE_UE_FLOW = "camara.qod.service.ueflow"
)

type DbApi struct {
	cli *DbClient
}

func NewDbApi(dbName, dbUrl string) *DbApi {
	dbApi := &DbApi{}
	dbApi.cli = NewDbClient(dbName, dbUrl)
	return dbApi
}

func (d *DbApi) GetWrapper() *DbClient {
	return d.cli
}

// Connect to DB
func (d *DbApi) Connect() error {
	return d.cli.ConnectDB()
}

// Disconnect
func (d *DbApi) Disconnect() {
	d.cli.DisconnectDB()
}

// Drop DB
func (d *DbApi) DropDb() {
	d.cli.DropDb()
}
