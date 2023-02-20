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

import (
	"github.com/mitchellh/mapstructure"
	"github.com/sfnuser/camara/qodmodels/db"
	"go.mongodb.org/mongo-driver/bson"
)

func decodeProveAppServerData(data interface{}) (*db.ProvQoDAppServerData, error) {
	var prov db.ProvQoDAppServerData
	config := mapstructure.DecoderConfig{
		Result: &prov,
	}

	err := decodeMapStructure(&config, data)
	if err != nil {
		return nil, errCtx(err)
	}
	return &prov, nil
}

// Get Provisioned UE data with asIpv4Addr as key
func (d *DbApi) GetCamaraProvQoDAppServerData(asIpv4Addr string) (*db.ProvQoDAppServerData, error) {
	filter := bson.M{
		"asIpv4Addr": asIpv4Addr,
	}
	getData, err := d.cli.GetOne(COLLECTION_CAMARA_QOD_PROV_SESSION, filter)
	if err != nil {
		return nil, errCtx(err)
	}

	prov, err := decodeProveAppServerData(getData)
	if err != nil {
		return nil, errCtx(err)
	}
	return prov, nil
}

func (d *DbApi) PutCamaraProvQoDAppServerData(asIpv4Addr string, data *db.ProvQoDAppServerData) (int, error) {
	filter := bson.M{
		"asIpv4Addr": asIpv4Addr,
	}
	putData, err := toBsonM(data)
	if err != nil {
		return 0, errCtx(err)
	}
	return d.cli.UpdateInsertOne(COLLECTION_CAMARA_QOD_PROV_SESSION, filter, putData)
}

// To provision multiple records are once
func (d *DbApi) PutManyCamaraProvQoDAppServerData(data *[]db.ProvQoDAppServerData) error {
	var putData []interface{}
	for _, val := range *data {
		bsonData, err := toBsonM(val)
		if err != nil {
			return err
		}
		putData = append(putData, bsonData)
	}
	return d.cli.InsertMany(COLLECTION_CAMARA_QOD_PROV_SESSION, putData)
}
